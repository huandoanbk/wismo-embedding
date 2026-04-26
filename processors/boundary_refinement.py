"""LLM-assisted transcript boundary refinement with safe fallback."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from typing import Any, Callable

import httpx

from core.config import settings

logger = logging.getLogger(__name__)

BOUNDARY_REFINEMENT_EVENTS_KEY = "boundary_refinement_events"
BOUNDARY_REFINEMENT_INFO_KEY = "boundary_refinement_info"
BOUNDARY_REFINEMENT_PROMPT_VERSION = "transcript-merge-v1"
BOUNDARY_REFINEMENT_MAX_TOKENS = 800


@dataclass(frozen=True)
class BoundaryRefinementDecision:
    """One parseable merge decision between adjacent chunks."""

    decision: str
    reason: str

    @property
    def should_merge(self) -> bool:
        """Return whether the runtime should merge the candidate pair."""
        return self.decision == "merge"


@dataclass(frozen=True)
class BoundaryRefinementResult:
    """Refinement output including final chunks and operational events."""

    chunks: list[dict[str, Any]]
    events: list[dict[str, Any]]
    info: dict[str, Any]


class LLMBoundaryRefiner:
    """Refine transcript chunk boundaries with merge-first decisions."""

    def __init__(
        self,
        provider: Callable[[dict[str, Any]], Any] | None = None,
        *,
        max_retries: int = 1,
        prompt_version: str = BOUNDARY_REFINEMENT_PROMPT_VERSION,
        model_identifier: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.provider = provider
        self.max_retries = max_retries
        self.prompt_version = prompt_version
        self.model_identifier = model_identifier or settings.enrichment_model or "unconfigured-boundary-model"
        self.client = client or httpx.Client(timeout=settings.enrichment_timeout_seconds)

    def refine_chunks(
        self,
        chunks: list[dict[str, Any]],
        *,
        content_profile: str,
        transcript_quality_tier: str | None,
    ) -> BoundaryRefinementResult:
        """Return refined transcript chunks or baseline chunks with fallback metadata."""
        baseline_chunks = [dict(chunk) for chunk in chunks]
        if content_profile != "transcript" or transcript_quality_tier != "medium":
            return BoundaryRefinementResult(chunks=baseline_chunks, events=[], info=self._info())

        first_pass, events = self._merge_pass(baseline_chunks, pass_name="merge_first")
        second_pass, consistency_events = self._merge_pass(first_pass, pass_name="cross_window_consistency")
        events.extend(consistency_events)
        events.append(
            {
                "event": "merge_rate_summary",
                "decisions": sum(1 for event in events if event.get("event") == "decision"),
                "merges": sum(1 for event in events if event.get("decision") == "merge"),
            }
        )
        return BoundaryRefinementResult(chunks=second_pass, events=events, info=self._info())

    def _merge_pass(
        self,
        chunks: list[dict[str, Any]],
        *,
        pass_name: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        output: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = [{"event": pass_name}]
        index = 0
        while index < len(chunks):
            current = dict(chunks[index])
            if index == len(chunks) - 1:
                output.append(current)
                break

            following = dict(chunks[index + 1])
            decision, decision_events = self._decide(current, following)
            events.extend(decision_events)
            events.append(
                {
                    "event": "decision",
                    "pass": pass_name,
                    "decision": decision.decision,
                    "reason": decision.reason,
                }
            )
            if decision.should_merge:
                merged = self._merge_pair(current, following)
                if merged["token_count"] > BOUNDARY_REFINEMENT_MAX_TOKENS:
                    events.append(
                        {
                            "event": "chunk_gt_threshold_after_merge",
                            "token_count": merged["token_count"],
                        }
                    )
                    events.append(
                        {
                            "event": "merge_anomaly",
                            "reason": "merged_chunk_exceeds_threshold",
                        }
                    )
                current = merged
                index += 2
                output.append(current)
                continue

            output.append(current)
            index += 1
        return output, events

    def _decide(
        self,
        chunk_a: dict[str, Any],
        chunk_b: dict[str, Any],
    ) -> tuple[BoundaryRefinementDecision, list[dict[str, Any]]]:
        payload = {
            "prompt": self.build_prompt(chunk_a["content"], chunk_b["content"]),
            "model": self.model_identifier,
            "temperature": 0.0,
        }
        events: list[dict[str, Any]] = []
        last_error = "provider_unavailable"
        for attempt in range(self.max_retries + 1):
            try:
                response_payload = self._call_provider(payload)
                return self._parse_response(response_payload), events
            except (ValueError, json.JSONDecodeError, httpx.HTTPError, KeyError, TypeError) as exc:
                last_error = str(exc)
                events.append({"event": "parse_failed", "attempt": attempt + 1, "error": last_error})
                logger.warning("boundary_refinement_parse_failed", extra={"error": last_error, "attempt": attempt + 1})
        events.append({"event": "fallback_used", "reason": last_error})
        return BoundaryRefinementDecision(decision="keep", reason="fallback_to_baseline"), events

    def build_prompt(self, chunk_a: str, chunk_b: str) -> str:
        """Build one compact merge-first prompt."""
        return (
            "Return JSON with keys: decision, reason. "
            "decision must be 'merge' or 'keep'. "
            "Merge only when B continues or explains A and the combined unit remains coherent.\n"
            f"Chunk A:\n{chunk_a}\n\nChunk B:\n{chunk_b}"
        )

    def _call_provider(self, payload: dict[str, Any]) -> Any:
        if self.provider is not None:
            return self.provider(payload)
        if not settings.enrichment_api_base_url:
            raise ValueError("provider_unavailable")
        headers = {}
        if settings.enrichment_api_key:
            headers["Authorization"] = f"Bearer {settings.enrichment_api_key}"
        response = self.client.post(settings.enrichment_api_base_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def _parse_response(self, payload: Any) -> BoundaryRefinementDecision:
        if isinstance(payload, dict) and "content" in payload and isinstance(payload["content"], str):
            payload = payload["content"]
        if isinstance(payload, str):
            normalized = payload.strip()
            if normalized.startswith("```"):
                normalized = normalized.strip("`")
                normalized = normalized.replace("json\n", "", 1).strip()
            payload = json.loads(normalized)
        if not isinstance(payload, dict):
            raise ValueError("invalid_boundary_refinement_payload")

        if "should_merge" in payload:
            decision = "merge" if bool(payload["should_merge"]) else "keep"
        else:
            decision = str(payload.get("decision") or "").strip().lower()
        if decision not in {"merge", "keep"}:
            raise ValueError("invalid_boundary_refinement_decision")
        reason = str(payload.get("reason") or "").strip() or "no_reason_provided"
        return BoundaryRefinementDecision(decision=decision, reason=reason)

    def _merge_pair(self, chunk_a: dict[str, Any], chunk_b: dict[str, Any]) -> dict[str, Any]:
        content = f"{chunk_a['content']} {chunk_b['content']}".strip()
        return {
            **chunk_a,
            "content": content,
            "token_count": self._estimate_tokens(content),
            "source_segment": chunk_a.get("source_segment"),
        }

    def _estimate_tokens(self, text: str) -> int:
        word_count = len(text.split())
        return max(1, int(word_count * 1.3))

    def _info(self) -> dict[str, Any]:
        return {
            "model_identifier": self.model_identifier,
            "prompt_version": self.prompt_version,
        }
