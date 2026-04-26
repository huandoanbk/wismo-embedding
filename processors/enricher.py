"""Chunk metadata enrichment with provider fallback."""

from __future__ import annotations

import json
from typing import Any

import httpx

from core.config import settings

ENRICHMENT_PROMPT_VERSION = "source-aware-enrichment-v1"


class ChunkEnricher:
    """Enrich chunk content into normalized retrieval metadata."""

    DEFAULT_METADATA = {
        "topic": "",
        "topic_tags": [],
        "summary": "",
        "content_type": "explanation",
        "teaching_type": "explanation",
        "dharma_concepts": [],
        "applicable_issues": [],
        "depth_level": "beginner",
        "emotional_tone": "neutral",
        "prev_summary": "",
        "next_summary": "",
        "enrichment_fallback_used": False,
    }

    def __init__(self, client: httpx.Client | None = None, *, max_retries: int = 1) -> None:
        self.client = client or httpx.Client(timeout=settings.enrichment_timeout_seconds)
        self.max_retries = max_retries
        self.prompt_version = ENRICHMENT_PROMPT_VERSION

    def enrich(self, content: str, source_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        """Return normalized metadata, falling back safely on provider failures."""
        if not settings.enrichment_api_base_url:
            return self._fallback_metadata()

        source_metadata = source_metadata or {}
        payload = {
            "model": settings.enrichment_model,
            "prompt": self.build_prompt(content=content, source_metadata=source_metadata),
            "temperature": 0.1,
        }
        headers = {}
        if settings.enrichment_api_key:
            headers["Authorization"] = f"Bearer {settings.enrichment_api_key}"

        for _attempt in range(self.max_retries + 1):
            try:
                response = self.client.post(settings.enrichment_api_base_url, json=payload, headers=headers)
                response.raise_for_status()
                parsed = self._parse_response(response.json())
                return self._normalize_metadata(parsed, source_metadata=source_metadata)
            except (ValueError, json.JSONDecodeError, httpx.HTTPError, KeyError, TypeError):
                continue
        return self._fallback_metadata()

    def build_prompt(self, content: str, source_metadata: dict[str, Any]) -> str:
        """Build the canonical enrichment prompt."""
        content_profile = str(source_metadata.get("content_profile") or "").strip().lower()
        if content_profile == "sutra":
            return self._build_sutra_prompt(content, source_metadata)
        if content_profile == "transcript":
            return self._build_transcript_prompt(content, source_metadata)
        return self._build_book_prompt(content, source_metadata)

    def _parse_response(self, payload: dict[str, Any]) -> dict[str, Any]:
        content = payload.get("content", payload)
        if isinstance(content, dict):
            return content

        if not isinstance(content, str):
            raise ValueError("Invalid enrichment payload")

        normalized = content.strip()
        if normalized.startswith("```"):
            normalized = normalized.strip("`")
            normalized = normalized.replace("json\n", "", 1).strip()
        return json.loads(normalized)

    def _normalize_metadata(self, payload: dict[str, Any], *, source_metadata: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(self.DEFAULT_METADATA)
        content_type = str(
            payload.get("content_type")
            or payload.get("teaching_type")
            or "explanation"
        )
        normalized.update(
            {
                "topic": str(payload.get("topic", "") or ""),
                "summary": str(payload.get("summary", "") or ""),
                "content_type": content_type,
                "teaching_type": content_type,
                "depth_level": str(payload.get("depth_level", "beginner") or "beginner"),
                "emotional_tone": str(payload.get("emotional_tone", "neutral") or "neutral"),
                "prev_summary": str(payload.get("prev_summary", "") or ""),
                "next_summary": str(payload.get("next_summary", "") or ""),
                "enrichment_fallback_used": False,
            }
        )
        concept_source = payload.get("dharma_concepts", payload.get("topic_tags", []))
        normalized["topic_tags"] = [str(item) for item in payload.get("topic_tags", [])[:6]]
        if not normalized["topic_tags"]:
            normalized["topic_tags"] = [str(item) for item in concept_source[:6]]
        normalized["dharma_concepts"] = [str(item) for item in concept_source[:8]]
        normalized["applicable_issues"] = [str(item) for item in payload.get("applicable_issues", [])[:4]]
        if source_metadata.get("content_profile") != "transcript":
            normalized["prev_summary"] = ""
            normalized["next_summary"] = ""
        return normalized

    def _fallback_metadata(self) -> dict[str, Any]:
        fallback = dict(self.DEFAULT_METADATA)
        fallback["enrichment_fallback_used"] = True
        return fallback

    def _build_book_prompt(self, content: str, source_metadata: dict[str, Any]) -> str:
        return (
            "Return JSON with keys: summary, topic, content_type, dharma_concepts. "
            "Analyze one Buddhist book chunk.\n"
            f"Source metadata: {json.dumps(source_metadata, ensure_ascii=True)}\n"
            f"Content: {content}"
        )

    def _build_sutra_prompt(self, content: str, source_metadata: dict[str, Any]) -> str:
        return (
            "Return JSON with keys: summary, topic, content_type, dharma_concepts. "
            "Analyze one Buddhist sutra chunk with scripture-aware framing.\n"
            f"Source metadata: {json.dumps(source_metadata, ensure_ascii=True)}\n"
            f"Content: {content}"
        )

    def _build_transcript_prompt(self, content: str, source_metadata: dict[str, Any]) -> str:
        return (
            "Return JSON with keys: summary, topic, content_type, dharma_concepts, prev_summary, next_summary. "
            "Analyze one Buddhist talk transcript chunk using boundary context.\n"
            f"Source metadata: {json.dumps(source_metadata, ensure_ascii=True)}\n"
            f"Content: {content}"
        )
