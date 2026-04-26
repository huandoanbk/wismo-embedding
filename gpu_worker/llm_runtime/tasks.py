"""Celery tasks for the LLM GPU worker lane — boundary refinement and enrichment."""

from __future__ import annotations

import json
import logging
import ssl as _ssl
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from uuid import uuid4

from celery import Celery
import httpx

from core.llm_lane import (
    LLM_OPERATION_ENRICHMENT,
    LLM_OPERATION_REFINEMENT,
    LLM_QUEUE_ENRICHMENT,
    LLM_QUEUE_REFINEMENT,
)
from gpu_worker.llm_runtime.config import load_settings
from gpu_worker.llm_runtime.runtime import LlmRuntime

settings = load_settings()
app = Celery("wismo_llm_gpu", broker=settings.broker_url)
app.conf.task_serializer = "json"
app.conf.result_backend = None
if settings.broker_url.startswith("rediss://"):
    app.conf.broker_use_ssl = {"ssl_cert_reqs": _ssl.CERT_NONE}

logger = logging.getLogger(__name__)
LLM_RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
LLM_STALE_STATUS_CODES = {404, 409}


def _runtime() -> LlmRuntime:
    """Return a runtime facade built from environment variables."""
    return LlmRuntime(settings)


def _retry_countdown(retries: int) -> int:
    """Return a bounded exponential backoff in seconds for LLM retries."""
    return min(60 * (2**retries), 300)


def _current_in_flight_limit(runtime: LlmRuntime, *, operation: str) -> int:
    """Read the current live in-flight request limit from VPS."""
    try:
        config = runtime.get_runtime_config(operation=operation)
    except Exception:
        return 1
    return max(1, int(config.get("vastai_max_in_flight_requests") or 1))


# ---------------------------------------------------------------------------
# Prompt builders — boundary refinement
# ---------------------------------------------------------------------------


def _build_boundary_prompt(chunk_a: str, chunk_b: str) -> str:
    """Return a compact merge-first prompt for two adjacent transcript chunks."""
    return (
        "Return JSON with keys: decision, reason. "
        "decision must be 'merge' or 'keep'. "
        "Merge only when B continues or explains A and the combined unit remains coherent.\n"
        f"Chunk A:\n{chunk_a}\n\nChunk B:\n{chunk_b}"
    )


def _parse_merge_decision(text: str) -> bool:
    """Parse an LLM merge/keep decision from raw text. Fallback: keep (False)."""
    normalized = text.strip()
    if normalized.startswith("```"):
        normalized = normalized.strip("`")
        normalized = normalized.replace("json\n", "", 1).strip()
    try:
        data = json.loads(normalized)
    except (json.JSONDecodeError, ValueError):
        return False
    if "should_merge" in data:
        return bool(data["should_merge"])
    return str(data.get("decision") or "").strip().lower() == "merge"


# ---------------------------------------------------------------------------
# Prompt builders — enrichment
# ---------------------------------------------------------------------------


def _build_enrichment_prompt(
    content: str,
    content_profile: str,
    source_metadata: dict[str, object],
) -> str:
    """Build the canonical enrichment prompt for one chunk."""
    # Truncate content to avoid overflowing context window — enrichment
    # only needs enough text to produce a summary and topic, not the full chunk.
    MAX_CONTENT_CHARS = 2000
    trimmed = content[:MAX_CONTENT_CHARS]

    meta_str = json.dumps(source_metadata, ensure_ascii=False)
    lang_instruction = "Trả lời bằng tiếng Việt. Tất cả các giá trị trong JSON phải bằng tiếng Việt. Giữ summary tối đa 3 câu ngắn gọn."
    if content_profile == "sutra":
        return (
            "Trả về JSON với các key: summary, topic, content_type, dharma_concepts. "
            "Phân tích một đoạn kinh điển Phật giáo với góc nhìn kinh điển phù hợp. "
            f"{lang_instruction}\n"
            f"Metadata nguồn: {meta_str}\n"
            f"Nội dung: {trimmed}"
        )
    if content_profile == "transcript":
        return (
            "Trả về JSON với các key: summary, topic, content_type, dharma_concepts, "
            "prev_summary, next_summary. "
            "Phân tích một đoạn transcript pháp thoại Phật giáo. "
            f"{lang_instruction}\n"
            f"Metadata nguồn: {meta_str}\n"
            f"Nội dung: {trimmed}"
        )
    return (
        "Trả về JSON với các key: summary, topic, content_type, dharma_concepts. "
        "Phân tích một đoạn sách Phật giáo. "
        f"{lang_instruction}\n"
        f"Metadata nguồn: {meta_str}\n"
        f"Nội dung: {trimmed}"
    )


def _parse_enrichment_response(text: str) -> dict[str, object]:
    """Parse an LLM enrichment JSON response from raw text. Raises on failure.

    Handles:
    - Plain JSON
    - Markdown code blocks (```json ... ```)
    - Qwen3 thinking mode output (<think>...</think> before JSON)
    """
    import re as _re
    normalized = text.strip()
    # Strip Qwen3 thinking mode tags
    normalized = _re.sub(r"<think>.*?</think>", "", normalized, flags=_re.DOTALL).strip()
    # Strip markdown code fences
    if normalized.startswith("```"):
        normalized = _re.sub(r"^```[a-z]*\n?", "", normalized).rstrip("`").strip()
    # Extract first JSON object if there is surrounding text
    match = _re.search(r"\{.*\}", normalized, _re.DOTALL)
    if match:
        normalized = match.group(0)
    if not normalized:
        raise ValueError("empty response from LLM")
    return json.loads(normalized)


# ---------------------------------------------------------------------------
# Task: run_llm_boundary_refinement
# ---------------------------------------------------------------------------


@app.task(
    name="gpu_worker.tasks.run_llm_boundary_refinement",
    queue=LLM_QUEUE_REFINEMENT,
    bind=True,
    max_retries=2,
    time_limit=3600,
)
def run_llm_boundary_refinement(self, source_item_id: str) -> dict[str, object]:
    """Fetch candidate transcript chunks, make pairwise merge decisions via Gemma,
    and callback boundary refinement results to VPS.

    Inputs:
        source_item_id: IngestionSourceItem UUID string.

    Output:
        dict with source_item_id, success_count, failure_count.

    Side effects:
        POSTs merge decisions to VPS /api/internal/llm-refinement-callback/.
    """
    task_id = getattr(self.request, "id", None)
    runtime = _runtime()
    batch_id = f"refine-{source_item_id}"
    attempt_id = task_id or str(uuid4())

    try:
        items_resp = runtime.get_refinement_items(source_item_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in LLM_STALE_STATUS_CODES:
            runtime.send_alert(
                severity="warning",
                event_type="refinement_items_fetch_skipped",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "status": "skipped_stale_source_item",
                "status_code": str(exc.response.status_code),
            }
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in LLM_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_refinement_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "status_code": exc.response.status_code,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_refinement_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc), "status_code": exc.response.status_code},
        )
        return {"source_item_id": source_item_id, "status": "skipped_after_error"}
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_refinement_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(exc), "retry_count": retry_count + 1},
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_refinement_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        return {"source_item_id": source_item_id, "status": "skipped_after_retries_exhausted"}

    candidate_chunks = items_resp["candidate_chunks"]
    if not candidate_chunks:
        # Nothing to refine — send empty callback so VPS can advance pipeline
        runtime.post_refinement_callback({
            "batch_id": batch_id,
            "attempt_id": attempt_id,
            "source_item_id": source_item_id,
            "job_type": "run_llm_boundary_refinement",
            "total_items": 0,
            "success_count": 0,
            "failure_count": 0,
            "segments": [],
            "model_name": settings.model_name,
            "model_version": "",
            "mode": settings.llm_mode,
        })
        return {"source_item_id": source_item_id, "success_count": 0, "failure_count": 0}

    # --- Make pairwise merge decisions ---
    # decisions[chunk_index] = {merged_with_next, parse_status, failure_reason}
    decisions: dict[int, dict[str, object]] = {}
    success_count = 0
    failure_count = 0

    def _resolve_refinement_pair(index: int) -> tuple[int, bool, str | None, str]:
        chunk_a = candidate_chunks[index]
        chunk_b = candidate_chunks[index + 1]
        prompt = _build_boundary_prompt(chunk_a["content"], chunk_b["content"])
        try:
            text = runtime.call_vllm(prompt, temperature=0.0)
            should_merge = _parse_merge_decision(text)
            return chunk_a["chunk_index"], should_merge, None, "ok"
        except Exception as exc:
            return chunk_a["chunk_index"], False, str(exc), "fallback"

    max_workers = max(1, len(candidate_chunks) - 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        next_pair_index = 0
        in_flight_limit = _current_in_flight_limit(runtime, operation=LLM_OPERATION_REFINEMENT)
        future_map: dict[Future[tuple[int, bool, str | None, str]], int] = {}

        while next_pair_index < len(candidate_chunks) - 1 or future_map:
            while next_pair_index < len(candidate_chunks) - 1 and len(future_map) < in_flight_limit:
                future = executor.submit(_resolve_refinement_pair, next_pair_index)
                future_map[future] = next_pair_index
                next_pair_index += 1

            done, _ = wait(set(future_map.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                idx, should_merge, failure_reason, parse_status = future.result()
                del future_map[future]
                if parse_status == "ok":
                    decisions[idx] = {
                        "merged_with_next": should_merge,
                        "parse_status": "ok",
                        "failure_reason": None,
                    }
                    success_count += 1
                else:
                    logger.warning(
                        "boundary_decision_failed chunk_index=%s error=%s", idx, failure_reason
                    )
                    decisions[idx] = {
                        "merged_with_next": False,
                        "parse_status": "fallback",
                        "failure_reason": failure_reason,
                    }
                    failure_count += 1
                in_flight_limit = min(
                    max_workers,
                    _current_in_flight_limit(runtime, operation=LLM_OPERATION_REFINEMENT),
                )

    # Build segments list — one entry per original candidate chunk
    segments = []
    for chunk in candidate_chunks:
        idx = chunk["chunk_index"]
        dec = decisions.get(idx, {
            "merged_with_next": False,
            "parse_status": "ok",
            "failure_reason": None,
        })
        segments.append({
            "segment_index": idx,
            "refined_content": "",  # merge-only; no content rewrite on worker side
            "merged_with_next": bool(dec["merged_with_next"]),
            "parse_status": str(dec["parse_status"]),
            "failure_reason": dec["failure_reason"],
        })

    try:
        runtime.post_refinement_callback({
            "batch_id": batch_id,
            "attempt_id": attempt_id,
            "source_item_id": source_item_id,
            "job_type": "run_llm_boundary_refinement",
            "total_items": len(candidate_chunks),
            "success_count": success_count,
            "failure_count": failure_count,
            "segments": segments,
            "model_name": settings.model_name,
            "model_version": "",
            "mode": settings.llm_mode,
        })
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in LLM_STALE_STATUS_CODES:
            runtime.send_alert(
                severity="warning",
                event_type="refinement_callback_skipped",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "success_count": success_count,
                "failure_count": failure_count,
                "status": "skipped_stale_callback",
            }
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in LLM_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_refinement_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "status_code": exc.response.status_code,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_refinement_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc), "status_code": exc.response.status_code},
        )
        return {
            "source_item_id": source_item_id,
            "success_count": success_count,
            "failure_count": failure_count,
            "status": "skipped_after_callback_error",
        }
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_refinement_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(exc), "retry_count": retry_count + 1},
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_refinement_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        return {
            "source_item_id": source_item_id,
            "success_count": success_count,
            "failure_count": failure_count,
            "status": "skipped_after_retries_exhausted",
        }

    return {
        "source_item_id": source_item_id,
        "success_count": success_count,
        "failure_count": failure_count,
    }


# ---------------------------------------------------------------------------
# Task: run_llm_enrichment
# ---------------------------------------------------------------------------


@app.task(
    name="gpu_worker.tasks.run_llm_enrichment",
    queue=LLM_QUEUE_ENRICHMENT,
    bind=True,
    max_retries=2,
    time_limit=3600,
)
def run_llm_enrichment(self, source_item_id: str) -> dict[str, object]:
    """Fetch finalized chunks, enrich each with Gemma metadata, and callback to VPS.

    Inputs:
        source_item_id: IngestionSourceItem UUID string.

    Output:
        dict with source_item_id, success_count, failure_count.

    Side effects:
        POSTs enrichment results to VPS /api/internal/llm-enrichment-callback/.
    """
    task_id = getattr(self.request, "id", None)
    runtime = _runtime()
    batch_id = f"enrich-{source_item_id}"
    attempt_id = task_id or str(uuid4())

    try:
        items_resp = runtime.get_enrichment_items(source_item_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in LLM_STALE_STATUS_CODES:
            runtime.send_alert(
                severity="warning",
                event_type="enrichment_items_fetch_skipped",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "status": "skipped_stale_source_item",
                "status_code": str(exc.response.status_code),
            }
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in LLM_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_enrichment_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "status_code": exc.response.status_code,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_enrichment_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc), "status_code": exc.response.status_code},
        )
        return {"source_item_id": source_item_id, "status": "skipped_after_error"}
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_enrichment_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(exc), "retry_count": retry_count + 1},
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_enrichment_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        return {"source_item_id": source_item_id, "status": "skipped_after_retries_exhausted"}

    chunks = items_resp["chunks"]
    content_profile = str(items_resp.get("content_profile") or "book")
    source_metadata = dict(items_resp.get("source_metadata") or {})

    result_items: list[dict[str, object] | None] = [None] * len(chunks)
    success_count = 0
    failure_count = 0

    def _enrich_chunk(chunk: dict[str, object]) -> dict[str, object]:
        chunk_id = str(chunk["chunk_id"])
        content = str(chunk["content"])
        try:
            prompt = _build_enrichment_prompt(content, content_profile, source_metadata)
            text = runtime.call_vllm(prompt, temperature=0.1)
            parsed = _parse_enrichment_response(text)
            return {
                "chunk_id": chunk_id,
                "topic": str(parsed.get("topic") or ""),
                "topic_tags": [str(t) for t in (parsed.get("topic_tags") or [])[:6]],
                "summary": str(parsed.get("summary") or ""),
                "teaching_type": str(
                    parsed.get("content_type") or parsed.get("teaching_type") or "explanation"
                ),
                "applicable_issues": [str(i) for i in (parsed.get("applicable_issues") or [])[:4]],
                "depth_level": str(parsed.get("depth_level") or "general"),
                "emotional_tone": str(parsed.get("emotional_tone") or "neutral"),
                "dharma_concepts": [
                    str(c) for c in (parsed.get("dharma_concepts") or parsed.get("topic_tags") or [])[:8]
                ],
                "parse_status": "ok",
                "failure_reason": None,
            }
        except Exception as exc:
            logger.warning("enrichment_failed chunk_id=%s error=%s", chunk_id, exc)
            return {
                "chunk_id": chunk_id,
                "topic": "",
                "topic_tags": [],
                "summary": "",
                "teaching_type": "explanation",
                "applicable_issues": [],
                "depth_level": "general",
                "emotional_tone": "neutral",
                "dharma_concepts": [],
                "parse_status": "fallback",
                "failure_reason": str(exc),
            }

    max_workers = max(1, len(chunks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        next_chunk_index = 0
        in_flight_limit = _current_in_flight_limit(runtime, operation=LLM_OPERATION_ENRICHMENT)
        future_map: dict[Future[dict[str, object]], int] = {}

        while next_chunk_index < len(chunks) or future_map:
            while next_chunk_index < len(chunks) and len(future_map) < in_flight_limit:
                future = executor.submit(_enrich_chunk, chunks[next_chunk_index])
                future_map[future] = next_chunk_index
                next_chunk_index += 1

            done, _ = wait(set(future_map.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                result = future.result()
                result_index = future_map[future]
                del future_map[future]
                result_items[result_index] = result
                if result["parse_status"] == "ok":
                    success_count += 1
                else:
                    failure_count += 1
                in_flight_limit = min(
                    max_workers,
                    _current_in_flight_limit(runtime, operation=LLM_OPERATION_ENRICHMENT),
                )

    try:
        runtime.post_enrichment_callback({
            "batch_id": batch_id,
            "attempt_id": attempt_id,
            "source_item_id": source_item_id,
            "job_type": "run_llm_enrichment",
            "total_items": len(chunks),
            "success_count": success_count,
            "failure_count": failure_count,
            "items": [item for item in result_items if item is not None],
            "model_name": settings.model_name,
            "model_version": "",
            "mode": settings.llm_mode,
        })
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in LLM_STALE_STATUS_CODES:
            runtime.send_alert(
                severity="warning",
                event_type="enrichment_callback_skipped",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": exc.response.text, "status_code": exc.response.status_code},
            )
            return {
                "source_item_id": source_item_id,
                "success_count": success_count,
                "failure_count": failure_count,
                "status": "skipped_stale_callback",
            }
        retry_count = int(getattr(self.request, "retries", 0))
        if exc.response.status_code in LLM_RETRYABLE_STATUS_CODES and retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_enrichment_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={
                    "message": str(exc),
                    "status_code": exc.response.status_code,
                    "retry_count": retry_count + 1,
                },
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_enrichment_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc), "status_code": exc.response.status_code},
        )
        return {
            "source_item_id": source_item_id,
            "success_count": success_count,
            "failure_count": failure_count,
            "status": "skipped_after_callback_error",
        }
    except Exception as exc:
        retry_count = int(getattr(self.request, "retries", 0))
        if retry_count < int(getattr(self, "max_retries", 0)):
            runtime.send_alert(
                severity="warning",
                event_type="llm_enrichment_retrying",
                source="llm_worker",
                task_id=task_id,
                source_item_id=source_item_id,
                details={"message": str(exc), "retry_count": retry_count + 1},
            )
            raise self.retry(exc=exc, countdown=_retry_countdown(retry_count))
        runtime.send_alert(
            severity="warning",
            event_type="llm_enrichment_skipped",
            source="llm_worker",
            task_id=task_id,
            source_item_id=source_item_id,
            details={"message": str(exc)},
        )
        return {
            "source_item_id": source_item_id,
            "success_count": success_count,
            "failure_count": failure_count,
            "status": "skipped_after_retries_exhausted",
        }

    return {
        "source_item_id": source_item_id,
        "success_count": success_count,
        "failure_count": failure_count,
    }
