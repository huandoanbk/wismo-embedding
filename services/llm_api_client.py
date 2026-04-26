"""LLM API mode: OpenAI-compatible LLM client running inline on VPS.

Used when an operation-scoped LLM runtime config chooses ``provider='api'``.
Calls any OpenAI-compatible endpoint (DeepInfra, OpenRouter, self-hosted, etc.)
directly from the VPS, bypassing Celery and Vast.ai entirely.
"""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import ContentChunk, LlmOperationConfig, LlmProviderConfig, SourceDocument
from core.ingestion_models import IngestionQueue, IngestionSourceItem
from core.llm_lane import (
    LLM_JOB_TYPES,
    LLM_OPERATION_ENRICHMENT,
    LLM_OPERATION_REFINEMENT,
    LlmBatchOutcome,
    build_llm_gate_summary,
    classify_llm_batch_outcome,
    llm_operation_for_job_type,
    record_llm_callback_seen,
)
from processors.runtime_artifacts import merge_gate_summary, set_pipeline_checkpoint

logger = logging.getLogger(__name__)

LLM_RUNTIME_MODE_STRICT = "strict"


# ---------------------------------------------------------------------------
# Prompt builders and response parsers (shared with gpu_worker inline)
# ---------------------------------------------------------------------------

import json as _json
import re as _re


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
        data = _json.loads(normalized)
    except (_json.JSONDecodeError, ValueError):
        return False
    if "should_merge" in data:
        return bool(data["should_merge"])
    return str(data.get("decision") or "").strip().lower() == "merge"


def _build_enrichment_prompt(
    content: str,
    content_profile: str,
    source_metadata: dict[str, object],
) -> str:
    """Build the canonical enrichment prompt for one chunk."""
    MAX_CONTENT_CHARS = 2000
    trimmed = content[:MAX_CONTENT_CHARS]
    meta_str = _json.dumps(source_metadata, ensure_ascii=False)
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

    Handles plain JSON, markdown code blocks, and Qwen3 thinking mode output.
    """
    normalized = text.strip()
    normalized = _re.sub(r"<think>.*?</think>", "", normalized, flags=_re.DOTALL).strip()
    if normalized.startswith("```"):
        normalized = _re.sub(r"^```[a-z]*\n?", "", normalized).rstrip("`").strip()
    match = _re.search(r"\{.*\}", normalized, _re.DOTALL)
    if match:
        normalized = match.group(0)
    if not normalized:
        raise ValueError("empty response from LLM")
    return _json.loads(normalized)


def _apply_llm_enrichment_debug(
    chunk: ContentChunk,
    *,
    parse_status: str,
    failure_reason: str | None,
    model_name: str,
    model_version: str,
) -> None:
    """Persist per-chunk API-mode enrichment debug info into chunk metadata."""
    payload = dict(chunk.paragraph_ids or {})
    if parse_status == "ok":
        payload["enrichment_fallback_used"] = False
        payload.pop("llm_enrichment_debug", None)
    else:
        payload["enrichment_fallback_used"] = True
        payload["llm_enrichment_debug"] = {
            "parse_status": parse_status,
            "failure_reason": failure_reason or "",
            "batch_id": "api-inline",
            "attempt_id": "api-inline",
            "model_name": model_name,
            "model_version": model_version,
            "mode": LLM_RUNTIME_MODE_STRICT,
        }
    chunk.paragraph_ids = payload


# ---------------------------------------------------------------------------
# Config persistence helpers
# ---------------------------------------------------------------------------


def get_llm_provider_config(db: Session) -> LlmProviderConfig | None:
    """Return the current LLM provider config row, or None if not yet set."""
    return db.execute(select(LlmProviderConfig)).scalars().first()


def get_llm_operation_config(db: Session, operation: str) -> LlmOperationConfig | None:
    """Return the operation-scoped LLM config row if it exists."""
    return db.execute(
        select(LlmOperationConfig).where(LlmOperationConfig.operation == operation)
    ).scalars().first()


def resolve_llm_operation_config(db: Session, operation: str) -> LlmOperationConfig:
    """Return operation config with fallback from legacy singleton config.

    If the new table does not yet contain a row for ``operation``, the function
    derives a transient config from the legacy singleton row so current installs
    keep working during migration.
    """
    config = get_llm_operation_config(db, operation)
    if config is not None:
        return config

    legacy = get_llm_provider_config(db)
    if legacy is not None:
        return LlmOperationConfig(
            operation=operation,
            provider=legacy.provider,
            api_base_url=legacy.api_base_url,
            api_key=legacy.api_key,
            api_model=legacy.api_model,
            vastai_max_in_flight_requests=legacy.vastai_max_in_flight_requests,
        )

    return LlmOperationConfig(
        operation=operation,
        provider="vastai",
        api_base_url="",
        api_key="",
        api_model=DEFAULT_LLM_API_MODEL,
        vastai_max_in_flight_requests=1,
    )


def resolve_shared_vastai_max_in_flight_requests(db: Session) -> int:
    """Return the shared Vast.ai in-flight limit for the LLM worker pool.

    Vast.ai runs one shared LLM worker process that consumes both refinement and
    enrichment queues, so the in-flight limit must be instance-scoped rather than
    operation-scoped. During migration, prefer the legacy singleton row when it
    exists; otherwise collapse operation rows into one effective shared value.
    """
    legacy = get_llm_provider_config(db)
    if legacy is not None:
        return max(1, int(legacy.vastai_max_in_flight_requests or 1))

    configs = list_llm_operation_configs(db).values()
    values = [int(config.vastai_max_in_flight_requests or 1) for config in configs]
    if not values:
        return 1
    return max(1, max(values))


def list_llm_operation_configs(db: Session) -> dict[str, LlmOperationConfig]:
    """Return resolved configs for both refinement and enrichment."""
    return {
        LLM_OPERATION_REFINEMENT: resolve_llm_operation_config(db, LLM_OPERATION_REFINEMENT),
        LLM_OPERATION_ENRICHMENT: resolve_llm_operation_config(db, LLM_OPERATION_ENRICHMENT),
    }


def save_llm_operation_config(
    db: Session,
    *,
    operation: str,
    provider: str,
    api_base_url: str,
    api_key: str,
    api_model: str,
    vastai_max_in_flight_requests: int,
) -> LlmOperationConfig:
    """Upsert operation-scoped LLM runtime config.

    The Vast.ai in-flight limit is shared by the LLM worker pool. Persist the
    provided value onto both operation rows so older readers and mixed-version
    code paths observe a consistent instance-level setting.
    """
    normalized_in_flight = max(1, int(vastai_max_in_flight_requests or 1))
    existing = get_llm_operation_config(db, operation)
    if existing is None:
        config = LlmOperationConfig(
            operation=operation,
            provider=provider,
            api_base_url=api_base_url,
            api_key=api_key,
            api_model=api_model,
            vastai_max_in_flight_requests=normalized_in_flight,
        )
        db.add(config)
    else:
        existing.provider = provider
        existing.api_base_url = api_base_url
        existing.api_key = api_key
        existing.api_model = api_model
        existing.vastai_max_in_flight_requests = normalized_in_flight
        existing.updated_at = datetime.utcnow()
        config = existing

    sibling_operation = (
        LLM_OPERATION_ENRICHMENT
        if operation == LLM_OPERATION_REFINEMENT
        else LLM_OPERATION_REFINEMENT
    )
    sibling = get_llm_operation_config(db, sibling_operation)
    if sibling is not None:
        sibling.vastai_max_in_flight_requests = normalized_in_flight
        sibling.updated_at = datetime.utcnow()

    legacy = get_llm_provider_config(db)
    if legacy is not None:
        legacy.vastai_max_in_flight_requests = normalized_in_flight
        legacy.updated_at = datetime.utcnow()
    db.flush()
    return config


def save_llm_provider_config(
    db: Session,
    *,
    provider: str,
    api_base_url: str,
    api_key: str,
    api_model: str,
    vastai_max_in_flight_requests: int,
) -> LlmProviderConfig:
    """Upsert the LLM provider config (singleton row).

    Inputs:
        db: active SQLAlchemy session.
        provider: ``"vastai"`` or ``"api"``.
        api_base_url: base URL for OpenAI-compatible API.
        api_key: API key for the external endpoint.
        api_model: model name to pass to the API.

    Output:
        Persisted ``LlmProviderConfig`` instance (flushed, not committed).

    Side effects:
        Inserts or updates the single row in ``llm_provider_config``.
    """
    existing = get_llm_provider_config(db)
    if existing is None:
        config = LlmProviderConfig(
            provider=provider,
            api_base_url=api_base_url,
            api_key=api_key,
            api_model=api_model,
            vastai_max_in_flight_requests=vastai_max_in_flight_requests,
        )
        db.add(config)
    else:
        existing.provider = provider
        existing.api_base_url = api_base_url
        existing.api_key = api_key
        existing.api_model = api_model
        existing.vastai_max_in_flight_requests = vastai_max_in_flight_requests
        existing.updated_at = datetime.utcnow()
        config = existing
    db.flush()
    return config


# ---------------------------------------------------------------------------
# OpenAI-compatible API client
# ---------------------------------------------------------------------------


class LlmApiClient:
    """Thin wrapper around the OpenAI SDK for any OpenAI-compatible endpoint.

    Inputs:
        base_url: API base URL (e.g. ``https://api.deepinfra.com/v1/openai``).
        api_key: API key for the endpoint.
        model: model name to request.

    Side effects:
        Each ``complete`` call makes one HTTP request to the API endpoint.
    """

    def __init__(self, *, base_url: str, api_key: str, model: str) -> None:
        from openai import OpenAI

        self._client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def complete(self, prompt: str, *, temperature: float = 0.1) -> tuple[str, int, int]:
        """Send one chat completion request and return (text, prompt_tokens, completion_tokens).

        Requests JSON mode (``response_format={"type": "json_object"}``) so
        the model is constrained to emit valid JSON.  Falls back to plain text
        if the provider does not support JSON mode.

        Handles Qwen3 thinking mode: when ``content`` is empty or None the
        model may have put the answer in ``reasoning_content``.  Both fields
        are concatenated so the caller's parser can extract JSON regardless.
        """
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": 2048,
            "response_format": {"type": "json_object"},
        }
        try:
            response = self._client.chat.completions.create(**kwargs)
        except Exception:
            # Provider does not support response_format — retry without it
            kwargs.pop("response_format")
            response = self._client.chat.completions.create(**kwargs)
        msg = response.choices[0].message
        content = msg.content or ""
        reasoning = getattr(msg, "reasoning_content", None) or ""
        combined = f"{reasoning}\n{content}".strip() if reasoning else content
        logger.debug("llm api raw response: %r", combined[:500])
        usage = response.usage
        prompt_tokens = usage.prompt_tokens if usage else 0
        completion_tokens = usage.completion_tokens if usage else 0
        return combined, prompt_tokens, completion_tokens

    @classmethod
    def from_config(cls, config: LlmProviderConfig) -> "LlmApiClient":
        """Construct a client from a ``LlmProviderConfig`` DB row."""
        return cls(
            base_url=config.api_base_url,
            api_key=config.api_key,
            model=config.api_model,
        )


# ---------------------------------------------------------------------------
# Inline operation executors — run on VPS, no Celery/Vast.ai
# ---------------------------------------------------------------------------


def run_llm_enrichment_via_api(
    db: Session,
    source_item_id: str | uuid.UUID,
    client: LlmApiClient,
) -> dict[str, Any]:
    """Enrich all chunks for one source item by calling the API inline.

    Mirrors the logic of the ``/api/internal/llm-enrichment-callback/`` handler
    but runs directly on the VPS using the OpenAI-compatible API client.

    Inputs:
        db: active SQLAlchemy session.
        source_item_id: UUID of the ``IngestionSourceItem`` to enrich.
        client: pre-configured ``LlmApiClient``.

    Output:
        Dict with ``success``, ``failed``, and ``outcome`` keys.

    Side effects:
        Updates ``ContentChunk`` enrichment fields in DB.
        Updates ``IngestionSourceItem.metadata_json`` with pipeline checkpoint.
        Enqueues embedding batch when outcome is not FAILED.
        Marks the related ``IngestionQueue`` item as completed.
        Commits the session.
    """
    from tasks import enqueue_embedding_batch

    item = db.get(IngestionSourceItem, uuid.UUID(str(source_item_id)))
    if item is None:
        raise ValueError(f"source_item_not_found: {source_item_id}")

    content_profile = (item.metadata_json or {}).get("content_profile", "book")
    source_metadata: dict[str, Any] = {
        "source_type": item.source_type,
        "content_profile": content_profile,
        "display_name": item.display_name,
    }

    # Fetch non-duplicate chunks
    chunks: list[ContentChunk] = list(
        db.execute(
            select(ContentChunk).where(
                ContentChunk.source_id == item.source_document_id,
                ContentChunk.duplicate_of.is_(None),
            )
        ).scalars()
    )

    batch_id = f"api-enrich-{source_item_id}"
    attempt_id = str(uuid.uuid4())
    success_count = 0
    failure_count = 0

    def _enrich_one(chunk: ContentChunk) -> tuple[uuid.UUID, dict[str, Any] | None, Exception | None, int, int]:
        """Call API for one chunk. Returns (chunk_id, parsed, exc, prompt_tokens, completion_tokens)."""
        try:
            prompt = _build_enrichment_prompt(chunk.content or "", content_profile, source_metadata)
            text, pt, ct = client.complete(prompt, temperature=0.1)
            parsed = _parse_enrichment_response(text)
            return chunk.id, parsed, None, pt, ct
        except Exception as exc:
            return chunk.id, None, exc, 0, 0

    # Enrich chunks concurrently — each call is independent, DB writes happen after
    chunk_by_id = {c.id: c for c in chunks}
    results: dict[uuid.UUID, dict[str, Any] | None] = {}
    failures: dict[uuid.UUID, str] = {}
    total_prompt_tokens = 0
    total_completion_tokens = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_enrich_one, chunk): chunk.id for chunk in chunks}
        for future in as_completed(futures):
            chunk_id, parsed, exc, pt, ct = future.result()
            total_prompt_tokens += pt
            total_completion_tokens += ct
            if exc is not None:
                logger.warning("chunk %s enrichment failed: %s", chunk_id, exc)
                failures[chunk_id] = str(exc)
                failure_count += 1
            else:
                results[chunk_id] = parsed
                success_count += 1

    # Write results back to DB (single-threaded, safe)
    for chunk_id, parsed in results.items():
        chunk = chunk_by_id[chunk_id]
        _apply_llm_enrichment_debug(
            chunk,
            parse_status="ok",
            failure_reason=None,
            model_name=client.model,
            model_version="api",
        )
        chunk.topic = parsed.get("topic") or chunk.topic
        chunk.topic_tags = parsed.get("topic_tags") or chunk.topic_tags
        chunk.summary = parsed.get("summary") or chunk.summary
        chunk.teaching_type = parsed.get("content_type") or chunk.teaching_type
        chunk.applicable_issues = parsed.get("applicable_issues") or chunk.applicable_issues
        chunk.depth_level = parsed.get("depth_level") or chunk.depth_level
        chunk.emotional_tone = parsed.get("emotional_tone") or chunk.emotional_tone

    for chunk_id, failure_reason in failures.items():
        chunk = chunk_by_id[chunk_id]
        _apply_llm_enrichment_debug(
            chunk,
            parse_status="fallback",
            failure_reason=failure_reason,
            model_name=client.model,
            model_version="api",
        )

    logger.info(
        "enrichment done: %d success, %d failed / %d chunks | tokens in=%d out=%d total=%d",
        success_count, failure_count, len(chunks),
        total_prompt_tokens, total_completion_tokens, total_prompt_tokens + total_completion_tokens,
    )

    total_items = len(chunks)

    gate_summary = build_llm_gate_summary(
        parse_failure_count=failure_count,
        schema_invalid_count=0,
        fallback_count=0,
        total_items=total_items,
        mode=LLM_RUNTIME_MODE_STRICT,
    )
    outcome = classify_llm_batch_outcome(
        success_count=success_count,
        total_items=total_items,
        mode=LLM_RUNTIME_MODE_STRICT,
    )

    metadata_json = dict(item.metadata_json or {})
    metadata_json = record_llm_callback_seen(metadata_json, batch_id, attempt_id)
    metadata_json["llm_enrichment_model"] = client.model
    metadata_json["llm_enrichment_model_version"] = "api"

    # Enqueue embedding when outcome is not FAILED; set pipeline status accordingly.
    if outcome != LlmBatchOutcome.FAILED and item.source_document_id is not None:
        metadata_json = set_pipeline_checkpoint(metadata_json, "llm_enrichment_successful")
        metadata_json = merge_gate_summary(metadata_json, "llm_enrichment_successful", gate_summary)
        item.metadata_json = metadata_json
        db.flush()
        source_document = db.get(SourceDocument, item.source_document_id)
        if source_document is not None:
            enriched_chunks = list(
                db.execute(
                    select(ContentChunk).where(
                        ContentChunk.source_id == item.source_document_id,
                        ContentChunk.duplicate_of.is_(None),
                    )
                ).scalars()
            )
            enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=item,
                chunks=enriched_chunks,
            )
        item.processing_status = "awaiting_embedding"
        item.updated_at = datetime.utcnow()
    else:
        metadata_json = set_pipeline_checkpoint(metadata_json, "failed_enrichment")
        metadata_json = merge_gate_summary(metadata_json, "failed_enrichment", gate_summary)
        item.metadata_json = metadata_json
        item.processing_status = "llm_enrichment_failed"
        item.updated_at = datetime.utcnow()

    # Mark the queue item completed
    queue_item = db.execute(
        select(IngestionQueue).where(
            IngestionQueue.source_item_id == item.id,
            IngestionQueue.job_type.in_(LLM_JOB_TYPES),
            IngestionQueue.status == "processing",
        )
    ).scalars().first()
    if queue_item is not None:
        if outcome != LlmBatchOutcome.FAILED:
            queue_item.status = "completed"
            queue_item.completed_at = datetime.utcnow()
            queue_item.error_message = None
        else:
            queue_item.status = "failed"
            queue_item.retry_count += 1
            queue_item.error_message = f"llm_enrichment_failed:{outcome.value}"

    db.commit()
    return {
        "success": success_count,
        "failed": failure_count,
        "outcome": outcome.value,
        "tokens_in": total_prompt_tokens,
        "tokens_out": total_completion_tokens,
        "tokens_total": total_prompt_tokens + total_completion_tokens,
    }


def run_llm_refinement_via_api(
    db: Session,
    source_item_id: str | uuid.UUID,
    client: LlmApiClient,
) -> dict[str, Any]:
    """Run boundary refinement inline via API and persist the same contract as callback mode."""
    from tasks import enqueue_llm_enrichment_batch

    item = db.get(IngestionSourceItem, uuid.UUID(str(source_item_id)))
    if item is None:
        raise ValueError(f"source_item_not_found: {source_item_id}")
    if item.source_document_id is None:
        raise ValueError(f"source_item_missing_source_document: {source_item_id}")

    chunks: list[ContentChunk] = list(
        db.execute(
            select(ContentChunk)
            .where(
                ContentChunk.source_id == item.source_document_id,
                ContentChunk.duplicate_of.is_(None),
            )
            .order_by(ContentChunk.chunk_index.asc())
        ).scalars()
    )

    batch_id = f"api-refine-{source_item_id}"
    attempt_id = str(uuid.uuid4())
    total_items = len(chunks)
    success_count = 0
    failure_count = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    decisions: dict[int, dict[str, Any]] = {}

    for index in range(max(0, len(chunks) - 1)):
        chunk_a = chunks[index]
        chunk_b = chunks[index + 1]
        prompt = _build_boundary_prompt(chunk_a.content or "", chunk_b.content or "")
        try:
            text, pt, ct = client.complete(prompt, temperature=0.0)
            total_prompt_tokens += pt
            total_completion_tokens += ct
            decisions[chunk_a.chunk_index] = {
                "merged_with_next": _parse_merge_decision(text),
                "parse_status": "ok",
                "failure_reason": None,
            }
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            decisions[chunk_a.chunk_index] = {
                "merged_with_next": False,
                "parse_status": "fallback",
                "failure_reason": str(exc),
            }
            failure_count += 1

    parse_failure_count = sum(1 for d in decisions.values() if d["parse_status"] == "parse_failure")
    schema_invalid_count = sum(1 for d in decisions.values() if d["parse_status"] == "schema_invalid")
    fallback_count = sum(1 for d in decisions.values() if d["parse_status"] == "fallback")
    refinement_pair_count = max(0, total_items - 1)
    gate_summary = build_llm_gate_summary(
        parse_failure_count=parse_failure_count,
        schema_invalid_count=schema_invalid_count,
        fallback_count=fallback_count,
        total_items=total_items,
        mode=LLM_RUNTIME_MODE_STRICT,
    )
    outcome = classify_llm_batch_outcome(
        success_count=success_count,
        total_items=refinement_pair_count,
        mode=LLM_RUNTIME_MODE_STRICT,
    )

    chunk_by_index = {chunk.chunk_index: chunk for chunk in chunks}
    for chunk_index, decision in decisions.items():
        if not decision["merged_with_next"]:
            continue
        chunk = chunk_by_index.get(chunk_index)
        next_chunk = chunk_by_index.get(chunk_index + 1)
        if chunk is None or next_chunk is None or next_chunk.duplicate_of is not None:
            continue
        chunk.content = ((chunk.content or "") + " " + (next_chunk.content or "")).strip()
        next_chunk.duplicate_of = chunk.id

    metadata_json = dict(item.metadata_json or {})
    metadata_json = record_llm_callback_seen(metadata_json, batch_id, attempt_id)
    metadata_json["llm_refinement_model"] = client.model
    metadata_json["llm_refinement_model_version"] = "api"
    metadata_json["llm_refined_segments"] = [
        {
            "segment_index": chunk.chunk_index,
            "refined_content": "",
            "merged_with_next": bool(decisions.get(chunk.chunk_index, {}).get("merged_with_next", False)),
            "parse_status": str(decisions.get(chunk.chunk_index, {}).get("parse_status", "ok")),
        }
        for chunk in chunks
    ]

    queue_item = db.execute(
        select(IngestionQueue).where(
            IngestionQueue.source_item_id == item.id,
            IngestionQueue.job_type.in_(LLM_JOB_TYPES),
            IngestionQueue.status == "processing",
        )
    ).scalars().first()

    if outcome != LlmBatchOutcome.FAILED:
        metadata_json = set_pipeline_checkpoint(metadata_json, "refinement_successful")
        metadata_json = merge_gate_summary(metadata_json, "refinement_successful", gate_summary)
        item.metadata_json = metadata_json
        db.flush()
        enrichment_queue = enqueue_llm_enrichment_batch(db, source_item=item)
        metadata_json = dict(item.metadata_json or {})
        metadata_json["refinement_triggered_enrichment_queue_id"] = str(enrichment_queue.id)
        item.metadata_json = metadata_json
        item.processing_status = "awaiting_llm_enrichment"
        item.updated_at = datetime.utcnow()
        if queue_item is not None:
            queue_item.status = "completed"
            queue_item.completed_at = datetime.utcnow()
            queue_item.error_message = None
    else:
        metadata_json = set_pipeline_checkpoint(metadata_json, "failed_refinement")
        metadata_json = merge_gate_summary(metadata_json, "failed_refinement", gate_summary)
        item.metadata_json = metadata_json
        item.processing_status = "llm_refinement_failed"
        item.updated_at = datetime.utcnow()
        if queue_item is not None:
            queue_item.status = "failed"
            queue_item.retry_count += 1
            queue_item.error_message = f"llm_refinement_failed:{outcome.value}"

    db.commit()
    return {
        "success": success_count,
        "failed": failure_count,
        "outcome": outcome.value,
        "tokens_in": total_prompt_tokens,
        "tokens_out": total_completion_tokens,
        "tokens_total": total_prompt_tokens + total_completion_tokens,
    }
DEFAULT_LLM_API_MODEL = "Qwen/Qwen3-14B"
