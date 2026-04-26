"""Internal callback routes for GPU worker coordination."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import settings
from core.database import get_db
from core.ingestion_models import EmbeddingBatch, IngestionQueue, IngestionSourceItem
from core.llm_lane import (
    LLM_GATE_REASON_FALLBACK,
    LLM_GATE_REASON_PARSE_FAILURE,
    LLM_GATE_REASON_SCHEMA_INVALID,
    LlmBatchOutcome,
    LlmCallbackStatus,
    LLM_RUNTIME_MODE_STRICT,
    build_llm_gate_summary,
    classify_llm_batch_outcome,
    classify_llm_callback,
    record_llm_callback_seen,
)
from core.llm_lane import LLM_OPERATION_ENRICHMENT, LLM_OPERATION_REFINEMENT
from core.models import ChunkEmbedding, ContentChunk, SourceDocument
from core.versions import EMBEDDING_VERSION
from processors.runtime_artifacts import (
    PIPELINE_STAGE_EMBEDDING_SUCCESSFUL,
    PIPELINE_STAGE_FAILED_EMBEDDING,
    PIPELINE_STAGE_FAILED_ENRICHMENT,
    PIPELINE_STAGE_FAILED_REFINEMENT,
    PIPELINE_STAGE_FAILED_TRANSCRIPT,
    PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
    PIPELINE_STAGE_REFINEMENT_SUCCESSFUL,
    merge_gate_summary,
    set_pipeline_checkpoint,
)
from services.worker_alerts import WorkerAlertPayload, WorkerAlertStatusUpdate, ingest_worker_alert, update_worker_alert_status
from tasks import enqueue_embedding_batch, enqueue_llm_enrichment_batch, enqueue_transcript_followup

router = APIRouter(prefix="/api/internal", tags=["internal"])


def _latest_llm_queue_row(
    db: Session,
    *,
    source_item_id: UUID,
    job_type: str,
) -> IngestionQueue | None:
    """Return the latest pending/processing LLM queue row for one source item and job type."""
    stmt = (
        select(IngestionQueue)
        .where(
            IngestionQueue.source_item_id == source_item_id,
            IngestionQueue.job_type == job_type,
            IngestionQueue.status.in_(("pending", "processing")),
        )
        .order_by(IngestionQueue.created_at.desc())
    )
    return db.execute(stmt).scalars().first()


def _apply_llm_enrichment_debug(
    chunk: ContentChunk,
    *,
    parse_status: str,
    failure_reason: str | None,
    batch_id: str,
    attempt_id: str,
    model_name: str,
    model_version: str,
    mode: str,
) -> None:
    """Persist per-chunk enrichment debug info into additive chunk metadata."""
    payload = dict(chunk.paragraph_ids or {})
    if parse_status == "ok":
        payload["enrichment_fallback_used"] = False
        payload.pop("llm_enrichment_debug", None)
    else:
        payload["enrichment_fallback_used"] = True
        payload["llm_enrichment_debug"] = {
            "parse_status": parse_status,
            "failure_reason": failure_reason or "",
            "batch_id": batch_id,
            "attempt_id": attempt_id,
            "model_name": model_name,
            "model_version": model_version,
            "mode": mode,
        }
    chunk.paragraph_ids = payload


class TranscriptReadyRequest(BaseModel):
    """Payload sent by GPU worker when a transcript extract is ready."""

    source_id: UUID
    extract_key: str


class TranscriptStatusRequest(BaseModel):
    """Execution status update for one transcript job."""

    source_id: UUID
    status: str
    error_message: str | None = None


class ChunkBatchRequest(BaseModel):
    """Chunk ids requested by the GPU embedding worker."""

    chunk_ids: list[UUID] = Field(default_factory=list)


class SourceItemLookupRequest(BaseModel):
    """Source item lookup payload for GPU worker task bootstrap."""

    source_item_id: UUID


class EmbeddingItem(BaseModel):
    """One embedding vector payload returned by the GPU worker."""

    chunk_id: UUID
    embedding: list[float]


class StoreEmbeddingsRequest(BaseModel):
    """Batch embedding write payload."""

    items: list[EmbeddingItem] = Field(default_factory=list)


class EmbeddingBatchStatusRequest(BaseModel):
    """Execution status update request for one embedding batch."""

    batch_id: UUID
    status: str
    error_message: str | None = None


class LlmRuntimeConfigResponse(BaseModel):
    """Live runtime config for the LLM worker lane."""

    operation: str = LLM_OPERATION_ENRICHMENT
    provider: str = "vastai"
    vastai_max_in_flight_requests: int = 1


class WorkerAlertRequest(BaseModel):
    """Operational alert payload submitted by GPU workers or local services."""

    severity: str
    event_type: str
    source: str
    occurred_at: datetime
    instance_id: str | None = None
    task_id: str | None = None
    source_item_id: UUID | None = None
    chunk_ids: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)


class WorkerAlertStatusRequest(BaseModel):
    """Status update request for worker alerts."""

    status: str
    resolved_by: str | None = None


def verify_internal(x_internal_secret: str = Header(...)) -> None:
    """Reject requests that do not carry the shared internal secret."""
    if x_internal_secret != settings.internal_shared_secret:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")


@router.post("/transcript-ready/")
async def transcript_ready(
    body: TranscriptReadyRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Queue transcript follow-up processing from the canonical extract layer."""
    item = db.get(IngestionSourceItem, body.source_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")
    if item.processing_status not in {"awaiting_transcript", "transcript_processing"}:
        return {
            "status": "late",
            "source_id": str(item.id),
            "processing_status": item.processing_status,
        }

    queue_item = enqueue_transcript_followup(db, item.id, extract_key=body.extract_key)
    transcript_queue = (
        db.execute(
            select(IngestionQueue)
            .where(
                IngestionQueue.source_item_id == item.id,
                IngestionQueue.job_type == "transcribe_audio",
                IngestionQueue.status == "processing",
            )
            .order_by(IngestionQueue.created_at.desc())
        )
        .scalars()
        .first()
    )
    if transcript_queue is not None:
        transcript_queue.status = "completed"
        transcript_queue.completed_at = datetime.utcnow()
        transcript_queue.error_message = None
    db.commit()
    return {
        "status": "queued",
        "queue_id": str(queue_item.id),
        "source_id": str(item.id),
    }


@router.post("/transcript-status/")
async def transcript_status(
    body: TranscriptStatusRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Persist terminal transcript failure from the Whisper worker."""
    item = db.get(IngestionSourceItem, body.source_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")
    if item.processing_status not in {"awaiting_transcript", "transcript_processing"}:
        return {
            "status": "late",
            "source_id": str(item.id),
            "processing_status": item.processing_status,
            "transcript_status": body.status,
        }

    queue_row = (
        db.execute(
            select(IngestionQueue)
            .where(
                IngestionQueue.source_item_id == item.id,
                IngestionQueue.job_type == "transcribe_audio",
                IngestionQueue.status.in_(("pending", "processing")),
            )
            .order_by(IngestionQueue.created_at.desc())
        )
        .scalars()
        .first()
    )
    if body.status == "failed":
        metadata_json = dict(item.metadata_json or {})
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_TRANSCRIPT)
        if body.error_message:
            metadata_json["transcript_error"] = body.error_message
        item.metadata_json = metadata_json
        item.processing_status = "transcript_failed"
        item.last_error_code = "transcript_failed"
        item.last_error_message = body.error_message
        item.updated_at = datetime.utcnow()
        if queue_row is not None:
            queue_row.status = "failed"
            queue_row.retry_count += 1
            queue_row.error_message = body.error_message
        db.commit()
    return {"status": "ok", "source_id": str(item.id), "transcript_status": body.status}


@router.post("/get-source-item/")
async def get_source_item(
    body: SourceItemLookupRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return raw location and identity fields for a mirrored media registry item."""
    item = db.get(IngestionSourceItem, body.source_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")
    if not item.raw_key:
        raise HTTPException(status_code=409, detail="source_item_missing_raw_key")

    return {
        "source_id": str(item.id),
        "source_type": item.source_type,
        "raw_bucket": item.raw_bucket,
        "raw_key": item.raw_key,
        "requested_at": item.updated_at.isoformat() if item.updated_at else None,
    }


class RefinementItemsRequest(BaseModel):
    """Source item lookup payload for LLM boundary refinement job bootstrap."""

    source_item_id: UUID


@router.post("/get-refinement-items/")
async def get_refinement_items(
    body: RefinementItemsRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return candidate chunk contents for one source item's LLM boundary refinement batch.

    The GPU worker calls this after receiving a ``run_llm_boundary_refinement`` task
    to discover which candidate chunks to refine before calling back with decisions.
    """
    item = db.get(IngestionSourceItem, body.source_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")
    if item.source_document_id is None:
        raise HTTPException(status_code=409, detail="source_item_missing_source_document")

    stmt = (
        select(ContentChunk)
        .where(
            ContentChunk.source_id == item.source_document_id,
            ContentChunk.duplicate_of.is_(None),
        )
        .order_by(ContentChunk.chunk_index.asc())
    )
    chunks = list(db.execute(stmt).scalars())
    metadata = item.metadata_json or {}
    return {
        "source_item_id": str(body.source_item_id),
        "source_document_id": str(item.source_document_id),
        "content_profile": str(metadata.get("content_profile") or "transcript"),
        "transcript_quality_tier": str(metadata.get("transcript_quality_tier") or "medium"),
        "candidate_chunks": [
            {
                "chunk_id": str(chunk.id),
                "content": chunk.content,
                "chunk_index": chunk.chunk_index,
            }
            for chunk in chunks
        ],
    }


class EnrichmentItemsRequest(BaseModel):
    """Source item lookup payload for LLM enrichment job bootstrap."""

    source_item_id: UUID


@router.post("/get-enrichment-items/")
async def get_enrichment_items(
    body: EnrichmentItemsRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return chunk contents for one source item's LLM enrichment batch.

    The GPU worker calls this after receiving a ``run_llm_enrichment`` task
    to discover which chunks to enrich and retrieve their content.
    """
    item = db.get(IngestionSourceItem, body.source_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")
    if item.source_document_id is None:
        raise HTTPException(status_code=409, detail="source_item_missing_source_document")

    stmt = select(ContentChunk).where(
        ContentChunk.source_id == item.source_document_id,
        ContentChunk.duplicate_of.is_(None),
    )
    chunks = list(db.execute(stmt).scalars())
    metadata = item.metadata_json or {}
    source_metadata = {
        k: v for k, v in metadata.items()
        if k in {
            "content_profile", "extension", "speaker", "talk_title",
            "book_title", "author", "sutra_name", "canon", "collection",
            "translator", "transcript_tier", "series", "episode",
        }
    }
    return {
        "source_item_id": str(body.source_item_id),
        "source_document_id": str(item.source_document_id),
        "content_profile": str(metadata.get("content_profile") or "book"),
        "source_metadata": source_metadata,
        "chunks": [
            {
                "chunk_id": str(chunk.id),
                "content": chunk.content,
                "chunk_index": chunk.chunk_index,
            }
            for chunk in chunks
        ],
    }


@router.get("/llm-runtime-config/")
async def get_llm_runtime_config(
    _: None = Depends(verify_internal),
    operation: str = Query(default=LLM_OPERATION_ENRICHMENT),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return live LLM runtime tuning config for worker polling."""
    from services.llm_api_client import (
        resolve_llm_operation_config,
        resolve_shared_vastai_max_in_flight_requests,
    )

    if operation not in {LLM_OPERATION_REFINEMENT, LLM_OPERATION_ENRICHMENT}:
        raise HTTPException(status_code=400, detail="unknown_llm_operation")
    config = resolve_llm_operation_config(db, operation)
    return LlmRuntimeConfigResponse(
        operation=operation,
        provider=config.provider,
        vastai_max_in_flight_requests=resolve_shared_vastai_max_in_flight_requests(db),
    ).model_dump()


@router.post("/get-chunks/")
async def get_chunks(
    body: ChunkBatchRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return chunk contents for GPU embedding batches."""
    if not body.chunk_ids:
        return {"chunks": []}

    stmt = select(ContentChunk).where(ContentChunk.id.in_(body.chunk_ids))
    chunks = list(db.execute(stmt).scalars())
    return {
        "chunks": [
            {"id": str(chunk.id), "content": chunk.content}
            for chunk in chunks
        ]
    }


@router.post("/store-embeddings/")
async def store_embeddings(
    body: StoreEmbeddingsRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Upsert chunk embeddings returned by the GPU worker."""
    stored = 0
    source_item_ids: set[UUID] = set()
    for item in body.items:
        chunk = db.get(ContentChunk, item.chunk_id)
        if chunk is None:
            continue
        existing = db.execute(
            select(ChunkEmbedding).where(ChunkEmbedding.chunk_id == item.chunk_id)
        ).scalar_one_or_none()
        if existing is None:
            db.add(
                ChunkEmbedding(
                    chunk_id=item.chunk_id,
                    embedding=item.embedding,
                    model_name="bge-m3",
                )
            )
        else:
            existing.embedding = item.embedding
            existing.model_name = "bge-m3"
        chunk.embedding_version = EMBEDDING_VERSION
        source_document = chunk.source
        if source_document is not None:
            source_document.embedding_version = EMBEDDING_VERSION
            if source_document.registry_item_id is not None:
                source_item_ids.add(source_document.registry_item_id)
        stored += 1

    for source_item_id in source_item_ids:
        source_item = db.get(IngestionSourceItem, source_item_id)
        if source_item is None:
            continue
        metadata_json = dict(source_item.metadata_json)
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_EMBEDDING_SUCCESSFUL)
        metadata_json = merge_gate_summary(
            metadata_json,
            PIPELINE_STAGE_EMBEDDING_SUCCESSFUL,
            {"stage": "embedding", "stored_embeddings_count": stored, "anomaly_count": 0, "fallback_count": 0},
        )
        source_item.metadata_json = metadata_json

    db.commit()
    return {"stored": stored, "errors": 0}


@router.post("/embedding-batch-status/")
async def set_embedding_batch_status(
    body: EmbeddingBatchStatusRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Update DB-backed embedding batch execution lifecycle."""
    batch = db.get(EmbeddingBatch, body.batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="embedding_batch_not_found")

    batch.status = body.status
    batch.error_message = body.error_message
    if batch.source_item_id is not None:
        source_item = db.get(IngestionSourceItem, batch.source_item_id)
        if source_item is not None:
            metadata_json = dict(source_item.metadata_json)
            source_item.metadata_json = metadata_json
            # Advance pipeline status based on embedding outcome.
            if body.status == "completed":
                source_item.processing_status = "completed"
                source_item.metadata_json = set_pipeline_checkpoint(source_item.metadata_json, PIPELINE_STAGE_EMBEDDING_SUCCESSFUL)
            elif body.status in {"failed", "skipped"}:
                source_item.processing_status = "embedding_failed"
                source_item.metadata_json = set_pipeline_checkpoint(source_item.metadata_json, PIPELINE_STAGE_FAILED_EMBEDDING)
            source_item.updated_at = datetime.utcnow()
    if body.status == "completed":
        batch.completed_at = datetime.utcnow()
    elif body.status in {"failed", "skipped"}:
        batch.retry_count += 1
    db.commit()
    return {"status": "ok", "batch_id": str(batch.id), "batch_status": batch.status}


@router.post("/alert/")
async def ingest_alert(
    body: WorkerAlertRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Persist or throttle a structured worker alert."""
    payload = WorkerAlertPayload.model_validate(body.model_dump())
    result = ingest_worker_alert(db, payload)
    db.commit()
    return {
        "status": "ok",
        "action": result.action,
        "alert_id": str(result.alert.id),
        "occurrence_count": result.alert.occurrence_count,
    }


@router.post("/alert/{alert_id}/status/")
async def set_alert_status(
    alert_id: UUID,
    body: WorkerAlertStatusRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Update worker alert lifecycle state from dashboard actions."""
    payload = WorkerAlertStatusUpdate.model_validate(body.model_dump())
    alert = update_worker_alert_status(
        db,
        alert_id,
        status=payload.status,
        resolved_by=payload.resolved_by,
    )
    db.commit()
    return {
        "status": "ok",
        "alert_id": str(alert.id),
        "alert_status": alert.status,
        "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
    }


# ---------------------------------------------------------------------------
# LLM lane callback models and endpoints
# ---------------------------------------------------------------------------

class LlmEnrichmentResultItemRequest(BaseModel):
    """One enrichment result for a single chunk, returned by the GPU worker."""

    chunk_id: UUID
    topic: str = ""
    topic_tags: list[str] = Field(default_factory=list)
    summary: str = ""
    teaching_type: str = "unknown"
    applicable_issues: list[str] = Field(default_factory=list)
    depth_level: str = "general"
    emotional_tone: str = "neutral"
    dharma_concepts: list[str] = Field(default_factory=list)
    parse_status: str = "ok"
    failure_reason: str | None = None


class LlmEnrichmentCallbackRequest(BaseModel):
    """Callback payload from GPU worker after a `run_llm_enrichment` batch."""

    batch_id: str
    attempt_id: str
    source_item_id: UUID
    job_type: str = "run_llm_enrichment"
    total_items: int = 0
    success_count: int = 0
    failure_count: int = 0
    items: list[LlmEnrichmentResultItemRequest] = Field(default_factory=list)
    model_name: str = ""
    model_version: str = ""
    mode: str = LLM_RUNTIME_MODE_STRICT


class LlmRefinementResultSegmentRequest(BaseModel):
    """One refined segment boundary result, returned by the GPU worker."""

    segment_index: int
    refined_content: str = ""
    merged_with_next: bool = False
    parse_status: str = "ok"
    failure_reason: str | None = None


class LlmBoundaryRefinementCallbackRequest(BaseModel):
    """Callback payload from GPU worker after a `run_llm_boundary_refinement` batch."""

    batch_id: str
    attempt_id: str
    source_item_id: UUID
    job_type: str = "run_llm_boundary_refinement"
    total_items: int = 0
    success_count: int = 0
    failure_count: int = 0
    segments: list[LlmRefinementResultSegmentRequest] = Field(default_factory=list)
    model_name: str = ""
    model_version: str = ""
    mode: str = LLM_RUNTIME_MODE_STRICT


@router.post("/llm-enrichment-callback/")
async def llm_enrichment_callback(
    body: LlmEnrichmentCallbackRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Persist LLM enrichment results from the GPU worker.

    Idempotency: same batch_id + attempt_id is safe to replay — no duplicate
    side effects will occur. Duplicate callbacks return status='duplicate'.
    Late callbacks (same batch_id, older attempt_id) are silently ignored.
    """
    item = db.get(IngestionSourceItem, body.source_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")

    # --- Idempotency check ---
    callback_status = classify_llm_callback(
        item.metadata_json,
        batch_id=body.batch_id,
        attempt_id=body.attempt_id,
    )
    if callback_status == LlmCallbackStatus.DUPLICATE:
        return {
            "status": "duplicate",
            "batch_id": body.batch_id,
            "attempt_id": body.attempt_id,
            "message": "callback already processed",
        }
    if callback_status == LlmCallbackStatus.LATE:
        return {
            "status": "late",
            "batch_id": body.batch_id,
            "attempt_id": body.attempt_id,
            "message": "older attempt superseded by newer",
        }

    # --- Persist enrichment results per chunk ---
    for result in body.items:
        chunk = db.get(ContentChunk, result.chunk_id)
        if chunk is None:
            continue
        _apply_llm_enrichment_debug(
            chunk,
            parse_status=result.parse_status,
            failure_reason=result.failure_reason,
            batch_id=body.batch_id,
            attempt_id=body.attempt_id,
            model_name=body.model_name,
            model_version=body.model_version,
            mode=body.mode,
        )
        if result.parse_status == "ok":
            chunk.topic = result.topic or chunk.topic
            chunk.topic_tags = result.topic_tags or chunk.topic_tags
            chunk.summary = result.summary or chunk.summary
            chunk.teaching_type = result.teaching_type or chunk.teaching_type
            chunk.applicable_issues = result.applicable_issues or chunk.applicable_issues
            chunk.depth_level = result.depth_level or chunk.depth_level
            chunk.emotional_tone = result.emotional_tone or chunk.emotional_tone

    # --- Build gate summary with per-failure-reason taxonomy ---
    parse_failure_count = sum(
        1 for r in body.items if r.parse_status == LLM_GATE_REASON_PARSE_FAILURE
    )
    schema_invalid_count = sum(
        1 for r in body.items if r.parse_status == LLM_GATE_REASON_SCHEMA_INVALID
    )
    fallback_count = sum(
        1 for r in body.items if r.parse_status == LLM_GATE_REASON_FALLBACK
    )
    gate_summary = build_llm_gate_summary(
        parse_failure_count=parse_failure_count,
        schema_invalid_count=schema_invalid_count,
        fallback_count=fallback_count,
        total_items=body.total_items,
        mode=body.mode,
    )

    # --- Determine batch outcome ---
    outcome = classify_llm_batch_outcome(
        success_count=body.success_count,
        total_items=body.total_items,
        mode=body.mode,
    )
    queue_row = _latest_llm_queue_row(
        db,
        source_item_id=body.source_item_id,
        job_type=body.job_type,
    )

    # --- Update metadata_json ---
    metadata_json = dict(item.metadata_json)
    metadata_json = record_llm_callback_seen(metadata_json, body.batch_id, body.attempt_id)
    metadata_json["llm_enrichment_model"] = body.model_name
    metadata_json["llm_enrichment_model_version"] = body.model_version

    # --- Enqueue embedding only when enrichment outcome is not FAILED ---
    embedding_batch_id: str | None = None
    if outcome != LlmBatchOutcome.FAILED and item.source_document_id is not None:
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL)
        metadata_json = merge_gate_summary(
            metadata_json,
            PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
            gate_summary,
        )
        item.metadata_json = metadata_json
        db.flush()
        source_document = db.get(SourceDocument, item.source_document_id)
        if source_document is not None:
            chunk_stmt = select(ContentChunk).where(
                ContentChunk.source_id == item.source_document_id,
                ContentChunk.duplicate_of.is_(None),
            )
            enriched_chunks = list(db.execute(chunk_stmt).scalars())
            batch = enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=item,
                chunks=enriched_chunks,
            )
            if batch is not None:
                embedding_batch_id = str(batch.id)
        item.processing_status = "awaiting_embedding"
        item.updated_at = datetime.utcnow()
        if queue_row is not None:
            queue_row.status = "completed"
            queue_row.completed_at = datetime.utcnow()
            queue_row.error_message = None
    else:
        # FAILED outcome — no embedding enqueued.
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_ENRICHMENT)
        metadata_json = merge_gate_summary(
            metadata_json,
            PIPELINE_STAGE_FAILED_ENRICHMENT,
            gate_summary,
        )
        item.metadata_json = metadata_json
        item.processing_status = "llm_enrichment_failed"
        item.updated_at = datetime.utcnow()
        if queue_row is not None:
            queue_row.status = "failed"
            queue_row.retry_count += 1
            queue_row.error_message = f"llm_enrichment_failed:{outcome.value}"

    db.commit()
    return {
        "status": "ok",
        "batch_id": body.batch_id,
        "attempt_id": body.attempt_id,
        "outcome": outcome.value,
        "gate_summary": gate_summary,
        "embedding_batch_id": embedding_batch_id,
    }


@router.post("/llm-refinement-callback/")
async def llm_refinement_callback(
    body: LlmBoundaryRefinementCallbackRequest,
    _: None = Depends(verify_internal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Persist LLM boundary refinement results from the GPU worker.

    Idempotency: same batch_id + attempt_id is safe to replay without
    duplicate side effects. Duplicate callbacks return status='duplicate'.
    """
    item = db.get(IngestionSourceItem, body.source_item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="source_item_not_found")

    # --- Idempotency check ---
    callback_status = classify_llm_callback(
        item.metadata_json,
        batch_id=body.batch_id,
        attempt_id=body.attempt_id,
    )
    if callback_status == LlmCallbackStatus.DUPLICATE:
        return {
            "status": "duplicate",
            "batch_id": body.batch_id,
            "attempt_id": body.attempt_id,
            "message": "callback already processed",
        }
    if callback_status == LlmCallbackStatus.LATE:
        return {
            "status": "late",
            "batch_id": body.batch_id,
            "attempt_id": body.attempt_id,
            "message": "older attempt superseded by newer",
        }

    # --- Build gate summary ---
    parse_failure_count = sum(
        1 for s in body.segments if s.parse_status == LLM_GATE_REASON_PARSE_FAILURE
    )
    schema_invalid_count = sum(
        1 for s in body.segments if s.parse_status == LLM_GATE_REASON_SCHEMA_INVALID
    )
    fallback_count = sum(
        1 for s in body.segments if s.parse_status == LLM_GATE_REASON_FALLBACK
    )
    gate_summary = build_llm_gate_summary(
        parse_failure_count=parse_failure_count,
        schema_invalid_count=schema_invalid_count,
        fallback_count=fallback_count,
        total_items=body.total_items,
        mode=body.mode,
    )

    # Boundary refinement processes adjacent pairs (N-1 for N chunks). The GPU
    # worker sends total_items=N (chunk count) but success_count counts pairs.
    # Normalise to pairs so classify_llm_batch_outcome compares like-for-like.
    refinement_pair_count = max(0, body.total_items - 1) if body.total_items > 0 else 0
    outcome = classify_llm_batch_outcome(
        success_count=body.success_count,
        total_items=refinement_pair_count,
        mode=body.mode,
    )
    queue_row = _latest_llm_queue_row(
        db,
        source_item_id=body.source_item_id,
        job_type=body.job_type,
    )

    # --- Apply refined segment decisions to existing ContentChunk records ---
    if item.source_document_id is not None and body.segments:
        chunk_stmt = (
            select(ContentChunk)
            .where(ContentChunk.source_id == item.source_document_id)
            .order_by(ContentChunk.chunk_index.asc())
        )
        candidate_chunks = list(db.execute(chunk_stmt).scalars())
        chunk_by_index = {c.chunk_index: c for c in candidate_chunks}

        for seg in body.segments:
            chunk = chunk_by_index.get(seg.segment_index)
            if chunk is None:
                continue
            if seg.refined_content:
                chunk.content = seg.refined_content
            if seg.merged_with_next:
                next_chunk = chunk_by_index.get(seg.segment_index + 1)
                if next_chunk is not None and next_chunk.duplicate_of is None:
                    # Merge next chunk's content into current and mark it as duplicate.
                    chunk.content = (chunk.content + " " + next_chunk.content).strip()
                    next_chunk.duplicate_of = chunk.id

    # --- Persist refined segment boundaries into metadata ---
    refined_segments = [
        {
            "segment_index": seg.segment_index,
            "refined_content": seg.refined_content,
            "merged_with_next": seg.merged_with_next,
            "parse_status": seg.parse_status,
        }
        for seg in body.segments
    ]
    metadata_json = dict(item.metadata_json)
    metadata_json = record_llm_callback_seen(metadata_json, body.batch_id, body.attempt_id)
    metadata_json["llm_refinement_model"] = body.model_name
    metadata_json["llm_refinement_model_version"] = body.model_version
    metadata_json["llm_refined_segments"] = refined_segments

    # --- Dispatch enrichment if refinement outcome is not FAILED ---
    if outcome != LlmBatchOutcome.FAILED:
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_REFINEMENT_SUCCESSFUL)
        metadata_json = merge_gate_summary(
            metadata_json,
            PIPELINE_STAGE_REFINEMENT_SUCCESSFUL,
            gate_summary,
        )
        item.metadata_json = metadata_json
        db.flush()
        enrichment_queue = enqueue_llm_enrichment_batch(db, source_item=item)
        metadata_json = dict(item.metadata_json)
        metadata_json["refinement_triggered_enrichment_queue_id"] = str(enrichment_queue.id)
        item.metadata_json = metadata_json
        item.processing_status = "awaiting_llm_enrichment"
        item.updated_at = datetime.utcnow()
        if queue_row is not None:
            queue_row.status = "completed"
            queue_row.completed_at = datetime.utcnow()
            queue_row.error_message = None
        db.flush()
    else:
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_REFINEMENT)
        metadata_json = merge_gate_summary(
            metadata_json,
            PIPELINE_STAGE_FAILED_REFINEMENT,
            gate_summary,
        )
        item.metadata_json = metadata_json
        if queue_row is not None:
            queue_row.status = "failed"
            queue_row.retry_count += 1
            queue_row.error_message = f"llm_refinement_failed:{outcome.value}"
        item.processing_status = "llm_refinement_failed"
        item.updated_at = datetime.utcnow()

    db.commit()
    return {
        "status": "ok",
        "batch_id": body.batch_id,
        "attempt_id": body.attempt_id,
        "outcome": outcome.value,
        "gate_summary": gate_summary,
        "enrichment_dispatched": outcome != LlmBatchOutcome.FAILED,
    }
