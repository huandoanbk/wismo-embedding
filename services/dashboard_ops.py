"""Shared helpers for dashboard pages and CLI operations."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from typing import Any
from uuid import UUID

import boto3
from botocore.exceptions import ClientError
from fastapi import UploadFile
from sqlalchemy import delete as sql_delete, func, select
from sqlalchemy.orm import Session

from connectors import ManualUploadConnector
from core.content_profiles import (
    CONTENT_PROFILE_KEY,
    normalize_content_profile,
    with_declared_content_profile,
)
from core.ingestion_models import EmbeddingBatch, IngestionQueue, IngestionSourceConfig, IngestionSourceItem
from core.ingestion_queue_service import IngestionQueueService
from core.ingestion_registry import IngestionRegistryService
from core.config import settings
from core.models import ChunkEmbedding, ContentChunk, SourceDocument, WorkerAlert, WorkerImage
from processors.runtime_artifacts import (
    set_pipeline_checkpoint,
)
from services.pipeline_scheduler_ops import STAGE_SCAN, run_locked_scheduler_stage
from services.vastai_manager import VastAIManager
from services.worker_images import (
    activate_worker_image,
    create_worker_image,
    get_worker_image_lane_status,
    list_worker_images,
    validate_worker_image,
)

LLM_IMAGE_LANES = ("llm_refinement", "llm_enrichment")


def get_overview_metrics(
    db: Session,
    *,
    vastai_manager_cls: type[VastAIManager] = VastAIManager,
) -> dict[str, Any]:
    """Return top-level embedding control plane metrics."""
    pending_queue = db.scalar(select(func.count()).select_from(IngestionQueue).where(IngestionQueue.status == "pending")) or 0
    failed_queue = db.scalar(select(func.count()).select_from(IngestionQueue).where(IngestionQueue.status == "failed")) or 0
    pending_embedding_batches = db.scalar(
        select(func.count()).select_from(EmbeddingBatch).where(EmbeddingBatch.status == "pending")
    ) or 0
    failed_embedding_batches = db.scalar(
        select(func.count()).select_from(EmbeddingBatch).where(EmbeddingBatch.status == "failed")
    ) or 0
    unresolved_alerts = db.scalar(
        select(func.count()).select_from(WorkerAlert).where(WorkerAlert.status == "unresolved")
    ) or 0
    try:
        running_instances = len(vastai_manager_cls().get_running_instances())
    except Exception:
        running_instances = 0

    return {
        "source_items": db.scalar(select(func.count()).select_from(IngestionSourceItem)) or 0,
        "source_configs": db.scalar(select(func.count()).select_from(IngestionSourceConfig)) or 0,
        "source_documents": db.scalar(select(func.count()).select_from(SourceDocument)) or 0,
        "active_sources": db.scalar(
            select(func.count()).select_from(SourceDocument).where(SourceDocument.is_active.is_(True))
        ) or 0,
        "chunks": db.scalar(select(func.count()).select_from(ContentChunk)) or 0,
        "embeddings": db.scalar(select(func.count()).select_from(ChunkEmbedding)) or 0,
        "queue_pending": pending_queue + pending_embedding_batches,
        "queue_failed": failed_queue + failed_embedding_batches,
        "embedding_batches_pending": pending_embedding_batches,
        "embedding_batches_failed": failed_embedding_batches,
        "worker_alerts_unresolved": unresolved_alerts,
        "vastai_running_instances": running_instances,
    }


def list_source_rows(
    db: Session,
    *,
    processing_status: str | None = None,
    search: str | None = None,
    limit: int | None = 50,
) -> list[dict[str, Any]]:
    """Return registry/source-document rows for the Sources page.

    Args:
        processing_status: SQL-level filter on processing_status.
        search: case-insensitive substring match on display_name or source_path.
            When provided, limit is ignored (returns all matches).
        limit: max rows returned ordered by most recently updated. Defaults to 50.
            Pass None to return all rows.
    """
    stmt = (
        select(IngestionSourceItem, SourceDocument)
        .outerjoin(SourceDocument, SourceDocument.registry_item_id == IngestionSourceItem.id)
        .order_by(IngestionSourceItem.updated_at.desc(), IngestionSourceItem.first_seen_at.desc())
    )
    if processing_status:
        stmt = stmt.where(IngestionSourceItem.processing_status == processing_status)
    if search:
        pattern = f"%{search}%"
        stmt = stmt.where(
            IngestionSourceItem.display_name.ilike(pattern)
            | IngestionSourceItem.source_path.ilike(pattern)
        )
    else:
        if limit is not None:
            stmt = stmt.limit(limit)
    rows = db.execute(stmt).all()
    payload: list[dict[str, Any]] = []
    for item, document in rows:
        payload.append(
            {
                "source_item_id": str(item.id),
                "source_document_id": str(document.id) if document else None,
                "display_name": item.display_name,
                "source_type": item.source_type,
                "source_path": item.source_path,
                "discovery_status": item.discovery_status,
                "processing_status": item.processing_status,
                "raw_storage_status": item.raw_storage_status,
                "content_profile": item.metadata_json.get(CONTENT_PROFILE_KEY),
                "pipeline_review_markers": list(item.metadata_json.get("pipeline_review_markers") or []),
                "pipeline_checkpoint_stage": item.metadata_json.get("pipeline_checkpoint_stage"),
                "pipeline_provenance": dict(item.metadata_json.get("pipeline_provenance") or {}),
                "pipeline_gate_summary": dict(item.metadata_json.get("pipeline_gate_summary") or {}),
                "runtime_enabled": document.is_active if document else None,
                "title": document.title if document else None,
                "total_chunks": document.total_chunks if document else 0,
                "updated_at": item.updated_at.isoformat() if item.updated_at else None,
            }
        )
    return payload


def set_source_enabled(db: Session, source_item_id: UUID | str, enabled: bool) -> SourceDocument:
    """Toggle source runtime enablement on the linked source document."""
    document = _require_source_document(db, source_item_id)
    document.is_active = enabled
    document.updated_at = datetime.utcnow()
    db.flush()
    return document


def reprocess_source_item(
    db: Session,
    source_item_id: UUID | str,
    *,
    content_profile: str | None = None,
) -> dict[str, Any]:
    """Reset a source item to initial state and re-enqueue from scratch.

    Deletes all derived data: pending queue rows, embedding batches, content
    chunks (cascades to chunk embeddings), and the source document. Optionally
    applies a content profile override before re-enqueueing.
    """
    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")

    # Apply profile change if requested.
    if content_profile is not None:
        meta = dict(item.metadata_json)
        meta[CONTENT_PROFILE_KEY] = normalize_content_profile(content_profile)
        item.metadata_json = meta

    previous_checkpoint_stage = (item.metadata_json or {}).get("pipeline_checkpoint_stage")

    _purge_source_derived_data(db, item)

    # Strip pipeline-generated metadata so the item starts clean.
    item.metadata_json = _strip_pipeline_metadata(item.metadata_json)
    item.discovery_status = "discovered"
    item.processing_status = "new"
    item.source_document_id = None
    item.last_processed_at = None
    item.retry_count = 0
    item.last_error_code = None
    item.last_error_message = None
    item.updated_at = datetime.utcnow()
    db.flush()

    # Enqueue for fresh processing — sets awaiting_chunking or awaiting_transcript.
    queue_service = IngestionQueueService(db)
    decision = queue_service.enqueue_if_needed(item)
    if previous_checkpoint_stage:
        metadata_json = dict(item.metadata_json or {})
        metadata_json["pipeline_checkpoint_stage"] = previous_checkpoint_stage
        item.metadata_json = metadata_json
    db.flush()

    return {
        "source_item_id": str(item.id),
        "processing_status": item.processing_status,
        "queued": decision.queued,
        "queue_id": str(decision.queue_item.id) if decision.queue_item else None,
    }


def retry_source_item(db: Session, source_item_id: UUID | str) -> dict[str, Any]:
    """Retry the most recent failed pipeline step without resetting earlier work.

    Supports the new end-to-end state machine by recreating the concrete queue
    row or embedding batch for the failed stage.

    Raises ``ValueError`` for any status that is not a failed state.
    """
    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")

    status = item.processing_status

    if status in {"transcript_failed", "extract_artifact_failed", "chunking_failed", "llm_refinement_failed", "llm_enrichment_failed"}:
        from core.llm_lane import JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT, JOB_TYPE_RUN_LLM_ENRICHMENT
        from tasks import (
            JOB_TYPE_EXTRACT_TEXT_ARTIFACT,
            JOB_TYPE_PROCESS_CANONICAL_EXTRACT,
            JOB_TYPE_TRANSCRIBE_AUDIO,
            enqueue_llm_enrichment_batch,
            enqueue_llm_refinement_batch,
        )

        def _pending_queue(job_type: str, priority: int = 5) -> IngestionQueue:
            existing = db.execute(
                select(IngestionQueue).where(
                    IngestionQueue.source_item_id == item.id,
                    IngestionQueue.job_type == job_type,
                    IngestionQueue.status.in_(["pending", "processing"]),
                )
            ).scalars().first()
            if existing is not None:
                return existing
            row = IngestionQueue(source_item_id=item.id, job_type=job_type, status="pending", priority=priority)
            db.add(row)
            db.flush()
            return row

        metadata_json = dict(item.metadata_json or {})

        if status == "transcript_failed":
            queue_item = _pending_queue(JOB_TYPE_TRANSCRIBE_AUDIO)
            item.processing_status = "awaiting_transcript"
            item.metadata_json = metadata_json
            item.updated_at = datetime.utcnow()
            db.flush()
            return {
                "source_item_id": str(item.id),
                "action": "retry_transcript",
                "processing_status": item.processing_status,
                "queue_id": str(queue_item.id),
            }

        if status == "extract_artifact_failed":
            queue_item = _pending_queue(JOB_TYPE_EXTRACT_TEXT_ARTIFACT)
            item.processing_status = "awaiting_extract_artifact"
            item.metadata_json = metadata_json
            item.updated_at = datetime.utcnow()
            db.flush()
            return {
                "source_item_id": str(item.id),
                "action": "retry_extract_artifact",
                "processing_status": item.processing_status,
                "queue_id": str(queue_item.id),
            }

        if status == "chunking_failed":
            pending_extract_key = metadata_json.get("pending_extract_key")
            if not pending_extract_key:
                raise ValueError("missing_pending_extract_key_for_retry")
            queue_item = _pending_queue(JOB_TYPE_PROCESS_CANONICAL_EXTRACT)
            item.processing_status = "awaiting_chunking"
            item.metadata_json = metadata_json
            item.updated_at = datetime.utcnow()
            db.flush()
            return {
                "source_item_id": str(item.id),
                "action": "retry_chunking",
                "processing_status": item.processing_status,
                "queue_id": str(queue_item.id),
            }

        latest_llm_row = db.execute(
            select(IngestionQueue)
            .where(
                IngestionQueue.source_item_id == item.id,
                IngestionQueue.job_type.in_(
                    [JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT, JOB_TYPE_RUN_LLM_ENRICHMENT]
                ),
            )
            .order_by(IngestionQueue.created_at.desc())
        ).scalars().first()

        retry_refinement = status == "llm_refinement_failed"
        if not retry_refinement and latest_llm_row is not None:
            retry_refinement = latest_llm_row.job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT

        if retry_refinement:
            # Clear the previous refinement callback record so the new attempt
            # is not rejected as LATE by the idempotency check in the callback
            # handler (batch_id is fixed as "refine-{source_item_id}").
            from core.llm_lane import LLM_CALLBACK_SEEN_KEY
            seen = dict(metadata_json.get(LLM_CALLBACK_SEEN_KEY) or {})
            seen.pop(f"refine-{source_item_id}", None)
            metadata_json[LLM_CALLBACK_SEEN_KEY] = seen
            queue_item = enqueue_llm_refinement_batch(db, source_item=item)
            item.processing_status = "awaiting_llm_refinement"
            action = "retry_llm_refinement"
        else:
            # Clear the previous enrichment callback record for the same reason.
            from core.llm_lane import LLM_CALLBACK_SEEN_KEY
            seen = dict(metadata_json.get(LLM_CALLBACK_SEEN_KEY) or {})
            seen.pop(f"enrich-{source_item_id}", None)
            metadata_json[LLM_CALLBACK_SEEN_KEY] = seen
            queue_item = enqueue_llm_enrichment_batch(db, source_item=item)
            item.processing_status = "awaiting_llm_enrichment"
            action = "retry_llm_enrichment"
        item.metadata_json = metadata_json
        item.updated_at = datetime.utcnow()
        db.flush()
        return {
            "source_item_id": str(item.id),
            "action": action,
            "processing_status": item.processing_status,
            "queue_id": str(queue_item.id),
        }

    if status in {"embedding_failed", "failed"}:
        if item.source_document_id is None:
            raise ValueError("no_source_document_for_retry")
        missing_ids = _chunk_ids_missing_embedding(db, item.source_document_id)
        if not missing_ids:
            raise ValueError("no_missing_embeddings_for_retry")
        # Remove any prior failed batches to avoid stale batch_key conflicts.
        db.execute(
            sql_delete(EmbeddingBatch).where(
                EmbeddingBatch.source_item_id == item.id,
                EmbeddingBatch.status == "failed",
            )
        )
        batch = _create_embedding_batch(db, item, missing_ids)
        item.processing_status = "awaiting_embedding"
        item.updated_at = datetime.utcnow()
        db.flush()
        return {
            "source_item_id": str(item.id),
            "action": "retry_embedding",
            "processing_status": item.processing_status,
            "batch_id": str(batch.id),
            "missing_chunk_count": len(missing_ids),
        }

    raise ValueError(f"retry_not_applicable_for_status:{status}")


def re_embed_source_item(db: Session, source_item_id: UUID | str) -> dict[str, Any]:
    """Delete all existing embeddings for a source and re-embed all chunks from scratch.

    Removes all ``ChunkEmbedding`` rows and ``EmbeddingBatch`` rows for the
    source, then creates a fresh pending batch covering every chunk.
    """
    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")
    if item.source_document_id is None:
        raise ValueError("no_source_document_for_re_embed")

    # Delete all embedding batches for this source item.
    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_item_id == item.id))
    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_document_id == item.source_document_id))

    # Delete all chunk embeddings — bulk SQL; DB cascade would handle chunk deletion
    # but we only want to drop embeddings, not the chunks themselves.
    chunk_subq = select(ContentChunk.id).where(ContentChunk.source_id == item.source_document_id).scalar_subquery()
    db.execute(sql_delete(ChunkEmbedding).where(ChunkEmbedding.chunk_id.in_(chunk_subq)))
    db.flush()

    all_chunk_ids = list(
        db.execute(select(ContentChunk.id).where(ContentChunk.source_id == item.source_document_id)).scalars()
    )
    if not all_chunk_ids:
        raise ValueError("no_chunks_for_re_embed")

    batch = _create_embedding_batch(db, item, [str(cid) for cid in all_chunk_ids])
    item.processing_status = "awaiting_embedding"
    item.updated_at = datetime.utcnow()
    db.flush()

    return {
        "source_item_id": str(item.id),
        "processing_status": item.processing_status,
        "batch_id": str(batch.id),
        "chunk_count": len(all_chunk_ids),
    }


def rechunk_source_item(db: Session, source_item_id: UUID | str) -> dict[str, Any]:
    """Restart one source from canonical extract into the chunking stage.

    Keeps the existing canonical extract artifact and resets downstream runtime
    state so the pipeline resumes from ``awaiting_chunking``.
    """
    from processors import PIPELINE_STAGE_ARTIFACT_EXTRACTED, PIPELINE_STAGE_TRANSCRIPTED
    from tasks import enqueue_canonical_extract_followup

    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")

    source_document = (
        db.get(SourceDocument, item.source_document_id)
        if item.source_document_id is not None
        else None
    )
    extract_key = (
        (item.metadata_json or {}).get("pending_extract_key")
        or (source_document.s3_text_key if source_document is not None else None)
    )
    if not extract_key:
        raise ValueError("missing_extract_artifact_for_rechunk")

    db.execute(sql_delete(IngestionQueue).where(IngestionQueue.source_item_id == item.id))
    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_item_id == item.id))
    if source_document is not None:
        db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_document_id == source_document.id))
        db.execute(sql_delete(ContentChunk).where(ContentChunk.source_id == source_document.id))

    metadata_json = dict(item.metadata_json or {})
    checkpoint_stage = metadata_json.get("pipeline_checkpoint_stage") or (
        PIPELINE_STAGE_TRANSCRIPTED
        if (metadata_json.get("content_profile") == "transcript" or str(item.mime_type or "").startswith("audio/") or str(item.mime_type or "").startswith("video/"))
        else PIPELINE_STAGE_ARTIFACT_EXTRACTED
    )
    metadata_json = _strip_pipeline_metadata(metadata_json)
    item.metadata_json = metadata_json
    item.last_processed_at = None
    item.last_error_code = None
    item.last_error_message = None
    item.updated_at = datetime.utcnow()
    db.flush()

    queue_item = enqueue_canonical_extract_followup(
        db,
        item.id,
        extract_key=extract_key,
        checkpoint_stage=checkpoint_stage,
    )
    item.retry_count = 0
    db.flush()

    return {
        "source_item_id": str(item.id),
        "processing_status": item.processing_status,
        "queue_id": str(queue_item.id),
        "extract_key": extract_key,
    }


def delete_source_item(
    db: Session,
    source_item_id: UUID | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Permanently remove a source item and every derived artifact from the system.

    Deletion order:
    1. Delete canonical text extract artifact when ``s3_text_key`` exists.
    2. EmbeddingBatch rows (no DB cascade).
    3. SourceDocument → cascades ContentChunk → cascades ChunkEmbedding.
    4. IngestionSourceItem → cascades IngestionQueue rows.

    Raw source objects are intentionally preserved. Delete only cleans up
    derived corpus state plus the text extract artifact in
    ``settings.s3_bucket_extracts``.
    """
    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")

    source_document = db.get(SourceDocument, item.source_document_id) if item.source_document_id is not None else None
    s3_text_key = source_document.s3_text_key if source_document is not None else None
    if s3_text_key:
        client = s3_client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
        )
        try:
            client.delete_object(Bucket=settings.s3_bucket_extracts, Key=s3_text_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise

    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_item_id == item.id))
    if item.source_document_id is not None:
        db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_document_id == item.source_document_id))
        db.execute(sql_delete(SourceDocument).where(SourceDocument.id == item.source_document_id))
    db.execute(sql_delete(IngestionSourceItem).where(IngestionSourceItem.id == item.id))
    db.flush()

    return {"source_item_id": str(source_item_id), "deleted": True}


def batch_action_by_status(
    db: Session,
    *,
    status: str,
    action: str,
    content_profile: str | None = None,
) -> dict[str, Any]:
    """Apply one source action to every item that currently has the given processing_status.

    Supported actions: ``reprocess``, ``rechunk``, ``retry``, ``enable``, ``disable``, ``delete``.
    ``content_profile`` is only used when action is ``reprocess``.

    Returns a summary with per-item success/error breakdown.
    """
    retryable_statuses = {
        "transcript_failed",
        "extract_artifact_failed",
        "chunking_failed",
        "llm_refinement_failed",
        "llm_enrichment_failed",
        "embedding_failed",
        "failed",
    }
    if action == "retry" and status not in retryable_statuses:
        raise ValueError(f"retry_not_applicable_for_status:{status}")

    items = list(
        db.execute(
            select(IngestionSourceItem).where(IngestionSourceItem.processing_status == status)
        ).scalars()
    )

    results: dict[str, Any] = {"total": len(items), "success": 0, "errors": []}

    for item in items:
        try:
            if action == "reprocess":
                reprocess_source_item(db, item.id, content_profile=content_profile)
            elif action == "rechunk":
                rechunk_source_item(db, item.id)
            elif action == "retry":
                retry_source_item(db, item.id)
            elif action == "enable":
                set_source_enabled(db, item.id, True)
            elif action == "disable":
                set_source_enabled(db, item.id, False)
            elif action == "delete":
                delete_source_item(db, item.id)
            else:
                raise ValueError(f"unknown_action:{action}")
            results["success"] += 1
        except Exception as exc:  # noqa: BLE001
            results["errors"].append({"source_item_id": str(item.id), "error": str(exc)})

    return results


def list_source_config_rows(db: Session) -> list[dict[str, Any]]:
    """Return source configuration rows for admin ops."""
    stmt = select(IngestionSourceConfig).order_by(IngestionSourceConfig.updated_at.desc(), IngestionSourceConfig.display_name.asc())
    configs = list(db.execute(stmt).scalars())
    return [
        {
            "id": str(config.id),
            "source_type": config.source_type,
            "display_name": config.display_name,
            "is_enabled": config.is_enabled,
            "last_scan_status": config.last_scan_status,
            "last_scan_at": config.last_scan_at.isoformat() if config.last_scan_at else None,
            "content_profile": config.config_json.get(CONTENT_PROFILE_KEY),
            "config_json": dict(config.config_json),
        }
        for config in configs
    ]


def list_worker_image_rows(db: Session) -> list[dict[str, Any]]:
    """Return worker image rows with lane launch readiness context."""
    lane_status = {
        "whisper": get_worker_image_lane_status(db, "whisper"),
        "embedding": get_worker_image_lane_status(db, "embedding"),
        "llm": get_worker_image_lane_status(db, "llm"),
    }
    return [
        {
            "id": str(record.id),
            "lane": record.lane,
            "image_ref": record.image_ref,
            "display_name": record.display_name,
            "notes": record.notes,
            "is_active": record.is_active,
            "validation_status": record.validation_status,
            "last_validated_at": record.last_validated_at.isoformat() if record.last_validated_at else None,
            "validation_error": record.validation_error,
            "activated_at": record.activated_at.isoformat() if record.activated_at else None,
            "launch_ready": (
                lane_status.get(record.lane, lane_status.get("llm", {})).get("launch_ready", False)
                if record.is_active
                else False
            ),
        }
        for record in list_worker_images(db)
    ]


def create_worker_image_record(
    db: Session,
    *,
    lane: str,
    image_ref: str,
    display_name: str | None = None,
    notes: str | None = None,
) -> WorkerImage:
    """Persist one new worker image record."""
    record = create_worker_image(db, lane=lane, image_ref=image_ref, display_name=display_name, notes=notes)
    db.flush()
    return record


def validate_worker_image_record(
    db: Session,
    worker_image_id: UUID | str,
    *,
    client: Any | None = None,
) -> WorkerImage:
    """Validate one stored worker image record against its registry manifest."""
    return validate_worker_image(db, worker_image_id, client=client)


def activate_worker_image_record(db: Session, worker_image_id: UUID | str) -> WorkerImage:
    """Activate one already validated worker image record."""
    return activate_worker_image(db, worker_image_id)


def create_source_config(
    db: Session,
    *,
    source_type: str,
    display_name: str,
    config_json: dict[str, Any],
    content_profile: str,
    schedule_cron: str | None = None,
) -> IngestionSourceConfig:
    """Persist a new source config for the shared scan scheduler."""
    normalized_config = with_declared_content_profile(config_json, content_profile)
    config = IngestionSourceConfig(
        source_type=source_type,
        display_name=display_name,
        config_json=normalized_config,
        # Legacy column retained in schema, but source-level cron no longer drives automation.
        schedule_cron=None,
        is_enabled=True,
    )
    db.add(config)
    db.flush()
    return config


def update_source_config(
    db: Session,
    config_id: UUID | str,
    *,
    display_name: str,
    config_json: dict[str, Any],
    content_profile: str,
    schedule_cron: str | None = None,
    is_enabled: bool,
) -> IngestionSourceConfig:
    """Update an existing source config without changing its connector type."""
    config = db.get(IngestionSourceConfig, UUID(str(config_id)))
    if config is None:
        raise ValueError("source_config_not_found")

    config.display_name = display_name
    config.config_json = with_declared_content_profile(config_json, content_profile)
    config.schedule_cron = None
    config.is_enabled = is_enabled
    config.updated_at = datetime.utcnow()
    db.flush()
    return config


def set_source_config_enabled(db: Session, config_id: UUID | str, enabled: bool) -> IngestionSourceConfig:
    """Toggle one source config."""
    config = db.get(IngestionSourceConfig, UUID(str(config_id)))
    if config is None:
        raise ValueError("source_config_not_found")
    config.is_enabled = enabled
    config.updated_at = datetime.utcnow()
    db.flush()
    return config


def trigger_source_config_scan(db: Session, config_id: UUID | str) -> dict[str, Any]:
    """Run discovery for a single configured source."""
    from api.routes.scan import scan_source_config

    result = run_locked_scheduler_stage(
        db,
        STAGE_SCAN,
        lambda: asyncio.run(scan_source_config(UUID(str(config_id)), db)),
    )
    if result["status"] != "completed":
        return result
    payload = result.get("result")
    if isinstance(payload, dict):
        return {"status": "completed", **payload}
    return {"status": "completed", "result": payload}


def scan_all_enabled_source_configs(db: Session) -> list[dict[str, Any]]:
    """Run discovery for all enabled source configs."""
    from api.routes.scan import scan_source_config

    stmt = select(IngestionSourceConfig.id).where(IngestionSourceConfig.is_enabled.is_(True))
    config_ids = list(db.execute(stmt).scalars())
    result = run_locked_scheduler_stage(
        db,
        STAGE_SCAN,
        lambda: [asyncio.run(scan_source_config(UUID(str(config_id)), db)) for config_id in config_ids],
    )
    if result["status"] == "completed":
        payload = result.get("result")
        return payload if isinstance(payload, list) else []
    return [result]


def register_manual_upload_source(
    db: Session,
    *,
    upload: Any,
    raw_bucket: str | None = None,
    content_profile: str,
) -> list[dict[str, Any]]:
    """Normalize a manual upload through the connector and enqueue resulting items."""
    if not hasattr(upload, "file"):
        original_upload = upload
        upload = UploadFile(
            filename=getattr(original_upload, "name", None),
            file=BytesIO(original_upload.getvalue()),
        )
    connector = ManualUploadConnector()
    normalized_raw_bucket = (raw_bucket or "").strip() or "raw-manual-upload"
    if not connector.bucket_exists(normalized_raw_bucket):
        raise ValueError(f"raw_bucket_not_found:{normalized_raw_bucket}")
    registry = IngestionRegistryService(db)
    queue_service = IngestionQueueService(db)
    results: list[dict[str, Any]] = []
    for candidate in connector.discover_items(upload=upload, raw_bucket=normalized_raw_bucket):
        connector.mirror_candidate_to_raw_storage(candidate)
        candidate.metadata_json = with_declared_content_profile(
            connector.serializable_metadata(candidate),
            content_profile,
        )
        decision = registry.upsert_candidate(candidate)
        queue_decision = queue_service.enqueue_if_needed(decision.item)
        results.append(
            {
                "source_item_id": str(decision.item.id),
                "display_name": decision.item.display_name,
                "raw_bucket": normalized_raw_bucket,
                "content_profile": decision.item.metadata_json.get(CONTENT_PROFILE_KEY),
                "queued": queue_decision.queued,
                "processing_status": decision.item.processing_status,
                "skip_reason": decision.skip_reason or queue_decision.skip_reason,
            }
        )
    db.flush()
    return results



def list_queue_rows(
    db: Session,
    *,
    status: str | None = None,
    search: str | None = None,
    limit: int | None = 50,
) -> list[dict[str, Any]]:
    """Return queue rows with linked registry context.

    Args:
        status: SQL-level filter on queue status.
        search: case-insensitive substring match on source display_name or source_path.
            When provided, limit is ignored (returns all matches).
        limit: max rows returned ordered by most recently created. Defaults to 50.
            Pass None to return all rows.
    """
    ingest_stmt = (
        select(IngestionQueue, IngestionSourceItem)
        .join(IngestionSourceItem, IngestionSourceItem.id == IngestionQueue.source_item_id)
        .order_by(IngestionQueue.created_at.desc())
    )
    if status:
        ingest_stmt = ingest_stmt.where(IngestionQueue.status == status)
    if search:
        pattern = f"%{search}%"
        ingest_stmt = ingest_stmt.where(
            IngestionSourceItem.display_name.ilike(pattern)
            | IngestionSourceItem.source_path.ilike(pattern)
        )
    elif limit is not None:
        ingest_stmt = ingest_stmt.limit(limit)

    payload: list[dict[str, Any]] = []
    for queue_item, source_item in db.execute(ingest_stmt).all():
        checkpoint_stage = source_item.metadata_json.get("pipeline_checkpoint_stage")
        payload.append(
            {
                "queue_id": str(queue_item.id),
                "queue_kind": "ingestion",
                "source_item_id": str(source_item.id),
                "display_name": source_item.display_name,
                "job_type": queue_item.job_type,
                "status": queue_item.status,
                "priority": queue_item.priority,
                "retry_count": queue_item.retry_count,
                "error_message": queue_item.error_message,
                "processing_status": source_item.processing_status,
                "content_profile": source_item.metadata_json.get(CONTENT_PROFILE_KEY),
                "pipeline_checkpoint_stage": checkpoint_stage,
                "pipeline_review_markers": list(source_item.metadata_json.get("pipeline_review_markers") or []),
                "pipeline_artifact_pointers": dict(source_item.metadata_json.get("pipeline_artifact_pointers") or {}),
                "created_at": queue_item.created_at.isoformat() if queue_item.created_at else None,
            }
        )

    batch_stmt = select(EmbeddingBatch).order_by(EmbeddingBatch.created_at.desc())
    if status:
        batch_stmt = batch_stmt.where(EmbeddingBatch.status == status)
    if not search and limit is not None:
        batch_stmt = batch_stmt.limit(limit)

    for batch in db.execute(batch_stmt).scalars():
        source_item = db.get(IngestionSourceItem, batch.source_item_id) if batch.source_item_id else None
        source_document = db.get(SourceDocument, batch.source_document_id) if batch.source_document_id else None
        display_name = (
            source_item.display_name
            if source_item is not None
            else source_document.title
            if source_document is not None
            else None
        )
        if search:
            pattern = search.lower()
            source_path = source_item.source_path if source_item is not None else ""
            if pattern not in (display_name or "").lower() and pattern not in (source_path or "").lower():
                continue
        payload.append(
            {
                "queue_id": f"embedding:{batch.id}",
                "queue_kind": "embedding",
                "source_item_id": str(batch.source_item_id) if batch.source_item_id else None,
                "display_name": display_name,
                "job_type": "embed_chunk_batch",
                "status": batch.status,
                "priority": batch.priority,
                "retry_count": batch.retry_count,
                "error_message": batch.error_message,
                "processing_status": None,
                "content_profile": source_item.metadata_json.get(CONTENT_PROFILE_KEY) if source_item is not None else None,
                "pipeline_checkpoint_stage": source_item.metadata_json.get("pipeline_checkpoint_stage") if source_item is not None else None,
                "pipeline_review_markers": list(source_item.metadata_json.get("pipeline_review_markers") or []) if source_item is not None else [],
                "pipeline_artifact_pointers": dict(source_item.metadata_json.get("pipeline_artifact_pointers") or {}) if source_item is not None else {},
                "created_at": batch.created_at.isoformat() if batch.created_at else None,
            }
        )
    payload.sort(key=lambda row: row["created_at"] or "", reverse=True)
    if not search and limit is not None:
        payload = payload[:limit]
    return payload


def retry_queue_item(db: Session, queue_id: UUID | str) -> IngestionQueue | EmbeddingBatch:
    """Move a failed queue item back to pending state."""
    queue_id_str = str(queue_id)
    if queue_id_str.startswith("embedding:"):
        batch = db.get(EmbeddingBatch, UUID(queue_id_str.split(":", 1)[1]))
        if batch is None:
            raise ValueError("embedding_batch_not_found")
        batch.status = "pending"
        batch.error_message = None
        batch.available_at = datetime.utcnow()
        db.flush()
        return batch

    queue_item = db.get(IngestionQueue, UUID(str(queue_id)))
    if queue_item is None:
        raise ValueError("queue_item_not_found")
    queue_item.status = "pending"
    queue_item.error_message = None
    queue_item.available_at = datetime.utcnow()
    source_item = db.get(IngestionSourceItem, queue_item.source_item_id)
    if source_item is not None:
        if queue_item.job_type == "transcribe_audio":
            source_item.processing_status = "awaiting_transcript"
        elif queue_item.job_type == "extract_text_artifact":
            source_item.processing_status = "awaiting_extract_artifact"
        elif queue_item.job_type in {"process_canonical_extract", "process_transcript_extract"}:
            source_item.processing_status = "awaiting_chunking"
        elif queue_item.job_type == "run_llm_boundary_refinement":
            source_item.processing_status = "awaiting_llm_refinement"
        elif queue_item.job_type == "run_llm_enrichment":
            source_item.processing_status = "awaiting_llm_enrichment"
        source_item.updated_at = datetime.utcnow()
    db.flush()
    return queue_item


_DASHBOARD_HEALTH_TIMEOUT = 5.0  # shorter timeout for dashboard display


def list_vastai_rows(
    *,
    manager: VastAIManager | None = None,
) -> list[dict[str, Any]]:
    """Return Vast.ai instance rows with health status.

    Health checks are run in parallel so N unreachable instances cost
    max(timeout) instead of N × timeout.
    """
    instance_manager = manager or VastAIManager()
    lane_status = {
        "whisper": instance_manager.get_lane_launch_status("whisper"),
        "embedding": instance_manager.get_lane_launch_status("embedding"),
        "llm": instance_manager.get_lane_launch_status("llm"),
    }
    instances = instance_manager.get_running_instances()
    if not instances:
        return []

    def _inspect(inst):
        return inst, instance_manager.inspect_instance(inst, timeout=_DASHBOARD_HEALTH_TIMEOUT)

    inspections: dict = {}
    with ThreadPoolExecutor(max_workers=len(instances)) as pool:
        futures = {pool.submit(_inspect, inst): inst for inst in instances}
        for future in as_completed(futures):
            inst, result = future.result()
            inspections[inst.instance_id] = result

    rows: list[dict[str, Any]] = []
    for instance in instances:
        inspection = inspections.get(instance.instance_id, {})
        worker_mode = inspection.get("worker_mode")
        if worker_mode in LLM_IMAGE_LANES:
            worker_mode = "llm"
        rows.append(
            {
                "instance_id": instance.instance_id,
                "public_ipaddr": instance.public_ipaddr,
                "direct_port_start": instance.direct_port_start,
                "status": instance.actual_status,
                "worker_mode": worker_mode,
                "healthy": inspection.get("healthy", False),
                "image_ref": inspection.get("image_ref") or instance.image_ref,
                "future_launch_image_ref": lane_status.get(worker_mode, {}).get("active_image_ref"),
                "launch_ready": lane_status.get(worker_mode, {}).get("launch_ready"),
            }
        )
    return rows


def get_llm_lane_readiness_summary(
    *,
    manager: VastAIManager | None = None,
    lane: str = "llm",
    callback_url: str | None = None,
) -> dict[str, Any]:
    """Return full LLM lane readiness state for one LLM operation surface.

    Checks all four readiness criteria:
    1. image_validated_active: active worker image in DB registry.
    2. worker_health_pass: health endpoint returns healthy.
    3. model_usable: /model-ready endpoint returns usable.
    4. callback_path_usable: VPS callback URL is configured.

    Inputs:
        manager: optional VastAIManager; creates a default instance when absent.
        callback_url: optional override for the callback URL check.

    Output:
        dict with ``launch_ready``, per-criterion booleans, and ``details``.
    """
    instance_manager = manager or VastAIManager()
    running = instance_manager.get_running_instances()
    llm_instance = next(
        (i for i in running if i.worker_mode == "llm" or i.worker_mode in LLM_IMAGE_LANES),
        None,
    )
    readiness = instance_manager.get_llm_lane_readiness(llm_instance, lane=lane, callback_url=callback_url)
    return {
        "lane": lane,
        "launch_ready": readiness.launch_ready,
        "image_validated_active": readiness.image_validated_active,
        "worker_health_pass": readiness.worker_health_pass,
        "model_usable": readiness.model_usable,
        "callback_path_usable": readiness.callback_path_usable,
        "details": readiness.details,
    }


def launch_vastai_instance(
    *,
    mode: str = "both",
    manager: VastAIManager | None = None,
) -> str:
    """Launch the cheapest available Vast.ai worker instance."""
    instance_manager = manager or VastAIManager()
    offer = instance_manager.find_cheapest_offer(mode=mode)
    if offer is None:
        raise ValueError("no_suitable_offer")
    return instance_manager.launch_instance(str(offer["id"]), mode=mode)


def destroy_vastai_instance(instance_id: str, *, manager: VastAIManager | None = None) -> None:
    """Destroy one running Vast.ai instance."""
    (manager or VastAIManager()).destroy_instance(instance_id)


def process_text_queue(*, batch_size: int = 20) -> int:
    """Process pending canonical extract queue items (chunking stage)."""
    from tasks import process_ingestion_queue

    return process_ingestion_queue(batch_size=batch_size)


def dispatch_extract_artifact_jobs(*, batch_size: int = 20) -> int:
    """Process pending text-based artifact extraction jobs locally."""
    from tasks import process_text_extract_artifact_queue

    return process_text_extract_artifact_queue(batch_size=batch_size)


def process_embedding_queue(
    db: Session,
    *,
    batch_size: int = 20,
    celery_app: Any,
    manager: VastAIManager | None = None,
) -> dict[str, Any]:
    """Dispatch DB-backed embedding batches into the GPU embedding queue."""
    from tasks import dispatch_embedding_batches

    return dispatch_embedding_batches(batch_size=batch_size, db=db, celery_app=celery_app, manager=manager)


def dispatch_audio_jobs(
    db: Session,
    *,
    batch_size: int = 20,
    celery_app: Any,
    manager: VastAIManager | None = None,
) -> dict[str, Any]:
    """Dispatch pending media items to the GPU whisper queue.

    This stage is dispatch-only. Autoscale is controlled separately by the
    dedicated automation stage so manual dispatches do not implicitly launch or
    destroy instances.
    """
    from tasks import JOB_TYPE_TRANSCRIBE_AUDIO

    stmt = (
        select(IngestionQueue)
        .where(
            IngestionQueue.job_type == JOB_TYPE_TRANSCRIBE_AUDIO,
            IngestionQueue.status == "pending",
        )
        .order_by(IngestionQueue.priority.desc(), IngestionQueue.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    queue_rows = list(db.execute(stmt).scalars())
    if not queue_rows:
        fallback_items = list(
            db.execute(
                select(IngestionSourceItem)
                .where(
                    IngestionSourceItem.processing_status.in_(["awaiting_transcript", "reprocess_requested"]),
                    IngestionSourceItem.raw_storage_status == "stored",
                )
                .order_by(IngestionSourceItem.updated_at.asc())
                .limit(batch_size)
            ).scalars()
        )
        for item in fallback_items:
            queue_row = IngestionQueue(
                source_item_id=item.id,
                job_type=JOB_TYPE_TRANSCRIBE_AUDIO,
                status="pending",
            )
            db.add(queue_row)
            queue_rows.append(queue_row)
        db.flush()
    dispatched = 0
    skipped = 0
    dispatched_ids: list[str] = []
    for queue_row in queue_rows:
        item = db.execute(
            select(IngestionSourceItem)
            .where(IngestionSourceItem.id == queue_row.source_item_id)
            .with_for_update(skip_locked=True)
        ).scalar_one_or_none()
        if item is None:
            skipped += 1
            continue
        if item.raw_storage_status != "stored":
            continue
        if item.processing_status not in {"awaiting_transcript", "reprocess_requested"}:
            skipped += 1
            continue
        celery_app.send_task("gpu_worker.tasks.transcribe_audio", args=[str(item.id)], queue="whisper")
        queue_row.status = "processing"
        queue_row.started_at = datetime.utcnow()
        queue_row.error_message = None
        item.processing_status = "transcript_processing"
        item.updated_at = datetime.utcnow()
        dispatched += 1
        dispatched_ids.append(str(item.id))
    db.flush()
    return {"dispatched": dispatched, "skipped": skipped, "source_item_ids": dispatched_ids}


def dispatch_llm_jobs(
    db: Session,
    *,
    batch_size: int = 20,
    celery_app: Any,
    api_celery_app: Any | None = None,
    manager: VastAIManager | None = None,
) -> dict[str, Any]:
    """Dispatch pending LLM jobs using operation-scoped provider selection."""
    from core.content_profiles import CONTENT_PROFILE_KEY
    from core.llm_lane import (
        JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT,
        JOB_TYPE_RUN_LLM_ENRICHMENT,
        LLM_JOB_TYPES,
        llm_operation_for_job_type,
        llm_queue_for_job_type,
    )
    from core.models import ContentChunk, SourceDocument
    from processors.runtime_artifacts import set_pipeline_checkpoint
    from services.llm_api_client import (
        resolve_llm_operation_config,
    )
    from services.pipeline_options_service import get_llm_profiles, resolve_llm_option_profile
    from tasks import enqueue_embedding_batch

    if api_celery_app is None:
        from tasks import app as api_celery_app

    llm_profiles = get_llm_profiles(db)

    stmt = (
        select(IngestionQueue)
        .where(
            IngestionQueue.status == "pending",
            IngestionQueue.job_type.in_(LLM_JOB_TYPES),
        )
        .order_by(IngestionQueue.priority.desc(), IngestionQueue.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    items = list(db.execute(stmt).scalars())
    dispatched = 0
    bypassed = 0
    skipped = 0
    queue_ids: list[str] = []

    def _bypass_llm(queue_item: IngestionQueue) -> bool:
        """Complete the queue entry and enqueue embedding when profile bypasses all LLM work.

        Returns True when bypass was applied (caller should skip normal dispatch).
        """
        source_item = db.get(IngestionSourceItem, queue_item.source_item_id)
        if source_item is None:
            return False
        profile = (source_item.metadata_json or {}).get(CONTENT_PROFILE_KEY, "book")
        source_kind = None
        if source_item.source_document_id is not None:
            source_document = db.get(SourceDocument, source_item.source_document_id)
            source_kind = source_document.source_kind if source_document is not None else None
        llm_option_profile = resolve_llm_option_profile(content_profile=profile, source_kind=source_kind)
        if llm_option_profile in llm_profiles:
            return False

        metadata_json = dict(source_item.metadata_json or {})
        metadata_json = set_pipeline_checkpoint(metadata_json, "llm_enrichment_successful")
        metadata_json["llm_bypassed_operation"] = llm_operation_for_job_type(queue_item.job_type)
        metadata_json["llm_enrichment_bypassed"] = True
        metadata_json["llm_enrichment_bypass_reason"] = (
            f"profile '{llm_option_profile}' not in llm_profiles"
        )
        source_item.metadata_json = metadata_json

        if source_item.source_document_id is not None:
            source_document = db.get(SourceDocument, source_item.source_document_id)
            if source_document is not None:
                chunks = list(
                    db.execute(
                        select(ContentChunk).where(
                            ContentChunk.source_id == source_item.source_document_id,
                            ContentChunk.duplicate_of.is_(None),
                        )
                    ).scalars()
                )
                enqueue_embedding_batch(
                    db,
                    source_document=source_document,
                    registry_item=source_item,
                    chunks=chunks,
                )

        source_item.processing_status = "awaiting_embedding"
        source_item.updated_at = datetime.utcnow()
        queue_item.status = "completed"
        queue_item.completed_at = datetime.utcnow()
        db.flush()
        return True

    total_tokens_in = 0
    total_tokens_out = 0
    providers_used: dict[str, int] = {}

    for queue_item in items:
        if _bypass_llm(queue_item):
            bypassed += 1
            continue
        source_item = db.execute(
            select(IngestionSourceItem)
            .where(IngestionSourceItem.id == queue_item.source_item_id)
            .with_for_update(skip_locked=True)
        ).scalar_one_or_none()
        operation = llm_operation_for_job_type(queue_item.job_type)
        provider_config = resolve_llm_operation_config(db, operation)
        provider = provider_config.provider or "vastai"
        providers_used[provider] = providers_used.get(provider, 0) + 1
        try:
            queue_item.status = "processing"
            queue_item.started_at = datetime.utcnow()
            queue_item.error_message = None
            if source_item is not None:
                if queue_item.job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT:
                    source_item.processing_status = "llm_refinement_processing"
                elif queue_item.job_type == JOB_TYPE_RUN_LLM_ENRICHMENT:
                    source_item.processing_status = "llm_enrichment_processing"
                source_item.updated_at = datetime.utcnow()
            db.flush()

            if provider == "api":
                if not provider_config.api_key or not provider_config.api_base_url:
                    raise RuntimeError(
                        f"llm_{operation}_api_not_configured"
                    )
                if queue_item.job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT:
                    api_celery_app.send_task(
                        "tasks.run_llm_refinement_via_api_async",
                        args=[str(queue_item.source_item_id)],
                        queue="celery",
                    )
                else:
                    api_celery_app.send_task(
                        "tasks.run_llm_enrichment_via_api_async",
                        args=[str(queue_item.source_item_id)],
                        queue="celery",
                    )
            else:
                celery_app.send_task(
                    f"gpu_worker.tasks.{queue_item.job_type}",
                    args=[str(queue_item.source_item_id)],
                    queue=llm_queue_for_job_type(queue_item.job_type),
                )

            dispatched += 1
            queue_ids.append(str(queue_item.id))
        except Exception as exc:
            queue_item.status = "failed"
            queue_item.retry_count += 1
            queue_item.error_message = str(exc)
            if source_item is not None:
                if queue_item.job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT:
                    source_item.processing_status = "llm_refinement_failed"
                elif queue_item.job_type == JOB_TYPE_RUN_LLM_ENRICHMENT:
                    source_item.processing_status = "llm_enrichment_failed"
                source_item.updated_at = datetime.utcnow()
            skipped += 1
    db.flush()

    # --- Triage: surface items silently stuck at awaiting_llm_enrichment ---
    # Covers two failure modes:
    # 1. GPU worker task was dispatched (queue=processing) but crashed without
    #    calling back — queue row stays processing indefinitely.
    # 2. send_task raised for run_llm_boundary_refinement in a prior run before
    #    the bug-fix above — queue row is failed but processing_status was not
    #    updated.
    # In both cases: no pending/processing LLM queue row remains → item is
    # orphaned. Move to llm_enrichment_failed so it surfaces for retry.
    from sqlalchemy import exists as _sql_exists

    stuck_stmt = (
        select(IngestionSourceItem)
        .where(IngestionSourceItem.processing_status == "awaiting_llm_enrichment")
        .where(
            ~_sql_exists(
                select(IngestionQueue.id).where(
                    IngestionQueue.source_item_id == IngestionSourceItem.id,
                    IngestionQueue.job_type.in_(LLM_JOB_TYPES),
                    IngestionQueue.status.in_(["pending", "processing"]),
                )
            )
        )
        .where(
            _sql_exists(
                select(IngestionQueue.id).where(
                    IngestionQueue.source_item_id == IngestionSourceItem.id,
                    IngestionQueue.job_type.in_(LLM_JOB_TYPES),
                    IngestionQueue.status == "failed",
                )
            )
        )
    )
    stuck_items = list(db.execute(stuck_stmt).scalars())
    triaged = 0
    for stuck_item in stuck_items:
        stuck_item.processing_status = "llm_enrichment_failed"
        stuck_item.updated_at = datetime.utcnow()
        triaged += 1
    if triaged:
        db.flush()

    return {
        "dispatched": dispatched,
        "bypassed": bypassed,
        "skipped": skipped,
        "triaged_to_failed": triaged,
        "queue_ids": queue_ids,
        "providers": providers_used,
        "tokens_in": total_tokens_in,
        "tokens_out": total_tokens_out,
        "tokens_total": total_tokens_in + total_tokens_out,
    }


def _infer_registry_media_type(item: IngestionSourceItem) -> str | None:
    extension = item.metadata_json.get("extension")
    if extension:
        return str(extension).lower()
    candidate = item.source_path or item.display_name
    if candidate and "." in candidate:
        return candidate.rsplit(".", 1)[-1].lower()
    mime_type = (item.mime_type or "").lower()
    if "audio" in mime_type:
        return "mp3"
    if "video" in mime_type:
        return "mp4"
    return None


def _require_source_document(db: Session, source_item_id: UUID | str) -> SourceDocument:
    item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if item is None:
        raise ValueError("source_item_not_found")
    if not item.source_document_id:
        raise ValueError("source_document_not_found")
    document = db.get(SourceDocument, item.source_document_id)
    if document is None:
        raise ValueError("source_document_not_found")
    return document


def _purge_source_derived_data(db: Session, item: IngestionSourceItem) -> None:
    """Delete all data derived from a source item: queue rows, embedding batches,
    and the source document (which cascades to content chunks and chunk embeddings).

    Does NOT delete the IngestionSourceItem itself — call ``delete_source_item``
    for a full removal.
    """
    db.execute(sql_delete(IngestionQueue).where(IngestionQueue.source_item_id == item.id))
    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_item_id == item.id))
    if item.source_document_id is not None:
        db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_document_id == item.source_document_id))
        # Deleting SourceDocument cascades → ContentChunk → ChunkEmbedding via DB FK.
        db.execute(sql_delete(SourceDocument).where(SourceDocument.id == item.source_document_id))
    db.flush()


_PIPELINE_METADATA_KEYS = frozenset({
    "pipeline_gate_summary",
    "pipeline_review_markers",
    "pipeline_artifact_pointers",
    "pipeline_provenance",
    "transcript_quality_tier",
    "transcript_quality_signals",
    "pending_extract_key",
    "embedding_batch_status",
    "embedding_batch_error",
    "llm_enrichment_batch_outcome",
    "llm_refinement_batch_outcome",
    "llm_enrichment_model",
    "llm_enrichment_model_version",
    "llm_refinement_model",
    "llm_refinement_model_version",
    "llm_refined_segments",
    "llm_enrichment_bypassed",
    "llm_enrichment_bypass_reason",
    "refinement_triggered_enrichment_queue_id",
    "llm_callback_seen",
    "transcript_preprocessing_signals",
    "boundary_refinement_events",
    "boundary_refinement_info",
})


def _strip_pipeline_metadata(metadata_json: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of metadata_json with all pipeline-generated keys removed."""
    return {k: v for k, v in metadata_json.items() if k not in _PIPELINE_METADATA_KEYS}


def _chunk_ids_missing_embedding(db: Session, source_document_id: UUID) -> list[str]:
    """Return IDs of chunks under source_document_id that have no ChunkEmbedding row."""
    stmt = (
        select(ContentChunk.id)
        .outerjoin(ChunkEmbedding, ChunkEmbedding.chunk_id == ContentChunk.id)
        .where(
            ContentChunk.source_id == source_document_id,
            ChunkEmbedding.chunk_id.is_(None),
        )
    )
    return [str(row) for row in db.execute(stmt).scalars()]


def _create_embedding_batch(
    db: Session,
    item: IngestionSourceItem,
    chunk_ids: list[str],
) -> EmbeddingBatch:
    """Persist a new pending EmbeddingBatch for the given chunk IDs."""
    import hashlib

    batch_key = hashlib.sha256(
        f"{item.id}:{datetime.utcnow().isoformat()}".encode()
    ).hexdigest()[:64]
    batch = EmbeddingBatch(
        source_item_id=item.id,
        source_document_id=item.source_document_id,
        chunk_ids=chunk_ids,
        batch_key=batch_key,
        status="pending",
    )
    db.add(batch)
    db.flush()
    return batch
