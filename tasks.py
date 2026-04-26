"""Queue-driven text processing pipeline."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
from celery import Celery
from celery.schedules import crontab
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.content_profiles import CONTENT_PROFILE_KEY, require_content_profile
from core.config import settings
from core.database import SessionLocal
from core.ingestion_models import EmbeddingBatch, IngestionQueue, IngestionSourceConfig, IngestionSourceItem
from core.ingestion_registry import IngestionRegistryService
from core.llm_lane import (
    JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT,
    JOB_TYPE_RUN_LLM_ENRICHMENT,
    LLM_OPERATION_ENRICHMENT,
    LLM_OPERATION_REFINEMENT,
)
from core.models import ContentChunk, SourceDocument
from core.transcript_tiers import (
    TRANSCRIPT_QUALITY_SIGNALS_KEY,
    TRANSCRIPT_QUALITY_TIER_KEY,
    TranscriptTierDecision,
    classify_transcript_quality,
    transcript_processing_route_for_tier,
)
from core.versions import CHUNKING_VERSION, ENRICHMENT_VERSION, EXTRACTION_VERSION, get_processing_version
from processors import (
    BOUNDARY_REFINEMENT_EVENTS_KEY,
    BOUNDARY_REFINEMENT_INFO_KEY,
    AdaptiveChunker,
    ChunkEnricher,
    LLMBoundaryRefiner,
    PIPELINE_GATE_SUMMARY_KEY,
    PIPELINE_REVIEW_MARKERS_KEY,
    PIPELINE_STAGE_ARTIFACT_EXTRACTED,
    PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
    PIPELINE_STAGE_FAILED_CHUNKING,
    PIPELINE_STAGE_FAILED_EXTRACT_ARTIFACT,
    PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
    PIPELINE_STAGE_REFINEMENT_SUCCESSFUL,
    PIPELINE_STAGE_TRANSCRIPTED,
    PROVENANCE_KEY,
    TextExtractor,
    add_artifact_pointer,
    add_review_marker,
    attach_provenance,
    build_provenance_tuple,
    harden_profile_extract,
    invalidate_stage_outputs_for_version_change,
    merge_gate_summary,
    resolve_pipeline_resume_stage,
    set_pipeline_checkpoint,
    set_pipeline_resume_from,
)
from processors.enricher import ENRICHMENT_PROMPT_VERSION
from processors.transcript_preprocessing import (
    TRANSCRIPT_PREPROCESSING_SIGNALS_KEY,
    TranscriptPreprocessingResult,
    preprocess_transcript_extract,
    transcript_chunker_kwargs_for_tier,
)
from services import DeduplicatorService, VastAIManager

SUPPORTED_TEXT_FORMATS = {"pdf", "docx", "epub", "txt", "pptx"}
SUPPORTED_MEDIA_FORMATS = {"mp3", "wav", "m4a", "mp4", "mkv", "mov"}
JOB_TYPE_TRANSCRIBE_AUDIO = "transcribe_audio"
JOB_TYPE_EXTRACT_TEXT_ARTIFACT = "extract_text_artifact"
JOB_TYPE_PROCESS_CANONICAL_EXTRACT = "process_canonical_extract"

app = Celery("wismo_embedding", broker=settings.redis_url)

# Separate Celery app for dispatching tasks to Vast.ai workers.
# Uses vastai_redis_url (Upstash) when configured so workers on remote instances
# can reach the same queue. Falls back to redis_url when both are the same Redis.
_vastai_broker_url = settings.vastai_redis_url or settings.redis_url
vastai_app = Celery("wismo_vastai_dispatch", broker=_vastai_broker_url)
vastai_app.conf.task_serializer = "json"
vastai_app.conf.result_backend = None

app.conf.beat_schedule = {
    "run-db-backed-pipeline-schedulers": {
        "task": "tasks.run_pipeline_schedulers",
        "schedule": 60.0,
    }
}


def process_ingestion_queue(batch_size: int = 20, db: Session | None = None) -> int:
    """Process pending local queue items in priority order.

    New canonical flow:
    - ``extract_text_artifact``: raw text-based source -> json.gz artifact
    - ``process_canonical_extract``: json.gz artifact -> chunking/llm/embed path

    Legacy ``process_transcript_extract`` rows are still accepted as an alias for
    ``process_canonical_extract`` so older data can keep moving.
    """
    managed_session = db is None
    session = db or SessionLocal()
    processed = 0

    try:
        stmt = (
            select(IngestionQueue)
            .where(IngestionQueue.status == "pending")
            .order_by(IngestionQueue.priority.desc(), IngestionQueue.created_at.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        queue_items = list(session.execute(stmt).scalars())
        for queue_item in queue_items:
            # Lock ingestion_source_items FIRST (same order as scan_source_config_async)
            # to prevent deadlock. SKIP LOCKED: if scan holds the row, defer to next cycle.
            registry_item = session.execute(
                select(IngestionSourceItem)
                .where(IngestionSourceItem.id == queue_item.source_item_id)
                .with_for_update(skip_locked=True)
            ).scalar_one_or_none()
            if registry_item is None:
                continue
            if queue_item.job_type == JOB_TYPE_EXTRACT_TEXT_ARTIFACT:
                file_type = _infer_file_type(registry_item)
                if file_type not in SUPPORTED_TEXT_FORMATS:
                    continue
                process_text_extract_artifact_queue_item(
                    session,
                    queue_item,
                    file_type=file_type,
                )
                processed += 1
                continue
            if queue_item.job_type in {JOB_TYPE_PROCESS_CANONICAL_EXTRACT, "process_transcript_extract"}:
                extract_key = registry_item.metadata_json.get("pending_extract_key")
                if not extract_key:
                    continue
                process_canonical_extract_queue_item(session, queue_item, extract_key=extract_key)
                processed += 1
                continue
            raise ValueError(f"unsupported_queue_job_type:{queue_item.job_type}")

        if managed_session:
            session.commit()
        return processed
    except Exception:
        if managed_session:
            session.rollback()
        raise
    finally:
        if managed_session:
            session.close()


def process_text_extract_artifact_queue(batch_size: int = 20, db: Session | None = None) -> int:
    """Process pending text-based artifact extraction queue rows."""
    managed_session = db is None
    session = db or SessionLocal()
    processed = 0
    try:
        stmt = (
            select(IngestionQueue)
            .where(
                IngestionQueue.status == "pending",
                IngestionQueue.job_type == JOB_TYPE_EXTRACT_TEXT_ARTIFACT,
            )
            .order_by(IngestionQueue.priority.desc(), IngestionQueue.created_at.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        queue_items = list(session.execute(stmt).scalars())
        for queue_item in queue_items:
            registry_item = session.execute(
                select(IngestionSourceItem)
                .where(IngestionSourceItem.id == queue_item.source_item_id)
                .with_for_update(skip_locked=True)
            ).scalar_one_or_none()
            if registry_item is None:
                continue
            file_type = _infer_file_type(registry_item)
            if file_type not in SUPPORTED_TEXT_FORMATS:
                continue
            process_text_extract_artifact_queue_item(session, queue_item, file_type=file_type)
            processed += 1
        if managed_session:
            session.commit()
        return processed
    except Exception:
        if managed_session:
            session.rollback()
        raise
    finally:
        if managed_session:
            session.close()


@app.task(name="tasks.auto_scale_gpu_workers")
def auto_scale_gpu_workers() -> None:
    """Celery wrapper task for Vast.ai autoscale reconciliation."""
    VastAIManager().auto_scale()


@app.task(name="tasks.dispatch_embedding_batches")
def dispatch_embedding_batches_task(batch_size: int = 20) -> dict[str, Any]:
    """Celery wrapper for DB-backed embedding batch dispatch."""
    return dispatch_embedding_batches(batch_size=batch_size, celery_app=vastai_app)


@app.task(name="tasks.dispatch_llm_jobs")
def dispatch_llm_jobs_task(batch_size: int = 20) -> dict[str, Any]:
    """Celery wrapper for scheduled LLM lane job dispatch (enrichment/refinement)."""
    from services.dashboard_ops import dispatch_llm_jobs

    session = SessionLocal()
    try:
        payload = dispatch_llm_jobs(
            session,
            batch_size=batch_size,
            celery_app=vastai_app,
            api_celery_app=app,
        )
        session.commit()
        return payload
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _mark_llm_api_queue_failure(
    session: Session,
    *,
    source_item_id: str,
    job_type: str,
    error_message: str,
) -> None:
    """Persist failed state for one async API-mode LLM queue row."""
    stmt = (
        select(IngestionQueue)
        .where(
            IngestionQueue.source_item_id == UUID(str(source_item_id)),
            IngestionQueue.job_type == job_type,
            IngestionQueue.status == "processing",
        )
        .order_by(IngestionQueue.created_at.desc())
    )
    queue_item = session.execute(stmt).scalars().first()
    if queue_item is not None:
        queue_item.status = "failed"
        queue_item.retry_count += 1
        queue_item.error_message = error_message
        queue_item.completed_at = datetime.utcnow()

    source_item = session.get(IngestionSourceItem, UUID(str(source_item_id)))
    if source_item is not None:
        if job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT:
            source_item.processing_status = "llm_refinement_failed"
        else:
            source_item.processing_status = "llm_enrichment_failed"
        source_item.updated_at = datetime.utcnow()

    session.commit()


def _run_llm_api_job(source_item_id: str, *, operation: str) -> dict[str, Any]:
    """Execute one API-mode LLM queue item asynchronously on the local Celery worker."""
    from services.llm_api_client import (
        LlmApiClient,
        resolve_llm_operation_config,
        run_llm_enrichment_via_api,
        run_llm_refinement_via_api,
    )

    session = SessionLocal()
    try:
        config = resolve_llm_operation_config(session, operation)
        if config.provider != "api":
            raise RuntimeError(f"llm_{operation}_provider_not_api")
        if not config.api_key or not config.api_base_url:
            raise RuntimeError(f"llm_{operation}_api_not_configured")
        client = LlmApiClient.from_config(config)
        if operation == LLM_OPERATION_REFINEMENT:
            return run_llm_refinement_via_api(session, source_item_id, client)
        return run_llm_enrichment_via_api(session, source_item_id, client)
    except Exception as exc:
        _mark_llm_api_queue_failure(
            session,
            source_item_id=source_item_id,
            job_type=(
                JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT
                if operation == LLM_OPERATION_REFINEMENT
                else JOB_TYPE_RUN_LLM_ENRICHMENT
            ),
            error_message=str(exc),
        )
        return {"status": "failed", "error": str(exc)}
    finally:
        session.close()


@app.task(name="tasks.run_llm_refinement_via_api_async", queue="celery")
def run_llm_refinement_via_api_async(source_item_id: str) -> dict[str, Any]:
    """Execute one boundary-refinement job through the configured API backend."""
    return _run_llm_api_job(source_item_id, operation=LLM_OPERATION_REFINEMENT)


@app.task(name="tasks.run_llm_enrichment_via_api_async", queue="celery")
def run_llm_enrichment_via_api_async(source_item_id: str) -> dict[str, Any]:
    """Execute one enrichment job through the configured API backend."""
    return _run_llm_api_job(source_item_id, operation=LLM_OPERATION_ENRICHMENT)


@app.task(name="tasks.dispatch_audio_jobs")
def dispatch_audio_jobs_task(batch_size: int = 20) -> dict[str, Any]:
    """Celery wrapper for scheduled media-to-whisper dispatch."""
    session = SessionLocal()
    try:
        payload = _dispatch_audio_jobs_with_session(session, batch_size=batch_size)
        session.commit()
        return payload
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@app.task(name="tasks.dispatch_extract_artifact_jobs")
def dispatch_extract_artifact_jobs_task(batch_size: int = 20) -> dict[str, object]:
    """Celery wrapper for scheduled text-based artifact extraction."""
    return {"processed": process_text_extract_artifact_queue(batch_size=batch_size)}


@app.task(name="tasks.scan_source_config_async")
def scan_source_config_async_task(config_id: str) -> dict[str, Any]:
    """Run one source-config scan in its own local Celery task."""
    session = SessionLocal()
    config_uuid = UUID(str(config_id))
    try:
        return _scan_source_config(session, config_uuid)
    except Exception as exc:
        session.rollback()
        config = session.get(IngestionSourceConfig, config_uuid)
        if config is not None:
            config.last_scan_at = datetime.utcnow()
            config.last_scan_status = "failed"
            session.commit()
        raise exc
    finally:
        session.close()


@app.task(name="tasks.scan_scheduled_source_configs")
def scan_scheduled_source_configs(batch_size: int = 20, celery_app: Celery | None = None) -> dict[str, object]:
    """Dispatch enabled source-config scans onto local Redis/Celery."""
    session = SessionLocal()
    broker = celery_app or app
    try:
        config_ids = _list_enabled_source_config_ids(session, limit=batch_size)
        for config_id in config_ids:
            broker.send_task(
                "tasks.scan_source_config_async",
                args=[config_id],
                queue="celery",
            )
        return {
            "checked_at": datetime.utcnow().isoformat(),
            "dispatched": len(config_ids),
            "source_config_ids": config_ids,
        }
    finally:
        session.close()


@app.task(name="tasks.process_text_queue")
def process_text_queue_task(batch_size: int = 20) -> dict[str, object]:
    """Celery wrapper for scheduled local text queue consumption."""
    return {"processed": process_text_queue(batch_size=batch_size)}


@app.task(name="tasks.run_pipeline_schedulers")
def run_pipeline_schedulers() -> dict[str, object]:
    """Evaluate DB-backed scheduler configs and run due stages with lock protection."""
    from services.pipeline_scheduler_ops import get_all_scheduler_stage_configs, run_scheduler_stage

    session = SessionLocal()
    try:
        now = datetime.utcnow()
        results: list[dict[str, Any]] = []
        for cfg in get_all_scheduler_stage_configs(session):
            if not cfg.is_enabled:
                continue
            if not cfg.cron_expression or not _cron_matches(cfg.cron_expression, now):
                continue
            if cfg.stage != "scan_source_configs" and _already_scanned_this_minute(cfg.last_run_at, now):
                continue
            results.append(run_scheduler_stage(session, cfg.stage))
        return {"checked_at": now.isoformat(), "results": results}
    finally:
        session.close()


def dispatch_embedding_batches(
    batch_size: int = 20,
    db: Session | None = None,
    *,
    celery_app: Celery | None = None,
    manager: VastAIManager | None = None,
) -> dict[str, Any]:
    """Dispatch DB-backed embedding batches into the GPU embedding queue.

    This stage is dispatch-only. GPU instance launch/destroy decisions belong to
    the separate autoscale stage.
    """
    managed_session = db is None
    session = db or SessionLocal()
    broker = celery_app or app
    dispatched = 0
    failed = 0
    batch_ids: list[str] = []

    try:
        stmt = (
            select(EmbeddingBatch)
            .where(EmbeddingBatch.status == "pending")
            .order_by(EmbeddingBatch.priority.desc(), EmbeddingBatch.created_at.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        batches = list(session.execute(stmt).scalars())
        for batch in batches:
            if not batch.chunk_ids:
                batch.status = "completed"
                batch.completed_at = datetime.utcnow()
                continue
            source_item = (
                session.execute(
                    select(IngestionSourceItem)
                    .where(IngestionSourceItem.id == batch.source_item_id)
                    .with_for_update(skip_locked=True)
                ).scalar_one_or_none()
                if batch.source_item_id is not None
                else None
            )
            batch.status = "processing"
            batch.started_at = datetime.utcnow()
            batch.error_message = None
            if source_item is not None:
                source_item.processing_status = "embedding_processing"
                source_item.updated_at = datetime.utcnow()
            session.flush()
            try:
                broker.send_task(
                    "gpu_worker.tasks.embed_chunks_batch",
                    args=[str(batch.id), list(batch.chunk_ids)],
                    queue="embedding",
                )
                dispatched += 1
                batch_ids.append(str(batch.id))
            except Exception as exc:
                batch.status = "failed"
                batch.retry_count += 1
                batch.error_message = str(exc)
                if source_item is not None:
                    source_item.processing_status = "embedding_failed"
                    source_item.last_error_code = "embedding_dispatch_failed"
                    source_item.last_error_message = str(exc)
                    source_item.updated_at = datetime.utcnow()
                failed += 1
        session.flush()
        if managed_session:
            session.commit()
        return {"dispatched": dispatched, "failed": failed, "batch_ids": batch_ids}
    except Exception:
        if managed_session:
            session.rollback()
        raise
    finally:
        if managed_session:
            session.close()


def process_text_item(
    db: Session,
    queue_item: IngestionQueue,
    *,
    file_type: str | None = None,
    extractor: TextExtractor | None = None,
    chunker: AdaptiveChunker | None = None,
    enricher: ChunkEnricher | None = None,
    deduplicator: DeduplicatorService | None = None,
    s3_client: Any | None = None,
    dispatch_llm_enrichment: bool = True,
) -> SourceDocument:
    """Run the text pipeline for one execution queue item.

    When ``dispatch_llm_enrichment=True`` (default), chunks are persisted with
    default metadata and enrichment is dispatched to the LLM lane via
    ``enqueue_llm_enrichment_batch``. Embedding is NOT enqueued until the
    enrichment callback succeeds.

    When ``dispatch_llm_enrichment=False``, the inline ChunkEnricher is used
    (degraded/dev mode or test override).
    """
    registry_item = db.get(IngestionSourceItem, queue_item.source_item_id)
    if registry_item is None:
        raise ValueError("Queue item references unknown ingestion source item")

    resolved_file_type = file_type or _infer_file_type(registry_item)
    if resolved_file_type not in SUPPORTED_TEXT_FORMATS:
        raise ValueError(f"Unsupported text file type: {resolved_file_type}")

    text_extractor = extractor or TextExtractor()
    chunk_enricher = enricher or ChunkEnricher()
    dedup_service = deduplicator or DeduplicatorService(db)
    registry_service = IngestionRegistryService(db)
    client = s3_client or _get_s3_client()

    temp_path: Path | None = None
    try:
        queue_item.status = "processing"
        queue_item.started_at = datetime.utcnow()
        db.flush()
        _record_pipeline_resume_state(db, registry_item)
        content_profile = _resolve_content_profile_for_pipeline(registry_item)

        raw_bytes = _download_raw_asset(client, registry_item)
        registry_item.content_sha256 = registry_service.compute_content_sha256(raw_bytes)

        temp_path = _write_temp_file(raw_bytes, resolved_file_type)
        extract_payload = text_extractor.extract(temp_path, resolved_file_type)
        prepared_extract = _prepare_profile_extract_for_pipeline(
            db,
            registry_item,
            extract_payload=extract_payload,
            content_profile=content_profile,
        )
        adaptive_chunker = _resolve_chunker_for_profile(content_profile, chunker)
        source_document = _get_or_create_source_document(
            db,
            registry_item=registry_item,
            file_type=resolved_file_type,
            source_kind="text_document",
        )
        _prepare_source_document_for_reprocess(db, source_document)

        extract_key = _upload_extract_artifact(client, source_document.id, registry_item.source_type, extract_payload)
        source_document.s3_text_key = extract_key

        chunk_payloads = adaptive_chunker.chunk(
            prepared_extract["full_text"],
            segments=prepared_extract.get("segments"),
        )
        _record_pipeline_checkpoint(
            db,
            registry_item,
            stage=PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
            gate_summary=_summarize_chunking_stage(content_profile, chunk_payloads, registry_item),
        )
        stored_chunks = _persist_chunks(
            db,
            source_document=source_document,
            chunk_payloads=chunk_payloads,
            registry_item=registry_item,
            content_profile=content_profile,
            deduplicator=dedup_service,
            enricher=None if dispatch_llm_enrichment else chunk_enricher,
        )
        source_document.total_chunks = len(stored_chunks)
        source_document.updated_at = datetime.utcnow()
        registry_item.source_document_id = source_document.id
        db.flush()

        llm_option_profile = _llm_option_profile_for_pipeline(
            content_profile=content_profile,
            source_kind=source_document.source_kind,
        )
        llm_enabled_for_profile = _profile_uses_llm(db, llm_option_profile)

        if dispatch_llm_enrichment and llm_enabled_for_profile:
            # LLM lane path: chunking done, wait for GPU enrichment.
            # Embedding will be enqueued after enrichment callback succeeds.
            enqueue_llm_enrichment_batch(db, source_item=registry_item)
            registry_item.processing_status = "awaiting_llm_enrichment"
            registry_item.updated_at = datetime.utcnow()
            queue_item.status = "completed"
            queue_item.completed_at = datetime.utcnow()
            queue_item.error_message = None
            db.flush()
        elif dispatch_llm_enrichment:
            # Bypass path: profile not in llm_profiles — skip all LLM work,
            # go straight to embedding. Record provenance as skipped (no model).
            _persist_finalized_provenance(
                db,
                registry_item=registry_item,
                source_document=source_document,
                stored_chunks=stored_chunks,
                prompt_version="skipped",
                llm_model="skipped",
                llm_model_version="skipped",
            )
            _record_pipeline_checkpoint(
                db,
                registry_item,
                stage=PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
                gate_summary={
                    "stage": PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
                    "enrichment_skipped": True,
                    "chunk_count": len(stored_chunks),
                },
            )
            enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=registry_item,
                chunks=stored_chunks,
            )
            registry_item.processing_status = "awaiting_embedding"
            registry_item.updated_at = datetime.utcnow()
            queue_item.status = "completed"
            queue_item.completed_at = datetime.utcnow()
            queue_item.error_message = None
            db.flush()
        else:
            # Inline enrichment path (degraded / dev / test override).
            _persist_finalized_provenance(
                db,
                registry_item=registry_item,
                source_document=source_document,
                stored_chunks=stored_chunks,
                prompt_version=ENRICHMENT_PROMPT_VERSION,
            )
            _record_pipeline_checkpoint(
                db,
                registry_item,
                stage=PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
                gate_summary=_summarize_enrichment_stage(stored_chunks),
            )
            enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=registry_item,
                chunks=stored_chunks,
            )
            registry_item.processing_status = "awaiting_embedding"
            registry_item.updated_at = datetime.utcnow()
            queue_item.status = "completed"
            queue_item.completed_at = datetime.utcnow()
            queue_item.error_message = None
            db.flush()

        return source_document
    except Exception as exc:
        metadata_json = dict(registry_item.metadata_json or {})
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_EXTRACT_ARTIFACT)
        registry_item.metadata_json = metadata_json
        registry_item.processing_status = "extract_artifact_failed"
        registry_item.last_error_code = "extract_artifact_failed"
        registry_item.last_error_message = str(exc)
        registry_item.updated_at = datetime.utcnow()
        queue_item.status = "failed"
        queue_item.retry_count += 1
        queue_item.error_message = str(exc)
        db.flush()
        raise
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


def process_text_extract_artifact_queue_item(
    db: Session,
    queue_item: IngestionQueue,
    *,
    file_type: str,
    extractor: TextExtractor | None = None,
    s3_client: Any | None = None,
) -> str:
    """Create canonical json.gz artifact for one text-based raw source.

    This is the text-based equivalent of Whisper transcript output: once the
    artifact exists, the rest of the pipeline can continue from a unified
    canonical-extract stage.
    """
    registry_item = db.get(IngestionSourceItem, queue_item.source_item_id)
    if registry_item is None:
        raise ValueError("Queue item references unknown ingestion source item")

    text_extractor = extractor or TextExtractor()
    client = s3_client or _get_s3_client()
    registry_service = IngestionRegistryService(db)
    temp_path: Path | None = None

    try:
        queue_item.status = "processing"
        queue_item.started_at = datetime.utcnow()
        registry_item.processing_status = "extract_artifact_processing"
        registry_item.updated_at = datetime.utcnow()
        db.flush()

        raw_bytes = _download_raw_asset(client, registry_item)
        registry_item.content_sha256 = registry_service.compute_content_sha256(raw_bytes)

        temp_path = _write_temp_file(raw_bytes, file_type)
        extract_payload = text_extractor.extract(temp_path, file_type)
        extract_key = _upload_extract_artifact(client, registry_item.id, registry_item.source_type, extract_payload)

        enqueue_canonical_extract_followup(
            db,
            registry_item.id,
            extract_key=extract_key,
            checkpoint_stage=PIPELINE_STAGE_ARTIFACT_EXTRACTED,
        )

        queue_item.status = "completed"
        queue_item.completed_at = datetime.utcnow()
        queue_item.error_message = None
        db.flush()
        return extract_key
    except Exception as exc:
        metadata_json = dict(registry_item.metadata_json or {})
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_EXTRACT_ARTIFACT)
        registry_item.metadata_json = metadata_json
        registry_item.processing_status = "extract_artifact_failed"
        registry_item.retry_count += 1
        registry_item.last_error_code = "extract_artifact_failed"
        registry_item.last_error_message = str(exc)
        registry_item.updated_at = datetime.utcnow()
        queue_item.status = "failed"
        queue_item.retry_count += 1
        queue_item.error_message = str(exc)
        db.flush()
        raise
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


def process_canonical_extract(
    db: Session,
    source_item_id: UUID | str,
    *,
    extract_payload: dict[str, Any],
    extract_key: str | None = None,
    chunker: AdaptiveChunker | None = None,
    enricher: ChunkEnricher | None = None,
    deduplicator: DeduplicatorService | None = None,
    boundary_refiner: LLMBoundaryRefiner | None = None,
    dispatch_llm_refinement: bool = True,
    dispatch_llm_enrichment: bool = True,
) -> SourceDocument:
    """Process an already-created canonical extract into corpus records.

    When ``dispatch_llm_refinement=True`` (default) for transcript items, candidate
    chunks are persisted and boundary refinement is dispatched to the LLM lane via
    ``enqueue_llm_refinement_batch``. The pipeline stops at ``awaiting_llm_refinement``.

    When ``dispatch_llm_refinement=False``, inline LLMBoundaryRefiner is used (degraded/
    dev/test mode).

    When ``dispatch_llm_enrichment=True`` (default), after refinement the pipeline stops
    at ``awaiting_llm_enrichment`` and enqueues a ``run_llm_enrichment`` job.

    When ``dispatch_llm_enrichment=False``, inline enrichment proceeds (degraded/dev/test).
    """
    registry_item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if registry_item is None:
        raise ValueError("Unknown source item for canonical extract processing")

    chunk_enricher = enricher or ChunkEnricher()
    dedup_service = deduplicator or DeduplicatorService(db)
    registry_service = IngestionRegistryService(db)
    transcript_boundary_refiner = boundary_refiner or LLMBoundaryRefiner()

    try:
        _record_pipeline_resume_state(db, registry_item)
        content_profile = _resolve_content_profile_for_pipeline(registry_item)
        hardened_extract = _prepare_profile_extract_for_pipeline(
            db,
            registry_item,
            extract_payload=extract_payload,
            content_profile=content_profile,
        )
        transcript_tier = _resolve_transcript_tier_for_pipeline(
            db,
            registry_item,
            extract_payload=hardened_extract,
            content_profile=content_profile,
            registry_service=registry_service,
        )
        prepared_extract = _prepare_transcript_extract_for_pipeline(
            db,
            registry_item,
            extract_payload=hardened_extract,
            content_profile=content_profile,
            transcript_tier=transcript_tier,
        )
        adaptive_chunker = _resolve_chunker_for_profile(
            content_profile,
            chunker,
            transcript_quality_tier=transcript_tier.tier if transcript_tier is not None else None,
        )
        inferred_format = _infer_extract_format(registry_item)
        source_document = _get_or_create_source_document(
            db,
            registry_item=registry_item,
            file_type=inferred_format,
            source_kind=_source_kind_for_canonical_extract(inferred_format),
        )
        _prepare_source_document_for_reprocess(db, source_document)
        if extract_key:
            source_document.s3_text_key = extract_key

        chunk_payloads = adaptive_chunker.chunk(
            prepared_extract.get("full_text", ""),
            segments=prepared_extract.get("segments"),
        )
        _record_pipeline_checkpoint(
            db,
            registry_item,
            stage=PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
            gate_summary=_summarize_chunking_stage(content_profile, chunk_payloads, registry_item),
        )
        llm_option_profile = _llm_option_profile_for_pipeline(
            content_profile=content_profile,
            source_kind=source_document.source_kind,
        )
        llm_enabled_for_profile = _profile_uses_llm(db, llm_option_profile)

        if dispatch_llm_refinement and llm_enabled_for_profile:
            # LLM lane refinement path: persist candidate chunks, then stop at
            # awaiting_llm_refinement. Enrichment is dispatched after refinement callback.
            stored_chunks = _persist_chunks(
                db,
                source_document=source_document,
                chunk_payloads=chunk_payloads,
                registry_item=registry_item,
                content_profile=content_profile,
                deduplicator=dedup_service,
                enricher=None,
            )
            source_document.total_chunks = len(stored_chunks)
            source_document.updated_at = datetime.utcnow()
            registry_item.source_document_id = source_document.id
            enqueue_llm_refinement_batch(db, source_item=registry_item)
            registry_item.processing_status = "awaiting_llm_refinement"
            registry_item.updated_at = datetime.utcnow()
            db.flush()
            return source_document

        # Inline refinement path (non-transcript, or dispatch_llm_refinement=False).
        chunk_payloads = _maybe_refine_transcript_chunks(
            db,
            registry_item,
            chunk_payloads=chunk_payloads,
            content_profile=content_profile,
            transcript_tier=transcript_tier,
            boundary_refiner=transcript_boundary_refiner,
        )
        _record_pipeline_checkpoint(
            db,
            registry_item,
            stage=PIPELINE_STAGE_REFINEMENT_SUCCESSFUL,
            gate_summary=_summarize_refinement_stage(registry_item),
        )
        stored_chunks = _persist_chunks(
            db,
            source_document=source_document,
            chunk_payloads=chunk_payloads,
            registry_item=registry_item,
            content_profile=content_profile,
            deduplicator=dedup_service,
            enricher=None if dispatch_llm_enrichment else chunk_enricher,
        )
        source_document.total_chunks = len(stored_chunks)
        source_document.updated_at = datetime.utcnow()
        registry_item.source_document_id = source_document.id
        db.flush()

        if dispatch_llm_enrichment and llm_enabled_for_profile:
            # LLM lane enrichment path: chunking done, wait for GPU enrichment.
            enqueue_llm_enrichment_batch(db, source_item=registry_item)
            registry_item.processing_status = "awaiting_llm_enrichment"
            registry_item.updated_at = datetime.utcnow()
            db.flush()
        elif dispatch_llm_enrichment:
            # Bypass path: profile not in llm_profiles — skip all LLM work,
            # go straight to embedding. Record provenance as skipped (no model).
            _persist_finalized_provenance(
                db,
                registry_item=registry_item,
                source_document=source_document,
                stored_chunks=stored_chunks,
                prompt_version="skipped",
                llm_model="skipped",
                llm_model_version="skipped",
            )
            _record_pipeline_checkpoint(
                db,
                registry_item,
                stage=PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
                gate_summary={
                    "stage": PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
                    "enrichment_skipped": True,
                    "chunk_count": len(stored_chunks),
                },
            )
            enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=registry_item,
                chunks=stored_chunks,
            )
            registry_item.processing_status = "awaiting_embedding"
            registry_item.updated_at = datetime.utcnow()
            db.flush()
        else:
            # Inline enrichment path (degraded / dev / test override).
            _persist_finalized_provenance(
                db,
                registry_item=registry_item,
                source_document=source_document,
                stored_chunks=stored_chunks,
                prompt_version=ENRICHMENT_PROMPT_VERSION,
            )
            _record_pipeline_checkpoint(
                db,
                registry_item,
                stage=PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
                gate_summary=_summarize_enrichment_stage(stored_chunks),
            )
            enqueue_embedding_batch(
                db,
                source_document=source_document,
                registry_item=registry_item,
                chunks=stored_chunks,
            )
            registry_item.processing_status = "awaiting_embedding"
            registry_item.updated_at = datetime.utcnow()
            db.flush()

        return source_document
    except Exception as exc:
        metadata_json = dict(registry_item.metadata_json or {})
        metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_CHUNKING)
        registry_item.metadata_json = metadata_json
        registry_item.processing_status = "chunking_failed"
        registry_item.retry_count += 1
        registry_item.last_error_code = "canonical_extract_processing_failed"
        registry_item.last_error_message = str(exc)
        registry_item.updated_at = datetime.utcnow()
        db.flush()
        raise


def process_canonical_extract_from_storage(
    db: Session,
    source_item_id: UUID | str,
    *,
    extract_key: str,
    s3_client: Any | None = None,
    chunker: AdaptiveChunker | None = None,
    enricher: ChunkEnricher | None = None,
    deduplicator: DeduplicatorService | None = None,
) -> SourceDocument:
    """Download a canonical extract artifact and process it into corpus records."""
    client = s3_client or _get_s3_client()
    payload = _download_extract_artifact(client, extract_key)
    return process_canonical_extract(
        db,
        source_item_id,
        extract_payload=payload,
        extract_key=extract_key,
        chunker=chunker,
        enricher=enricher,
        deduplicator=deduplicator,
    )


def enqueue_canonical_extract_followup(
    db: Session,
    source_item_id: UUID | str,
    *,
    extract_key: str,
    checkpoint_stage: str,
    priority: int = 5,
) -> IngestionQueue:
    """Create a local follow-up queue item for canonical extract processing."""
    registry_item = db.get(IngestionSourceItem, UUID(str(source_item_id)))
    if registry_item is None:
        raise ValueError("Unknown source item for canonical extract follow-up")
    metadata_json = dict(registry_item.metadata_json)
    metadata_json["pending_extract_key"] = extract_key
    metadata_json = set_pipeline_checkpoint(metadata_json, checkpoint_stage)
    registry_item.metadata_json = metadata_json
    registry_item.processing_status = "awaiting_chunking"
    existing = db.execute(
        select(IngestionQueue).where(
            IngestionQueue.source_item_id == UUID(str(source_item_id)),
            IngestionQueue.job_type.in_([JOB_TYPE_PROCESS_CANONICAL_EXTRACT, "process_transcript_extract"]),
            IngestionQueue.status.in_(["pending", "processing"]),
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    queue_item = IngestionQueue(
        source_item_id=UUID(str(source_item_id)),
        job_type=JOB_TYPE_PROCESS_CANONICAL_EXTRACT,
        status="pending",
        priority=priority,
    )
    db.add(queue_item)
    db.flush()
    return queue_item


def enqueue_transcript_followup(
    db: Session,
    source_item_id: UUID | str,
    *,
    extract_key: str,
    priority: int = 5,
) -> IngestionQueue:
    """Backward-compatible wrapper for media transcript -> canonical extract."""
    return enqueue_canonical_extract_followup(
        db,
        source_item_id,
        extract_key=extract_key,
        checkpoint_stage=PIPELINE_STAGE_TRANSCRIPTED,
        priority=priority,
    )


def process_canonical_extract_queue_item(
    db: Session,
    queue_item: IngestionQueue,
    *,
    extract_key: str,
    s3_client: Any | None = None,
) -> SourceDocument:
    """Process one canonical json.gz artifact into chunking/llm/embed outputs."""
    registry_item = db.get(IngestionSourceItem, queue_item.source_item_id)
    queue_item.status = "processing"
    queue_item.started_at = datetime.utcnow()
    if registry_item is not None:
        registry_item.processing_status = "chunking_processing"
        registry_item.updated_at = datetime.utcnow()
    db.flush()
    try:
        document = process_canonical_extract_from_storage(
            db,
            queue_item.source_item_id,
            extract_key=extract_key,
            s3_client=s3_client,
        )
        registry_item = db.get(IngestionSourceItem, queue_item.source_item_id)
        if registry_item is not None:
            metadata_json = dict(registry_item.metadata_json)
            metadata_json.pop("pending_extract_key", None)
            registry_item.metadata_json = metadata_json
        queue_item.status = "completed"
        queue_item.completed_at = datetime.utcnow()
        queue_item.error_message = None
        db.flush()
        return document
    except Exception as exc:
        if registry_item is not None:
            metadata_json = dict(registry_item.metadata_json or {})
            metadata_json = set_pipeline_checkpoint(metadata_json, PIPELINE_STAGE_FAILED_CHUNKING)
            registry_item.metadata_json = metadata_json
            registry_item.processing_status = "chunking_failed"
            registry_item.retry_count += 1
            registry_item.last_error_code = "chunking_failed"
            registry_item.last_error_message = str(exc)
            registry_item.updated_at = datetime.utcnow()
        queue_item.status = "failed"
        queue_item.retry_count += 1
        queue_item.error_message = str(exc)
        db.flush()
        raise


def process_transcript_extract_queue_item(
    db: Session,
    queue_item: IngestionQueue,
    *,
    extract_key: str,
    s3_client: Any | None = None,
) -> SourceDocument:
    """Backward-compatible alias for canonical extract processing."""
    return process_canonical_extract_queue_item(db, queue_item, extract_key=extract_key, s3_client=s3_client)


def _dispatch_audio_jobs_with_session(
    session: Session,
    *,
    batch_size: int = 20,
    manager: VastAIManager | None = None,
) -> dict[str, Any]:
    """Reuse the dashboard audio-dispatch helper inside scheduled task context."""
    from services.dashboard_ops import dispatch_audio_jobs

    return dispatch_audio_jobs(session, batch_size=batch_size, celery_app=vastai_app, manager=manager)


def enqueue_embedding_batch(
    db: Session,
    *,
    source_document: SourceDocument,
    registry_item: IngestionSourceItem,
    chunks: list[ContentChunk],
    priority: int = 5,
) -> EmbeddingBatch | None:
    """Persist one DB-backed embedding batch for canonical chunks that need vectors."""
    chunk_ids = [str(chunk.id) for chunk in chunks if chunk.duplicate_of is None]
    if not chunk_ids:
        return None

    batch_key = _embedding_batch_key(source_document.id, chunk_ids)
    existing = db.execute(select(EmbeddingBatch).where(EmbeddingBatch.batch_key == batch_key)).scalar_one_or_none()
    if existing is not None:
        return existing

    batch = EmbeddingBatch(
        source_document_id=source_document.id,
        source_item_id=registry_item.id,
        chunk_ids=chunk_ids,
        batch_key=batch_key,
        status="pending",
        priority=priority,
    )
    db.add(batch)
    db.flush()
    return batch


def enqueue_llm_enrichment_batch(
    db: Session,
    *,
    source_item: IngestionSourceItem,
    priority: int = 5,
) -> IngestionQueue:
    """Create one IngestionQueue item to dispatch enrichment for finalized chunks.

    Purpose: replaces inline ChunkEnricher.enrich() for production paths.
    The LLM GPU worker picks this up via dispatch_llm_jobs and calls back
    via /api/internal/llm-enrichment-callback/ with structured results.

    Inputs:
        source_item: the registry item whose chunks need enrichment.
        priority: queue dispatch priority (higher = sooner).

    Output:
        The newly created IngestionQueue item in pending state.

    Side effects:
        Writes one IngestionQueue row; flushes but does not commit.
        Idempotent: existing pending/processing enrichment items for the
        same source_item are not duplicated.
    """
    existing = db.execute(
        select(IngestionQueue).where(
            IngestionQueue.source_item_id == source_item.id,
            IngestionQueue.job_type == JOB_TYPE_RUN_LLM_ENRICHMENT,
            IngestionQueue.status.in_(["pending", "processing"]),
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    queue_item = IngestionQueue(
        source_item_id=source_item.id,
        job_type=JOB_TYPE_RUN_LLM_ENRICHMENT,
        status="pending",
        priority=priority,
    )
    db.add(queue_item)
    db.flush()
    return queue_item


def enqueue_llm_refinement_batch(
    db: Session,
    *,
    source_item: IngestionSourceItem,
    priority: int = 5,
) -> IngestionQueue:
    """Create one IngestionQueue item to dispatch boundary refinement for transcript chunks.

    Purpose: replaces inline LLMBoundaryRefiner.refine_chunks() for production paths.
    The LLM GPU worker picks this up via dispatch_llm_jobs and calls back via
    /api/internal/llm-refinement-callback/ with refined segment decisions.

    Inputs:
        source_item: the registry item whose candidate chunks need boundary refinement.
        priority: queue dispatch priority (higher = sooner).

    Output:
        The newly created IngestionQueue item in pending state.

    Side effects:
        Writes one IngestionQueue row; flushes but does not commit.
        Idempotent: existing pending/processing refinement items for the same
        source_item are not duplicated.
    """
    existing = db.execute(
        select(IngestionQueue).where(
            IngestionQueue.source_item_id == source_item.id,
            IngestionQueue.job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT,
            IngestionQueue.status.in_(["pending", "processing"]),
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    queue_item = IngestionQueue(
        source_item_id=source_item.id,
        job_type=JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT,
        status="pending",
        priority=priority,
    )
    db.add(queue_item)
    db.flush()
    return queue_item


def _embedding_batch_key(source_document_id: UUID | str, chunk_ids: list[str]) -> str:
    """Return a stable idempotency key for one embedding batch."""
    normalized = f"{source_document_id}:{','.join(sorted(chunk_ids))}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _scan_source_config(db: Session, config_id) -> dict[str, Any]:
    """Run one source-config scan inline (no FastAPI dependency injection)."""
    from connectors import ContaboConnector, DownloadableConnector
    from core.content_profiles import CONTENT_PROFILE_KEY, with_declared_content_profile
    from core.ingestion_queue_service import IngestionQueueService
    from core.ingestion_registry import IngestionRegistryService

    source_config = db.get(IngestionSourceConfig, config_id)
    if source_config is None:
        raise ValueError(f"source_config_not_found:{config_id}")
    if not source_config.is_enabled:
        return {"source_config_id": str(config_id), "items_found": 0, "skipped": "disabled"}

    config_json = dict(source_config.config_json)
    content_profile = config_json.get(CONTENT_PROFILE_KEY)

    if source_config.source_type == "contabo_prefix":
        bucket = config_json.get("bucket")
        if not bucket:
            raise ValueError(f"source_config_bucket_required:{config_id}")
        prefix = config_json.get("prefix", "")
        candidates = ContaboConnector().discover_items(bucket=bucket, prefix=prefix)
    elif source_config.source_type == "downloadable_file":
        url = config_json.get("url")
        if not url:
            raise ValueError(f"source_config_url_required:{config_id}")
        candidates = DownloadableConnector().discover_items(url=url)
    else:
        raise ValueError(f"source_config_scan_not_supported:{source_config.source_type}")

    registry = IngestionRegistryService(db)
    queue_service = IngestionQueueService(db)
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        if content_profile:
            candidate.metadata_json = with_declared_content_profile(candidate.metadata_json, content_profile)
        decision = registry.upsert_candidate(candidate)
        queue_decision = queue_service.enqueue_if_needed(decision.item)
        results.append({
            "item_id": str(decision.item.id),
            "discovery_status": decision.item.discovery_status,
            "processing_status": decision.item.processing_status,
            "skip_reason": decision.skip_reason or queue_decision.skip_reason,
            "queued": queue_decision.queued,
        })

    source_config.last_scan_at = datetime.utcnow()
    source_config.last_scan_status = "success"
    db.commit()
    return {"source_config_id": str(config_id), "items_found": len(candidates), "results": results}


def _list_enabled_source_config_ids(db: Session, *, limit: int) -> list[str]:
    """Return enabled source-config ids for scan dispatch batching."""
    stmt = (
        select(IngestionSourceConfig.id)
        .where(IngestionSourceConfig.is_enabled.is_(True))
        .order_by(IngestionSourceConfig.updated_at.desc(), IngestionSourceConfig.display_name.asc())
        .limit(limit)
    )
    return [str(config_id) for config_id in db.execute(stmt).scalars()]


def _already_scanned_this_minute(last_scan_at: datetime | None, now: datetime) -> bool:
    """Return True when the config has already been scanned in the current minute."""
    if last_scan_at is None:
        return False
    return last_scan_at.replace(second=0, microsecond=0) >= now.replace(second=0, microsecond=0)


def _cron_matches(expression: str, now: datetime) -> bool:
    """Return True when a basic 5-field cron expression matches the given UTC minute."""
    fields = expression.split()
    if len(fields) != 5:
        return False

    minute, hour, day_of_month, month, day_of_week = fields
    cron_weekday = now.isoweekday() % 7
    return (
        _field_matches(minute, now.minute, 0, 59)
        and _field_matches(hour, now.hour, 0, 23)
        and _field_matches(day_of_month, now.day, 1, 31)
        and _field_matches(month, now.month, 1, 12)
        and _field_matches(day_of_week, cron_weekday, 0, 7)
    )


def _field_matches(field: str, value: int, minimum: int, maximum: int) -> bool:
    """Return True when one cron field matches the given value."""
    normalized_value = 0 if maximum == 7 and value == 0 else value
    for part in field.split(","):
        if _part_matches(part.strip(), normalized_value, minimum, maximum):
            return True
    return False


def _part_matches(part: str, value: int, minimum: int, maximum: int) -> bool:
    """Evaluate one cron field part such as '*', '5', '1-5', or '*/15'."""
    if not part:
        return False

    if "/" in part:
        base, step_text = part.split("/", 1)
        if not step_text.isdigit():
            return False
        step = int(step_text)
        if step <= 0:
            return False
    else:
        base = part
        step = 1

    allowed = _expand_base_values(base, minimum, maximum)
    if not allowed:
        return False
    return value in {candidate for candidate in allowed if (candidate - min(allowed)) % step == 0}


def _expand_base_values(base: str, minimum: int, maximum: int) -> set[int]:
    """Expand a cron base field into concrete values."""
    if base == "*":
        values = set(range(minimum, maximum + 1))
    elif "-" in base:
        start_text, end_text = base.split("-", 1)
        if not start_text.isdigit() or not end_text.isdigit():
            return set()
        start = int(start_text)
        end = int(end_text)
        if start > end:
            return set()
        values = set(range(start, end + 1))
    else:
        if not base.isdigit():
            return set()
        values = {int(base)}

    normalized: set[int] = set()
    for candidate in values:
        if maximum == 7 and candidate == 7:
            candidate = 0
        if minimum <= candidate <= maximum:
            normalized.add(candidate)
    return normalized


def _download_raw_asset(client: Any, registry_item: IngestionSourceItem) -> bytes:
    """Download raw bytes for a registry item from object storage."""
    bucket = registry_item.raw_bucket or settings.s3_bucket_raw
    key = registry_item.raw_key
    if not key:
        raise ValueError("Registry item is missing raw_key")
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _download_extract_artifact(client: Any, extract_key: str) -> dict[str, Any]:
    """Download and parse a canonical extract artifact from object storage."""
    response = client.get_object(Bucket=settings.s3_bucket_extracts, Key=extract_key)
    raw_payload = response["Body"].read()
    decoded = gzip.decompress(raw_payload).decode("utf-8")
    return json.loads(decoded)


def _write_temp_file(raw_bytes: bytes, file_type: str) -> Path:
    """Write raw bytes to a temporary file for extractor consumption."""
    handle = tempfile.NamedTemporaryFile(suffix=f".{file_type}", delete=False)
    handle.write(raw_bytes)
    handle.flush()
    handle.close()
    return Path(handle.name)


def _upload_extract_artifact(client: Any, source_id, source_type: str, extract_payload: dict[str, Any]) -> str:
    """Upload canonical extract artifact as compressed JSON."""
    key = f"{source_id}.json.gz"
    payload = {
        "source_id": str(source_id),
        "source_type": source_type,
        "extracted_at": datetime.utcnow().isoformat(),
        "extractor_version": EXTRACTION_VERSION,
        **extract_payload,
    }
    body = gzip.compress(json.dumps(payload).encode("utf-8"))
    client.put_object(
        Bucket=settings.s3_bucket_extracts,
        Key=key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    return key


def _get_s3_client() -> Any:
    """Create the default S3 client used by pipeline helpers."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )


def _get_or_create_source_document(
    db: Session,
    *,
    registry_item: IngestionSourceItem,
    file_type: str,
    source_kind: str,
) -> SourceDocument:
    """Return the linked source document for a registry item or create one."""
    if registry_item.source_document_id:
        source_document = db.get(SourceDocument, registry_item.source_document_id)
        if source_document is not None:
            source_document.source_type = registry_item.source_type
            source_document.source_kind = source_kind
            source_document.source_format = file_type
            source_document.title = registry_item.display_name
            source_document.content_sha256 = registry_item.content_sha256
            source_document.file_hash = registry_item.content_sha256
            source_document.s3_raw_key = registry_item.raw_key
            source_document.type_metadata = dict(registry_item.metadata_json)
            return source_document

    source_document = SourceDocument(
        source_type=registry_item.source_type,
        source_kind=source_kind,
        source_format=file_type,
        title=registry_item.display_name,
        language="vi",
        registry_item_id=registry_item.id,
        content_sha256=registry_item.content_sha256,
        file_hash=registry_item.content_sha256,
        s3_raw_key=registry_item.raw_key,
        type_metadata=dict(registry_item.metadata_json),
        total_chunks=0,
        is_active=True,
        extraction_version=EXTRACTION_VERSION,
        chunking_version=CHUNKING_VERSION,
        enrichment_version=ENRICHMENT_VERSION,
        embedding_version=None,
    )
    db.add(source_document)
    db.flush()
    return source_document


def _persist_chunks(
    db: Session,
    *,
    source_document: SourceDocument,
    chunk_payloads: list[dict[str, Any]],
    registry_item: IngestionSourceItem,
    content_profile: str,
    deduplicator: DeduplicatorService,
    enricher: ChunkEnricher | None,
) -> list[ContentChunk]:
    """Persist chunk rows and duplicate markers for extracted content.

    When ``enricher`` is ``None``, all chunks receive ``DEFAULT_METADATA``
    placeholders — enrichment will be applied later via the LLM callback.
    """
    stored_chunks: list[ContentChunk] = []
    for chunk_index, payload in enumerate(chunk_payloads):
        content = payload["content"]
        content_hash = deduplicator.compute_content_hash(content)
        duplicate_of = deduplicator.find_near_duplicate(content)
        source_metadata = _build_chunk_source_metadata(
            chunk_payloads,
            chunk_index=chunk_index,
            registry_item=registry_item,
            source_document=source_document,
            content_profile=content_profile,
            payload=payload,
        )
        if enricher is None or duplicate_of is not None:
            enrichment = dict(ChunkEnricher.DEFAULT_METADATA)
        else:
            enrichment = enricher.enrich(
                content,
                source_metadata=source_metadata,
            )

        locator = _build_locator(payload)
        additive_metadata = {
            "content_profile": content_profile,
            "dharma_concepts": enrichment.get("dharma_concepts") or [],
            "prev_summary": enrichment.get("prev_summary") or "",
            "next_summary": enrichment.get("next_summary") or "",
            "prev_tail_text": source_metadata.get("prev_tail_text") or "",
            "next_head_text": source_metadata.get("next_head_text") or "",
            "enrichment_fallback_used": bool(enrichment.get("enrichment_fallback_used")),
        }
        chunk = ContentChunk(
            source_id=source_document.id,
            chunk_index=chunk_index,
            content=content,
            language="vi",
            token_count=payload.get("token_count"),
            topic=enrichment.get("topic") or "",
            topic_tags=enrichment.get("topic_tags") or [],
            summary=enrichment.get("summary") or "",
            teaching_type=enrichment.get("content_type") or enrichment.get("teaching_type") or "unknown",
            applicable_issues=enrichment.get("applicable_issues") or [],
            depth_level=enrichment.get("depth_level") or "general",
            emotional_tone=enrichment.get("emotional_tone") or "neutral",
            page_start=locator.get("page_start"),
            page_end=locator.get("page_end"),
            section_heading=locator.get("section_heading"),
            paragraph_ids=additive_metadata,
            start_time=locator.get("start_time"),
            end_time=locator.get("end_time"),
            content_hash=content_hash,
            sha256_checksum=content_hash,
            duplicate_of=duplicate_of,
            state="active",
            chunking_version=CHUNKING_VERSION,
            enrichment_version=ENRICHMENT_VERSION,
            embedding_version=None,
            processing_version=get_processing_version(),
        )
        db.add(chunk)
        db.flush()
        stored_chunks.append(chunk)
    return stored_chunks


def _build_chunk_source_metadata(
    chunk_payloads: list[dict[str, Any]],
    *,
    chunk_index: int,
    registry_item: IngestionSourceItem,
    source_document: SourceDocument,
    content_profile: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build branch-specific context passed into enrichment."""
    previous_content = chunk_payloads[chunk_index - 1]["content"] if chunk_index > 0 else ""
    next_content = chunk_payloads[chunk_index + 1]["content"] if chunk_index + 1 < len(chunk_payloads) else ""
    metadata = {
        "source_type": registry_item.source_type,
        CONTENT_PROFILE_KEY: content_profile,
        "title": source_document.title or registry_item.display_name,
        "source_kind": source_document.source_kind,
        "section_heading": payload.get("section_heading") or "",
        "chapter": payload.get("chapter") or _heading_from_content(payload["content"]),
    }
    if content_profile == "transcript":
        metadata["prev_tail_text"] = _tail_text(previous_content)
        metadata["next_head_text"] = _head_text(next_content)
    return metadata


def _profile_uses_llm(db: Session, content_profile: str) -> bool:
    """Return True when pipeline options enable the LLM lane for this profile.

    This gate applies to both:
    - transcript boundary refinement
    - source-aware chunk enrichment
    """
    from services.pipeline_options_service import get_llm_profiles

    return content_profile in get_llm_profiles(db)


def _llm_option_profile_for_pipeline(
    *,
    content_profile: str,
    source_kind: str | None,
) -> str:
    """Return the Pipeline Options profile key for one pipeline item."""
    from services.pipeline_options_service import resolve_llm_option_profile

    return resolve_llm_option_profile(
        content_profile=content_profile,
        source_kind=source_kind,
    )


def _persist_finalized_provenance(
    db: Session,
    *,
    registry_item: IngestionSourceItem,
    source_document: SourceDocument,
    stored_chunks: list[ContentChunk],
    prompt_version: str,
    llm_model: str | None = None,
    llm_model_version: str | None = None,
) -> None:
    """Persist one normalized provenance tuple and non-blocking benchmark contracts.

    ``llm_model`` and ``llm_model_version`` override the default enrichment model
    values — pass ``"skipped"`` when enrichment was bypassed for this profile.
    """
    current_metadata = dict(registry_item.metadata_json)
    enrichment_skipped = llm_model == "skipped"
    provenance = build_provenance_tuple(
        llm_model=llm_model if llm_model is not None else settings.enrichment_model,
        llm_model_version=llm_model_version if llm_model_version is not None else ENRICHMENT_VERSION,
        pipeline_version=get_processing_version(include_enrichment=not enrichment_skipped),
        prompt_version=prompt_version,
    )

    metadata_json = attach_provenance(current_metadata, provenance)
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()

    source_metadata = dict(source_document.type_metadata or {})
    source_metadata[PROVENANCE_KEY] = dict(provenance)
    source_document.type_metadata = source_metadata

    for chunk in stored_chunks:
        additive_metadata = dict(chunk.paragraph_ids or {})
        additive_metadata[PROVENANCE_KEY] = dict(provenance)
        chunk.paragraph_ids = additive_metadata

    db.flush()


def _tail_text(content: str, *, max_words: int = 12) -> str:
    """Return a short tail snippet from one neighboring chunk."""
    words = content.split()
    return " ".join(words[-max_words:]).strip()


def _head_text(content: str, *, max_words: int = 12) -> str:
    """Return a short head snippet from one neighboring chunk."""
    words = content.split()
    return " ".join(words[:max_words]).strip()


def _heading_from_content(content: str) -> str:
    """Return one lightweight heading hint from a chunk body when present."""
    first_line = content.splitlines()[0].strip() if content.strip() else ""
    if len(first_line.split()) <= 12:
        return first_line
    return ""


def _prepare_source_document_for_reprocess(db: Session, source_document: SourceDocument) -> None:
    """Clear prior chunks and pending embedding batches before reprocessing one document."""
    from sqlalchemy import delete as sql_delete

    # Delete via direct SQL to avoid ORM identity-map mismatches that cause
    # "expected N deletes; 0 matched" warnings when the session is stale.
    db.execute(sql_delete(EmbeddingBatch).where(EmbeddingBatch.source_document_id == source_document.id))
    db.execute(sql_delete(ContentChunk).where(ContentChunk.source_id == source_document.id))
    db.flush()


def _resolve_content_profile_for_pipeline(registry_item: IngestionSourceItem) -> str:
    """Return the canonical content profile required before choosing a chunking branch."""
    return require_content_profile(registry_item.metadata_json)


def _resolve_transcript_tier_for_pipeline(
    db: Session,
    registry_item: IngestionSourceItem,
    *,
    extract_payload: dict[str, Any],
    content_profile: str,
    registry_service: IngestionRegistryService,
) -> TranscriptTierDecision | None:
    """Resolve transcript quality tier for transcript follow-up items only."""
    if content_profile != "transcript":
        return None

    decision = classify_transcript_quality(extract_payload)
    previous_tier = registry_item.metadata_json.get(TRANSCRIPT_QUALITY_TIER_KEY)
    metadata_json = dict(registry_item.metadata_json)
    metadata_json[TRANSCRIPT_QUALITY_TIER_KEY] = decision.tier
    metadata_json[TRANSCRIPT_QUALITY_SIGNALS_KEY] = decision.signals
    if previous_tier != decision.tier:
        metadata_json = invalidate_stage_outputs_for_version_change(
            metadata_json,
            changed_fields={"transcript_quality_tier"},
        )
    registry_item.metadata_json = metadata_json
    if previous_tier != decision.tier and registry_item.source_document_id is not None:
        registry_service.mark_reprocess_requested(registry_item)
    else:
        registry_item.updated_at = datetime.utcnow()
        db.flush()
    return decision


def _resolve_chunker_for_profile(
    content_profile: str,
    chunker: AdaptiveChunker | None = None,
    *,
    transcript_quality_tier: str | None = None,
) -> AdaptiveChunker:
    """Return one runtime chunker honoring content-profile policy selection."""
    if chunker is not None:
        return chunker
    if content_profile == "transcript" and transcript_quality_tier is not None:
        transcript_processing_route_for_tier(transcript_quality_tier)
        return AdaptiveChunker(
            content_profile=content_profile,
            **transcript_chunker_kwargs_for_tier(transcript_quality_tier),
        )
    return AdaptiveChunker.for_content_profile(content_profile)


def _prepare_transcript_extract_for_pipeline(
    db: Session,
    registry_item: IngestionSourceItem,
    *,
    extract_payload: dict[str, Any],
    content_profile: str,
    transcript_tier: TranscriptTierDecision | None,
) -> dict[str, Any]:
    """Return one transcript-preprocessed payload for transcript lane only."""
    if content_profile != "transcript" or transcript_tier is None:
        return extract_payload

    prepared: TranscriptPreprocessingResult = preprocess_transcript_extract(
        extract_payload,
        transcript_quality_tier=transcript_tier.tier,
    )
    metadata_json = dict(registry_item.metadata_json)
    metadata_json[TRANSCRIPT_PREPROCESSING_SIGNALS_KEY] = prepared.signals
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()
    db.flush()
    return prepared.payload


def _prepare_profile_extract_for_pipeline(
    db: Session,
    registry_item: IngestionSourceItem,
    *,
    extract_payload: dict[str, Any],
    content_profile: str,
) -> dict[str, Any]:
    """Return one hardened extract for book/sutra lanes, or baseline payload otherwise."""
    hardened = harden_profile_extract(extract_payload, content_profile=content_profile)
    if hardened is None:
        return extract_payload

    metadata_json = dict(registry_item.metadata_json)
    gate_summary = dict(metadata_json.get(PIPELINE_GATE_SUMMARY_KEY) or {})
    gate_summary["chunking"] = hardened.gate_summary
    metadata_json[PIPELINE_GATE_SUMMARY_KEY] = gate_summary
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()
    db.flush()
    return hardened.payload


def _record_pipeline_resume_state(db: Session, registry_item: IngestionSourceItem) -> None:
    """Record the current resume stage marker into registry metadata."""
    metadata_json = dict(registry_item.metadata_json)
    resume_stage = resolve_pipeline_resume_stage(metadata_json)
    metadata_json = set_pipeline_resume_from(metadata_json, resume_stage)
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()
    db.flush()


def _record_pipeline_checkpoint(
    db: Session,
    registry_item: IngestionSourceItem,
    *,
    stage: str,
    gate_summary: dict[str, Any],
) -> None:
    """Persist one pipeline stage checkpoint and standardized gate summary."""
    metadata_json = dict(registry_item.metadata_json)
    metadata_json = set_pipeline_checkpoint(metadata_json, stage)
    metadata_json = merge_gate_summary(metadata_json, stage, gate_summary)
    metadata_json = _apply_runtime_review_markers(metadata_json, gate_summary)
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()
    db.flush()


def _apply_runtime_review_markers(metadata: dict[str, Any], gate_summary: dict[str, Any]) -> dict[str, Any]:
    """Attach review markers and artifact pointers for anomaly/fallback paths."""
    payload = dict(metadata)
    if gate_summary.get("parse_failures", 0) > 0:
        payload = add_review_marker(payload, "parse_failed")
    if gate_summary.get("fallback_count", 0) > 0:
        payload = add_review_marker(payload, "fallback_used")
    if gate_summary.get("anomaly_count", 0) > 0:
        payload = add_review_marker(payload, "anomaly_detected")
    if any(marker in payload.get(PIPELINE_REVIEW_MARKERS_KEY, []) for marker in ["parse_failed", "fallback_used", "anomaly_detected"]):
        payload = add_artifact_pointer(payload, "runtime_review", f"metadata_json.{PIPELINE_REVIEW_MARKERS_KEY}")
        payload = add_artifact_pointer(payload, "gate_summary", f"metadata_json.{PIPELINE_GATE_SUMMARY_KEY}")
    return payload


def _summarize_chunking_stage(
    content_profile: str,
    chunk_payloads: list[dict[str, Any]],
    registry_item: IngestionSourceItem,
) -> dict[str, Any]:
    """Return standardized gate summary for chunking stage."""
    token_counts = [int(payload.get("token_count") or 0) for payload in chunk_payloads]
    existing = dict((registry_item.metadata_json.get(PIPELINE_GATE_SUMMARY_KEY) or {}).get("chunking") or {})
    return {
        "stage": "chunking",
        "branch": content_profile,
        "chunk_count": len(chunk_payloads),
        "min_tokens": min(token_counts) if token_counts else 0,
        "max_tokens": max(token_counts) if token_counts else 0,
        "anomaly_count": int(bool(existing.get("dangling_quote_context") or existing.get("gatha_split_violation"))),
        **existing,
    }


def _summarize_refinement_stage(registry_item: IngestionSourceItem) -> dict[str, Any]:
    """Return standardized gate summary for refinement stage."""
    events = list(registry_item.metadata_json.get(BOUNDARY_REFINEMENT_EVENTS_KEY) or [])
    return {
        "stage": "refinement",
        "parse_failures": sum(1 for event in events if event.get("event") == "parse_failed"),
        "fallback_count": sum(1 for event in events if event.get("event") == "fallback_used"),
        "anomaly_count": sum(
            1
            for event in events
            if event.get("event") in {"merge_anomaly", "chunk_gt_threshold_after_merge"}
        ),
    }


def _summarize_enrichment_stage(stored_chunks: list[ContentChunk]) -> dict[str, Any]:
    """Return standardized gate summary for enrichment stage."""
    fallback_count = 0
    missing_required_fields = 0
    for chunk in stored_chunks:
        payload = dict(chunk.paragraph_ids or {})
        if payload.get("enrichment_fallback_used") is True:
            fallback_count += 1
        if not chunk.summary or not chunk.topic:
            missing_required_fields += 1
    return {
        "stage": "enrichment",
        "fallback_count": fallback_count,
        "parse_failures": fallback_count,
        "missing_required_fields_count": missing_required_fields,
        "anomaly_count": 0,
    }


def _maybe_refine_transcript_chunks(
    db: Session,
    registry_item: IngestionSourceItem,
    *,
    chunk_payloads: list[dict[str, Any]],
    content_profile: str,
    transcript_tier: TranscriptTierDecision | None,
    boundary_refiner: LLMBoundaryRefiner,
) -> list[dict[str, Any]]:
    """Optionally refine transcript candidate chunks with merge-first runtime."""
    tier = transcript_tier.tier if transcript_tier is not None else None
    result = boundary_refiner.refine_chunks(
        chunk_payloads,
        content_profile=content_profile,
        transcript_quality_tier=tier,
    )
    if content_profile != "transcript":
        return chunk_payloads
    metadata_json = dict(registry_item.metadata_json)
    metadata_json[BOUNDARY_REFINEMENT_EVENTS_KEY] = result.events
    metadata_json[BOUNDARY_REFINEMENT_INFO_KEY] = result.info
    registry_item.metadata_json = metadata_json
    registry_item.updated_at = datetime.utcnow()
    db.flush()
    return result.chunks


def _build_locator(payload: dict[str, Any]) -> dict[str, Any]:
    """Map chunk payload fields into schema locator columns."""
    source_segment = payload.get("source_segment")
    return {
        "page_start": source_segment + 1 if isinstance(source_segment, int) else None,
        "page_end": source_segment + 1 if isinstance(source_segment, int) else None,
        "section_heading": None,
        "paragraph_ids": None,
        "start_time": None,
        "end_time": None,
    }


def _infer_file_type(registry_item: IngestionSourceItem) -> str:
    """Infer a normalized file type from registry item metadata."""
    candidates = [
        registry_item.metadata_json.get("extension"),
        _suffix_from_path(registry_item.source_path),
        _suffix_from_path(registry_item.display_name),
    ]
    mime_type = (registry_item.mime_type or "").lower()
    if "pdf" in mime_type:
        candidates.append("pdf")
    elif "wordprocessingml" in mime_type:
        candidates.append("docx")
    elif "epub" in mime_type:
        candidates.append("epub")
    elif "plain" in mime_type:
        candidates.append("txt")
    elif "presentation" in mime_type:
        candidates.append("pptx")

    for candidate in candidates:
        if candidate:
            return str(candidate).lower()
    raise ValueError("Unable to infer file type from registry item")


def _infer_extract_format(registry_item: IngestionSourceItem) -> str:
    """Infer a normalized extract format for transcript-based canonical extracts."""
    candidates = [
        registry_item.metadata_json.get("transcript_format"),
        registry_item.metadata_json.get("extension"),
        _suffix_from_path(registry_item.source_path),
        _suffix_from_path(registry_item.display_name),
    ]
    mime_type = (registry_item.mime_type or "").lower()
    if "audio" in mime_type:
        candidates.append("mp3")
    elif "video" in mime_type:
        candidates.append("mp4")

    for candidate in candidates:
        if candidate:
            return str(candidate).lower()
    return "mp3"


def _source_kind_for_canonical_extract(file_type: str) -> str:
    """Return the correct source_kind for canonical extract processing."""
    return "media_transcript" if file_type in SUPPORTED_MEDIA_FORMATS else "text_document"


def _suffix_from_path(path: str | None) -> str | None:
    if not path or "." not in path:
        return None
    return os.path.basename(path).rsplit(".", 1)[-1].lower()
