"""CRUD and execution helpers for DB-backed pipeline stage scheduling."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import redis
from sqlalchemy.orm import Session

from core.config import settings
from core.models import PipelineSchedulerConfig
from services import VastAIManager

STAGE_SCAN = "scan_source_configs"
STAGE_AUDIO = "dispatch_audio"
STAGE_EXTRACT_ARTIFACT = "dispatch_extract_artifact"
STAGE_TEXT = "process_text_queue"
STAGE_LLM = "dispatch_llm"
STAGE_EMBEDDING = "dispatch_embedding"
STAGE_AUTOSCALE = "auto_scale_gpu"

SCHEDULER_STAGES = (
    STAGE_SCAN,
    STAGE_AUDIO,
    STAGE_EXTRACT_ARTIFACT,
    STAGE_TEXT,
    STAGE_LLM,
    STAGE_EMBEDDING,
    STAGE_AUTOSCALE,
)

_DEFAULTS: dict[str, dict[str, Any]] = {
    STAGE_SCAN: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_AUDIO: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_EXTRACT_ARTIFACT: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_TEXT: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_LLM: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_EMBEDDING: {"cron_expression": "* * * * *", "batch_size": 20, "is_enabled": True},
    STAGE_AUTOSCALE: {"cron_expression": "*/5 * * * *", "batch_size": None, "is_enabled": True},
}

_LOCK_TTLS: dict[str, int] = {
    STAGE_SCAN: 300,
    STAGE_AUDIO: 300,
    STAGE_EXTRACT_ARTIFACT: 1800,
    STAGE_TEXT: 1800,
    STAGE_LLM: 1800,
    STAGE_EMBEDDING: 1800,
    STAGE_AUTOSCALE: 300,
}


@dataclass
class SchedulerStageConfig:
    """Resolved scheduler config for one pipeline stage."""

    stage: str
    cron_expression: str
    is_enabled: bool
    batch_size: int | None
    last_run_at: datetime | None
    last_run_status: str | None
    from_db: bool


def _redis_client() -> redis.Redis:
    """Return Redis client used for lightweight stage locks."""
    return redis.from_url(settings.redis_url)


def get_scheduler_stage_config(db: Session, stage: str) -> SchedulerStageConfig:
    """Return resolved config for one stage, falling back to defaults."""
    if stage not in SCHEDULER_STAGES:
        raise ValueError(f"unknown_scheduler_stage:{stage}")
    defaults = _DEFAULTS[stage]
    row = db.query(PipelineSchedulerConfig).filter_by(stage=stage).first()
    if row is None:
        return SchedulerStageConfig(
            stage=stage,
            cron_expression=defaults["cron_expression"],
            is_enabled=defaults["is_enabled"],
            batch_size=defaults["batch_size"],
            last_run_at=None,
            last_run_status=None,
            from_db=False,
        )
    return SchedulerStageConfig(
        stage=stage,
        cron_expression=row.cron_expression or defaults["cron_expression"],
        is_enabled=row.is_enabled,
        batch_size=row.batch_size if row.batch_size is not None else defaults["batch_size"],
        last_run_at=row.last_run_at,
        last_run_status=row.last_run_status,
        from_db=True,
    )


def get_all_scheduler_stage_configs(db: Session) -> list[SchedulerStageConfig]:
    """Return resolved config for all pipeline stages."""
    return [get_scheduler_stage_config(db, stage) for stage in SCHEDULER_STAGES]


def upsert_scheduler_stage_config(
    db: Session,
    stage: str,
    *,
    cron_expression: str | None = None,
    is_enabled: bool | None = None,
    batch_size: int | None = None,
) -> PipelineSchedulerConfig:
    """Create or update scheduler config for one stage."""
    if stage not in SCHEDULER_STAGES:
        raise ValueError(f"unknown_scheduler_stage:{stage}")
    row = db.query(PipelineSchedulerConfig).filter_by(stage=stage).first()
    if row is None:
        row = PipelineSchedulerConfig(stage=stage)
        db.add(row)
    row.cron_expression = cron_expression
    row.is_enabled = True if is_enabled is None else is_enabled
    row.batch_size = batch_size
    db.commit()
    db.refresh(row)
    return row


def mark_scheduler_stage_run(db: Session, stage: str, status: str) -> None:
    """Persist last-run metadata for one stage."""
    row = db.query(PipelineSchedulerConfig).filter_by(stage=stage).first()
    if row is None:
        row = PipelineSchedulerConfig(stage=stage)
        db.add(row)
    row.last_run_at = datetime.utcnow()
    row.last_run_status = status
    db.flush()


def try_acquire_scheduler_stage_lock(stage: str) -> bool:
    """Acquire a lightweight Redis lock for one stage."""
    if stage not in SCHEDULER_STAGES:
        raise ValueError(f"unknown_scheduler_stage:{stage}")
    return bool(
        _redis_client().set(
            f"pipeline_scheduler:lock:{stage}",
            datetime.utcnow().isoformat(),
            nx=True,
            ex=_LOCK_TTLS[stage],
        )
    )


def release_scheduler_stage_lock(stage: str) -> None:
    """Release a lightweight Redis lock for one stage."""
    _redis_client().delete(f"pipeline_scheduler:lock:{stage}")


def run_locked_scheduler_stage(
    db: Session,
    stage: str,
    callback: Callable[[], Any],
) -> dict[str, Any]:
    """Run one stage callback under a shared Redis lock.

    This is used by both Celery Beat and manual dashboard triggers so the same
    stage cannot double-fire when an operator clicks while the scheduled tick is
    already dispatching work.
    """
    if not try_acquire_scheduler_stage_lock(stage):
        return {"stage": stage, "status": "locked"}

    try:
        result = callback()
        mark_scheduler_stage_run(db, stage, "completed")
        db.commit()
        return {"stage": stage, "status": "completed", "result": result}
    except Exception as exc:
        db.rollback()
        mark_scheduler_stage_run(db, stage, "failed")
        db.commit()
        return {"stage": stage, "status": "failed", "error": str(exc)}
    finally:
        release_scheduler_stage_lock(stage)


def run_scheduler_stage(db: Session, stage: str) -> dict[str, Any]:
    """Execute one configured stage with a shared manual/auto lock."""
    cfg = get_scheduler_stage_config(db, stage)
    return run_locked_scheduler_stage(db, stage, lambda: _run_stage_body(db, stage, cfg.batch_size))


def _run_stage_body(db: Session, stage: str, batch_size: int | None) -> Any:
    """Dispatch the concrete implementation for one stage."""
    from services.dashboard_ops import (
        dispatch_audio_jobs,
        dispatch_extract_artifact_jobs,
        dispatch_llm_jobs,
        process_embedding_queue,
        process_text_queue,
    )
    from tasks import app as embedding_celery_app, scan_scheduled_source_configs, vastai_app

    if stage == STAGE_SCAN:
        return scan_scheduled_source_configs(batch_size=batch_size or 20, celery_app=embedding_celery_app)
    if stage == STAGE_AUDIO:
        return dispatch_audio_jobs(db, batch_size=batch_size or 20, celery_app=vastai_app)
    if stage == STAGE_EXTRACT_ARTIFACT:
        return {"processed": dispatch_extract_artifact_jobs(batch_size=batch_size or 20)}
    if stage == STAGE_TEXT:
        return {"processed": process_text_queue(batch_size=batch_size or 20)}
    if stage == STAGE_LLM:
        return dispatch_llm_jobs(
            db,
            batch_size=batch_size or 20,
            celery_app=vastai_app,
            api_celery_app=embedding_celery_app,
        )
    if stage == STAGE_EMBEDDING:
        return process_embedding_queue(db, batch_size=batch_size or 20, celery_app=vastai_app)
    if stage == STAGE_AUTOSCALE:
        VastAIManager().auto_scale()
        return {"autoscaled": True}
    raise ValueError(f"unknown_scheduler_stage:{stage}")
