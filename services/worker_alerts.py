"""Operational alert service for GPU worker observability."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from core.config import settings
from core.database import SessionLocal
from core.models import WorkerAlert

AlertSeverity = Literal["info", "warning", "error", "critical"]
AlertStatus = Literal["unresolved", "resolved", "ignored"]


class WorkerAlertPayload(BaseModel):
    """Validated worker alert payload used by internal API and local emitters."""

    severity: AlertSeverity
    event_type: str
    source: str
    occurred_at: datetime
    instance_id: str | None = None
    task_id: str | None = None
    source_item_id: UUID | None = None
    chunk_ids: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type", "source")
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be blank")
        return normalized

    def details_json(self) -> dict[str, Any]:
        """Return the JSON payload persisted in the alert history row."""
        return {
            "chunk_ids": list(self.chunk_ids),
            "details": self.details,
        }

    def fingerprint(self) -> str:
        """Create a stable fingerprint for short-window deduplication."""
        details = self.details
        message = (
            details.get("message")
            or details.get("error")
            or details.get("reason")
            or details.get("stage")
            or ""
        )
        payload = {
            "source": self.source,
            "event_type": self.event_type,
            "instance_id": self.instance_id or "",
            "task_id": self.task_id or "",
            "source_item_id": str(self.source_item_id) if self.source_item_id else "",
            "message": str(message).strip(),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


class WorkerAlertStatusUpdate(BaseModel):
    """State transition payload used by dashboard actions."""

    status: Literal["resolved", "ignored", "unresolved"]
    resolved_by: str | None = None


@dataclass
class WorkerAlertIngestResult:
    """Result of alert ingest after deduplication rules are applied."""

    action: Literal["created", "throttled"]
    alert: WorkerAlert


def _base_query() -> Select[tuple[WorkerAlert]]:
    return select(WorkerAlert)


def ingest_worker_alert(db: Session, payload: WorkerAlertPayload) -> WorkerAlertIngestResult:
    """Persist a worker alert or collapse it into an active alert row."""
    fingerprint = payload.fingerprint()
    window_start = payload.occurred_at - timedelta(minutes=settings.worker_alert_throttle_window_minutes)
    existing = db.execute(
        _base_query()
        .where(
            WorkerAlert.fingerprint == fingerprint,
            WorkerAlert.status == "unresolved",
            WorkerAlert.last_seen_at >= window_start,
        )
        .order_by(WorkerAlert.last_seen_at.desc())
    ).scalar_one_or_none()

    if existing is not None and existing.severity == payload.severity:
        existing.occurrence_count += 1
        existing.last_seen_at = payload.occurred_at
        existing.details_json = payload.details_json()
        existing.instance_id = payload.instance_id
        existing.task_id = payload.task_id
        existing.source_item_id = payload.source_item_id
        existing.updated_at = datetime.utcnow()
        db.flush()
        return WorkerAlertIngestResult(action="throttled", alert=existing)

    alert = WorkerAlert(
        severity=payload.severity,
        event_type=payload.event_type,
        source=payload.source,
        status="unresolved",
        fingerprint=fingerprint,
        occurrence_count=1,
        occurred_at=payload.occurred_at,
        first_seen_at=payload.occurred_at,
        last_seen_at=payload.occurred_at,
        instance_id=payload.instance_id,
        task_id=payload.task_id,
        source_item_id=payload.source_item_id,
        details_json=payload.details_json(),
    )
    db.add(alert)
    db.flush()
    return WorkerAlertIngestResult(action="created", alert=alert)


def list_worker_alerts(
    db: Session,
    *,
    severity: str | None = None,
    source: str | None = None,
    status: str | None = None,
) -> list[WorkerAlert]:
    """Return worker alerts sorted by most recent sighting first."""
    stmt = _base_query().order_by(WorkerAlert.last_seen_at.desc(), WorkerAlert.created_at.desc())
    if severity:
        stmt = stmt.where(WorkerAlert.severity == severity)
    if source:
        stmt = stmt.where(WorkerAlert.source == source)
    if status:
        stmt = stmt.where(WorkerAlert.status == status)
    return list(db.execute(stmt).scalars())


def update_worker_alert_status(
    db: Session,
    alert_id: UUID | str,
    *,
    status: AlertStatus,
    resolved_by: str | None = None,
) -> WorkerAlert:
    """Update alert lifecycle state from the ops surface."""
    alert = db.get(WorkerAlert, UUID(str(alert_id)))
    if alert is None:
        raise ValueError("worker_alert_not_found")

    alert.status = status
    alert.updated_at = datetime.utcnow()
    if status in {"resolved", "ignored"}:
        alert.resolved_at = datetime.utcnow()
        alert.resolved_by = resolved_by
    else:
        alert.resolved_at = None
        alert.resolved_by = None
    db.flush()
    return alert


def emit_local_worker_alert(payload: WorkerAlertPayload) -> WorkerAlertIngestResult:
    """Persist a worker alert from a local service without going through HTTP."""
    session = SessionLocal()
    try:
        result = ingest_worker_alert(session, payload)
        session.commit()
        session.refresh(result.alert)
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
