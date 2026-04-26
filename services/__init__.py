"""Service package for text pipeline helpers."""

from .dedup import DeduplicatorService
from .vastai_manager import VastAIManager
from .worker_alerts import (
    WorkerAlertPayload,
    WorkerAlertStatusUpdate,
    emit_local_worker_alert,
    ingest_worker_alert,
    list_worker_alerts,
    update_worker_alert_status,
)

__all__ = [
    "DeduplicatorService",
    "VastAIManager",
    "WorkerAlertPayload",
    "WorkerAlertStatusUpdate",
    "emit_local_worker_alert",
    "ingest_worker_alert",
    "list_worker_alerts",
    "update_worker_alert_status",
]
