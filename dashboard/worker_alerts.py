"""Worker alerts dashboard page."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.database import SessionLocal
from services.worker_alerts import list_worker_alerts, update_worker_alert_status


def fetch_worker_alert_rows(
    *,
    severity: str | None = None,
    source: str | None = None,
    status: str | None = None,
    session_factory=SessionLocal,
) -> list[dict[str, Any]]:
    """Return worker alerts formatted for dashboard rendering."""
    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        alerts = list_worker_alerts(session, severity=severity, source=source, status=status)
        rows: list[dict[str, Any]] = []
        for alert in alerts:
            rows.append(
                {
                    "id": str(alert.id),
                    "severity": alert.severity,
                    "event_type": alert.event_type,
                    "source": alert.source,
                    "status": alert.status,
                    "occurrence_count": alert.occurrence_count,
                    "last_seen_at": alert.last_seen_at.isoformat(),
                    "instance_id": alert.instance_id,
                    "task_id": alert.task_id,
                    "source_item_id": str(alert.source_item_id) if alert.source_item_id else None,
                }
            )
        return rows
    finally:
        if should_close:
            session.close()


def apply_alert_status_action(
    alert_id: UUID | str,
    *,
    status: str,
    resolved_by: str = "dashboard",
    session_factory=SessionLocal,
) -> None:
    """Apply a resolve or ignore action from the dashboard."""
    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        update_worker_alert_status(session, alert_id, status=status, resolved_by=resolved_by)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()


def render_worker_alerts_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Streamlit page for worker alert ops review."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("Worker Alerts")
    severity = st_module.selectbox("Severity", ["", "info", "warning", "error", "critical"], index=0)
    source = st_module.text_input("Source")
    status = st_module.selectbox("Status", ["", "unresolved", "resolved", "ignored"], index=0)

    rows = fetch_worker_alert_rows(
        severity=severity or None,
        source=source or None,
        status=status or None,
        session_factory=session_factory,
    )
    st_module.dataframe(rows, width="stretch")

    selected_alert_id = st_module.text_input("Alert ID")
    action = st_module.selectbox("Action", ["resolved", "ignored"], index=0)
    resolved_by = st_module.text_input("Resolved By", value="dashboard")
    if st_module.button("Apply Action") and selected_alert_id:
        apply_alert_status_action(
            selected_alert_id,
            status=action,
            resolved_by=resolved_by,
            session_factory=session_factory,
        )
        st_module.success(f"Updated alert {selected_alert_id} to {action}")
