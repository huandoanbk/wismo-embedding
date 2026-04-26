"""Overview page for embedding control plane metrics."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.dashboard_ops import get_overview_metrics


def render_overview_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the overview page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        metrics = get_overview_metrics(session)
    finally:
        if should_close:
            session.close()

    st_module.title("Overview")
    columns = st_module.columns(4)
    ordered_metrics = [
        ("Source Items", metrics["source_items"]),
        ("Source Configs", metrics["source_configs"]),
        ("Source Documents", metrics["source_documents"]),
        ("Active Sources", metrics["active_sources"]),
        ("Chunks", metrics["chunks"]),
        ("Embeddings", metrics["embeddings"]),
        ("Queue Pending", metrics["queue_pending"]),
        ("Queue Failed", metrics["queue_failed"]),
        ("Unresolved Alerts", metrics["worker_alerts_unresolved"]),
        ("Vast.ai Running", metrics["vastai_running_instances"]),
    ]
    for index, (label, value) in enumerate(ordered_metrics):
        columns[index % len(columns)].metric(label, value)
