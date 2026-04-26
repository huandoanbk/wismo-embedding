"""Queue page for ingestion queue inspection and retry actions."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.dashboard_ops import list_queue_rows, retry_queue_item


def render_queue_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Queue page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.title("Queue")
        col_filter, col_search = st_module.columns([1, 2])
        with col_filter:
            status_filter = st_module.selectbox(
                "Queue Status",
                ["", "pending", "processing", "completed", "failed"],
                index=0,
            )
        with col_search:
            search_query = st_module.text_input(
                "Search (display name / path)",
                placeholder="Leave empty to show 50 most recent",
            )
        rows = list_queue_rows(
            session,
            status=status_filter or None,
            search=search_query.strip() or None,
        )
        st_module.caption(f"{len(rows)} record(s) shown")
        st_module.dataframe(rows, use_container_width=True)

        queue_options = {
            f"{row['queue_kind']}:{row['display_name']} [{row['queue_id']}]": row
            for row in rows
        }
        selected_label = st_module.selectbox("Queue Item", [""] + list(queue_options.keys()), index=0)
        selected_row = queue_options.get(selected_label)
        selected_queue_id = selected_row["queue_id"] if selected_row else ""
        if selected_row:
            with st_module.expander("Queue Detail", expanded=False):
                st_module.json(
                    {
                        "queue_kind": selected_row["queue_kind"],
                        "content_profile": selected_row["content_profile"],
                        "pipeline_checkpoint_stage": selected_row["pipeline_checkpoint_stage"],
                        "pipeline_review_markers": selected_row["pipeline_review_markers"],
                        "pipeline_artifact_pointers": selected_row["pipeline_artifact_pointers"],
                        "error_message": selected_row["error_message"],
                    }
                )
        if st_module.button("Retry Failed Queue Item") and selected_queue_id:
            retry_queue_item(session, selected_queue_id)
            session.commit()
            st_module.success(f"Retried queue item {selected_queue_id}")
    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()
