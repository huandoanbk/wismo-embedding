"""Vast.ai page for worker visibility and guarded actions."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.dashboard_ops import destroy_vastai_instance, launch_vastai_instance, list_vastai_rows
from services.worker_images import get_worker_image_lane_status


def render_vastai_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Vast.ai page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal

    st_module.title("Vast.ai")
    try:
        rows = list_vastai_rows()
    except Exception as exc:
        st_module.error(f"Could not fetch Vast.ai instances: {exc}")
        rows = []
    try:
        lane_rows = [
            get_worker_image_lane_status(session, "whisper"),
            get_worker_image_lane_status(session, "embedding"),
            get_worker_image_lane_status(session, "llm"),
        ]
        st_module.subheader("Lane Launch Readiness")
        st_module.dataframe(lane_rows, width="stretch")
        st_module.subheader("Running Instances")
        st_module.dataframe(rows, width="stretch")

        mode = st_module.selectbox(
            "Worker Mode",
            ["whisper", "embedding", "llm"],
            index=0,
        )
        if st_module.button("Launch Cheapest Instance"):
            instance_id = launch_vastai_instance(mode=mode)
            st_module.success(f"Launched instance {instance_id}")

        destroy_instance_id = st_module.text_input("Destroy Instance ID")
        if st_module.button("Destroy Instance") and destroy_instance_id:
            destroy_vastai_instance(destroy_instance_id)
            st_module.success(f"Destroyed instance {destroy_instance_id}")
    finally:
        if should_close:
            session.close()
