"""Dashboard page for per-lane Vast.ai search filter configuration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.vastai_lane_config_ops import LANES, get_all_lane_configs, upsert_lane_config

_LANE_LABELS = {
    "whisper": "Whisper (transcription)",
    "embedding": "Embedding (BGE-M3)",
    "llm": "LLM (shared refine + enrich)",
}


def render_vastai_lane_config_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Vast.ai per-lane filter configuration page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("Vast.ai Lane Config")
    st_module.caption("Customise autoscale and GPU filter per lane. Leave blank to use the default values from settings.")

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        configs = {cfg.lane: cfg for cfg in get_all_lane_configs(session)}

        for lane in LANES:
            cfg = configs[lane]
            label = _LANE_LABELS.get(lane, lane)
            st_module.subheader(f"{label}")
            if cfg.from_db:
                st_module.caption("Using custom values from DB.")
            else:
                st_module.caption("Using default values from settings.")

            with st_module.form(key=f"form_{lane}"):
                search_filter = st_module.text_area(
                    "Search filter",
                    value=cfg.search_filter,
                    height=80,
                    help="Vast.ai search filter string, e.g. verified=true cuda_max_good>=12.1 gpu_ram>=16 ...",
                    key=f"filter_{lane}",
                )
                col1, col2 = st_module.columns(2)
                with col1:
                    disk_gb = st_module.number_input(
                        "Disk (GB)",
                        min_value=10,
                        max_value=500,
                        value=cfg.disk_gb,
                        step=10,
                        key=f"disk_{lane}",
                    )
                with col2:
                    max_hourly_cost = st_module.number_input(
                        "Max hourly cost (USD)",
                        min_value=0.05,
                        max_value=10.0,
                        value=cfg.max_hourly_cost,
                        step=0.05,
                        format="%.2f",
                        key=f"cost_{lane}",
                    )
                col_capacity, col3, col4 = st_module.columns(3)
                with col_capacity:
                    max_instances = st_module.number_input(
                        "Max instances",
                        min_value=1,
                        max_value=50,
                        value=cfg.max_instances,
                        step=1,
                        key=f"max_instances_{lane}",
                    )
                with col3:
                    destroy_idle_enabled = st_module.selectbox(
                        "Destroy idle instance",
                        options=[True, False],
                        index=0 if cfg.destroy_idle_enabled else 1,
                        format_func=lambda value: "Yes" if value else "No",
                        key=f"destroy_idle_{lane}",
                    )
                with col4:
                    idle_timeout_minutes = st_module.number_input(
                        "Idle timeout (minutes)",
                        min_value=1,
                        max_value=720,
                        value=cfg.idle_timeout_minutes,
                        step=5,
                        key=f"idle_timeout_{lane}",
                    )

                col_save, col_reset = st_module.columns([1, 1])
                with col_save:
                    submitted = st_module.form_submit_button("Save", type="primary")
                with col_reset:
                    reset = st_module.form_submit_button("Reset to default", type="secondary")

                if submitted:
                    upsert_lane_config(
                        session,
                        lane,
                        search_filter=search_filter.strip() or None,
                        disk_gb=int(disk_gb),
                        max_hourly_cost=float(max_hourly_cost),
                        max_instances=int(max_instances),
                        destroy_idle_enabled=bool(destroy_idle_enabled),
                        idle_timeout_minutes=int(idle_timeout_minutes),
                    )
                    st_module.success(f"Saved config for lane '{lane}'.")
                    st_module.rerun()

                if reset:
                    upsert_lane_config(
                        session,
                        lane,
                        search_filter=None,
                        disk_gb=None,
                        max_hourly_cost=None,
                        max_instances=None,
                        destroy_idle_enabled=None,
                        idle_timeout_minutes=None,
                    )
                    st_module.success(f"Reset lane '{lane}' to default.")
                    st_module.rerun()

            st_module.divider()

    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()
