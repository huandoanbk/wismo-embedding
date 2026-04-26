"""Dashboard page for DB-backed pipeline stage scheduling and manual triggers."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.pipeline_scheduler_ops import (
    SCHEDULER_STAGES,
    get_all_scheduler_stage_configs,
    run_scheduler_stage,
    upsert_scheduler_stage_config,
)

_LABELS = {
    "scan_source_configs": "Scan Source Configs",
    "dispatch_audio": "Dispatch Audio",
    "dispatch_extract_artifact": "Dispatch Extract Artifact",
    "process_text_queue": "Dispatch Chunking",
    "dispatch_llm": "Dispatch LLM",
    "dispatch_embedding": "Dispatch Embedding",
    "auto_scale_gpu": "Auto Scale GPU",
}


def render_automation_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render scheduler config and manual controls for pipeline stages."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("Automation")
    st_module.caption(
        "Each stage has its own cron for Celery Beat and a manual run button. "
        "Manual and auto triggers share the same Redis lock to prevent double-fire."
    )
    st_module.caption("`Scan Source Configs` runs against every enabled source config and checks each connector for newly discovered files.")

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        configs = {cfg.stage: cfg for cfg in get_all_scheduler_stage_configs(session)}
        for stage in SCHEDULER_STAGES:
            cfg = configs[stage]
            st_module.subheader(_LABELS.get(stage, stage))
            with st_module.form(f"scheduler_form_{stage}"):
                is_enabled = st_module.selectbox(
                    "Enabled",
                    options=[True, False],
                    index=0 if cfg.is_enabled else 1,
                    format_func=lambda value: "Yes" if value else "No",
                    key=f"scheduler_enabled_{stage}",
                )
                cron_expression = st_module.text_input(
                    "Cron expression (UTC)",
                    value=cfg.cron_expression,
                    key=f"scheduler_cron_{stage}",
                    help="Example: */5 * * * * or 0 * * * *",
                )
                batch_size = None
                if stage != "auto_scale_gpu":
                    batch_size = st_module.number_input(
                        "Batch size",
                        min_value=1,
                        max_value=1000,
                        value=int(cfg.batch_size or 20),
                        step=1,
                        key=f"scheduler_batch_{stage}",
                    )

                col_save, col_run = st_module.columns(2)
                with col_save:
                    save = st_module.form_submit_button("Save", type="primary")
                with col_run:
                    run_now = st_module.form_submit_button("Run Now")

                if save:
                    upsert_scheduler_stage_config(
                        session,
                        stage,
                        cron_expression=cron_expression.strip() or None,
                        is_enabled=bool(is_enabled),
                        batch_size=int(batch_size) if batch_size is not None else None,
                    )
                    st_module.success(f"Saved scheduler config for stage '{stage}'.")
                    st_module.rerun()

                if run_now:
                    result = run_scheduler_stage(session, stage)
                    if result["status"] == "completed":
                        st_module.success(f"Stage '{stage}' completed.")
                    elif result["status"] == "locked":
                        st_module.warning(f"Stage '{stage}' is already running from another trigger.")
                    else:
                        st_module.error(f"Stage '{stage}' error: {result.get('error', 'unknown_error')}")
                    st_module.write(result)

            st_module.caption(
                f"Last run: {cfg.last_run_at or '-'} | Last status: {cfg.last_run_status or '-'} | Source: {'DB' if cfg.from_db else 'default'}"
            )
            st_module.divider()
    finally:
        if should_close:
            session.close()
