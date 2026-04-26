"""Upstash / Celery task queue management page."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.upstash_queue_ops import MANAGED_QUEUES, get_broker_url, get_queue_stats, purge_queue, remove_task


def render_upstash_queue_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Upstash/Celery task queue management page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("Task Queue (Upstash)")

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.button("Refresh")  # triggers rerun on click

        # Show which broker we're connected to
        broker_url, is_upstash = get_broker_url()
        display_url = broker_url[:40] + "…" if len(broker_url) > 40 else broker_url
        if not is_upstash:
            st_module.warning(
                f"⚠️ `VASTAI_REDIS_URL` is not configured — connected to local Redis: `{display_url}`. "
                "Upstash queues will not be visible."
            )
        else:
            st_module.caption(f"Broker: `{display_url}`")

        try:
            queue_stats = get_queue_stats(session)
        except Exception as exc:
            st_module.error(f"Cannot connect to Redis/Upstash: {exc}")
            return

        # --- Summary metrics ---
        cols = st_module.columns(len(MANAGED_QUEUES))
        for col, stat in zip(cols, queue_stats):
            col.metric(label=stat["queue"], value=stat["pending"])

        st_module.divider()

        # --- Per-queue detail ---
        for stat in queue_stats:
            queue_name = stat["queue"]
            tasks: list[dict[str, Any]] = stat["tasks"]
            header = f"{queue_name} — {stat['pending']} pending"

            with st_module.expander(header, expanded=stat["pending"] > 0):
                if stat["pending"] == 0:
                    st_module.info("Queue is empty.")
                    continue

                if not tasks and stat["pending"] > 0:
                    st_module.error(
                        f"Read {stat['pending']} messages from Redis but could not decode them. "
                        "Message format does not match Celery protocol v2."
                    )
                    continue

                # Surface decode errors
                decode_errors = [t for t in tasks if t.get("decode_error")]
                ok_tasks = [t for t in tasks if not t.get("decode_error")]

                if decode_errors:
                    st_module.warning(
                        f"{len(decode_errors)} message(s) could not be decoded: "
                        f"{decode_errors[0]['decode_error']}"
                    )
                    with st_module.expander("Raw message (debug)", expanded=False):
                        st_module.code(decode_errors[0]["raw"])

                if not ok_tasks:
                    continue

                # Task table
                st_module.dataframe(
                    [
                        {
                            "file": t["display_name"],
                            "source_path": t["source_path"] or "",
                            "task": t["task_name"],
                            "task_id": t["task_id"],
                            "source_item_id": t["source_item_id"] or "",
                            "batch_id": t["batch_id"] or "",
                        }
                        for t in ok_tasks
                    ],
                    width="stretch",
                )

                st_module.markdown("**Actions**")

                # Remove one task
                task_options = {
                    f"{t['display_name']}  [{t['task_name']}]  ({t['task_id'][:8]}…)": t
                    for t in ok_tasks
                }
                selected_label = st_module.selectbox(
                    "Select task to remove",
                    [""] + list(task_options.keys()),
                    index=0,
                    key=f"select_{queue_name}",
                )
                selected_task = task_options.get(selected_label)

                col1, col2 = st_module.columns([1, 1])
                with col1:
                    if st_module.button("Remove selected task", key=f"remove_{queue_name}"):
                        if selected_task:
                            ok = remove_task(queue_name, selected_task["raw"])
                            if ok:
                                st_module.success(f"Removed: {selected_task['display_name']}")
                            else:
                                st_module.warning(
                                    "Task not found — may have already been picked up by a worker."
                                )
                        else:
                            st_module.warning("Select a task first.")

                with col2:
                    if st_module.button(
                        f"Purge entire '{queue_name}' queue",
                        key=f"purge_{queue_name}",
                        type="secondary",
                    ):
                        count = purge_queue(queue_name)
                        st_module.success(f"Purged {count} tasks from '{queue_name}'.")

    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()
