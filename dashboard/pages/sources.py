"""Sources page for registry and runtime governance operations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.content_profiles import VALID_CONTENT_PROFILES
from core.database import SessionLocal
from services.dashboard_ops import (
    batch_action_by_status,
    delete_source_item,
    list_source_rows,
    rechunk_source_item,
    re_embed_source_item,
    reprocess_source_item,
    retry_source_item,
    set_source_enabled,
)

# Actions that are only valid for items in a failed state.
_RETRY_APPLICABLE_STATUSES = {
    "transcript_failed",
    "extract_artifact_failed",
    "chunking_failed",
    "llm_refinement_failed",
    "llm_enrichment_failed",
    "embedding_failed",
    "failed",
}

# All known pipeline statuses shown in batch-by-status filter.
_ALL_PIPELINE_STATUSES = [
    "awaiting_transcript",
    "transcript_processing",
    "transcript_failed",
    "awaiting_extract_artifact",
    "extract_artifact_processing",
    "extract_artifact_failed",
    "awaiting_chunking",
    "chunking_processing",
    "chunking_failed",
    "awaiting_llm_refinement",
    "llm_refinement_processing",
    "llm_refinement_failed",
    "awaiting_llm_enrichment",
    "llm_enrichment_processing",
    "llm_enrichment_failed",
    "awaiting_embedding",
    "embedding_processing",
    "embedding_failed",
    "completed",
    "failed",
    "duplicate_known",
]


def render_sources_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Sources page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    session = session_factory()
    should_close = session_factory is SessionLocal
    try:
        st_module.title("Sources")

        # ── Source list ───────────────────────────────────────────────────────
        col_filter, col_search = st_module.columns([1, 2])
        with col_filter:
            processing_filter = st_module.selectbox(
                "Filter by Status",
                [""] + _ALL_PIPELINE_STATUSES,
                index=0,
            )
        with col_search:
            search_query = st_module.text_input(
                "Search (display name / path)",
                placeholder="Leave empty to show 50 most recent",
            )
        rows = list_source_rows(
            session,
            processing_status=processing_filter or None,
            search=search_query.strip() or None,
        )
        st_module.caption(f"{len(rows)} record(s) shown")
        st_module.dataframe(rows, use_container_width=True)

        # ── Individual source action ──────────────────────────────────────────
        st_module.subheader("Source Action")
        source_options = {
            f"{row['display_name']} [{row['source_item_id']}] ({row['content_profile']})": row
            for row in rows
        }
        selected_label = st_module.selectbox("Source", [""] + list(source_options.keys()), index=0)
        selected_row = source_options.get(selected_label)
        selected_source_item_id = selected_row["source_item_id"] if selected_row else ""

        if selected_row:
            with st_module.expander("Source Detail", expanded=False):
                st_module.json(
                    {
                        "content_profile": selected_row["content_profile"],
                        "pipeline_review_markers": selected_row["pipeline_review_markers"],
                        "pipeline_checkpoint_stage": selected_row["pipeline_checkpoint_stage"],
                        "pipeline_provenance": selected_row["pipeline_provenance"],
                        "pipeline_gate_summary": selected_row["pipeline_gate_summary"],
                    }
                )

        single_action = st_module.selectbox(
            "Action",
            ["reprocess", "rechunk", "retry", "re_embed", "enable", "disable", "delete"],
            index=0,
        )

        single_profile_override: str | None = None
        if single_action == "reprocess":
            use_override = st_module.checkbox("Override content profile", value=False)
            if use_override:
                single_profile_override = st_module.selectbox(
                    "New content profile",
                    sorted(VALID_CONTENT_PROFILES),
                    index=0,
                )
            st_module.caption(
                "Re-process resets the item to its initial state: all chunks, embeddings, "
                "queue rows, and the source document are deleted before re-enqueueing."
            )
        elif single_action == "retry":
            st_module.caption(
                "Retry re-runs only the failed step: "
                "LLM enrichment for llm_enrichment_failed, "
                "missing-chunk embedding for failed (embedding)."
            )
        elif single_action == "rechunk":
            st_module.caption(
                "Re Chunk keeps the canonical extract artifact and restarts the pipeline from awaiting_chunking."
            )
        elif single_action == "re_embed":
            st_module.caption(
                "Re-embedding deletes all existing embeddings for this source "
                "and creates a new batch covering every chunk."
            )
        elif single_action == "delete":
            st_module.warning("Delete permanently removes the source and all derived data. This cannot be undone.")

        if st_module.button("Apply to Selected Source") and selected_source_item_id:
            _apply_single_action(
                session,
                source_item_id=selected_source_item_id,
                action=single_action,
                content_profile=single_profile_override,
            )
            session.commit()
            st_module.success(f"Applied '{single_action}' to {selected_source_item_id}")

        # ── Batch action by status ────────────────────────────────────────────
        st_module.subheader("Batch Action by Status")
        batch_status = st_module.selectbox(
            "Apply to all sources with status",
            _ALL_PIPELINE_STATUSES,
            index=0,
        )
        batch_actions = ["reprocess", "rechunk", "enable", "disable", "delete"]
        if batch_status in _RETRY_APPLICABLE_STATUSES:
            batch_actions.insert(1, "retry")
        batch_action = st_module.selectbox(
            "Batch Action",
            batch_actions,
            index=0,
            key="batch_action",
        )

        batch_profile_override: str | None = None
        if batch_action == "reprocess":
            use_batch_override = st_module.checkbox("Override content profile (batch)", value=False)
            if use_batch_override:
                batch_profile_override = st_module.selectbox(
                    "New content profile (batch)",
                    sorted(VALID_CONTENT_PROFILES),
                    index=0,
                )
        elif batch_action == "retry":
            st_module.caption(f"Retry will re-run only the failed step for status `{batch_status}`.")
        elif batch_action == "delete":
            st_module.warning(
                f"This will permanently delete ALL sources with status '{batch_status}'. "
                "This cannot be undone."
            )

        if st_module.button(f"Apply '{batch_action}' to ALL '{batch_status}' sources"):
            result = batch_action_by_status(
                session,
                status=batch_status,
                action=batch_action,
                content_profile=batch_profile_override,
            )
            session.commit()
            st_module.success(
                f"Batch complete — {result['success']}/{result['total']} succeeded."
            )
            if result["errors"]:
                st_module.error(f"{len(result['errors'])} error(s):")
                for err in result["errors"]:
                    st_module.text(f"  {err['source_item_id']}: {err['error']}")

    except Exception:
        session.rollback()
        raise
    finally:
        if should_close:
            session.close()


def _apply_single_action(
    session: Any,
    *,
    source_item_id: str,
    action: str,
    content_profile: str | None,
) -> None:
    """Dispatch one source action. Raises on unknown action or invalid state."""
    if action == "reprocess":
        reprocess_source_item(session, source_item_id, content_profile=content_profile)
    elif action == "rechunk":
        rechunk_source_item(session, source_item_id)
    elif action == "retry":
        retry_source_item(session, source_item_id)
    elif action == "re_embed":
        re_embed_source_item(session, source_item_id)
    elif action == "enable":
        set_source_enabled(session, source_item_id, True)
    elif action == "disable":
        set_source_enabled(session, source_item_id, False)
    elif action == "delete":
        delete_source_item(session, source_item_id)
    else:
        raise ValueError(f"unknown_action:{action}")
