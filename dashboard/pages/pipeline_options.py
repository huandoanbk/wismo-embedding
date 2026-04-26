"""Pipeline Options page — per-profile LLM lane controls."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.database import SessionLocal
from services.pipeline_options_service import (
    ALL_PROFILES,
    DEFAULT_LLM_PROFILES,
    get_llm_profiles,
    get_pipeline_options,
    save_pipeline_options,
)

_PROFILE_LABELS = {
    "book": "Book (text-based)",
    "sutra": "Sutra / Scripture",
    "transcript": "Talk / Oral Transcript",
    "audiobook": "Audio Book (media/book)",
}

_PROFILE_HINTS = {
    "book": "Applies to text-based books (`text/book`).",
    "sutra": "Classical language with Pali/Sanskrit terms. Enabling LLM improves topic and concept enrichment.",
    "transcript": "When enabled, the LLM lane runs both boundary refinement and enrichment. When disabled, transcripts skip both LLM steps.",
    "audiobook": "Applies to `media/book`. Allows LLM to be toggled independently for audiobooks, separate from text-based books.",
}


def render_pipeline_options_page(st_module: Any | None = None, *, session_factory=SessionLocal) -> None:
    """Render the Pipeline Options configuration page."""
    if st_module is None:
        import streamlit as st_module  # type: ignore[import-not-found]

    st_module.title("Pipeline Options")
    st_module.caption("Customise pipeline behaviour per content type.")

    db = session_factory()
    should_close = session_factory is SessionLocal
    try:
        current_llm_profiles = get_llm_profiles(db)

        # ------------------------------------------------------------------ #
        # Section: LLM Runtime                                                #
        # ------------------------------------------------------------------ #
        st_module.subheader("LLM Runtime")
        st_module.markdown(
            "Select which content types go through the **LLM lane**. "
            "This setting applies to both **LLM refinement** and **LLM enrichment**. "
            "If a profile is disabled, the pipeline skips all LLM steps for that profile and proceeds directly to embedding."
        )

        selected_profiles: list[str] = []
        for profile in ALL_PROFILES:
            label = _PROFILE_LABELS.get(profile, profile)
            hint = _PROFILE_HINTS.get(profile, "")
            checked = profile in current_llm_profiles
            col_check, col_hint = st_module.columns([1, 4])
            with col_check:
                if st_module.checkbox(label, value=checked, key=f"llm_{profile}"):
                    selected_profiles.append(profile)
            with col_hint:
                st_module.caption(hint)

        # Show delta from default
        if set(selected_profiles) != set(DEFAULT_LLM_PROFILES):
            st_module.info(
                f"Recommended default: **{', '.join(_PROFILE_LABELS[p] for p in DEFAULT_LLM_PROFILES)}**"
            )

        st_module.divider()

        # ------------------------------------------------------------------ #
        # Save                                                                #
        # ------------------------------------------------------------------ #
        col_save, col_reset = st_module.columns([1, 1])
        with col_save:
            if st_module.button("Save configuration", type="primary"):
                opts = dict(get_pipeline_options(db).options_json if get_pipeline_options(db) else {})
                opts["llm_profiles"] = selected_profiles
                save_pipeline_options(db, options=opts)
                db.commit()
                if selected_profiles:
                    st_module.success(
                        f"Saved. LLM lane applies to: "
                        f"**{', '.join(_PROFILE_LABELS.get(p, p) for p in selected_profiles)}**"
                    )
                else:
                    st_module.warning(
                        "Saved. No profiles selected — "
                        "all items will skip refinement/enrichment and go directly to embedding."
                    )
                st_module.rerun()

        with col_reset:
            if st_module.button("Reset to default"):
                opts = dict(get_pipeline_options(db).options_json if get_pipeline_options(db) else {})
                opts["llm_profiles"] = list(DEFAULT_LLM_PROFILES)
                save_pipeline_options(db, options=opts)
                db.commit()
                st_module.success("Reset to default.")
                st_module.rerun()

        # ------------------------------------------------------------------ #
        # Info box                                                            #
        # ------------------------------------------------------------------ #
        with st_module.expander("How does the LLM lane affect the pipeline?"):
            st_module.markdown(
                """
The **LLM lane** currently does 2 things:
- `refinement` — adjusts chunk boundaries for transcripts before enrichment
- `enrichment` — adds metadata fields to each chunk

**LLM enrichment** adds metadata fields such as:
- `topic` — main topic of the passage
- `summary` — short summary (up to 3 sentences)
- `dharma_concepts` — related Buddhist concepts
- `depth_level`, `emotional_tone`, `applicable_issues`

These fields are used to **filter and rerank** search results, not to generate vector embeddings.

**Embedding** always uses the original `chunk.content` — even when enrichment is skipped.
**Audio Book** is configured separately in Pipeline Options:
- `book` applies to `text/book`
- `audiobook` applies to `media/book`
- refinement is currently only available for `transcript`

**Reranker** (BGE-reranker-v2-m3) is the step that makes the biggest difference in retrieval quality.
                """
            )
    finally:
        if should_close:
            db.close()
