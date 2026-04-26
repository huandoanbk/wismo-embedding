"""Pipeline options CRUD — singleton config for per-profile LLM lane behaviour."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import PipelineOptions

ALL_PROFILES = ["book", "sutra", "transcript", "audiobook"]

# Default: sutra, transcript, and audiobook use the LLM lane; text books skip.
DEFAULT_LLM_PROFILES = ["sutra", "transcript", "audiobook"]
LEGACY_LLM_ENRICH_PROFILES_KEY = "llm_enrich_profiles"
LLM_PROFILES_KEY = "llm_profiles"


def _normalize_llm_profiles(values: list[str] | None) -> list[str]:
    """Return canonical LLM-enabled content profiles."""
    normalized: list[str] = []
    for raw in values or []:
        value = str(raw or "").strip().lower()
        if value in ALL_PROFILES and value not in normalized:
            normalized.append(value)
    return normalized


def resolve_llm_option_profile(
    *,
    content_profile: str,
    source_kind: str | None = None,
) -> str:
    """Return the Pipeline Options profile key for one source.

    Audiobooks still persist ``content_profile = book`` in the ingestion
    pipeline. For LLM options only, they map to a distinct toggle profile
    ``audiobook`` when the downstream source kind is ``media_transcript``.
    """
    if content_profile == "book" and source_kind == "media_transcript":
        return "audiobook"
    return content_profile


def get_pipeline_options(db: Session) -> PipelineOptions | None:
    """Return the current pipeline options row, or None if not yet saved."""
    return db.execute(select(PipelineOptions)).scalars().first()


def get_llm_profiles(db: Session) -> list[str]:
    """Return content profiles that should use the LLM lane.

    This applies to both:
    - transcript boundary refinement
    - source-aware enrichment

    Legacy rows stored under ``llm_enrich_profiles`` are still supported and
    normalized so ``audiobook`` maps back to ``book``.
    """
    row = get_pipeline_options(db)
    if row is None:
        return list(DEFAULT_LLM_PROFILES)
    options = row.options_json or {}
    configured = options.get(LLM_PROFILES_KEY)
    if configured is None:
        configured = options.get(LEGACY_LLM_ENRICH_PROFILES_KEY, DEFAULT_LLM_PROFILES)
    return _normalize_llm_profiles(list(configured))


def get_llm_enrich_profiles(db: Session) -> list[str]:
    """Backward-compatible alias for callers not yet renamed to ``get_llm_profiles``."""
    return get_llm_profiles(db)


def save_pipeline_options(db: Session, *, options: dict[str, Any]) -> PipelineOptions:
    """Upsert the pipeline options singleton row.

    Inputs:
        db: active SQLAlchemy session.
        options: full options dict to persist (replaces existing options_json).

    Output:
        Persisted ``PipelineOptions`` instance (flushed, not committed).

    Side effects:
        Inserts or updates the single row in ``pipeline_options``.
    """
    payload = dict(options)
    payload[LLM_PROFILES_KEY] = _normalize_llm_profiles(
        list(
            payload.get(
                LLM_PROFILES_KEY,
                payload.get(LEGACY_LLM_ENRICH_PROFILES_KEY, DEFAULT_LLM_PROFILES),
            )
        )
    )
    payload.pop(LEGACY_LLM_ENRICH_PROFILES_KEY, None)

    existing = get_pipeline_options(db)
    if existing is None:
        row = PipelineOptions(options_json=payload)
        db.add(row)
    else:
        existing.options_json = payload
        existing.updated_at = datetime.utcnow()
        row = existing
    db.flush()
    return row
