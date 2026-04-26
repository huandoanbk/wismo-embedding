"""Provenance, rerun, and benchmark contract helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

PROVENANCE_KEY = "pipeline_provenance"


def build_provenance_tuple(
    *,
    llm_model: str,
    llm_model_version: str,
    pipeline_version: str,
    prompt_version: str,
) -> dict[str, str]:
    """Return one normalized provenance tuple with processed timestamp."""
    return {
        "llm_model": llm_model,
        "llm_model_version": llm_model_version,
        "pipeline_version": pipeline_version,
        "prompt_version": prompt_version,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def attach_provenance(metadata: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    """Return metadata with one stored provenance tuple."""
    payload = dict(metadata)
    payload[PROVENANCE_KEY] = dict(provenance)
    return payload


def invalidate_stage_outputs_for_version_change(
    metadata: dict[str, Any],
    *,
    changed_fields: set[str],
) -> dict[str, Any]:
    """Invalidate stage outputs when version-affecting inputs change."""
    payload = dict(metadata)
    if changed_fields & {"content_profile", "transcript_quality_tier", "pipeline_version", "prompt_version", "llm_model_version"}:
        payload["pipeline_invalidated"] = True
    return payload

