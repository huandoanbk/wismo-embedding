"""Runtime checkpoint, gate, and review marker helpers."""

from __future__ import annotations

from typing import Any

PIPELINE_CHECKPOINT_STAGE_KEY = "pipeline_checkpoint_stage"
PIPELINE_GATE_SUMMARY_KEY = "pipeline_gate_summary"
PIPELINE_REVIEW_MARKERS_KEY = "pipeline_review_markers"
PIPELINE_ARTIFACT_POINTERS_KEY = "pipeline_artifact_pointers"
PIPELINE_RESUME_FROM_KEY = "pipeline_resume_from"

PIPELINE_STAGE_NEW = "new"
PIPELINE_STAGE_TRANSCRIPTED = "transcripted"
PIPELINE_STAGE_ARTIFACT_EXTRACTED = "artifact_extracted"
PIPELINE_STAGE_CHUNKING_SUCCESSFUL = "chunking_successful"
PIPELINE_STAGE_REFINEMENT_SUCCESSFUL = "refinement_successful"
PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL = "llm_enrichment_successful"
PIPELINE_STAGE_EMBEDDING_SUCCESSFUL = "embedding_successful"
PIPELINE_STAGE_FAILED_TRANSCRIPT = "failed_transcript"
PIPELINE_STAGE_FAILED_EXTRACT_ARTIFACT = "failed_extract_artifact"
PIPELINE_STAGE_FAILED_CHUNKING = "failed_chunking"
PIPELINE_STAGE_FAILED_REFINEMENT = "failed_refinement"
PIPELINE_STAGE_FAILED_ENRICHMENT = "failed_enrichment"
PIPELINE_STAGE_FAILED_EMBEDDING = "failed_embedding"

VALID_PIPELINE_STAGES = {
    PIPELINE_STAGE_NEW,
    PIPELINE_STAGE_TRANSCRIPTED,
    PIPELINE_STAGE_ARTIFACT_EXTRACTED,
    PIPELINE_STAGE_CHUNKING_SUCCESSFUL,
    PIPELINE_STAGE_REFINEMENT_SUCCESSFUL,
    PIPELINE_STAGE_LLM_ENRICHMENT_SUCCESSFUL,
    PIPELINE_STAGE_EMBEDDING_SUCCESSFUL,
    PIPELINE_STAGE_FAILED_TRANSCRIPT,
    PIPELINE_STAGE_FAILED_EXTRACT_ARTIFACT,
    PIPELINE_STAGE_FAILED_CHUNKING,
    PIPELINE_STAGE_FAILED_REFINEMENT,
    PIPELINE_STAGE_FAILED_ENRICHMENT,
    PIPELINE_STAGE_FAILED_EMBEDDING,
}


def set_pipeline_checkpoint(metadata: dict[str, Any], stage: str) -> dict[str, Any]:
    """Return metadata with one normalized pipeline checkpoint stage."""
    if stage not in VALID_PIPELINE_STAGES:
        raise ValueError(f"invalid_pipeline_stage:{stage}")
    payload = dict(metadata)
    payload[PIPELINE_CHECKPOINT_STAGE_KEY] = stage
    return payload


def resolve_pipeline_resume_stage(metadata: dict[str, Any]) -> str | None:
    """Return one resumable stage name when present and valid."""
    stage = metadata.get(PIPELINE_CHECKPOINT_STAGE_KEY)
    return stage if stage in VALID_PIPELINE_STAGES else None


def set_pipeline_resume_from(metadata: dict[str, Any], stage: str | None) -> dict[str, Any]:
    """Return metadata with one resume-from marker when applicable."""
    payload = dict(metadata)
    if stage is None:
        payload.pop(PIPELINE_RESUME_FROM_KEY, None)
    else:
        payload[PIPELINE_RESUME_FROM_KEY] = stage
    return payload


def merge_gate_summary(metadata: dict[str, Any], stage: str, summary: dict[str, Any]) -> dict[str, Any]:
    """Return metadata with one gate summary merged under the requested stage."""
    payload = dict(metadata)
    all_summaries = dict(payload.get(PIPELINE_GATE_SUMMARY_KEY) or {})
    all_summaries[stage] = summary
    payload[PIPELINE_GATE_SUMMARY_KEY] = all_summaries
    return payload


def add_review_marker(metadata: dict[str, Any], marker: str) -> dict[str, Any]:
    """Return metadata with one deduplicated review marker."""
    payload = dict(metadata)
    markers = list(payload.get(PIPELINE_REVIEW_MARKERS_KEY) or [])
    if marker not in markers:
        markers.append(marker)
    payload[PIPELINE_REVIEW_MARKERS_KEY] = markers
    return payload


def add_artifact_pointer(metadata: dict[str, Any], name: str, pointer: str) -> dict[str, Any]:
    """Return metadata with one named artifact pointer."""
    payload = dict(metadata)
    pointers = dict(payload.get(PIPELINE_ARTIFACT_POINTERS_KEY) or {})
    pointers[name] = pointer
    payload[PIPELINE_ARTIFACT_POINTERS_KEY] = pointers
    return payload
