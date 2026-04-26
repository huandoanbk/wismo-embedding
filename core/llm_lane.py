"""LLM runtime lane contract — job types, serving schemas, callback reliability,
strictness modes, and gate summary taxonomy.

This module defines the first-class `llm` lane alongside `whisper` and `embedding`.
It owns:
- Job type constants for refinement and enrichment work dispatched to the LLM GPU lane.
- Request/response schemas for the Gemma serving lane.
- Callback idempotency contract (batch_id + attempt_id).
- Partial batch failure semantics.
- Strictness mode definitions for production vs degraded operation.
- Gate summary taxonomy for LLM runtime failures.
- Readiness criteria and production cutover rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

# ---------------------------------------------------------------------------
# Lane identifier
# ---------------------------------------------------------------------------

LLM_LANE = "llm"
LLM_OPERATION_REFINEMENT = "refinement"
LLM_OPERATION_ENRICHMENT = "enrichment"
LLM_QUEUE_REFINEMENT = "llm_refinement"
LLM_QUEUE_ENRICHMENT = "llm_enrichment"

# ---------------------------------------------------------------------------
# Job types dispatched to lane `llm`
# ---------------------------------------------------------------------------

JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT = "run_llm_boundary_refinement"
JOB_TYPE_RUN_LLM_ENRICHMENT = "run_llm_enrichment"

LLM_JOB_TYPES = {
    JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT,
    JOB_TYPE_RUN_LLM_ENRICHMENT,
}


def llm_operation_for_job_type(job_type: str) -> str:
    """Return the logical LLM operation name for a queue job type."""
    if job_type == JOB_TYPE_RUN_LLM_BOUNDARY_REFINEMENT:
        return LLM_OPERATION_REFINEMENT
    if job_type == JOB_TYPE_RUN_LLM_ENRICHMENT:
        return LLM_OPERATION_ENRICHMENT
    raise ValueError(f"unknown_llm_job_type:{job_type}")


def llm_queue_for_job_type(job_type: str) -> str:
    """Return the Celery/Upstash queue name for an LLM job type."""
    operation = llm_operation_for_job_type(job_type)
    if operation == LLM_OPERATION_REFINEMENT:
        return LLM_QUEUE_REFINEMENT
    return LLM_QUEUE_ENRICHMENT

# ---------------------------------------------------------------------------
# Strictness modes
# ---------------------------------------------------------------------------

LLM_RUNTIME_MODE_STRICT = "strict"
"""Production path: pipeline MUST NOT complete cleanly when lane `llm` is
required but unavailable. Any unavailability raises a hard error."""

LLM_RUNTIME_MODE_DEGRADED = "degraded"
"""Local/dev path: pipeline may continue in degraded state when lane `llm` is
unavailable, but MUST record the degraded reason explicitly in the gate summary."""

# ---------------------------------------------------------------------------
# Gate summary taxonomy for LLM runtime failures
# ---------------------------------------------------------------------------

LLM_GATE_REASON_PROVIDER_UNAVAILABLE = "provider_unavailable"
"""Lane `llm` is configured but the provider endpoint is unreachable or not
provisioned for this environment."""

LLM_GATE_REASON_PROVIDER_HTTP_FAILURE = "provider_http_failure"
"""Provider responded but with a non-success HTTP status code."""

LLM_GATE_REASON_PARSE_FAILURE = "parse_failure"
"""Provider returned a response that could not be parsed as expected."""

LLM_GATE_REASON_SCHEMA_INVALID = "schema_invalid"
"""Provider returned a parseable response but it did not match required schema."""

LLM_GATE_REASON_FALLBACK = "fallback"
"""Fallback path was used (e.g. default metadata) instead of real LLM output."""

LLM_GATE_REASONS = {
    LLM_GATE_REASON_PROVIDER_UNAVAILABLE,
    LLM_GATE_REASON_PROVIDER_HTTP_FAILURE,
    LLM_GATE_REASON_PARSE_FAILURE,
    LLM_GATE_REASON_SCHEMA_INVALID,
    LLM_GATE_REASON_FALLBACK,
}


def build_llm_gate_summary(
    *,
    provider_unavailable_count: int = 0,
    provider_http_failure_count: int = 0,
    parse_failure_count: int = 0,
    schema_invalid_count: int = 0,
    fallback_count: int = 0,
    total_items: int = 0,
    mode: str = LLM_RUNTIME_MODE_STRICT,
) -> dict[str, object]:
    """Return a structured gate summary for one LLM lane operation.

    Inputs:
        per-reason failure counts, total items dispatched, and strictness mode.

    Output:
        dict with typed failure counts plus derived ``has_degraded`` flag.
        ``has_degraded`` is True when any failure reason has a non-zero count.

    Side effects:
        None.
    """
    has_degraded = bool(
        provider_unavailable_count
        or provider_http_failure_count
        or parse_failure_count
        or schema_invalid_count
        or fallback_count
    )
    return {
        "lane": LLM_LANE,
        "mode": mode,
        "total_items": total_items,
        LLM_GATE_REASON_PROVIDER_UNAVAILABLE + "_count": provider_unavailable_count,
        LLM_GATE_REASON_PROVIDER_HTTP_FAILURE + "_count": provider_http_failure_count,
        LLM_GATE_REASON_PARSE_FAILURE + "_count": parse_failure_count,
        LLM_GATE_REASON_SCHEMA_INVALID + "_count": schema_invalid_count,
        LLM_GATE_REASON_FALLBACK + "_count": fallback_count,
        "has_degraded": has_degraded,
    }


def assert_llm_lane_clean(gate_summary: dict[str, object]) -> None:
    """Raise RuntimeError when gate summary indicates degraded state in strict mode.

    Purpose: enforce production path cannot silently complete when enrichment
    is mandatory but lane `llm` is unavailable or degraded.

    Inputs:
        gate_summary: dict produced by ``build_llm_gate_summary``.

    Raises:
        RuntimeError: when mode is ``strict`` and ``has_degraded`` is True.
    """
    if gate_summary.get("mode") == LLM_RUNTIME_MODE_STRICT and gate_summary.get("has_degraded"):
        reasons: list[str] = []
        for reason in LLM_GATE_REASONS:
            count = gate_summary.get(reason + "_count", 0)
            if count:
                reasons.append(f"{reason}={count}")
        raise RuntimeError(
            f"llm_lane_strict_violation: lane `llm` required but degraded — {', '.join(reasons)}"
        )


# ---------------------------------------------------------------------------
# Serving schemas — request/response for the Gemma lane (tasks 1.1, 1.2, 1.3)
# ---------------------------------------------------------------------------

@dataclass
class LlmEnrichmentItem:
    """One chunk item within a `run_llm_enrichment` batch request.

    Inputs:
        chunk_id: canonical chunk UUID string.
        content: chunk text to enrich.
        source_type: e.g. "book", "sutra", "transcript".
        language: ISO 639-1 code (default "vi").
        source_metadata: optional context dict passed to model prompt.
    """

    chunk_id: str
    content: str
    source_type: str
    language: str = "vi"
    source_metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class LlmEnrichmentRequest:
    """Request payload sent to the GPU worker for a `run_llm_enrichment` batch.

    Inputs:
        batch_id: unique batch identifier for idempotency tracking.
        attempt_id: monotonically increasing attempt counter for this batch.
        source_item_id: registry item this batch belongs to.
        items: list of chunk items to enrich.
        callback_url: VPS endpoint the worker must POST results to.

    Side effects:
        None — pure data schema.
    """

    batch_id: str
    attempt_id: str
    source_item_id: str
    items: list[LlmEnrichmentItem] = field(default_factory=list)
    callback_url: str = ""


@dataclass
class LlmRefinementSegment:
    """One segment from a transcript to be refined.

    Inputs:
        segment_index: position within the transcript.
        content: segment text.
        start_time: optional audio start timestamp in seconds.
        end_time: optional audio end timestamp in seconds.
    """

    segment_index: int
    content: str
    start_time: float | None = None
    end_time: float | None = None


@dataclass
class LlmBoundaryRefinementRequest:
    """Request payload sent to the GPU worker for `run_llm_boundary_refinement`.

    Inputs:
        batch_id: unique batch identifier for idempotency tracking.
        attempt_id: monotonically increasing attempt counter for this batch.
        source_item_id: registry item this batch belongs to.
        segments: list of transcript segments to refine boundaries for.
        callback_url: VPS endpoint the worker must POST results to.

    Side effects:
        None — pure data schema.
    """

    batch_id: str
    attempt_id: str
    source_item_id: str
    segments: list[LlmRefinementSegment] = field(default_factory=list)
    callback_url: str = ""


@dataclass
class LlmEnrichmentResultItem:
    """One enrichment result for a single chunk, returned by the GPU worker.

    Inputs:
        chunk_id: matches the chunk_id from the request.
        topic: enriched topic label.
        topic_tags: enriched tag list.
        summary: generated chunk summary.
        teaching_type: classified teaching type.
        applicable_issues: list of applicable issue strings.
        depth_level: depth classification.
        emotional_tone: tone classification.
        dharma_concepts: list of dharma concept strings.
        parse_status: one of "ok", "parse_failure", "schema_invalid", "fallback".
        failure_reason: optional detail when parse_status is not "ok".
    """

    chunk_id: str
    topic: str = ""
    topic_tags: list[str] = field(default_factory=list)
    summary: str = ""
    teaching_type: str = "unknown"
    applicable_issues: list[str] = field(default_factory=list)
    depth_level: str = "general"
    emotional_tone: str = "neutral"
    dharma_concepts: list[str] = field(default_factory=list)
    parse_status: str = "ok"
    failure_reason: str | None = None


@dataclass
class LlmRefinementResultSegment:
    """One refined segment boundary result, returned by the GPU worker.

    Inputs:
        segment_index: matches the segment_index from the request.
        refined_content: rewritten/re-segmented content.
        merged_with_next: True when this segment should be merged with the following one.
        parse_status: one of "ok", "parse_failure", "schema_invalid", "fallback".
        failure_reason: optional detail when parse_status is not "ok".
    """

    segment_index: int
    refined_content: str = ""
    merged_with_next: bool = False
    parse_status: str = "ok"
    failure_reason: str | None = None


@dataclass
class LlmCallbackResponse:
    """Minimum response payload sent by the GPU worker back to VPS for any LLM lane job.

    Inputs:
        batch_id: echoed from the request — used for idempotency.
        attempt_id: echoed from the request — used for idempotency.
        job_type: "run_llm_enrichment" or "run_llm_boundary_refinement".
        total_items: total number of items in the batch.
        success_count: items processed without error.
        failure_count: items that failed with a trackable reason.
        items: typed result list (LlmEnrichmentResultItem or LlmRefinementResultSegment).
        model_name: name of the model that served this batch.
        model_version: version string of the serving model.
    """

    batch_id: str
    attempt_id: str
    job_type: str
    total_items: int = 0
    success_count: int = 0
    failure_count: int = 0
    items: list[object] = field(default_factory=list)
    model_name: str = ""
    model_version: str = ""


# ---------------------------------------------------------------------------
# Callback idempotency contract (task 2.1)
# ---------------------------------------------------------------------------

LLM_CALLBACK_SEEN_KEY = "llm_callback_seen"
"""metadata_json key holding a dict of seen {batch_id: attempt_id} pairs."""


def make_callback_idempotency_key(batch_id: str, attempt_id: str) -> str:
    """Return a stable idempotency key for one callback invocation."""
    return f"{batch_id}:{attempt_id}"


class LlmCallbackStatus(str, Enum):
    """Result classification for one LLM callback invocation."""

    NEW = "new"
    """First time this batch_id + attempt_id has been seen."""

    DUPLICATE = "duplicate"
    """Same batch_id + attempt_id seen before — no side effects should be applied."""

    LATE = "late"
    """Same batch_id seen but with an older attempt_id — ignore silently."""


def classify_llm_callback(
    metadata_json: dict[str, object],
    batch_id: str,
    attempt_id: str,
) -> LlmCallbackStatus:
    """Classify an incoming LLM callback as new, duplicate, or late.

    Purpose: enforce idempotency — the same batch_id + attempt_id must not
    produce duplicate side effects.

    Inputs:
        metadata_json: the source item's current metadata_json.
        batch_id: from the callback payload.
        attempt_id: from the callback payload.

    Output:
        LlmCallbackStatus enum value.

    Side effects:
        None — caller must persist the updated metadata_json after recording.
    """
    seen: dict[str, str] = dict(metadata_json.get(LLM_CALLBACK_SEEN_KEY) or {})
    if batch_id not in seen:
        return LlmCallbackStatus.NEW
    if seen[batch_id] == attempt_id:
        return LlmCallbackStatus.DUPLICATE
    # same batch_id but different attempt_id seen before
    return LlmCallbackStatus.LATE


def record_llm_callback_seen(
    metadata_json: dict[str, object],
    batch_id: str,
    attempt_id: str,
) -> dict[str, object]:
    """Return metadata_json with this batch_id + attempt_id recorded.

    Inputs:
        metadata_json: current metadata_json dict (will not be mutated).
        batch_id, attempt_id: from the callback payload.

    Output:
        New metadata_json dict with updated LLM_CALLBACK_SEEN_KEY entry.

    Side effects:
        None — returns a new dict.
    """
    updated = dict(metadata_json)
    seen = dict(updated.get(LLM_CALLBACK_SEEN_KEY) or {})
    seen[batch_id] = attempt_id
    updated[LLM_CALLBACK_SEEN_KEY] = seen
    return updated


# ---------------------------------------------------------------------------
# Partial batch failure semantics (task 3.1)
# ---------------------------------------------------------------------------

class LlmBatchOutcome(str, Enum):
    """Final outcome classification for a completed LLM lane batch."""

    COMPLETED_CLEAN = "completed_clean"
    """All items succeeded without any parse/schema/provider failures."""

    COMPLETED_WITH_DEGRADED = "completed_with_degraded_llm_runtime"
    """Some items failed (below threshold) but batch is considered done in degraded mode."""

    FAILED = "failed"
    """Failure rate exceeded threshold or mode is strict — batch must retry or mark failed."""


# Minimum fraction of items that must succeed for batch to be COMPLETED (not FAILED).
LLM_BATCH_SUCCESS_THRESHOLD = 0.5
"""50% minimum success rate. Batches below this are always FAILED regardless of mode."""

LLM_BATCH_CLEAN_THRESHOLD = 1.0
"""100% success required to classify as COMPLETED_CLEAN."""


def classify_llm_batch_outcome(
    *,
    success_count: int,
    total_items: int,
    mode: str = LLM_RUNTIME_MODE_STRICT,
) -> LlmBatchOutcome:
    """Return the outcome classification for a completed LLM batch.

    Purpose: eliminate ambiguous "96/100 succeeded" semantics — every batch gets
    an explicit deterministic outcome.

    Inputs:
        success_count: number of items that succeeded in the batch.
        total_items: total number of items dispatched.
        mode: ``strict`` (production) or ``degraded`` (local/dev).

    Output:
        LlmBatchOutcome enum value.

    Side effects:
        None.
    """
    if total_items == 0:
        return LlmBatchOutcome.COMPLETED_CLEAN

    success_rate = success_count / total_items

    if success_rate < LLM_BATCH_SUCCESS_THRESHOLD:
        # Below minimum threshold — always FAILED regardless of mode
        return LlmBatchOutcome.FAILED

    if success_rate >= LLM_BATCH_CLEAN_THRESHOLD:
        return LlmBatchOutcome.COMPLETED_CLEAN

    # Partial success: between threshold and 100%
    if mode == LLM_RUNTIME_MODE_STRICT:
        return LlmBatchOutcome.FAILED
    return LlmBatchOutcome.COMPLETED_WITH_DEGRADED


# ---------------------------------------------------------------------------
# Readiness criteria and production cutover rules (tasks 3.2, 3.3)
# ---------------------------------------------------------------------------

LLM_READINESS_CRITERIA = [
    "image_validated_active",
    "worker_health_pass",
    "model_usable",
    "callback_path_usable",
]
"""All four criteria must be True for lane `llm` to be considered launch-ready."""


@dataclass
class LlmLaneReadiness:
    """Structured readiness state for lane `llm`.

    Inputs:
        image_validated_active: worker image is both validated (status=valid) and active.
        worker_health_pass: health endpoint returns healthy=True.
        model_usable: model serving responds to a minimal probe request.
        callback_path_usable: callback route is reachable with a valid secret.
        details: optional per-criterion detail strings for operator surfacing.
    """

    image_validated_active: bool = False
    worker_health_pass: bool = False
    model_usable: bool = False
    callback_path_usable: bool = False
    details: dict[str, str] = field(default_factory=dict)

    @property
    def launch_ready(self) -> bool:
        """True only when all four readiness criteria are satisfied."""
        return (
            self.image_validated_active
            and self.worker_health_pass
            and self.model_usable
            and self.callback_path_usable
        )

    def to_dict(self) -> dict[str, object]:
        """Return operator-facing readiness dict."""
        return {
            "lane": LLM_LANE,
            "launch_ready": self.launch_ready,
            "image_validated_active": self.image_validated_active,
            "worker_health_pass": self.worker_health_pass,
            "model_usable": self.model_usable,
            "callback_path_usable": self.callback_path_usable,
            "details": dict(self.details),
        }


class LlmCutoverDecision(str, Enum):
    """Production cutover decision for lane `llm`.

    Used by production readiness verification to decide what to do when
    a strategy-complete path encounters the LLM lane.
    """

    PASS = "pass"
    """Lane is ready — dispatch can proceed."""

    DEGRADED = "degraded"
    """Lane is not fully ready but mode allows degraded operation."""

    BLOCK = "block"
    """Lane is not ready and mode is strict — pipeline must not proceed."""


def resolve_llm_cutover_decision(
    readiness: LlmLaneReadiness,
    *,
    mode: str = LLM_RUNTIME_MODE_STRICT,
    strategy_complete_path: bool = False,
) -> LlmCutoverDecision:
    """Return the production cutover decision for lane `llm`.

    Purpose: deterministic rule for when pipeline should block, degrade, or pass.

    Inputs:
        readiness: LlmLaneReadiness state.
        mode: strict or degraded.
        strategy_complete_path: True when pipeline is on a strategy-complete
            path (i.e. enrichment/refinement is mandatory, not optional).

    Output:
        LlmCutoverDecision enum value.

    Side effects:
        None.
    """
    if readiness.launch_ready:
        return LlmCutoverDecision.PASS

    # Not launch-ready
    if strategy_complete_path or mode == LLM_RUNTIME_MODE_STRICT:
        return LlmCutoverDecision.BLOCK

    return LlmCutoverDecision.DEGRADED
