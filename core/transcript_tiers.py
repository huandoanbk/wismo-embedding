"""Heuristic transcript-tier classification for transcript follow-up routing."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any

TRANSCRIPT_QUALITY_TIER_KEY = "transcript_quality_tier"
TRANSCRIPT_QUALITY_SIGNALS_KEY = "transcript_quality_signals"

TRANSCRIPT_TIER_STRUCTURED = "structured"
TRANSCRIPT_TIER_MEDIUM = "medium"
TRANSCRIPT_TIER_LOW_QUALITY = "low_quality"

VALID_TRANSCRIPT_TIERS = frozenset(
    {
        TRANSCRIPT_TIER_STRUCTURED,
        TRANSCRIPT_TIER_MEDIUM,
        TRANSCRIPT_TIER_LOW_QUALITY,
    }
)

TRANSCRIPT_ROUTE_BASELINE = "baseline"
TRANSCRIPT_ROUTE_REFINEMENT_ELIGIBLE = "refinement_eligible"
TRANSCRIPT_ROUTE_RESTORATION_REFINEMENT_ELIGIBLE = "restoration_refinement_eligible"


@dataclass(frozen=True)
class TranscriptTierDecision:
    """Classifier output used for transcript-tier routing decisions."""

    tier: str
    signals: dict[str, Any]
    route: str


def normalize_transcript_quality_tier(value: str | None) -> str:
    """Return one validated canonical transcript quality tier."""
    normalized = (value or "").strip().lower()
    if normalized not in VALID_TRANSCRIPT_TIERS:
        raise ValueError(f"invalid_transcript_quality_tier:{value}")
    return normalized


def transcript_processing_route_for_tier(tier: str) -> str:
    """Map one transcript tier to the downstream routing contract."""
    normalized = normalize_transcript_quality_tier(tier)
    if normalized == TRANSCRIPT_TIER_STRUCTURED:
        return TRANSCRIPT_ROUTE_BASELINE
    if normalized == TRANSCRIPT_TIER_MEDIUM:
        return TRANSCRIPT_ROUTE_REFINEMENT_ELIGIBLE
    return TRANSCRIPT_ROUTE_RESTORATION_REFINEMENT_ELIGIBLE


def classify_transcript_quality(extract_payload: dict[str, Any]) -> TranscriptTierDecision:
    """Classify transcript quality from canonical extract signals only."""
    full_text = str(extract_payload.get("full_text") or "").strip()
    segments = extract_payload.get("segments") or []
    segment_texts = [_segment_text(segment) for segment in segments if _segment_text(segment)]

    if not full_text and segment_texts:
        full_text = " ".join(segment_texts).strip()

    token_counts = [_estimate_tokens(text) for text in segment_texts] or [_estimate_tokens(full_text)]
    avg_tokens = round(sum(token_counts) / max(len(token_counts), 1), 2)
    max_tokens = max(token_counts) if token_counts else 0
    variance = round(_variance(token_counts), 2)
    long_segment_anomalies = sum(1 for tokens in token_counts if tokens >= 140)

    punctuation_count = len(re.findall(r"[.!?,;:]", full_text))
    punctuation_density = round(punctuation_count / max(len(full_text), 1), 4)
    timecoded_segment_availability = any(_has_timecodes(segment) for segment in segments)
    speaker_marker_presence = any(_has_speaker_marker(text) for text in segment_texts) or _has_speaker_marker(full_text)

    signals = {
        "punctuation_density": punctuation_density,
        "segment_length_distribution": {
            "segment_count": len(token_counts),
            "avg_tokens": avg_tokens,
            "max_tokens": max_tokens,
            "variance": variance,
        },
        "long_segment_anomalies": long_segment_anomalies,
        "timecoded_segment_availability": timecoded_segment_availability,
        "speaker_marker_presence": speaker_marker_presence,
    }

    if _is_low_quality(
        punctuation_density=punctuation_density,
        avg_tokens=avg_tokens,
        max_tokens=max_tokens,
        long_segment_anomalies=long_segment_anomalies,
        segment_count=len(token_counts),
    ):
        tier = TRANSCRIPT_TIER_LOW_QUALITY
    elif _is_structured(
        punctuation_density=punctuation_density,
        avg_tokens=avg_tokens,
        max_tokens=max_tokens,
        long_segment_anomalies=long_segment_anomalies,
        timecoded_segment_availability=timecoded_segment_availability,
        speaker_marker_presence=speaker_marker_presence,
        segment_count=len(token_counts),
    ):
        tier = TRANSCRIPT_TIER_STRUCTURED
    else:
        tier = TRANSCRIPT_TIER_MEDIUM

    return TranscriptTierDecision(
        tier=tier,
        signals=signals,
        route=transcript_processing_route_for_tier(tier),
    )


def _is_structured(
    *,
    punctuation_density: float,
    avg_tokens: float,
    max_tokens: int,
    long_segment_anomalies: int,
    timecoded_segment_availability: bool,
    speaker_marker_presence: bool,
    segment_count: int,
) -> bool:
    return (
        punctuation_density >= 0.018
        and avg_tokens <= 45
        and max_tokens <= 90
        and long_segment_anomalies == 0
        and segment_count >= 2
        and (timecoded_segment_availability or speaker_marker_presence)
    )


def _is_low_quality(
    *,
    punctuation_density: float,
    avg_tokens: float,
    max_tokens: int,
    long_segment_anomalies: int,
    segment_count: int,
) -> bool:
    anomaly_threshold = max(1, math.ceil(segment_count * 0.3))
    return (
        punctuation_density < 0.006
        or max_tokens >= 220
        or avg_tokens >= 120
        or long_segment_anomalies >= anomaly_threshold
    )


def _estimate_tokens(text: str) -> int:
    word_count = len(text.split())
    return max(1, math.ceil(word_count * 1.3))


def _variance(values: list[int]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    return sum((value - mean) ** 2 for value in values) / len(values)


def _segment_text(segment: dict[str, Any]) -> str:
    return str(segment.get("text") or "").strip()


def _has_timecodes(segment: dict[str, Any]) -> bool:
    return isinstance(segment.get("start"), (int, float)) and isinstance(segment.get("end"), (int, float))


def _has_speaker_marker(text: str) -> bool:
    return bool(re.search(r"(^|\n)\s*[\wÀ-ỹ][^:\n]{0,30}:", text))
