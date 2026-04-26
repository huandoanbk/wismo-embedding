"""Transcript pre-processing and restoration before boundary refinement."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any

from core.transcript_tiers import (
    TRANSCRIPT_TIER_LOW_QUALITY,
    TRANSCRIPT_TIER_MEDIUM,
    TRANSCRIPT_TIER_STRUCTURED,
    normalize_transcript_quality_tier,
)

TRANSCRIPT_PREPROCESSING_SIGNALS_KEY = "transcript_preprocessing_signals"

_SAFE_STT_CORRECTIONS = {
    "bat nha": "Bat Nha",
    "kim cang": "Kim Cang",
    "phat phap": "Phat Phap",
    "tu niem xu": "Tu Niem Xu",
    "bat chanh dao": "Bat Chanh Dao",
    "vo thuong": "Vo Thuong",
}

_FILLER_PATTERNS = (
    re.compile(r"(?i)\b(thi la)\b"),
    re.compile(r"(?i)\b(phai khong)\b"),
    re.compile(r"(?i)\b(um+|uh+|erm)\b"),
)
_QUESTION_MARKER_PATTERN = re.compile(r"(?i)^\s*(hoi|phat tu|thua thay|q)\s*:")
_ANSWER_MARKER_PATTERN = re.compile(r"(?i)^\s*(dap|thay|su co|tra loi|a)\s*:")
_SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class TranscriptPreprocessingResult:
    """Normalized transcript payload used by downstream transcript chunking."""

    payload: dict[str, Any]
    signals: dict[str, Any]


def preprocess_transcript_extract(
    extract_payload: dict[str, Any],
    *,
    transcript_quality_tier: str,
) -> TranscriptPreprocessingResult:
    """Normalize one transcript extract into tier-aware candidate segments."""
    tier = normalize_transcript_quality_tier(transcript_quality_tier)
    raw_segments = _extract_segment_texts(extract_payload)
    cleaned_segments = [_normalize_segment_text(text, apply_filler_cleanup=True) for text in raw_segments if text.strip()]
    qa_segments = _merge_question_answer_pairs(cleaned_segments)

    if tier == TRANSCRIPT_TIER_LOW_QUALITY:
        sentence_units = _restore_low_quality_sentences(qa_segments, extract_payload)
    else:
        sentence_units = _segments_to_sentence_units(qa_segments)

    candidate_segments = _group_sentence_units(sentence_units, tier=tier)
    payload = {
        **extract_payload,
        "full_text": "\n\n".join(segment["text"] for segment in candidate_segments).strip(),
        "segments": candidate_segments,
    }
    signals = {
        "dictionary_corrections_applied": sum(_count_dictionary_hits(text) for text in raw_segments),
        "filler_cleanup_applied": any(_has_filler_pattern(text) for text in raw_segments),
        "qa_pairs_preserved": sum(1 for segment in candidate_segments if segment.get("qa_pair") is True),
        "restoration_applied": tier == TRANSCRIPT_TIER_LOW_QUALITY,
        "candidate_segment_count": len(candidate_segments),
    }
    return TranscriptPreprocessingResult(payload=payload, signals=signals)


def transcript_chunker_kwargs_for_tier(transcript_quality_tier: str) -> dict[str, Any]:
    """Return transcript chunker overrides for one transcript tier."""
    tier = normalize_transcript_quality_tier(transcript_quality_tier)
    if tier == TRANSCRIPT_TIER_STRUCTURED:
        return {
            "target_token_limit": 420,
            "minimum_merge_tokens": 0,
            "overlap_sentences": 0,
            "merge_small_segments": False,
        }
    if tier == TRANSCRIPT_TIER_MEDIUM:
        return {
            "target_token_limit": 560,
            "minimum_merge_tokens": 0,
            "overlap_sentences": 0,
            "merge_small_segments": False,
        }
    return {
        "target_token_limit": 680,
        "minimum_merge_tokens": 0,
        "overlap_sentences": 0,
        "merge_small_segments": False,
    }


def _extract_segment_texts(extract_payload: dict[str, Any]) -> list[str]:
    segments = extract_payload.get("segments") or []
    texts = [str(segment.get("text") or "").strip() for segment in segments if str(segment.get("text") or "").strip()]
    if texts:
        return texts
    full_text = str(extract_payload.get("full_text") or "").strip()
    return [full_text] if full_text else []


def _normalize_segment_text(text: str, *, apply_filler_cleanup: bool) -> str:
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized = _apply_dictionary_corrections(normalized)
    normalized = normalized.replace(" ,", ",").replace(" .", ".").replace(" !", "!").replace(" ?", "?")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if apply_filler_cleanup:
        normalized = _remove_fillers(normalized)
    if normalized and normalized[-1] not in ".!?":
        normalized = f"{normalized}."
    return normalized


def _apply_dictionary_corrections(text: str) -> str:
    normalized = text
    for raw, corrected in _SAFE_STT_CORRECTIONS.items():
        normalized = re.sub(rf"(?i)\b{re.escape(raw)}\b", corrected, normalized)
    return normalized


def _remove_fillers(text: str) -> str:
    normalized = text
    for pattern in _FILLER_PATTERNS:
        normalized = pattern.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _has_filler_pattern(text: str) -> bool:
    return any(pattern.search(text) for pattern in _FILLER_PATTERNS)


def _count_dictionary_hits(text: str) -> int:
    lowered = text.lower()
    return sum(1 for raw in _SAFE_STT_CORRECTIONS if raw in lowered)


def _merge_question_answer_pairs(segments: list[str]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index = 0
    while index < len(segments):
        current = segments[index]
        next_text = segments[index + 1] if index + 1 < len(segments) else ""
        if _is_question_segment(current) and next_text:
            merged.append({"text": f"{current} {next_text}".strip(), "qa_pair": True})
            index += 2
            continue
        merged.append({"text": current, "qa_pair": False})
        index += 1
    return merged


def _is_question_segment(text: str) -> bool:
    return bool(_QUESTION_MARKER_PATTERN.search(text)) or text.rstrip().endswith("?")


def _segments_to_sentence_units(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sentence_units: list[dict[str, Any]] = []
    for segment in segments:
        text = str(segment["text"]).strip()
        if segment.get("qa_pair") is True:
            sentence_units.append({"text": text, "qa_pair": True})
            continue
        for sentence in _SENTENCE_SPLIT_PATTERN.split(text):
            candidate = sentence.strip()
            if candidate:
                sentence_units.append({"text": candidate, "qa_pair": False})
    return sentence_units


def _restore_low_quality_sentences(
    segments: list[dict[str, Any]],
    extract_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    if segments and len(segments) > 1:
        restored = []
        for segment in segments:
            text = str(segment["text"]).strip()
            if text and text[-1] not in ".!?":
                text = f"{text}."
            restored.append({"text": text, "qa_pair": segment.get("qa_pair", False)})
        return restored

    full_text = str(extract_payload.get("full_text") or "").strip()
    if not full_text:
        return []

    words = full_text.split()
    window_size = 36
    restored: list[dict[str, Any]] = []
    for start in range(0, len(words), window_size):
        chunk_words = words[start : start + window_size]
        candidate = _normalize_segment_text(" ".join(chunk_words), apply_filler_cleanup=True)
        restored.append({"text": candidate, "qa_pair": False})
    return restored


def _group_sentence_units(sentence_units: list[dict[str, Any]], *, tier: str) -> list[dict[str, Any]]:
    target_tokens = _target_candidate_tokens_for_tier(tier)
    grouped: list[dict[str, Any]] = []
    current_parts: list[str] = []
    current_tokens = 0
    current_qa = False

    def flush() -> None:
        nonlocal current_parts, current_tokens, current_qa
        if not current_parts:
            return
        text = " ".join(current_parts).strip()
        grouped.append(
            {
                "text": text,
                "start": float(len(grouped)),
                "end": float(len(grouped) + 1),
                "qa_pair": current_qa,
            }
        )
        current_parts = []
        current_tokens = 0
        current_qa = False

    for unit in sentence_units:
        text = str(unit["text"]).strip()
        if not text:
            continue
        unit_tokens = _estimate_tokens(text)
        if unit.get("qa_pair") is True:
            flush()
            grouped.append(
                {
                    "text": text,
                    "start": float(len(grouped)),
                    "end": float(len(grouped) + 1),
                    "qa_pair": True,
                }
            )
            continue
        if current_parts and current_tokens + unit_tokens > target_tokens:
            flush()
        current_parts.append(text)
        current_tokens += unit_tokens
    flush()
    return grouped


def _target_candidate_tokens_for_tier(tier: str) -> int:
    if tier == TRANSCRIPT_TIER_STRUCTURED:
        return 420
    if tier == TRANSCRIPT_TIER_MEDIUM:
        return 520
    return 620


def _estimate_tokens(text: str) -> int:
    word_count = len(text.split())
    return max(1, math.ceil(word_count * 1.3))
