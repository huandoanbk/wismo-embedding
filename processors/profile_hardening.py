"""Hardening helpers for book and sutra chunking lanes."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from core.content_profiles import CONTENT_PROFILE_BOOK, CONTENT_PROFILE_SUTRA

PIPELINE_GATE_SUMMARY_KEY = "pipeline_gate_summary"

_HEADING_PATTERN = re.compile(r"(?i)^\s*(chuong|chapter|phan|muc|section)\b")
_SUTRA_UNIT_PATTERN = re.compile(r"(?i)\b(kinh|pham|sutra|nikaya|a ham)\b")
_GATHA_PATTERN = re.compile(r"(?i)\b(ke|gatha|thi ke)\b")
_QUOTE_PATTERN = re.compile(r'["“”]|trong kinh|kinh [A-ZÀ-Ỹa-zà-ỹ]')


@dataclass(frozen=True)
class ProfileHardeningResult:
    """Prepared segments and gate summary for one hardened lane."""

    payload: dict[str, Any]
    gate_summary: dict[str, Any]


def harden_profile_extract(
    extract_payload: dict[str, Any],
    *,
    content_profile: str,
) -> ProfileHardeningResult | None:
    """Return one hardened payload for book or sutra lanes only."""
    if content_profile == CONTENT_PROFILE_BOOK:
        return _harden_book_payload(extract_payload)
    if content_profile == CONTENT_PROFILE_SUTRA:
        return _harden_sutra_payload(extract_payload)
    return None


def _harden_book_payload(extract_payload: dict[str, Any]) -> ProfileHardeningResult:
    segments = extract_payload.get("segments") or _paragraph_segments(str(extract_payload.get("full_text") or ""))
    normalized = [_normalize_segment(segment) for segment in segments if _normalize_segment(segment)["text"]]
    if any(_looks_like_heading(segment["text"]) for segment in normalized):
        prepared_segments = _book_heading_segments(normalized)
    else:
        prepared_segments = _book_recursive_segments(normalized)
    prepared_segments = _attach_adjacent_quotes(prepared_segments)
    payload = {
        **extract_payload,
        "full_text": "\n\n".join(segment["text"] for segment in prepared_segments).strip(),
        "segments": prepared_segments,
    }
    gate_summary = {
        "branch": "book",
        "chunk_size_distribution": _segment_length_stats(prepared_segments),
        "overlap_consistency": "book_sentence_overlap_policy",
        "dangling_quote_context": any(segment.get("dangling_quote_context") for segment in prepared_segments),
    }
    return ProfileHardeningResult(payload=payload, gate_summary=gate_summary)


def _harden_sutra_payload(extract_payload: dict[str, Any]) -> ProfileHardeningResult:
    segments = extract_payload.get("segments") or _paragraph_segments(str(extract_payload.get("full_text") or ""))
    normalized = [_normalize_segment(segment) for segment in segments if _normalize_segment(segment)["text"]]
    prepared_segments = _group_parallel_repetition(_preserve_sutra_units(normalized))
    payload = {
        **extract_payload,
        "full_text": "\n\n".join(segment["text"] for segment in prepared_segments).strip(),
        "segments": prepared_segments,
    }
    gate_summary = {
        "branch": "sutra",
        "chunk_size_distribution": _segment_length_stats(prepared_segments),
        "gatha_split_violation": any(segment.get("gatha_split_violation") for segment in prepared_segments),
        "parallel_repetition": any(segment.get("parallel_repetition") for segment in prepared_segments),
    }
    return ProfileHardeningResult(payload=payload, gate_summary=gate_summary)


def _book_heading_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    current_parts: list[dict[str, Any]] = []
    for segment in segments:
        text = segment["text"]
        if _looks_like_heading(text) and current_parts:
            output.append(_merge_book_parts(current_parts))
            current_parts = [segment]
            continue
        current_parts.append(segment)
    if current_parts:
        output.append(_merge_book_parts(current_parts))
    return output


def _book_recursive_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    current_parts: list[dict[str, Any]] = []
    current_length = 0
    for segment in segments:
        text = segment["text"]
        candidate_length = current_length + len(text.split())
        if current_parts and candidate_length > 120:
            output.append(_merge_book_parts(current_parts))
            current_parts = [segment]
            current_length = len(text.split())
            continue
        current_parts.append(segment)
        current_length = candidate_length
    if current_parts:
        output.append(_merge_book_parts(current_parts))
    return output


def _attach_adjacent_quotes(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    index = 0
    while index < len(segments):
        current = dict(segments[index])
        next_segment = dict(segments[index + 1]) if index + 1 < len(segments) else None
        if next_segment and _looks_like_quote(current["text"]) and not _looks_like_heading(next_segment["text"]):
            current["text"] = f"{current['text']}\n{next_segment['text']}".strip()
            current["quote_attached"] = True
            index += 2
            output.append(current)
            continue
        current["dangling_quote_context"] = _looks_like_quote(current["text"]) and next_segment is None
        output.append(current)
        index += 1
    return output


def _preserve_sutra_units(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for segment in segments:
        text = segment["text"]
        record = {"text": text}
        if _looks_like_gatha(text):
            record["gatha"] = True
            record["gatha_split_violation"] = False
        if _looks_like_sutra_unit(text):
            record["sutra_unit"] = True
        output.append(record)
    return output


def _group_parallel_repetition(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    index = 0
    while index < len(segments):
        current = dict(segments[index])
        next_segment = dict(segments[index + 1]) if index + 1 < len(segments) else None
        if next_segment and _normalized_prefix(current["text"]) == _normalized_prefix(next_segment["text"]):
            current["text"] = f"{current['text']}\n{next_segment['text']}".strip()
            current["parallel_repetition"] = True
            index += 2
            output.append(current)
            continue
        output.append(current)
        index += 1
    return output


def _normalize_segment(segment: dict[str, Any]) -> dict[str, Any]:
    return {
        **segment,
        "text": str(segment.get("text") or "").strip(),
    }


def _paragraph_segments(full_text: str) -> list[dict[str, Any]]:
    return [{"text": part.strip()} for part in full_text.split("\n\n") if part.strip()]


def _looks_like_heading(text: str) -> bool:
    return bool(_HEADING_PATTERN.search(text.strip()))


def _looks_like_quote(text: str) -> bool:
    return bool(_QUOTE_PATTERN.search(text))


def _looks_like_gatha(text: str) -> bool:
    return bool(_GATHA_PATTERN.search(text))


def _looks_like_sutra_unit(text: str) -> bool:
    return bool(_SUTRA_UNIT_PATTERN.search(text))


def _normalized_prefix(text: str) -> str:
    return " ".join(text.lower().split()[:6])


def _segment_length_stats(segments: list[dict[str, Any]]) -> dict[str, int]:
    lengths = [len(segment["text"].split()) for segment in segments if segment["text"].strip()]
    if not lengths:
        return {"count": 0, "min_words": 0, "max_words": 0}
    return {"count": len(lengths), "min_words": min(lengths), "max_words": max(lengths)}


def _merge_book_parts(parts: list[dict[str, Any]]) -> dict[str, Any]:
    merged = {"text": "\n".join(part["text"] for part in parts).strip()}
    if len(parts) >= 2 and _looks_like_quote(parts[0]["text"]) and not _looks_like_heading(parts[1]["text"]):
        merged["quote_attached"] = True
    if any(part.get("quote_attached") is True for part in parts):
        merged["quote_attached"] = True
    if any(part.get("dangling_quote_context") is True for part in parts):
        merged["dangling_quote_context"] = True
    return merged
