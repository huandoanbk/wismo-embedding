"""Adaptive chunking for canonical text extracts."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any

from core.content_profiles import (
    CONTENT_PROFILE_BOOK,
    CONTENT_PROFILE_SUTRA,
    CONTENT_PROFILE_TRANSCRIPT,
)


@dataclass(frozen=True)
class ChunkingProfilePolicy:
    """Canonical runtime policy for one content-profile chunking branch."""

    content_profile: str
    target_token_limit: int
    minimum_merge_tokens: int
    overlap_sentences: int
    merge_small_segments: bool = True


CHUNKING_PROFILE_POLICIES: dict[str, ChunkingProfilePolicy] = {
    CONTENT_PROFILE_SUTRA: ChunkingProfilePolicy(
        content_profile=CONTENT_PROFILE_SUTRA,
        target_token_limit=420,
        minimum_merge_tokens=0,
        overlap_sentences=0,
        merge_small_segments=False,
    ),
    CONTENT_PROFILE_BOOK: ChunkingProfilePolicy(
        content_profile=CONTENT_PROFILE_BOOK,
        target_token_limit=680,
        minimum_merge_tokens=140,
        overlap_sentences=1,
        merge_small_segments=True,
    ),
    CONTENT_PROFILE_TRANSCRIPT: ChunkingProfilePolicy(
        content_profile=CONTENT_PROFILE_TRANSCRIPT,
        target_token_limit=560,
        minimum_merge_tokens=90,
        overlap_sentences=0,
        merge_small_segments=True,
    ),
}


class AdaptiveChunker:
    """Split canonical extract text into retrieval-facing chunks."""

    def __init__(
        self,
        target_token_limit: int = 600,
        minimum_merge_tokens: int = 120,
        overlap_sentences: int = 1,
        merge_small_segments: bool = True,
        content_profile: str | None = None,
    ) -> None:
        self.target_token_limit = target_token_limit
        self.minimum_merge_tokens = minimum_merge_tokens
        self.overlap_sentences = overlap_sentences
        self.merge_small_segments = merge_small_segments
        self.content_profile = content_profile

    @classmethod
    def for_content_profile(cls, content_profile: str) -> AdaptiveChunker:
        """Create one chunker configured for the requested content profile."""
        policy = chunking_policy_for_profile(content_profile)
        return cls(
            target_token_limit=policy.target_token_limit,
            minimum_merge_tokens=policy.minimum_merge_tokens,
            overlap_sentences=policy.overlap_sentences,
            merge_small_segments=policy.merge_small_segments,
            content_profile=policy.content_profile,
        )

    def chunk(self, full_text: str, segments: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Chunk text with a structure-aware first pass and sentence-aware second pass."""
        source_segments = segments or self._segments_from_full_text(full_text)
        tier_one_segments = self._merge_small_segments(source_segments) if self.merge_small_segments else source_segments

        output_chunks: list[dict[str, Any]] = []
        for segment_index, segment in enumerate(tier_one_segments):
            text = self._segment_text(segment).strip()
            if not text:
                continue
            segment_chunks = self._split_segment(text)
            for chunk_text in segment_chunks:
                output_chunks.append(
                    {
                        "content": chunk_text,
                        "chunk_index": len(output_chunks),
                        "token_count": self.estimate_tokens(chunk_text),
                        "source_segment": segment_index,
                    }
                )

        return self._inject_overlap(output_chunks)

    def estimate_tokens(self, text: str) -> int:
        """Approximate token count for a text span."""
        word_count = len(text.split())
        return max(1, math.ceil(word_count * 1.3))

    def _segments_from_full_text(self, full_text: str) -> list[dict[str, Any]]:
        parts = [part.strip() for part in full_text.split("\n\n") if part.strip()]
        return [{"text": part} for part in parts]

    def _merge_small_segments(self, segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        pending_text = ""
        for segment in segments:
            text = self._segment_text(segment).strip()
            if not text:
                continue
            combined = f"{pending_text}\n\n{text}".strip() if pending_text else text
            if self.estimate_tokens(combined) < self.minimum_merge_tokens:
                pending_text = combined
                continue
            if pending_text:
                merged.append({"text": combined})
                pending_text = ""
            else:
                merged.append(segment)

        if pending_text:
            merged.append({"text": pending_text})
        return merged

    def _split_segment(self, text: str) -> list[str]:
        if self.estimate_tokens(text) <= self.target_token_limit:
            return [text]

        sentences = self._split_sentences(text)
        if len(sentences) <= 1:
            return [text]

        chunks: list[str] = []
        current: list[str] = []
        for sentence in sentences:
            candidate = " ".join(current + [sentence]).strip()
            if current and self.estimate_tokens(candidate) > self.target_token_limit:
                chunks.append(" ".join(current).strip())
                current = [sentence]
                continue
            current.append(sentence)

        if current:
            chunks.append(" ".join(current).strip())
        return chunks

    def _inject_overlap(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self.overlap_sentences <= 0:
            return chunks

        for index in range(1, len(chunks)):
            previous_sentences = self._split_sentences(chunks[index - 1]["content"])
            overlap = " ".join(previous_sentences[-self.overlap_sentences :]).strip()
            if not overlap:
                continue
            chunks[index]["content"] = f"{overlap} {chunks[index]['content']}".strip()
            chunks[index]["token_count"] = self.estimate_tokens(chunks[index]["content"])
        return chunks

    def _split_sentences(self, text: str) -> list[str]:
        sentences = re.split(r"(?<=[.!?])\s+", text.strip())
        return [sentence.strip() for sentence in sentences if sentence.strip()]

    @staticmethod
    def _segment_text(segment: dict[str, Any]) -> str:
        return str(segment.get("text", "")).strip()


def chunking_policy_for_profile(content_profile: str) -> ChunkingProfilePolicy:
    """Return the canonical chunking policy for one content profile."""
    try:
        return CHUNKING_PROFILE_POLICIES[content_profile]
    except KeyError as exc:  # pragma: no cover - caller validates upstream
        raise ValueError(f"unsupported_content_profile:{content_profile}") from exc
