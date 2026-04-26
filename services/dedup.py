"""Chunk-level near-duplicate detection helpers."""

from __future__ import annotations

import hashlib
import re
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import settings
from core.models import ContentChunk


class DeduplicatorService:
    """Compute chunk content hashes and detect near-duplicates."""

    def __init__(self, db: Session, max_distance: int | None = None) -> None:
        self.db = db
        self.max_distance = max_distance if max_distance is not None else settings.dedup_simhash_max_distance

    def compute_content_hash(self, content: str) -> str:
        """Compute a 64-bit SimHash-like fingerprint stored as hex."""
        tokens = self._tokenize(content)
        if not tokens:
            return "0" * 16

        weights = [0] * 64
        for token in tokens:
            digest = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
            for bit_index in range(64):
                bit = 1 << bit_index
                weights[bit_index] += 1 if digest & bit else -1

        fingerprint = 0
        for bit_index, weight in enumerate(weights):
            if weight >= 0:
                fingerprint |= 1 << bit_index
        return f"{fingerprint:016x}"

    def find_near_duplicate(self, content: str) -> uuid.UUID | None:
        """Return the original chunk id for a near-duplicate, if any."""
        candidate_hash = self.compute_content_hash(content)
        stmt = select(ContentChunk.id, ContentChunk.content_hash).where(ContentChunk.duplicate_of.is_(None))
        for chunk_id, existing_hash in self.db.execute(stmt):
            if not existing_hash:
                continue
            if self._hamming_distance(candidate_hash, str(existing_hash)) <= self.max_distance:
                return chunk_id
        return None

    def is_near_duplicate(self, content: str) -> bool:
        """Return True when content matches an existing chunk within threshold."""
        return self.find_near_duplicate(content) is not None

    def _hamming_distance(self, left: str, right: str) -> int:
        return (int(left, 16) ^ int(right, 16)).bit_count()

    def _tokenize(self, content: str) -> list[str]:
        return re.findall(r"\w+", content.lower())
