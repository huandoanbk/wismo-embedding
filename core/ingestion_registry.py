from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import BinaryIO

from sqlalchemy import select
from sqlalchemy.orm import Session

from connectors.base import SourceCandidate
from core.content_profiles import CONTENT_PROFILE_KEY, merge_content_profile_metadata

from .ingestion_models import IngestionSourceItem


@dataclass
class RegistryDecision:
    """Outcome returned by registry upsert and dedup decisions."""

    item: IngestionSourceItem
    skip_reason: str | None = None


class IngestionRegistryService:
    """Persist source candidates into registry state and dedup lifecycle."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def find_by_external_item_id(self, source_type: str, external_item_id: str | None) -> IngestionSourceItem | None:
        """Look up a registry item by source type and external item id."""
        if not external_item_id:
            return None
        stmt = select(IngestionSourceItem).where(
            IngestionSourceItem.source_type == source_type,
            IngestionSourceItem.external_item_id == external_item_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def find_by_normalized_source_url(self, source_type: str, normalized_source_url: str | None) -> IngestionSourceItem | None:
        """Look up a registry item by source type and normalized URL."""
        if not normalized_source_url:
            return None
        stmt = select(IngestionSourceItem).where(
            IngestionSourceItem.source_type == source_type,
            IngestionSourceItem.normalized_source_url == normalized_source_url,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def find_by_raw_location(self, source_type: str, raw_bucket: str | None, raw_key: str | None) -> IngestionSourceItem | None:
        """Look up a registry item by raw storage identity when available."""
        if not raw_bucket or not raw_key:
            return None
        stmt = select(IngestionSourceItem).where(
            IngestionSourceItem.source_type == source_type,
            IngestionSourceItem.raw_bucket == raw_bucket,
            IngestionSourceItem.raw_key == raw_key,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def find_by_source_identity(self, candidate: SourceCandidate) -> IngestionSourceItem | None:
        """Resolve a registry item by canonical source identities in priority order."""
        for lookup in (
            self.find_by_external_item_id(candidate.source_type, candidate.external_item_id),
            self.find_by_normalized_source_url(candidate.source_type, candidate.normalized_source_url),
            self.find_by_raw_location(candidate.source_type, candidate.raw_bucket, candidate.raw_key),
        ):
            if lookup is not None:
                return lookup
        return None

    def source_metadata_unchanged(self, item: IngestionSourceItem, candidate: SourceCandidate) -> bool:
        """Return True when candidate metadata does not change registry state."""
        candidate_metadata = self._resolved_candidate_metadata(candidate, existing=item)
        return (
            item.display_name == candidate.display_name
            and item.source_path == candidate.source_path
            and item.mime_type == candidate.mime_type
            and item.size_bytes == candidate.size_bytes
            and item.last_modified == candidate.last_modified
            and item.raw_bucket == candidate.raw_bucket
            and item.raw_key == candidate.raw_key
            and item.metadata_json == candidate_metadata
        )

    def upsert_candidate(self, candidate: SourceCandidate) -> RegistryDecision:
        """Create or update a registry item from a connector candidate."""
        item = self.find_by_source_identity(candidate)
        now = datetime.utcnow()

        if item is not None and self.source_metadata_unchanged(item, candidate):
            item.discovery_status = "seen_unchanged"
            item.last_seen_at = now
            item.updated_at = now
            self.db.flush()
            return RegistryDecision(item=item, skip_reason="seen_unchanged")

        if item is None:
            metadata_json = self._resolved_candidate_metadata(candidate)
            item = IngestionSourceItem(
                source_type=candidate.source_type,
                external_item_id=candidate.external_item_id,
                normalized_source_url=candidate.normalized_source_url,
                display_name=candidate.display_name,
                source_path=candidate.source_path,
                mime_type=candidate.mime_type,
                size_bytes=candidate.size_bytes,
                last_modified=candidate.last_modified,
                source_etag=self._source_etag(candidate),
                raw_bucket=candidate.raw_bucket,
                raw_key=candidate.raw_key,
                metadata_json=metadata_json,
                discovery_status="discovered",
                processing_status="new",
                raw_storage_status=self._initial_raw_storage_status(candidate),
                first_seen_at=now,
                last_seen_at=now,
                updated_at=now,
            )
            self.db.add(item)
        else:
            previous_profile = item.metadata_json.get(CONTENT_PROFILE_KEY)
            metadata_json = self._resolved_candidate_metadata(candidate, existing=item)
            item.external_item_id = candidate.external_item_id
            item.normalized_source_url = candidate.normalized_source_url
            item.display_name = candidate.display_name
            item.source_path = candidate.source_path
            item.mime_type = candidate.mime_type
            item.size_bytes = candidate.size_bytes
            item.last_modified = candidate.last_modified
            item.source_etag = self._source_etag(candidate)
            item.raw_bucket = candidate.raw_bucket
            item.raw_key = candidate.raw_key
            item.metadata_json = metadata_json
            item.discovery_status = "updated_at_source"
            item.last_seen_at = now
            item.updated_at = now
            if previous_profile != metadata_json.get(CONTENT_PROFILE_KEY) and item.source_document_id is not None:
                item.processing_status = "reprocess_requested"
        self.db.flush()
        return RegistryDecision(item=item)

    def compute_content_sha256(self, stream: BinaryIO | bytes | bytearray) -> str:
        """Compute SHA-256 from a stream or bytes payload without losing position."""
        digest = hashlib.sha256()
        if isinstance(stream, (bytes, bytearray)):
            digest.update(stream)
            return digest.hexdigest()

        current_position = stream.tell() if hasattr(stream, "tell") else None
        if hasattr(stream, "seek"):
            stream.seek(0)
        while True:
            chunk = stream.read(8192)
            if not chunk:
                break
            digest.update(chunk)
        if current_position is not None and hasattr(stream, "seek"):
            stream.seek(current_position)
        return digest.hexdigest()

    def update_content_sha256(self, item: IngestionSourceItem, stream: BinaryIO | bytes | bytearray) -> str:
        """Compute and store content hash for a registry item."""
        item.content_sha256 = self.compute_content_sha256(stream)
        item.updated_at = datetime.utcnow()
        self.db.flush()
        return item.content_sha256

    def mark_duplicate_known(self, item: IngestionSourceItem) -> None:
        """Mark a registry item as known duplicate content."""
        item.processing_status = "duplicate_known"
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_reprocess_requested(self, item: IngestionSourceItem) -> None:
        """Move an item into explicit reprocess state."""
        item.processing_status = "reprocess_requested"
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def find_existing_completed_by_content_sha256(self, content_sha256: str | None) -> IngestionSourceItem | None:
        """Return a completed item for the same content hash when present."""
        if not content_sha256:
            return None
        stmt = select(IngestionSourceItem).where(
            IngestionSourceItem.content_sha256 == content_sha256,
            IngestionSourceItem.processing_status == "completed",
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def apply_content_dedup(self, item: IngestionSourceItem, stream: BinaryIO | bytes | bytearray) -> RegistryDecision:
        """Compute hash, detect duplicate content, and update processing state."""
        content_sha256 = self.update_content_sha256(item, stream)
        existing = self.find_existing_completed_by_content_sha256(content_sha256)
        if existing is not None and existing.id != item.id and item.processing_status != "reprocess_requested":
            self.mark_duplicate_known(item)
            return RegistryDecision(item=item, skip_reason="duplicate_known")
        return RegistryDecision(item=item)

    def mark_raw_deleted(self, item: IngestionSourceItem) -> None:
        """Persist deleted raw state without forgetting completed history."""
        item.raw_storage_status = "deleted"
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_raw_storage_status(self, item: IngestionSourceItem, status: str) -> None:
        """Persist raw storage lifecycle state."""
        item.raw_storage_status = status
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_storage_failed(self, item: IngestionSourceItem, error_code: str | None = None, error_message: str | None = None) -> None:
        """Persist raw storage failure context."""
        item.raw_storage_status = "storage_failed"
        item.last_error_code = error_code
        item.last_error_message = error_message
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_processing_failed(self, item: IngestionSourceItem, error_code: str | None = None, error_message: str | None = None) -> None:
        """Persist processing failure and increment retry count."""
        item.processing_status = "failed"
        item.retry_count += 1
        item.last_error_code = error_code
        item.last_error_message = error_message
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_llm_enrichment_failed(self, item: IngestionSourceItem, error_code: str | None = None, error_message: str | None = None) -> None:
        """Persist LLM enrichment failure without counting as a generic pipeline retry."""
        item.processing_status = "llm_enrichment_failed"
        item.last_error_code = error_code
        item.last_error_message = error_message
        item.updated_at = datetime.utcnow()
        self.db.flush()

    def mark_processing_completed(self, item: IngestionSourceItem) -> None:
        """Persist completed processing state."""
        now = datetime.utcnow()
        item.processing_status = "completed"
        item.last_processed_at = now
        item.updated_at = now
        self.db.flush()

    @staticmethod
    def _source_etag(candidate: SourceCandidate) -> str | None:
        return candidate.metadata_json.get("etag")

    @staticmethod
    def _initial_raw_storage_status(candidate: SourceCandidate) -> str:
        return "stored" if candidate.raw_bucket and candidate.raw_key else "not_mirrored"

    @staticmethod
    def _resolved_candidate_metadata(candidate: SourceCandidate, existing: IngestionSourceItem | None = None) -> dict:
        merged_metadata = dict(existing.metadata_json or {}) if existing is not None else {}
        merged_metadata.update(candidate.metadata_json)
        return merge_content_profile_metadata(
            merged_metadata,
            existing_metadata=existing.metadata_json if existing is not None else None,
        )


def bytes_stream(payload: bytes) -> BytesIO:
    """Create a reusable in-memory stream for hash/dedup tests."""
    return BytesIO(payload)
