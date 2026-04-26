from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from .ingestion_models import IngestionQueue, IngestionSourceItem

# File extensions that belong to the media (audio/video) pipeline.
_MEDIA_EXTENSIONS = frozenset({"mp3", "wav", "m4a", "mp4", "mkv", "mov"})


@dataclass
class QueueDecision:
    """Outcome returned by queue orchestration."""

    queued: bool
    queue_item: IngestionQueue | None = None
    skip_reason: str | None = None


class IngestionQueueService:
    """Create execution-only queue rows from registry items."""

    BLOCKED_PROCESSING_STATUSES = {
        "awaiting_transcript",
        "transcript_processing",
        "transcript_failed",
        "awaiting_extract_artifact",
        "extract_artifact_processing",
        "extract_artifact_failed",
        "awaiting_chunking",
        "chunking_processing",
        "chunking_failed",
        "awaiting_llm_refinement",
        "llm_refinement_processing",
        "llm_refinement_failed",
        "awaiting_llm_enrichment",
        "llm_enrichment_processing",
        "llm_enrichment_failed",
        "awaiting_embedding",
        "embedding_processing",
        "embedding_failed",
        "completed",
        "duplicate_known",
        "failed",
        # Legacy — kept so existing rows in the DB are not re-queued.
        "queued",
        "processing",
    }

    def __init__(self, db: Session) -> None:
        self.db = db

    def enqueue_if_needed(self, item: IngestionSourceItem) -> QueueDecision:
        """Create a queue row unless the item's state blocks execution.

        Sets initial source state and one concrete queue row for the next action:

        - media => ``awaiting_transcript`` + ``transcribe_audio``
        - text  => ``awaiting_extract_artifact`` + ``extract_text_artifact``
        """
        if item.processing_status in self.BLOCKED_PROCESSING_STATUSES:
            return QueueDecision(queued=False, skip_reason=item.processing_status)

        ext = self._infer_extension(item)
        if ext in _MEDIA_EXTENSIONS:
            target_status = "awaiting_transcript"
            job_type = "transcribe_audio"
        else:
            target_status = "awaiting_extract_artifact"
            job_type = "extract_text_artifact"

        metadata_json = dict(item.metadata_json or {})
        metadata_json["pipeline_checkpoint_stage"] = "new"
        item.metadata_json = metadata_json

        queue_item = IngestionQueue(source_item_id=item.id, job_type=job_type, status="pending")
        self.db.add(queue_item)
        item.processing_status = target_status
        item.updated_at = datetime.utcnow()
        self.db.flush()
        return QueueDecision(queued=True, queue_item=queue_item)

    @staticmethod
    def _infer_extension(item: IngestionSourceItem) -> str:
        """Return a normalized extension from metadata, path, filename, or mime type."""
        metadata = item.metadata_json or {}
        extension = str(metadata.get("extension") or "").strip().lower()
        if extension:
            return extension

        for candidate in (item.source_path, item.display_name):
            suffix = Path(candidate or "").suffix.lower().lstrip(".")
            if suffix:
                return suffix

        mime_type = (item.mime_type or "").lower()
        if "audio/" in mime_type:
            return "mp3"
        if "video/" in mime_type:
            return "mp4"
        return ""

    def get_active_queue_for_item(self, source_item_id) -> IngestionQueue | None:
        """Return an active queue row for an item when one exists."""
        stmt = select(IngestionQueue).where(
            IngestionQueue.source_item_id == source_item_id,
            IngestionQueue.status.in_(("pending", "processing")),
        )
        return self.db.execute(stmt).scalar_one_or_none()
