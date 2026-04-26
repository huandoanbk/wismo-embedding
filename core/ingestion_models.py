from __future__ import annotations

import uuid

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from core.models import Base


class IngestionSourceItem(Base):
    """Registry record for one discovered source item in the embedding domain."""

    __tablename__ = "ingestion_source_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_config_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    external_item_id: Mapped[str | None] = mapped_column(String(500), nullable=True)
    normalized_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    source_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_modified: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)
    source_etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_bucket: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_document_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    discovery_status: Mapped[str] = mapped_column(String(50), default="discovered", nullable=False)
    processing_status: Mapped[str] = mapped_column(String(50), default="new", nullable=False)
    raw_storage_status: Mapped[str] = mapped_column(String(50), default="not_mirrored", nullable=False)
    first_seen_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    last_seen_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    last_processed_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now(), nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("source_type", "external_item_id", name="uq_ingestion_source_type_external_item_id"),
        Index("ix_ingestion_source_type_normalized_source_url", "source_type", "normalized_source_url"),
        Index("ix_ingestion_source_type_raw_bucket_raw_key", "source_type", "raw_bucket", "raw_key"),
        Index("ix_ingestion_source_items_content_sha256", "content_sha256"),
        Index("ix_ingestion_source_items_processing_raw_storage", "processing_status", "raw_storage_status"),
    )


class IngestionSourceConfig(Base):
    """Optional scan configuration registry for recurring source discovery."""

    __tablename__ = "ingestion_source_configs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    schedule_cron: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_enabled: Mapped[bool] = mapped_column(default=True, nullable=False)
    last_scan_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)
    last_scan_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now(), nullable=False)


class IngestionQueue(Base):
    """Execution-only queue rows created from registry decisions."""

    __tablename__ = "ingestion_queue"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_item_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_source_items.id", ondelete="CASCADE"),
        nullable=False,
    )
    job_type: Mapped[str] = mapped_column(String(100), default="ingest_source_item", nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    available_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    started_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)
    completed_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        Index("ix_ingestion_queue_status_priority_available_at", "status", "priority", "available_at"),
    )


class EmbeddingBatch(Base):
    """DB-backed execution queue for canonical chunk embedding batches."""

    __tablename__ = "embedding_queue_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_document_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source_item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    chunk_ids: Mapped[list[str]] = mapped_column(JSONB, default=list, nullable=False)
    batch_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    available_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    started_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)
    completed_at: Mapped[object | None] = mapped_column(DateTime(timezone=False), nullable=True)

    __table_args__ = (
        Index("ix_embedding_queue_batches_status_priority_available_at", "status", "priority", "available_at"),
        Index("ix_embedding_queue_batches_source_document_id", "source_document_id"),
        Index("ix_embedding_queue_batches_source_item_id", "source_item_id"),
    )
