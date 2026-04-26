from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import ARRAY, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func

class Base(DeclarativeBase):
    pass

class SourceDocument(Base):
    __tablename__ = 'source_documents'

    id:                     Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type:            Mapped[str]              = mapped_column(String(30), nullable=False)
    source_url:             Mapped[Optional[str]]    = mapped_column(String(500))
    source_kind:            Mapped[str]              = mapped_column(String(20), nullable=False)
    source_format:          Mapped[Optional[str]]    = mapped_column(String(20))
    title:                  Mapped[Optional[str]]    = mapped_column(String(500))
    author:                 Mapped[Optional[str]]    = mapped_column(String(200))
    language:               Mapped[str]              = mapped_column(String(10), default='vi')
    topic_tags:             Mapped[Optional[list]]   = mapped_column(JSONB, default=list)
    registry_item_id:       Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    content_sha256:         Mapped[Optional[str]]    = mapped_column(String(64))
    file_hash:              Mapped[Optional[str]]    = mapped_column(String(64))
    s3_raw_key:             Mapped[Optional[str]]    = mapped_column(String(500))
    s3_text_key:            Mapped[Optional[str]]    = mapped_column(String(500))
    type_metadata:          Mapped[Optional[dict]]   = mapped_column(JSONB, default=dict)
    total_chunks:           Mapped[int]              = mapped_column(Integer, default=0)
    is_active:              Mapped[bool]             = mapped_column(Boolean, default=True)
    extraction_version:     Mapped[Optional[str]]    = mapped_column(String(20))
    chunking_version:       Mapped[Optional[str]]    = mapped_column(String(20))
    enrichment_version:     Mapped[Optional[str]]    = mapped_column(String(20))
    embedding_version:      Mapped[Optional[str]]    = mapped_column(String(20))
    active_corpus_version:  Mapped[Optional[str]]    = mapped_column(String(20))
    created_at:             Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now())
    ingested_at:            Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now())
    updated_at:             Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now(), onupdate=func.now())

    chunks: Mapped[List[ContentChunk]] = relationship('ContentChunk', back_populates='source', cascade='all, delete-orphan')


class ContentChunk(Base):
    __tablename__ = 'content_chunks'

    id:                 Mapped[uuid.UUID]           = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id:          Mapped[uuid.UUID]           = mapped_column(UUID(as_uuid=True), ForeignKey('source_documents.id', ondelete='CASCADE'), nullable=False)
    chunk_index:        Mapped[int]                 = mapped_column(Integer, nullable=False)
    content:            Mapped[str]                 = mapped_column(Text, nullable=False)
    language:           Mapped[str]                 = mapped_column(String(10), default='vi')
    token_count:        Mapped[Optional[int]]       = mapped_column(Integer)
    topic:              Mapped[str]                 = mapped_column(String(255), default="")
    topic_tags:         Mapped[list[str]]           = mapped_column(ARRAY(Text), default=list)
    summary:            Mapped[str]                 = mapped_column(Text, default="")
    teaching_type:      Mapped[str]                 = mapped_column(String(100), default="unknown")
    applicable_issues:  Mapped[list[str]]           = mapped_column(ARRAY(Text), default=list)
    depth_level:        Mapped[str]                 = mapped_column(String(50), default="general")
    emotional_tone:     Mapped[str]                 = mapped_column(String(50), default="neutral")
    page_start:         Mapped[Optional[int]]       = mapped_column(Integer)
    page_end:           Mapped[Optional[int]]       = mapped_column(Integer)
    section_heading:    Mapped[Optional[str]]       = mapped_column(String(300))
    paragraph_ids:      Mapped[Optional[dict]]      = mapped_column(JSONB)
    start_time:         Mapped[Optional[float]]     = mapped_column(Float)
    end_time:           Mapped[Optional[float]]     = mapped_column(Float)
    content_hash:       Mapped[Optional[str]]       = mapped_column(String(64))
    sha256_checksum:    Mapped[str]                 = mapped_column(String(64), default="")
    duplicate_of:       Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('content_chunks.id', ondelete='SET NULL'), nullable=True)
    state:              Mapped[str]                 = mapped_column(String(20), default='active')
    chunking_version:   Mapped[Optional[str]]       = mapped_column(String(20))
    enrichment_version: Mapped[Optional[str]]       = mapped_column(String(20))
    embedding_version:  Mapped[Optional[str]]       = mapped_column(String(20))
    processing_version: Mapped[Optional[str]]       = mapped_column(String(50))
    created_at:         Mapped[Optional[datetime]]  = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now())

    source:    Mapped[SourceDocument]           = relationship('SourceDocument', back_populates='chunks')
    embedding: Mapped[Optional[ChunkEmbedding]] = relationship('ChunkEmbedding', back_populates='chunk', uselist=False, cascade='all, delete-orphan')

    __table_args__ = (
        UniqueConstraint('source_id', 'chunk_index', name='uq_chunk_source_index'),
    )


class ChunkEmbedding(Base):
    __tablename__ = 'chunk_embeddings'

    id:               Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chunk_id:         Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), ForeignKey('content_chunks.id', ondelete='CASCADE'), unique=True, nullable=False)
    embedding:        Mapped[object]             = mapped_column(Vector(1024), nullable=False)
    model_name:       Mapped[str]                = mapped_column("embedding_model", String(100), default='bge-m3')
    chunking_version: Mapped[str]                = mapped_column(String(50), default='chunk-v1.0')
    created_at:       Mapped[Optional[datetime]] = mapped_column("embedded_at", DateTime, default=datetime.utcnow, server_default=func.now())

    chunk: Mapped[ContentChunk] = relationship('ContentChunk', back_populates='embedding')


class WorkerAlert(Base):
    """Operational alert history for GPU workers and related services."""

    __tablename__ = "worker_alerts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="unresolved", nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    occurrence_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    instance_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_item_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    details_json: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_worker_alerts_status_last_seen_at", "status", "last_seen_at"),
        Index("ix_worker_alerts_severity_source_status", "severity", "source", "status"),
        Index("ix_worker_alerts_fingerprint_last_seen_at", "fingerprint", "last_seen_at"),
    )


class WorkerImage(Base):
    """DB-backed control-plane record for one GPU worker image reference."""

    __tablename__ = "worker_images"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lane: Mapped[str] = mapped_column(String(20), nullable=False)
    image_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    validation_status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    last_validated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_worker_images_lane_created_at", "lane", "created_at"),
        Index(
            "ix_worker_images_lane_active_unique",
            "lane",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
    )


class LlmProviderConfig(Base):
    """Singleton config row for LLM provider selection and API credentials.

    Provider ``vastai`` dispatches enrichment tasks to a self-hosted vLLM
    instance on Vast.ai.  Provider ``api`` calls an OpenAI-compatible endpoint
    (e.g. DeepInfra) directly from the VPS, with no Celery/Vast.ai overhead.
    """

    __tablename__ = "llm_provider_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(String(20), nullable=False, default="vastai")
    api_base_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_key: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_model: Mapped[str] = mapped_column(String(200), nullable=False, default="Qwen/Qwen3-14B")
    vastai_max_in_flight_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class LlmOperationConfig(Base):
    """Operation-scoped config row for LLM refinement or enrichment runtime.

    Keeps runtime/provider selection independent per operation while allowing
    callers to set the same values for both operations when shared behavior is
    desired.
    """

    __tablename__ = "llm_operation_configs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    provider: Mapped[str] = mapped_column(String(20), nullable=False, default="vastai")
    api_base_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_key: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_model: Mapped[str] = mapped_column(String(200), nullable=False, default="Qwen/Qwen3-14B")
    vastai_max_in_flight_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class PipelineSchedulerConfig(Base):
    """DB-backed scheduler config for one pipeline stage.

    Used by Celery Beat polling logic so FastAPI/SQLAlchemy can expose
    cron-driven automation controls similar to django-celery-beat, while
    keeping manual dashboard triggers available.
    """

    __tablename__ = "pipeline_scheduler_configs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    stage: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    cron_expression: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    batch_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_run_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class VastaiLaneConfig(Base):
    """Per-lane Vast.ai search configuration editable from the dashboard.

    One row per lane (whisper, embedding, llm). All fields are optional —
    NULL means fall back to the value in core.config.settings.
    """

    __tablename__ = "vastai_lane_configs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lane: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    search_filter: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    disk_gb: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_hourly_cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    max_instances: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    destroy_idle_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    idle_timeout_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class PipelineOptions(Base):
    """Singleton config row for pipeline-wide behavioural options.

    ``options_json`` holds a flexible JSONB dict so new options can be added
    without schema migrations.  Current keys:

    - ``llm_enrich_profiles``: list[str] — content profiles that go through LLM
      enrichment (default: all profiles).  Items whose profile is NOT in this
      list skip enrichment and advance directly to embedding.
    """

    __tablename__ = "pipeline_options"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    options_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
