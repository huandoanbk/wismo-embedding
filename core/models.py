from __future__ import annotations
from typing import Optional, List
from sqlalchemy import String, Integer, Float, Boolean, Text, ARRAY, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from pgvector.sqlalchemy import Vector
from sqlalchemy.sql import func
import uuid

class Base(DeclarativeBase):
    pass

class SourceDocument(Base):
    __tablename__ = 'source_documents'

    id:                     Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type:            Mapped[str]              = mapped_column(String(30), nullable=False)
    source_kind:            Mapped[str]              = mapped_column(String(20), nullable=False)
    source_format:          Mapped[Optional[str]]    = mapped_column(String(20))
    title:                  Mapped[Optional[str]]    = mapped_column(String(500))
    author:                 Mapped[Optional[str]]    = mapped_column(String(200))
    language:               Mapped[str]              = mapped_column(String(10), default='vi')
    file_hash:              Mapped[Optional[str]]    = mapped_column(String(64))
    s3_raw_key:             Mapped[Optional[str]]    = mapped_column(String(500))
    s3_text_key:            Mapped[Optional[str]]    = mapped_column(String(500))
    type_metadata:          Mapped[Optional[dict]]   = mapped_column(JSONB, default=dict)
    total_chunks:           Mapped[int]              = mapped_column(Integer, default=0)
    is_active:              Mapped[bool]             = mapped_column(Boolean, default=True)
    is_production_approved: Mapped[bool]             = mapped_column(Boolean, default=False)
    extraction_version:     Mapped[Optional[str]]    = mapped_column(String(20))
    chunking_version:       Mapped[Optional[str]]    = mapped_column(String(20))
    enrichment_version:     Mapped[Optional[str]]    = mapped_column(String(20))
    embedding_version:      Mapped[Optional[str]]    = mapped_column(String(20))
    active_corpus_version:  Mapped[Optional[str]]    = mapped_column(String(20))
    ingested_at:            Mapped[Optional[object]] = mapped_column(DateTime, server_default=func.now())
    updated_at:             Mapped[Optional[object]] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    chunks: Mapped[List[ContentChunk]] = relationship('ContentChunk', back_populates='source', cascade='all, delete-orphan')


class ContentChunk(Base):
    __tablename__ = 'content_chunks'

    id:                 Mapped[uuid.UUID]           = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id:          Mapped[uuid.UUID]           = mapped_column(UUID(as_uuid=True), ForeignKey('source_documents.id', ondelete='CASCADE'), nullable=False)
    chunk_index:        Mapped[int]                 = mapped_column(Integer, nullable=False)
    content:            Mapped[str]                 = mapped_column(Text, nullable=False)
    language:           Mapped[str]                 = mapped_column(String(10), default='vi')
    token_count:        Mapped[Optional[int]]       = mapped_column(Integer)
    topic:              Mapped[Optional[str]]       = mapped_column(String(200))
    topic_tags:         Mapped[Optional[list]]      = mapped_column(ARRAY(Text))
    summary:            Mapped[Optional[str]]       = mapped_column(Text)
    teaching_type:      Mapped[Optional[str]]       = mapped_column(String(20))
    applicable_issues:  Mapped[Optional[list]]      = mapped_column(ARRAY(Text))
    depth_level:        Mapped[Optional[str]]       = mapped_column(String(15))
    emotional_tone:     Mapped[Optional[str]]       = mapped_column(String(15))
    page_start:         Mapped[Optional[int]]       = mapped_column(Integer)
    page_end:           Mapped[Optional[int]]       = mapped_column(Integer)
    section_heading:    Mapped[Optional[str]]       = mapped_column(String(300))
    paragraph_ids:      Mapped[Optional[dict]]      = mapped_column(JSONB)
    start_time:         Mapped[Optional[float]]     = mapped_column(Float)
    end_time:           Mapped[Optional[float]]     = mapped_column(Float)
    content_hash:       Mapped[Optional[str]]       = mapped_column(String(64))
    duplicate_of:       Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey('content_chunks.id', ondelete='SET NULL'), nullable=True)
    state:              Mapped[str]                 = mapped_column(String(20), default='active')
    chunking_version:   Mapped[Optional[str]]       = mapped_column(String(20))
    enrichment_version: Mapped[Optional[str]]       = mapped_column(String(20))
    embedding_version:  Mapped[Optional[str]]       = mapped_column(String(20))
    processing_version: Mapped[Optional[str]]       = mapped_column(String(50))
    created_at:         Mapped[Optional[object]]    = mapped_column(DateTime, server_default=func.now())

    source:    Mapped[SourceDocument]           = relationship('SourceDocument', back_populates='chunks')
    embedding: Mapped[Optional[ChunkEmbedding]] = relationship('ChunkEmbedding', back_populates='chunk', uselist=False, cascade='all, delete-orphan')

    __table_args__ = (
        UniqueConstraint('source_id', 'chunk_index', name='uq_chunk_source_index'),
    )


class ChunkEmbedding(Base):
    __tablename__ = 'chunk_embeddings'

    id:         Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chunk_id:   Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), ForeignKey('content_chunks.id', ondelete='CASCADE'), unique=True, nullable=False)
    embedding:  Mapped[object]           = mapped_column(Vector(1024), nullable=False)
    model_name: Mapped[str]              = mapped_column(String(50), default='bge-m3')
    created_at: Mapped[Optional[object]] = mapped_column(DateTime, server_default=func.now())

    chunk: Mapped[ContentChunk] = relationship('ContentChunk', back_populates='embedding')
