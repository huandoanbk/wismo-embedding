"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-01-01 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy import ARRAY, Text

revision = '001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS vector')

    op.create_table('source_documents',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('source_type', sa.String(30), nullable=False),
        sa.Column('source_kind', sa.String(20), nullable=False),
        sa.Column('source_format', sa.String(20)),
        sa.Column('title', sa.String(500)),
        sa.Column('author', sa.String(200)),
        sa.Column('language', sa.String(10), server_default='vi'),
        sa.Column('file_hash', sa.String(64)),
        sa.Column('s3_raw_key', sa.String(500)),
        sa.Column('s3_text_key', sa.String(500)),
        sa.Column('type_metadata', JSONB, server_default='{}'),
        sa.Column('total_chunks', sa.Integer, server_default='0'),
        sa.Column('is_active', sa.Boolean, server_default='true'),
        sa.Column('is_production_approved', sa.Boolean, server_default='false'),
        sa.Column('extraction_version', sa.String(20)),
        sa.Column('chunking_version', sa.String(20)),
        sa.Column('enrichment_version', sa.String(20)),
        sa.Column('embedding_version', sa.String(20)),
        sa.Column('active_corpus_version', sa.String(20)),
        sa.Column('ingested_at', sa.DateTime, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime, server_default=sa.func.now()),
    )

    op.create_table('content_chunks',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('source_id', UUID(as_uuid=True), sa.ForeignKey('source_documents.id', ondelete='CASCADE'), nullable=False),
        sa.Column('chunk_index', sa.Integer, nullable=False),
        sa.Column('content', sa.Text, nullable=False),
        sa.Column('language', sa.String(10), server_default='vi'),
        sa.Column('token_count', sa.Integer),
        sa.Column('topic', sa.String(200)),
        sa.Column('topic_tags', ARRAY(Text)),
        sa.Column('summary', sa.Text),
        sa.Column('teaching_type', sa.String(20)),
        sa.Column('applicable_issues', ARRAY(Text)),
        sa.Column('depth_level', sa.String(15)),
        sa.Column('emotional_tone', sa.String(15)),
        sa.Column('page_start', sa.Integer),
        sa.Column('page_end', sa.Integer),
        sa.Column('section_heading', sa.String(300)),
        sa.Column('paragraph_ids', JSONB),
        sa.Column('start_time', sa.Float),
        sa.Column('end_time', sa.Float),
        sa.Column('content_hash', sa.String(64)),
        sa.Column('duplicate_of', UUID(as_uuid=True), sa.ForeignKey('content_chunks.id', ondelete='SET NULL'), nullable=True),
        sa.Column('state', sa.String(20), server_default='active'),
        sa.Column('chunking_version', sa.String(20)),
        sa.Column('enrichment_version', sa.String(20)),
        sa.Column('embedding_version', sa.String(20)),
        sa.Column('processing_version', sa.String(50)),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now()),
        sa.UniqueConstraint('source_id', 'chunk_index', name='uq_chunk_source_index'),
    )

    op.create_table('chunk_embeddings',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('chunk_id', UUID(as_uuid=True), sa.ForeignKey('content_chunks.id', ondelete='CASCADE'), unique=True, nullable=False),
        sa.Column('embedding', sa.Text, nullable=False),
        sa.Column('model_name', sa.String(50), server_default='bge-m3'),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now()),
    )

    op.execute('ALTER TABLE chunk_embeddings ALTER COLUMN embedding TYPE vector(1024) USING embedding::vector(1024)')
    op.execute('CREATE INDEX idx_embedding_vector ON chunk_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)')
    op.execute('CREATE INDEX idx_topic_tags ON content_chunks USING GIN (topic_tags)')
    op.execute('CREATE INDEX idx_applicable_issues ON content_chunks USING GIN (applicable_issues)')

def downgrade() -> None:
    op.drop_table('chunk_embeddings')
    op.drop_table('content_chunks')
    op.drop_table('source_documents')
    op.execute('DROP EXTENSION IF EXISTS vector')
