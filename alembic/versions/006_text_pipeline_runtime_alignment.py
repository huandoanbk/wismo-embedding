"""align source_documents and content_chunks for text pipeline runtime

Revision ID: 006
Revises: 005
Create Date: 2026-04-07 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import UUID


revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    source_document_columns = {column["name"] for column in inspector.get_columns("source_documents")}
    content_chunk_columns = {column["name"] for column in inspector.get_columns("content_chunks")}

    source_document_additions = [
        ("s3_raw_key", sa.Column("s3_raw_key", sa.String(length=500), nullable=True)),
        ("s3_text_key", sa.Column("s3_text_key", sa.String(length=500), nullable=True)),
        ("total_chunks", sa.Column("total_chunks", sa.Integer(), nullable=False, server_default="0")),
        ("ingested_at", sa.Column("ingested_at", sa.DateTime(), nullable=True, server_default=sa.func.now())),
        ("updated_at", sa.Column("updated_at", sa.DateTime(), nullable=True, server_default=sa.func.now())),
        ("extraction_version", sa.Column("extraction_version", sa.String(length=20), nullable=True)),
        ("chunking_version", sa.Column("chunking_version", sa.String(length=20), nullable=True)),
        ("enrichment_version", sa.Column("enrichment_version", sa.String(length=20), nullable=True)),
        ("embedding_version", sa.Column("embedding_version", sa.String(length=20), nullable=True)),
        ("active_corpus_version", sa.Column("active_corpus_version", sa.String(length=20), nullable=True)),
    ]
    for name, column in source_document_additions:
        if name not in source_document_columns:
            op.add_column("source_documents", column)

    content_chunk_additions = [
        ("language", sa.Column("language", sa.String(length=10), nullable=True, server_default="vi")),
        ("token_count", sa.Column("token_count", sa.Integer(), nullable=True)),
        ("page_start", sa.Column("page_start", sa.Integer(), nullable=True)),
        ("page_end", sa.Column("page_end", sa.Integer(), nullable=True)),
        ("section_heading", sa.Column("section_heading", sa.String(length=300), nullable=True)),
        ("paragraph_ids", sa.Column("paragraph_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True)),
        ("start_time", sa.Column("start_time", sa.Float(), nullable=True)),
        ("end_time", sa.Column("end_time", sa.Float(), nullable=True)),
        ("content_hash", sa.Column("content_hash", sa.String(length=64), nullable=True)),
        ("duplicate_of", sa.Column("duplicate_of", UUID(as_uuid=True), nullable=True)),
        ("chunking_version", sa.Column("chunking_version", sa.String(length=20), nullable=True)),
        ("enrichment_version", sa.Column("enrichment_version", sa.String(length=20), nullable=True)),
        ("embedding_version", sa.Column("embedding_version", sa.String(length=20), nullable=True)),
        ("processing_version", sa.Column("processing_version", sa.String(length=50), nullable=True)),
    ]
    for name, column in content_chunk_additions:
        if name not in content_chunk_columns:
            op.add_column("content_chunks", column)

    topic_tags_type = next(column["type"] for column in inspector.get_columns("content_chunks") if column["name"] == "topic_tags")
    applicable_issues_type = next(
        column["type"] for column in inspector.get_columns("content_chunks") if column["name"] == "applicable_issues"
    )

    if not isinstance(topic_tags_type, postgresql.ARRAY):
        op.execute(
            """
            ALTER TABLE content_chunks
            ALTER COLUMN topic_tags TYPE TEXT[]
            USING CASE
              WHEN topic_tags IS NULL THEN NULL
              WHEN topic_tags = '[]'::jsonb THEN ARRAY[]::TEXT[]
              ELSE string_to_array(translate(topic_tags::text, '[]"', ''), ',')
            END
            """
        )
    if not isinstance(applicable_issues_type, postgresql.ARRAY):
        op.execute(
            """
            ALTER TABLE content_chunks
            ALTER COLUMN applicable_issues TYPE TEXT[]
            USING CASE
              WHEN applicable_issues IS NULL THEN NULL
              WHEN applicable_issues = '[]'::jsonb THEN ARRAY[]::TEXT[]
              ELSE string_to_array(translate(applicable_issues::text, '[]"', ''), ',')
            END
            """
        )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE content_chunks
        ALTER COLUMN applicable_issues TYPE JSONB
        USING CASE
          WHEN applicable_issues IS NULL THEN '[]'::jsonb
          ELSE to_jsonb(applicable_issues)
        END
        """
    )
    op.execute(
        """
        ALTER TABLE content_chunks
        ALTER COLUMN topic_tags TYPE JSONB
        USING CASE
          WHEN topic_tags IS NULL THEN '[]'::jsonb
          ELSE to_jsonb(topic_tags)
        END
        """
    )

    op.drop_column("content_chunks", "processing_version")
    op.drop_column("content_chunks", "embedding_version")
    op.drop_column("content_chunks", "enrichment_version")
    op.drop_column("content_chunks", "chunking_version")
    op.drop_column("content_chunks", "duplicate_of")
    op.drop_column("content_chunks", "content_hash")
    op.drop_column("content_chunks", "end_time")
    op.drop_column("content_chunks", "start_time")
    op.drop_column("content_chunks", "paragraph_ids")
    op.drop_column("content_chunks", "section_heading")
    op.drop_column("content_chunks", "page_end")
    op.drop_column("content_chunks", "page_start")
    op.drop_column("content_chunks", "token_count")
    op.drop_column("content_chunks", "language")

    op.drop_column("source_documents", "active_corpus_version")
    op.drop_column("source_documents", "embedding_version")
    op.drop_column("source_documents", "enrichment_version")
    op.drop_column("source_documents", "chunking_version")
    op.drop_column("source_documents", "extraction_version")
    op.drop_column("source_documents", "updated_at")
    op.drop_column("source_documents", "ingested_at")
    op.drop_column("source_documents", "total_chunks")
    op.drop_column("source_documents", "s3_text_key")
    op.drop_column("source_documents", "s3_raw_key")
