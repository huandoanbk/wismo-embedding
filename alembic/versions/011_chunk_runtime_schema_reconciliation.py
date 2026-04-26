"""reconcile chunk and embedding runtime schema drift

Revision ID: 011
Revises: 010
Create Date: 2026-04-10 00:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    content_chunk_columns = {column["name"] for column in inspector.get_columns("content_chunks")}
    if "sha256_checksum" not in content_chunk_columns:
        op.add_column(
            "content_chunks",
            sa.Column("sha256_checksum", sa.String(length=64), nullable=False, server_default=""),
        )
        op.execute("UPDATE content_chunks SET sha256_checksum = COALESCE(NULLIF(content_hash, ''), sha256_checksum)")

    chunk_embedding_columns = {column["name"] for column in inspector.get_columns("chunk_embeddings")}
    if "embedding_model" not in chunk_embedding_columns:
        op.add_column(
            "chunk_embeddings",
            sa.Column("embedding_model", sa.String(length=100), nullable=False, server_default="bge-m3"),
        )
        if "model_name" in chunk_embedding_columns:
            op.execute("UPDATE chunk_embeddings SET embedding_model = COALESCE(model_name, embedding_model)")

    if "chunking_version" not in chunk_embedding_columns:
        op.add_column(
            "chunk_embeddings",
            sa.Column("chunking_version", sa.String(length=50), nullable=False, server_default="chunk-v1.0"),
        )

    if "embedded_at" not in chunk_embedding_columns:
        op.add_column(
            "chunk_embeddings",
            sa.Column("embedded_at", sa.DateTime(), nullable=True, server_default=sa.func.now()),
        )
        if "created_at" in chunk_embedding_columns:
            op.execute("UPDATE chunk_embeddings SET embedded_at = COALESCE(created_at, embedded_at)")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    chunk_embedding_columns = {column["name"] for column in inspector.get_columns("chunk_embeddings")}
    content_chunk_columns = {column["name"] for column in inspector.get_columns("content_chunks")}

    if "embedded_at" in chunk_embedding_columns:
        op.drop_column("chunk_embeddings", "embedded_at")
    if "chunking_version" in chunk_embedding_columns:
        op.drop_column("chunk_embeddings", "chunking_version")
    if "embedding_model" in chunk_embedding_columns:
        op.drop_column("chunk_embeddings", "embedding_model")
    if "sha256_checksum" in content_chunk_columns:
        op.drop_column("content_chunks", "sha256_checksum")
