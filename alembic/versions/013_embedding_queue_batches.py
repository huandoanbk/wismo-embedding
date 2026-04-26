"""add embedding queue batches

Revision ID: 013
Revises: 012
Create Date: 2026-04-11 11:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "embedding_queue_batches",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source_document_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_item_id", UUID(as_uuid=True), nullable=True),
        sa.Column("chunk_ids", JSONB, server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("batch_key", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=50), server_default="pending", nullable=False),
        sa.Column("priority", sa.Integer(), server_default="5", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("available_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("batch_key", name="uq_embedding_queue_batches_batch_key"),
    )
    op.create_index(
        "ix_embedding_queue_batches_status_priority_available_at",
        "embedding_queue_batches",
        ["status", "priority", "available_at"],
    )
    op.create_index(
        "ix_embedding_queue_batches_source_document_id",
        "embedding_queue_batches",
        ["source_document_id"],
    )
    op.create_index(
        "ix_embedding_queue_batches_source_item_id",
        "embedding_queue_batches",
        ["source_item_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_embedding_queue_batches_source_item_id", table_name="embedding_queue_batches")
    op.drop_index("ix_embedding_queue_batches_source_document_id", table_name="embedding_queue_batches")
    op.drop_index("ix_embedding_queue_batches_status_priority_available_at", table_name="embedding_queue_batches")
    op.drop_table("embedding_queue_batches")
