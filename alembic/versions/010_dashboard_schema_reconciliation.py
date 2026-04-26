"""reconcile dashboard schema drift

Revision ID: 010
Revises: 009
Create Date: 2026-04-09 23:55:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    _ensure_source_documents_columns(inspector)
    _ensure_ingestion_source_configs(table_names)
    _ensure_ingestion_source_items(table_names)
    _ensure_ingestion_queue(table_names)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}
    if "created_at" in columns:
        op.drop_column("source_documents", "created_at")


def _ensure_source_documents_columns(inspector: sa.Inspector) -> None:
    columns = {column["name"] for column in inspector.get_columns("source_documents")}
    indexes = {index["name"] for index in inspector.get_indexes("source_documents")}

    if "created_at" not in columns:
        op.add_column(
            "source_documents",
            sa.Column("created_at", sa.DateTime(), nullable=True, server_default=sa.func.now()),
        )
        op.execute("UPDATE source_documents SET created_at = COALESCE(created_at, ingested_at, NOW())")

    if "topic_tags" not in columns:
        op.add_column(
            "source_documents",
            sa.Column("topic_tags", JSONB, nullable=True, server_default=sa.text("'[]'::jsonb")),
        )

    if "idx_source_registry_item_id" not in indexes:
        op.create_index("idx_source_registry_item_id", "source_documents", ["registry_item_id"])

    if "idx_source_content_sha256" not in indexes:
        op.create_index("idx_source_content_sha256", "source_documents", ["content_sha256"])


def _ensure_ingestion_source_configs(table_names: set[str]) -> None:
    if "ingestion_source_configs" in table_names:
        return

    op.create_table(
        "ingestion_source_configs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("config_json", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("schedule_cron", sa.String(length=100), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("last_scan_at", sa.DateTime(), nullable=True),
        sa.Column("last_scan_status", sa.String(length=50), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )


def _ensure_ingestion_source_items(table_names: set[str]) -> None:
    if "ingestion_source_items" in table_names:
        return

    op.create_table(
        "ingestion_source_items",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source_config_id", UUID(as_uuid=True), nullable=True),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("external_item_id", sa.String(length=500), nullable=True),
        sa.Column("normalized_source_url", sa.Text(), nullable=True),
        sa.Column("display_name", sa.String(length=500), nullable=True),
        sa.Column("source_path", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("last_modified", sa.DateTime(), nullable=True),
        sa.Column("source_etag", sa.String(length=255), nullable=True),
        sa.Column("raw_bucket", sa.String(length=255), nullable=True),
        sa.Column("raw_key", sa.Text(), nullable=True),
        sa.Column("metadata_json", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),
        sa.Column("source_document_id", UUID(as_uuid=True), nullable=True),
        sa.Column("discovery_status", sa.String(length=50), server_default="discovered", nullable=False),
        sa.Column("processing_status", sa.String(length=50), server_default="new", nullable=False),
        sa.Column("raw_storage_status", sa.String(length=50), server_default="not_mirrored", nullable=False),
        sa.Column("first_seen_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("last_processed_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.UniqueConstraint("source_type", "external_item_id", name="uq_ingestion_source_type_external_item_id"),
    )
    op.create_index("ix_ingestion_source_type_normalized_source_url", "ingestion_source_items", ["source_type", "normalized_source_url"])
    op.create_index("ix_ingestion_source_type_raw_bucket_raw_key", "ingestion_source_items", ["source_type", "raw_bucket", "raw_key"])
    op.create_index("ix_ingestion_source_items_content_sha256", "ingestion_source_items", ["content_sha256"])
    op.create_index("ix_ingestion_source_items_processing_raw_storage", "ingestion_source_items", ["processing_status", "raw_storage_status"])


def _ensure_ingestion_queue(table_names: set[str]) -> None:
    if "ingestion_queue" in table_names:
        return

    op.create_table(
        "ingestion_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "source_item_id",
            UUID(as_uuid=True),
            sa.ForeignKey("ingestion_source_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("job_type", sa.String(length=100), server_default="ingest_source_item", nullable=False),
        sa.Column("status", sa.String(length=50), server_default="pending", nullable=False),
        sa.Column("priority", sa.Integer(), server_default="5", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("available_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_ingestion_queue_status_priority_available_at", "ingestion_queue", ["status", "priority", "available_at"])
