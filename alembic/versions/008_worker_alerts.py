"""add worker alerts table

Revision ID: 008
Revises: 007
Create Date: 2026-04-09 18:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_alerts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="unresolved"),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("occurrence_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("occurred_at", sa.DateTime(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("resolved_by", sa.String(length=100), nullable=True),
        sa.Column("instance_id", sa.String(length=255), nullable=True),
        sa.Column("task_id", sa.String(length=255), nullable=True),
        sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_worker_alerts_status_last_seen_at", "worker_alerts", ["status", "last_seen_at"], unique=False)
    op.create_index(
        "ix_worker_alerts_severity_source_status",
        "worker_alerts",
        ["severity", "source", "status"],
        unique=False,
    )
    op.create_index(
        "ix_worker_alerts_fingerprint_last_seen_at",
        "worker_alerts",
        ["fingerprint", "last_seen_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_worker_alerts_fingerprint_last_seen_at", table_name="worker_alerts")
    op.drop_index("ix_worker_alerts_severity_source_status", table_name="worker_alerts")
    op.drop_index("ix_worker_alerts_status_last_seen_at", table_name="worker_alerts")
    op.drop_table("worker_alerts")
