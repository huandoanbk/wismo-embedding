"""add worker images registry

Revision ID: 014
Revises: 013
Create Date: 2026-04-12 10:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_images",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("lane", sa.String(length=20), nullable=False),
        sa.Column("image_ref", sa.String(length=500), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("validation_status", sa.String(length=20), server_default="pending", nullable=False),
        sa.Column("last_validated_at", sa.DateTime(), nullable=True),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("activated_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_worker_images_lane_created_at", "worker_images", ["lane", "created_at"])
    op.create_index(
        "ix_worker_images_lane_active_unique",
        "worker_images",
        ["lane"],
        unique=True,
        postgresql_where=sa.text("is_active = true"),
    )


def downgrade() -> None:
    op.drop_index("ix_worker_images_lane_active_unique", table_name="worker_images")
    op.drop_index("ix_worker_images_lane_created_at", table_name="worker_images")
    op.drop_table("worker_images")
