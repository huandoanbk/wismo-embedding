"""Add vastai_lane_configs table for per-lane dashboard-configurable search filters.

Revision ID: 019
Revises: 018
Create Date: 2026-04-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "vastai_lane_configs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("lane", sa.String(20), nullable=False, unique=True),
        sa.Column("search_filter", sa.Text(), nullable=True),
        sa.Column("disk_gb", sa.Integer(), nullable=True),
        sa.Column("max_hourly_cost", sa.Float(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("vastai_lane_configs")
