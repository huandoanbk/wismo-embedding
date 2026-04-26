"""Add per-lane max_instances to vastai_lane_configs.

Revision ID: 023_vastai_lane_max_instances
Revises: 022_pipeline_scheduler_configs
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "023_vastai_lane_max_instances"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vastai_lane_configs", sa.Column("max_instances", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("vastai_lane_configs", "max_instances")
