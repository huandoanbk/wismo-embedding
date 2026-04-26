"""Add per-lane idle destroy controls to vastai_lane_configs.

Revision ID: 020
Revises: 019
Create Date: 2026-04-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vastai_lane_configs", sa.Column("destroy_idle_enabled", sa.Boolean(), nullable=True))
    op.add_column("vastai_lane_configs", sa.Column("idle_timeout_minutes", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("vastai_lane_configs", "idle_timeout_minutes")
    op.drop_column("vastai_lane_configs", "destroy_idle_enabled")
