"""Add realtime in-flight request config to llm_provider_config.

Revision ID: 021
Revises: 020
Create Date: 2026-04-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "llm_provider_config",
        sa.Column("vastai_max_in_flight_requests", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade() -> None:
    op.drop_column("llm_provider_config", "vastai_max_in_flight_requests")
