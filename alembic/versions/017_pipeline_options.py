"""Pipeline options singleton — flexible JSONB config for per-profile behaviour.

Revision ID: 017
Revises: 016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipeline_options",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "options_json",
            JSONB(),
            nullable=False,
            server_default='{}',
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("pipeline_options")
