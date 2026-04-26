"""Add llm_provider_config table for dashboard-configurable LLM provider selection.

Revision ID: 015
Revises: 014
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "llm_provider_config",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("provider", sa.String(), nullable=False, server_default="vastai"),
        sa.Column("api_base_url", sa.String(), nullable=False, server_default=""),
        sa.Column("api_key", sa.String(), nullable=False, server_default=""),
        sa.Column("api_model", sa.String(), nullable=False, server_default="Qwen/Qwen3-14B"),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("llm_provider_config")
