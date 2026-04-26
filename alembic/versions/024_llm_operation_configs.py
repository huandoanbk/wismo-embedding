"""Add operation-scoped LLM runtime config table.

Revision ID: 024_llm_operation_configs
Revises: 023_vastai_lane_max_instances
Create Date: 2026-04-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "024_llm_operation_configs"
down_revision = "023_vastai_lane_max_instances"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "llm_operation_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation", sa.String(length=30), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False, server_default="vastai"),
        sa.Column("api_base_url", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("api_key", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("api_model", sa.String(length=200), nullable=False, server_default="Qwen/Qwen3-14B"),
        sa.Column("vastai_max_in_flight_requests", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation"),
    )

    op.execute(
        """
        INSERT INTO llm_operation_configs (
            id,
            operation,
            provider,
            api_base_url,
            api_key,
            api_model,
            vastai_max_in_flight_requests,
            updated_at
        )
        SELECT
            gen_random_uuid(),
            operation_name,
            provider,
            api_base_url,
            api_key,
            api_model,
            vastai_max_in_flight_requests,
            now()
        FROM llm_provider_config
        CROSS JOIN (
            VALUES ('refinement'), ('enrichment')
        ) AS ops(operation_name)
        WHERE NOT EXISTS (SELECT 1 FROM llm_operation_configs);
        """
    )


def downgrade() -> None:
    op.drop_table("llm_operation_configs")
