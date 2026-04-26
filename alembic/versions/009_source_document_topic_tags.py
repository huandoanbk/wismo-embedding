"""add topic_tags to source_documents

Revision ID: 009
Revises: 008
Create Date: 2026-04-09 23:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import JSONB


revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}
    if "topic_tags" not in columns:
        op.add_column(
            "source_documents",
            sa.Column(
                "topic_tags",
                JSONB,
                nullable=True,
                server_default=sa.text("'[]'::jsonb"),
            ),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}
    if "topic_tags" in columns:
        op.drop_column("source_documents", "topic_tags")
