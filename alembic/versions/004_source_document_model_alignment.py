"""align source_documents with runtime model

Revision ID: 004
Revises: 003
Create Date: 2026-04-07 00:10:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}

    if "source_kind" not in columns:
        op.add_column(
            "source_documents",
            sa.Column("source_kind", sa.String(length=20), nullable=True, server_default="text_document"),
        )
        op.execute("UPDATE source_documents SET source_kind = 'text_document' WHERE source_kind IS NULL")
        op.alter_column("source_documents", "source_kind", existing_type=sa.String(length=20), nullable=False)

    if "source_format" not in columns:
        op.add_column("source_documents", sa.Column("source_format", sa.String(length=20), nullable=True))


def downgrade() -> None:
    op.drop_column("source_documents", "source_format")
    op.drop_column("source_documents", "source_kind")
