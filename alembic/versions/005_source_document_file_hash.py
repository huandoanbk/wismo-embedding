"""add file_hash to source_documents

Revision ID: 005
Revises: 004
Create Date: 2026-04-07 00:20:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}
    if "file_hash" not in columns:
        op.add_column("source_documents", sa.Column("file_hash", sa.String(length=64), nullable=True))


def downgrade() -> None:
    op.drop_column("source_documents", "file_hash")
