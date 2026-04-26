"""relax legacy source_documents nullability

Revision ID: 007
Revises: 006
Create Date: 2026-04-07 00:40:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("source_documents")}

    if "title" in columns:
        op.alter_column("source_documents", "title", existing_type=sa.String(length=255), nullable=True)
    if "author" in columns:
        op.alter_column("source_documents", "author", existing_type=sa.String(length=255), nullable=True)
    if "source_url" in columns:
        op.alter_column("source_documents", "source_url", existing_type=sa.String(length=500), nullable=True)
    else:
        op.add_column("source_documents", sa.Column("source_url", sa.String(length=500), nullable=True))


def downgrade() -> None:
    op.alter_column("source_documents", "source_url", existing_type=sa.String(length=500), nullable=False)
    op.alter_column("source_documents", "author", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("source_documents", "title", existing_type=sa.String(length=255), nullable=False)
