"""text pipeline schema additions

Revision ID: 003
Revises: 002
Create Date: 2026-04-07 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("source_documents", sa.Column("registry_item_id", UUID(as_uuid=True), nullable=True))
    op.add_column("source_documents", sa.Column("content_sha256", sa.String(length=64), nullable=True))
    op.create_index("idx_source_registry_item_id", "source_documents", ["registry_item_id"])
    op.create_index("idx_source_content_sha256", "source_documents", ["content_sha256"])


def downgrade() -> None:
    op.drop_index("idx_source_content_sha256", table_name="source_documents")
    op.drop_index("idx_source_registry_item_id", table_name="source_documents")
    op.drop_column("source_documents", "content_sha256")
    op.drop_column("source_documents", "registry_item_id")
