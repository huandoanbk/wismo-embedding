"""Drop is_production_approved column from source_documents.

Revision ID: 018
Revises: 017
Create Date: 2026-04-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("source_documents", "is_production_approved")


def downgrade() -> None:
    op.add_column(
        "source_documents",
        sa.Column("is_production_approved", sa.Boolean(), server_default="false", nullable=False),
    )
