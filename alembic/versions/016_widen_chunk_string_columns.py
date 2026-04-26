"""Widen content_chunks string columns to match SQLAlchemy model definitions.

teaching_type was VARCHAR(20) but model defines String(100).
depth_level and emotional_tone were VARCHAR(20) but model defines String(50).

Revision ID: 016
Revises: 015
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("content_chunks", "teaching_type", type_=sa.String(100), existing_nullable=True)
    op.alter_column("content_chunks", "depth_level", type_=sa.String(50), existing_nullable=True)
    op.alter_column("content_chunks", "emotional_tone", type_=sa.String(50), existing_nullable=True)


def downgrade() -> None:
    op.alter_column("content_chunks", "teaching_type", type_=sa.String(20), existing_nullable=True)
    op.alter_column("content_chunks", "depth_level", type_=sa.String(20), existing_nullable=True)
    op.alter_column("content_chunks", "emotional_tone", type_=sa.String(20), existing_nullable=True)
