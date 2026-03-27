"""Add runtime score validation metadata to promoted strategies."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260326_000003"
down_revision = "20260326_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("promoted_strategies", sa.Column("runtime_score_validation_pass", sa.Boolean(), nullable=True))
    op.add_column("promoted_strategies", sa.Column("runtime_score_validation_reason", sa.String(length=255), nullable=True))
    op.add_column("promoted_strategies", sa.Column("runtime_computable_symbol_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("promoted_strategies", "runtime_computable_symbol_count")
    op.drop_column("promoted_strategies", "runtime_score_validation_reason")
    op.drop_column("promoted_strategies", "runtime_score_validation_pass")
