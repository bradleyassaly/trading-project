"""Add conditional promotion metadata columns."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260326_000002"
down_revision = "20260324_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("promoted_strategies", sa.Column("condition_id", sa.String(length=255), nullable=True))
    op.add_column("promoted_strategies", sa.Column("condition_type", sa.String(length=128), nullable=True))
    op.add_column("promoted_strategies", sa.Column("rationale", sa.Text(), nullable=True))
    op.create_index(op.f("ix_promoted_strategies_condition_id"), "promoted_strategies", ["condition_id"], unique=False)
    op.create_index(op.f("ix_promoted_strategies_condition_type"), "promoted_strategies", ["condition_type"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_promoted_strategies_condition_type"), table_name="promoted_strategies")
    op.drop_index(op.f("ix_promoted_strategies_condition_id"), table_name="promoted_strategies")
    op.drop_column("promoted_strategies", "rationale")
    op.drop_column("promoted_strategies", "condition_type")
    op.drop_column("promoted_strategies", "condition_id")
