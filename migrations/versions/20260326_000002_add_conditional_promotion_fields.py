"""Add conditional promotion metadata columns."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


def _column_exists(table: str, column: str) -> bool:
    conn = op.get_bind()
    return column in {c["name"] for c in inspect(conn).get_columns(table)}


def _index_exists(table: str, index_name: str) -> bool:
    conn = op.get_bind()
    return index_name in {i["name"] for i in inspect(conn).get_indexes(table)}


revision = "20260326_000002"
down_revision = "20260324_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if not _column_exists("promoted_strategies", "condition_id"):
        op.add_column("promoted_strategies", sa.Column("condition_id", sa.String(length=255), nullable=True))
    if not _column_exists("promoted_strategies", "condition_type"):
        op.add_column("promoted_strategies", sa.Column("condition_type", sa.String(length=128), nullable=True))
    if not _column_exists("promoted_strategies", "rationale"):
        op.add_column("promoted_strategies", sa.Column("rationale", sa.Text(), nullable=True))
    if not _index_exists("promoted_strategies", "ix_promoted_strategies_condition_id"):
        op.create_index(op.f("ix_promoted_strategies_condition_id"), "promoted_strategies", ["condition_id"], unique=False)
    if not _index_exists("promoted_strategies", "ix_promoted_strategies_condition_type"):
        op.create_index(op.f("ix_promoted_strategies_condition_type"), "promoted_strategies", ["condition_type"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_promoted_strategies_condition_type"), table_name="promoted_strategies")
    op.drop_index(op.f("ix_promoted_strategies_condition_id"), table_name="promoted_strategies")
    op.drop_column("promoted_strategies", "rationale")
    op.drop_column("promoted_strategies", "condition_type")
    op.drop_column("promoted_strategies", "condition_id")
