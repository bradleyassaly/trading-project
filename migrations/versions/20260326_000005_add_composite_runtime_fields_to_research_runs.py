"""Add composite runtime computability fields to research runs."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


def _column_exists(table: str, column: str) -> bool:
    conn = op.get_bind()
    return column in {c["name"] for c in inspect(conn).get_columns(table)}


revision = "20260326_000005"
down_revision = "20260326_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if not _column_exists("research_runs", "composite_runtime_computability_pass"):
        op.add_column("research_runs", sa.Column("composite_runtime_computability_pass", sa.Boolean(), nullable=True))
    if not _column_exists("research_runs", "composite_runtime_computability_reason"):
        op.add_column("research_runs", sa.Column("composite_runtime_computability_reason", sa.String(length=255), nullable=True))
    if not _column_exists("research_runs", "composite_runtime_computable_symbol_count"):
        op.add_column("research_runs", sa.Column("composite_runtime_computable_symbol_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("research_runs", "composite_runtime_computable_symbol_count")
    op.drop_column("research_runs", "composite_runtime_computability_reason")
    op.drop_column("research_runs", "composite_runtime_computability_pass")
