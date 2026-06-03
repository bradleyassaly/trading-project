"""Phase 1 observability columns + wallet quality score.

Captures the four ad-hoc schema additions made on 2026-06-03 during the
system-audit deployment session:

  1. live_trades: signed slippage + dollar cost
  2. live_trades: exit retry state machine
  3. wallet_profiles: rolling-window quality score (wq_*) — replaces
     alpha_score's lifetime-PnL ranking that surfaced volatile event winners

See docs/alpha_roadmap.md for the full audit context.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


def _column_exists(table: str, column: str) -> bool:
    conn = op.get_bind()
    return column in {c["name"] for c in inspect(conn).get_columns(table)}


revision = "20260603_000006"
down_revision = "20260326_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Signed slippage + dollar cost on live_trades
    if not _column_exists("live_trades", "slippage_signed"):
        op.add_column("live_trades", sa.Column("slippage_signed", sa.Float(), nullable=True))
    if not _column_exists("live_trades", "slippage_cost_usd"):
        op.add_column("live_trades", sa.Column("slippage_cost_usd", sa.Float(), nullable=True))

    # 2. Exit retry state machine on live_trades
    if not _column_exists("live_trades", "exit_attempts"):
        op.add_column(
            "live_trades",
            sa.Column("exit_attempts", sa.BigInteger(), nullable=False, server_default="0"),
        )
    if not _column_exists("live_trades", "exit_first_attempt_at"):
        op.add_column("live_trades", sa.Column("exit_first_attempt_at", sa.BigInteger(), nullable=True))
    if not _column_exists("live_trades", "exit_last_error"):
        op.add_column("live_trades", sa.Column("exit_last_error", sa.Text(), nullable=True))
    if not _column_exists("live_trades", "exit_stuck"):
        op.add_column(
            "live_trades",
            sa.Column("exit_stuck", sa.Integer(), nullable=True, server_default="0"),
        )

    # 3. Rolling-window wallet quality score on wallet_profiles
    if not _column_exists("wallet_profiles", "wq_wr_60d"):
        op.add_column("wallet_profiles", sa.Column("wq_wr_60d", sa.Float(), nullable=True))
    if not _column_exists("wallet_profiles", "wq_n_60d"):
        op.add_column("wallet_profiles", sa.Column("wq_n_60d", sa.BigInteger(), nullable=True))
    if not _column_exists("wallet_profiles", "wq_size_cv"):
        op.add_column("wallet_profiles", sa.Column("wq_size_cv", sa.Float(), nullable=True))
    if not _column_exists("wallet_profiles", "wq_recency_days"):
        op.add_column("wallet_profiles", sa.Column("wq_recency_days", sa.Float(), nullable=True))
    if not _column_exists("wallet_profiles", "wq_score"):
        op.add_column("wallet_profiles", sa.Column("wq_score", sa.Float(), nullable=True))
    if not _column_exists("wallet_profiles", "wq_updated_at"):
        op.add_column("wallet_profiles", sa.Column("wq_updated_at", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("wallet_profiles", "wq_updated_at")
    op.drop_column("wallet_profiles", "wq_score")
    op.drop_column("wallet_profiles", "wq_recency_days")
    op.drop_column("wallet_profiles", "wq_size_cv")
    op.drop_column("wallet_profiles", "wq_n_60d")
    op.drop_column("wallet_profiles", "wq_wr_60d")
    op.drop_column("live_trades", "exit_stuck")
    op.drop_column("live_trades", "exit_last_error")
    op.drop_column("live_trades", "exit_first_attempt_at")
    op.drop_column("live_trades", "exit_attempts")
    op.drop_column("live_trades", "slippage_cost_usd")
    op.drop_column("live_trades", "slippage_signed")
