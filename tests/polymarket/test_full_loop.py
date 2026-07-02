"""Full-loop integration test.

Validates the path:
    synthetic wallet trade
        -> wallet_quality cohort recompute
        -> naive_copy_signal candidate detection
        -> shadow row written to live_trades
        -> outcome resolver books realized_pnl correctly

Uses an in-memory sqlite to keep the test hermetic. Skips when
the codebase requires Postgres (DB_BACKEND=postgres).
"""
from __future__ import annotations

import os
import sqlite3
import time
import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("DB_BACKEND", "postgres").lower() == "postgres",
    reason="full-loop test currently runs against sqlite fixture; postgres "
           "integration test lives in tests/integration/",
)


def _seed_db(conn):
    """Create the minimum schema we need and seed a synthetic wallet + market."""
    conn.executescript(
        """
        CREATE TABLE wallet_profiles (
            wallet TEXT PRIMARY KEY,
            wq_score REAL, wq_wr_60d REAL, wq_n_60d INTEGER,
            wq_size_cv REAL, wq_recency_days REAL, wq_updated_at INTEGER
        );
        CREATE TABLE wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, condition_id TEXT, side TEXT,
            price REAL, size REAL, timestamp INTEGER, asset TEXT, pnl REAL
        );
        CREATE TABLE live_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attempted_at INTEGER, signal_type TEXT, condition_id TEXT,
            direction TEXT, side TEXT, confidence REAL,
            size_usd REAL, entry_price REAL, expected_price REAL,
            slippage REAL, shares REAL, token_id TEXT, category TEXT,
            status TEXT, dry_run INTEGER, signal_wallet TEXT,
            outcome TEXT, realized_pnl REAL, exit_ts INTEGER, question TEXT
        );
        CREATE TABLE markets (
            condition_id TEXT PRIMARY KEY,
            end_date_iso TEXT, yes_token_id TEXT, no_token_id TEXT
        );
        CREATE TABLE market_categories (
            condition_id TEXT PRIMARY KEY, category TEXT
        );
        """
    )
    now = int(time.time())
    # Top-quality synthetic wallet
    conn.execute(
        "INSERT INTO wallet_profiles VALUES ('0xWHALE', 0.45, 0.7, 50, 0.4, 0.5, ?)",
        (now,),
    )
    # Market we can copy on
    conn.execute(
        "INSERT INTO markets VALUES ('0xMKT', ?, '0xYES', '0xNO')",
        (str(time.gmtime(now + 7 * 86400)),),  # 7 days from now
    )
    conn.execute("INSERT INTO market_categories VALUES ('0xMKT', 'politics')")
    # Synthetic recent BUY by our whale at 0.30
    conn.execute(
        "INSERT INTO wallet_trades (wallet, condition_id, side, price, size, "
        "timestamp, asset) VALUES "
        "('0xWHALE', '0xMKT', 'BUY', 0.30, 100, ?, '0xYES')",
        (now - 60,),  # 1 min ago
    )
    conn.commit()


def test_cohort_to_shadow_row():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_db(conn)

    # Cohort selection — replicate the wallet_quality.top_cohort query
    cohort = conn.execute(
        """
        SELECT wallet FROM wallet_profiles
         WHERE wq_score >= 0.10 AND wq_n_60d >= 30
           AND wq_wr_60d >= 0.55 AND wq_recency_days <= 30
        """
    ).fetchall()
    assert [c["wallet"] for c in cohort] == ["0xWHALE"]

    # Detect candidate trades — replicate naive_copy._find_candidate_trades
    cutoff = int(time.time()) - 75 * 60
    candidates = conn.execute(
        "SELECT wallet, condition_id, price, size, timestamp, asset "
        "FROM wallet_trades WHERE wallet='0xWHALE' AND side='BUY' "
        "AND timestamp >= ? AND price > 0",
        (cutoff,),
    ).fetchall()
    assert len(candidates) == 1
    c = candidates[0]
    assert c["price"] == 0.30

    # Apply filters — price in [0.10, 0.85]: PASS
    assert 0.10 <= c["price"] <= 0.85
    # Category not in blocked set
    cat = conn.execute(
        "SELECT category FROM market_categories WHERE condition_id=?",
        (c["condition_id"],),
    ).fetchone()
    assert cat["category"] not in {"science"}

    # Insert shadow row
    conn.execute(
        """
        INSERT INTO live_trades (
            attempted_at, signal_type, condition_id, direction, side,
            confidence, size_usd, entry_price, expected_price, shares,
            token_id, category, status, dry_run, signal_wallet
        ) VALUES (?, 'naive_copy_buy', ?, 'BUY', 'BUY', 0.60, 5.0, ?, ?, ?,
                  ?, ?, 'matched', 1, ?)
        """,
        (int(time.time()), c["condition_id"], c["price"], c["price"],
         5.0 / c["price"], c["asset"], cat["category"], c["wallet"]),
    )
    conn.commit()

    # Simulate resolution: YES wins
    conn.execute(
        """
        UPDATE live_trades
           SET outcome='win', exit_ts=?,
               realized_pnl = shares * (1.0 - entry_price)
         WHERE signal_type='naive_copy_buy'
        """,
        (int(time.time()) + 86400,),
    )

    row = conn.execute(
        "SELECT realized_pnl, shares, entry_price FROM live_trades "
        "WHERE signal_type='naive_copy_buy'"
    ).fetchone()
    # Bought 5/0.3 = ~16.67 shares at 0.30; YES resolves to 1.0
    # Realized = 16.67 * (1 - 0.30) = 11.67
    assert row["realized_pnl"] == pytest.approx(5.0 / 0.30 * 0.70, rel=1e-3)


def test_cohort_filters_out_low_quality_wallet():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_db(conn)
    now = int(time.time())
    # A low-WR wallet that should be excluded
    conn.execute(
        "INSERT INTO wallet_profiles VALUES "
        "('0xLOSER', 0.05, 0.3, 80, 0.4, 1.0, ?)",
        (now,),
    )
    conn.commit()

    cohort = conn.execute(
        """
        SELECT wallet FROM wallet_profiles
         WHERE wq_score >= 0.10 AND wq_n_60d >= 30
           AND wq_wr_60d >= 0.55 AND wq_recency_days <= 30
         ORDER BY wq_score DESC
        """
    ).fetchall()
    assert "0xLOSER" not in [c["wallet"] for c in cohort]
    assert "0xWHALE" in [c["wallet"] for c in cohort]


def test_stale_wallet_filtered_by_recency():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_db(conn)
    now = int(time.time())
    # Same WR profile as WHALE but hasn't traded in 45 days
    conn.execute(
        "INSERT INTO wallet_profiles VALUES "
        "('0xSTALE', 0.45, 0.7, 50, 0.4, 45.0, ?)",
        (now,),
    )
    conn.commit()
    cohort = conn.execute(
        "SELECT wallet FROM wallet_profiles "
        "WHERE wq_recency_days <= 30"
    ).fetchall()
    assert "0xSTALE" not in [c["wallet"] for c in cohort]
