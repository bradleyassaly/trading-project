"""
Tests for the per-wallet, per-category alpha scoring.

Bootstraps a tiny isolated SQLite DB per test with the minimum schema
needed for ``compute_alpha_scores`` to run, populates trades, and
checks the resulting wallet_alpha_scores rows.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from trading_platform.polymarket.alpha_scores import (
    compute_alpha_scores,
    get_wallet_alpha,
    get_wallet_alpha_full,
    get_category_leaderboard,
    get_summary,
    MIN_SAMPLE,
    MIN_WR_COPYABLE,
)


def _bootstrap_db(tmp_path: Path) -> Path:
    db = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            category TEXT,
            side TEXT,
            size REAL,
            price REAL,
            timestamp INTEGER,
            pnl REAL,
            pnl_reliable INTEGER DEFAULT 1
        );
        """
    )
    conn.commit()
    conn.close()
    return db


def _insert_trades(db: Path, wallet: str, category: str, *,
                   wins: int, losses: int, win_pnl: float = 100.0,
                   loss_pnl: float = -50.0, ts_offset_days: int = 1,
                   reliable: int = 1) -> None:
    conn = sqlite3.connect(str(db))
    base_ts = int(time.time()) - (ts_offset_days * 86400)
    rows = []
    for i in range(wins):
        rows.append((wallet, category, "BUY", 100, 0.5, base_ts - i * 60, win_pnl, reliable))
    for i in range(losses):
        rows.append((wallet, category, "BUY", 100, 0.5, base_ts - (wins + i) * 60, loss_pnl, reliable))
    conn.executemany(
        "INSERT INTO wallet_trades (wallet, category, side, size, price, timestamp, pnl, pnl_reliable) VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


# ── Compute basics ──────────────────────────────────────────────────────────


class TestComputeAlphaScores:
    def test_high_wr_wallet_is_copyable(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        _insert_trades(db, "0xabc", "politics", wins=14, losses=6)
        result = compute_alpha_scores(db)
        assert result["scored"] == 1
        assert result["copyable"] == 1
        # Verify the row
        assert get_wallet_alpha(db, "0xabc", "politics") > 0

    def test_low_wr_not_copyable(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # 45% WR over 30 trades — fails MIN_WR_COPYABLE = 0.55
        _insert_trades(db, "0xabc", "politics", wins=14, losses=16)
        result = compute_alpha_scores(db)
        assert result["scored"] == 1
        assert result["copyable"] == 0
        assert get_wallet_alpha(db, "0xabc", "politics") == 0.0

    def test_low_sample_not_scored(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # Only 5 trades — below MIN_SAMPLE = 10
        _insert_trades(db, "0xabc", "politics", wins=4, losses=1)
        result = compute_alpha_scores(db)
        assert result["scored"] == 0
        assert result["copyable"] == 0

    def test_negative_ev_not_copyable(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # 60% WR but loss size > win size → negative EV
        _insert_trades(
            db, "0xabc", "politics",
            wins=12, losses=8, win_pnl=10.0, loss_pnl=-50.0,
        )
        result = compute_alpha_scores(db)
        assert result["scored"] == 1
        assert result["copyable"] == 0  # avg_pnl is negative

    def test_stale_wallet_not_copyable(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # 70% WR but last trade 100 days ago → recency below threshold
        _insert_trades(
            db, "0xabc", "politics",
            wins=14, losses=6, ts_offset_days=100,
        )
        result = compute_alpha_scores(db)
        assert result["scored"] == 1
        # recency_score should be < COPYABLE_MIN_RECENCY (0.30)
        # 100 days → recency = max(0.10, 1 - 100/90) = 0.10
        assert result["copyable"] == 0

    def test_pnl_reliable_filter(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # 20 reliable trades + 10 unreliable trades — only reliable counted
        _insert_trades(db, "0xabc", "politics", wins=14, losses=6)
        _insert_trades(
            db, "0xabc", "politics",
            wins=10, losses=0, reliable=0,  # unreliable wins should NOT pad WR
        )
        compute_alpha_scores(db)
        rows = get_wallet_alpha_full(db, "0xabc")
        assert len(rows) == 1
        # WR should be 14/20 = 0.70, NOT 24/30 = 0.80
        assert abs(rows[0]["win_rate"] - 0.70) < 0.01
        assert rows[0]["resolved_trades"] == 20

    def test_per_category_independence(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # Same wallet, different stats per category
        _insert_trades(db, "0xabc", "politics", wins=15, losses=5)   # 75%
        _insert_trades(db, "0xabc", "sports", wins=8, losses=12)     # 40%
        compute_alpha_scores(db)
        rows = {r["category"]: r for r in get_wallet_alpha_full(db, "0xabc")}
        assert "politics" in rows and "sports" in rows
        assert rows["politics"]["is_copyable"] == 1
        assert rows["sports"]["is_copyable"] == 0
        assert rows["politics"]["win_rate"] > rows["sports"]["win_rate"]


class TestLookups:
    def test_get_wallet_alpha_returns_zero_for_unknown(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        compute_alpha_scores(db)
        assert get_wallet_alpha(db, "0xnotreal", "politics") == 0.0

    def test_get_wallet_alpha_zero_for_non_copyable(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        _insert_trades(db, "0xabc", "politics", wins=5, losses=15)  # 25% WR
        compute_alpha_scores(db)
        assert get_wallet_alpha(db, "0xabc", "politics") == 0.0

    def test_get_wallet_alpha_full_returns_all_categories(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        _insert_trades(db, "0xabc", "politics", wins=14, losses=6)
        _insert_trades(db, "0xabc", "sports", wins=12, losses=8)
        compute_alpha_scores(db)
        rows = get_wallet_alpha_full(db, "0xabc")
        assert len(rows) == 2

    def test_category_leaderboard_filters_by_min_score(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        _insert_trades(db, "0xhigh", "politics", wins=18, losses=2)   # ~90%
        _insert_trades(db, "0xmid", "politics", wins=12, losses=8)    # 60%
        compute_alpha_scores(db)
        all_rows = get_category_leaderboard(db, "politics", min_score=0.0)
        # 0xhigh's composite score lands ~0.81 with 20-sample (sample
        # confidence saturates at 50). Use a threshold that splits the two.
        high_rows = get_category_leaderboard(db, "politics", min_score=0.75)
        assert len(all_rows) == 2
        assert len(high_rows) == 1
        assert high_rows[0]["wallet"] == "0xhigh"

    def test_summary_aggregates(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        _insert_trades(db, "0xa", "politics", wins=14, losses=6)
        _insert_trades(db, "0xb", "sports", wins=12, losses=8)
        compute_alpha_scores(db)
        s = get_summary(db)
        assert s["available"] is True
        assert s["total_scored"] == 2
        assert s["total_copyable"] >= 1


class TestCopyabilityScoreBounds:
    def test_score_in_unit_interval(self, tmp_path):
        db = _bootstrap_db(tmp_path)
        # Build several scenarios across the spectrum
        _insert_trades(db, "0xperfect", "politics", wins=50, losses=0)
        _insert_trades(db, "0xbad", "politics", wins=2, losses=18)
        _insert_trades(db, "0xmid", "politics", wins=10, losses=10)
        compute_alpha_scores(db)
        for w in ("0xperfect", "0xbad", "0xmid"):
            rows = get_wallet_alpha_full(db, w)
            for r in rows:
                assert 0.0 <= r["copyability"] <= 1.0
