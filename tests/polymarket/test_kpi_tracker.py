"""
Tests for KPITracker thesis scorecard.

Bootstraps a tiny isolated DB with the minimum schema needed for the
queries (wallet_trades, wallet_alpha_scores, wallet_alerts,
polymarket_paper_trades, trade_hypotheses) and seeds varied data to
exercise each verdict bucket.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from trading_platform.polymarket.kpi_tracker import (
    KPITracker,
    GO_LIVE_ACCURACY,
    GO_LIVE_MIN_SAMPLE,
)


def _bootstrap(tmp_path: Path) -> Path:
    db = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, category TEXT, side TEXT, size REAL, price REAL,
            timestamp INTEGER, pnl REAL, pnl_reliable INTEGER DEFAULT 1
        );
        CREATE TABLE wallet_alpha_scores (
            wallet TEXT NOT NULL, category TEXT NOT NULL,
            resolved_trades INTEGER, win_rate REAL, win_rate_30d REAL,
            avg_pnl REAL, total_pnl REAL, profit_factor REAL, avg_bet_size REAL,
            recency_score REAL, copyability REAL, is_copyable INTEGER,
            last_trade_at INTEGER, computed_at INTEGER,
            PRIMARY KEY (wallet, category)
        );
        CREATE TABLE wallet_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, paper_trade_fired INTEGER DEFAULT 0,
            detected_at INTEGER
        );
        CREATE TABLE polymarket_paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, signal_type TEXT, entry_ts INTEGER,
            exit_ts INTEGER, outcome TEXT, realized_pnl REAL
        );
        CREATE TABLE trade_hypotheses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER, wallet TEXT, category TEXT,
            signal_type TEXT, market_slug TEXT, market_question TEXT,
            direction TEXT, entry_price REAL, alpha_score REAL,
            wallet_wr REAL, wallet_resolved INTEGER, convergence_count INTEGER,
            thesis TEXT, confidence_factors TEXT, created_at INTEGER,
            actual_outcome TEXT, hypothesis_correct INTEGER,
            realized_pnl REAL, resolved_at INTEGER
        );
    """)
    conn.commit()
    conn.close()
    return db


def _seed_hypotheses(db: Path, *, total: int, correct: int, base_ts: int | None = None) -> None:
    """Seed N resolved hypotheses, ``correct`` of which are marked correct."""
    conn = sqlite3.connect(str(db))
    base_ts = base_ts or int(time.time())
    for i in range(total):
        is_correct = 1 if i < correct else 0
        conn.execute(
            """INSERT INTO trade_hypotheses
               (trade_id, wallet, category, signal_type, market_question,
                direction, entry_price, thesis, created_at,
                actual_outcome, hypothesis_correct, realized_pnl, resolved_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (i, f"0x{i}", "politics", "whale_entry", "Q?", "BUY", 0.5, "thesis text",
             base_ts - i, "win" if is_correct else "loss", is_correct, 10.0 if is_correct else -10.0,
             base_ts - i),
        )
    conn.commit()
    conn.close()


def _seed_clean_trades(db: Path, *, total: int, wins: int) -> None:
    conn = sqlite3.connect(str(db))
    base_ts = int(time.time())
    for i in range(total):
        pnl = 50.0 if i < wins else -25.0
        conn.execute(
            "INSERT INTO wallet_trades (wallet, category, side, size, price, timestamp, pnl, pnl_reliable) VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
            (f"0x{i}", "politics", "BUY", 100, 0.5, base_ts - i, pnl),
        )
    conn.commit()
    conn.close()


def _seed_alpha(db: Path, *, copyable: int, scored: int, category: str = "politics") -> None:
    conn = sqlite3.connect(str(db))
    for i in range(scored):
        is_copyable = 1 if i < copyable else 0
        conn.execute(
            """INSERT INTO wallet_alpha_scores
               (wallet, category, resolved_trades, win_rate, win_rate_30d,
                avg_pnl, total_pnl, recency_score, copyability, is_copyable,
                last_trade_at, computed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (f"0x{i}", category, 50, 0.75, 0.75, 100.0, 5000.0, 1.0, 0.85,
             is_copyable, int(time.time()), int(time.time())),
        )
    conn.commit()
    conn.close()


# ── Verdict logic ───────────────────────────────────────────────────────────


class TestVerdict:
    def test_scorecard_accumulating_below_10(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=5, correct=4)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert sc["total_hypotheses"] == 5
        assert "ACCUMULATING" in sc["verdict"]

    def test_scorecard_preliminary(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=20, correct=15)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert "PRELIMINARY" in sc["verdict"]

    def test_scorecard_go_live(self, tmp_path):
        db = _bootstrap(tmp_path)
        # 75% on 60 trades — clears the GO LIVE gate
        _seed_hypotheses(db, total=60, correct=45)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert sc["accuracy"] == 0.75
        assert sc["total_hypotheses"] == 60
        assert "GO LIVE" in sc["verdict"]

    def test_scorecard_promising(self, tmp_path):
        db = _bootstrap(tmp_path)
        # 60% on 60 trades — promising but not go-live
        _seed_hypotheses(db, total=60, correct=36)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert "PROMISING" in sc["verdict"]

    def test_scorecard_marginal(self, tmp_path):
        db = _bootstrap(tmp_path)
        # 52% on 60 trades — marginal
        _seed_hypotheses(db, total=60, correct=31)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert "MARGINAL" in sc["verdict"]

    def test_scorecard_rejected(self, tmp_path):
        db = _bootstrap(tmp_path)
        # 45% on 60 trades — rejected
        _seed_hypotheses(db, total=60, correct=27)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert "REJECTED" in sc["verdict"]


# ── Trend logic ─────────────────────────────────────────────────────────────


class TestTrend:
    def test_no_trend_when_insufficient(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=5, correct=3)
        sc = KPITracker(str(db))._thesis_scorecard()
        assert sc["trend"] is None

    def test_stable_trend(self, tmp_path):
        db = _bootstrap(tmp_path)
        # Interleave wins and losses so recent 20 ≈ prior 20.
        # Without interleaving, _seed_hypotheses puts all wins at low i
        # (highest resolved_at) and the trend ends up "improving".
        conn = sqlite3.connect(str(db))
        base = int(time.time())
        for i in range(40):
            is_correct = 1 if (i % 10 < 7) else 0  # 70/30 within each block
            conn.execute(
                """INSERT INTO trade_hypotheses
                   (trade_id, wallet, category, signal_type, market_question, direction,
                    entry_price, thesis, created_at, actual_outcome, hypothesis_correct,
                    realized_pnl, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (i, f"0x{i}", "politics", "whale_entry", "Q?", "BUY", 0.5,
                 "thesis", base - i, "win" if is_correct else "loss", is_correct,
                 10.0 if is_correct else -10.0, base - i),
            )
        conn.commit()
        conn.close()
        sc = KPITracker(str(db))._thesis_scorecard()
        assert sc["trend"] == "stable"


# ── Claim 1 ─────────────────────────────────────────────────────────────────


class TestClaim1:
    def test_claim_1_uses_clean_data_only(self, tmp_path):
        db = _bootstrap(tmp_path)
        # Insert reliable + unreliable trades; only reliable should count
        conn = sqlite3.connect(str(db))
        conn.executemany(
            "INSERT INTO wallet_trades (wallet, category, pnl, pnl_reliable, timestamp) VALUES (?, ?, ?, ?, ?)",
            [
                ("0xa", "politics", 10.0, 1, int(time.time())),
                ("0xa", "politics", 10.0, 1, int(time.time())),
                ("0xa", "politics", -100.0, 0, int(time.time())),  # unreliable
            ],
        )
        conn.commit()
        conn.close()
        c1 = KPITracker(str(db))._claim_1()
        # Only the 2 reliable trades count → 100% WR
        assert c1["clean_cohort_trades"] == 2
        assert c1["clean_cohort_wr"] == 1.0


# ── Claim 3 ─────────────────────────────────────────────────────────────────


class TestClaim3:
    def test_selectivity(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, copyable=10, scored=30)
        c3 = KPITracker(str(db))._claim_3()
        assert c3["total_scored"] == 30
        assert c3["copyable_combos"] == 10
        assert c3["selectivity"] == round(10 / 30, 3)


# ── Snapshot persistence ────────────────────────────────────────────────────


class TestSnapshot:
    def test_save_daily_snapshot(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=20, correct=15)
        kpi = KPITracker(str(db))
        out = kpi.save_daily_snapshot()
        assert out["saved"] is True
        history = kpi.get_history(days=10)
        assert len(history) == 1
        assert history[0]["total_hypotheses"] == 20
        assert history[0]["accuracy"] == 0.75

    def test_save_snapshot_idempotent_per_day(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=10, correct=7)
        kpi = KPITracker(str(db))
        kpi.save_daily_snapshot()
        kpi.save_daily_snapshot()
        # INSERT OR REPLACE means only one row per date
        assert len(kpi.get_history()) == 1


# ── compute_all sanity ──────────────────────────────────────────────────────


class TestComputeAll:
    def test_returns_all_sections(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_hypotheses(db, total=5, correct=4)
        _seed_clean_trades(db, total=20, wins=15)
        _seed_alpha(db, copyable=8, scored=20)
        out = KPITracker(str(db)).compute_all()
        for key in ("thesis_scorecard", "claim_1_persistence", "claim_2_category",
                     "claim_3_identification", "claim_4_timing", "claim_5_execution",
                     "daily_metrics", "computed_at"):
            assert key in out
