"""
Tests for the decision evaluation layer.

Covers:
- Kelly-based position sizing
- Enhanced hypothesis with distribution data
- Post-trade analysis
- Position monitoring
- Calibration data
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _bootstrap(tmp_path: Path) -> str:
    """Create a minimal wallet_intelligence.db with required tables."""
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE wallet_alpha_scores (
            wallet TEXT NOT NULL,
            category TEXT NOT NULL,
            resolved_trades INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            win_rate_30d REAL,
            avg_pnl REAL,
            total_pnl REAL,
            profit_factor REAL,
            avg_bet_size REAL,
            recency_score REAL,
            copyability REAL,
            is_copyable INTEGER,
            last_trade_at INTEGER,
            computed_at INTEGER NOT NULL,
            avg_win_pnl REAL,
            avg_loss_pnl REAL,
            median_pnl REAL,
            pnl_stddev REAL,
            max_win REAL,
            max_loss REAL,
            avg_entry_price REAL,
            sharpe_ratio REAL,
            avg_hold_days REAL,
            kelly_fraction REAL,
            streak_current INTEGER,
            streak_max_win INTEGER,
            streak_max_loss INTEGER,
            PRIMARY KEY (wallet, category)
        );
        CREATE TABLE wallet_trades (
            wallet TEXT, category TEXT, pnl REAL, price REAL,
            size REAL, timestamp INTEGER, pnl_reliable INTEGER,
            slug TEXT
        );
        CREATE TABLE polymarket_paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT, question TEXT, category TEXT, side TEXT,
            entry_price REAL, size_usd REAL, signal_type TEXT,
            confidence REAL, wallet TEXT, entry_ts INTEGER,
            exit_price REAL, exit_ts INTEGER, outcome TEXT,
            return_pct REAL, realized_pnl REAL, archived INTEGER DEFAULT 0,
            fusion_score REAL, fusion_components TEXT,
            wallet_tier_at_fire TEXT, wallet_bucket_at_fire TEXT,
            kelly_stake_pct REAL
        );
        CREATE TABLE wallet_profiles (
            wallet TEXT PRIMARY KEY, kelly_fraction REAL,
            sharpe_ratio REAL, pnl_trend REAL
        );
        CREATE TABLE market_signals (
            condition_id TEXT, price REAL, fired_at INTEGER,
            signal_type TEXT
        );
    """)
    conn.commit()
    conn.close()
    return db


def _seed_alpha(db: str, wallet: str = "0xtest", category: str = "politics",
                wr: float = 0.70, kelly: float = 0.12, pf: float = 2.6,
                avg_win: float = 2340, avg_loss: float = -890) -> None:
    conn = sqlite3.connect(db)
    conn.execute(
        """INSERT OR REPLACE INTO wallet_alpha_scores
           (wallet, category, resolved_trades, win_rate, win_rate_30d,
            avg_pnl, total_pnl, profit_factor, avg_bet_size,
            recency_score, copyability, is_copyable, computed_at,
            avg_win_pnl, avg_loss_pnl, kelly_fraction, sharpe_ratio,
            streak_current, streak_max_win, streak_max_loss)
           VALUES (?, ?, 50, ?, ?, 100, 5000, ?, 500, 0.9, 0.8, 1, 1000,
                   ?, ?, ?, 1.8, 3, 7, 2)""",
        (wallet, category, wr, wr, pf, avg_win, avg_loss, kelly),
    )
    conn.commit()
    conn.close()


# ── Position Sizing ─────────────────────────────────────────────────────────


class TestKellySizing:
    def test_kelly_sizing_positive_edge(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, kelly=0.12, pf=2.6)
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        ex = PolymarketPaperExecutor.__new__(PolymarketPaperExecutor)
        ex._wallet_db_path = db
        ex._wallet_conn = sqlite3.connect(db)
        ex._wallet_lock = MagicMock()
        ex._wallet_lock.__enter__ = MagicMock(return_value=None)
        ex._wallet_lock.__exit__ = MagicMock(return_value=False)
        stake, reason = ex._compute_stake("whale_entry", 0.5, "0xtest", "politics")
        assert stake >= 10  # minimum
        assert "half-Kelly" in reason
        assert "K=12.0%" in reason
        ex._wallet_conn.close()

    def test_kelly_sizing_no_edge(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, kelly=0.0)
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        ex = PolymarketPaperExecutor.__new__(PolymarketPaperExecutor)
        ex._wallet_db_path = db
        ex._wallet_conn = sqlite3.connect(db)
        ex._wallet_lock = MagicMock()
        ex._wallet_lock.__enter__ = MagicMock(return_value=None)
        ex._wallet_lock.__exit__ = MagicMock(return_value=False)
        stake, reason = ex._compute_stake("whale_entry", 0.5, "0xtest", "politics")
        # Kelly=0 falls through to profile-based or confidence-based
        assert stake >= 10
        ex._wallet_conn.close()

    def test_kelly_sizing_capped(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, kelly=0.25)  # max kelly
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        ex = PolymarketPaperExecutor.__new__(PolymarketPaperExecutor)
        ex._wallet_db_path = db
        ex._wallet_conn = sqlite3.connect(db)
        ex._wallet_lock = MagicMock()
        ex._wallet_lock.__enter__ = MagicMock(return_value=None)
        ex._wallet_lock.__exit__ = MagicMock(return_value=False)
        stake, reason = ex._compute_stake("whale_entry", 0.9, "0xtest", "politics")
        assert stake <= 50  # capped at $50 for paper
        ex._wallet_conn.close()

    def test_half_kelly_conservative(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, kelly=0.20)
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        ex = PolymarketPaperExecutor.__new__(PolymarketPaperExecutor)
        ex._wallet_db_path = db
        ex._wallet_conn = sqlite3.connect(db)
        ex._wallet_lock = MagicMock()
        ex._wallet_lock.__enter__ = MagicMock(return_value=None)
        ex._wallet_lock.__exit__ = MagicMock(return_value=False)
        stake, reason = ex._compute_stake("whale_entry", 0.5, "0xtest", "politics")
        assert "hK=10.0%" in reason  # half of 20%
        ex._wallet_conn.close()


# ── Hypothesis Distribution ─────────────────────────────────────────────────


class TestHypothesisDistribution:
    def test_hypothesis_includes_distribution(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, avg_win=2340, avg_loss=-890, pf=2.6, kelly=0.12)
        from trading_platform.polymarket.trade_hypotheses import build_hypothesis
        h = build_hypothesis(
            db, wallet="0xtest", category="politics",
            signal_type="whale_entry", market_slug="test",
            market_question="Will X?", direction="YES",
            entry_price=0.55,
        )
        assert h.expected_avg_win == 2340
        assert h.expected_avg_loss == -890
        assert h.expected_kelly == 0.12
        assert "Avg win: +$2340" in h.thesis
        assert "PF: 2.6x" in h.thesis

    def test_hypothesis_ev_after_spread(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db, wr=0.70, avg_win=100, avg_loss=-50)
        from trading_platform.polymarket.trade_hypotheses import build_hypothesis
        h = build_hypothesis(
            db, wallet="0xtest", category="politics",
            signal_type="whale_entry", market_slug="",
            market_question="Test?", direction="YES",
            entry_price=0.50,
        )
        assert h.ev_after_spread is not None
        # Raw EV = 0.70*100 + 0.30*(-50) = 55
        # After 2% spread: 55 - 1.1 = 53.9
        assert h.ev_after_spread < 55  # less than raw EV

    def test_hypothesis_streak(self, tmp_path):
        db = _bootstrap(tmp_path)
        _seed_alpha(db)
        from trading_platform.polymarket.trade_hypotheses import build_hypothesis
        h = build_hypothesis(
            db, wallet="0xtest", category="politics",
            signal_type="whale_entry", market_slug="",
            market_question="Test?", direction="YES",
            entry_price=0.50,
        )
        assert h.wallet_streak == "W3"  # seeded with streak_current=3
        assert "Streak: W3" in h.thesis


# ── Post-Trade Analysis ─────────────────────────────────────────────────────


class TestPostTradeAnalysis:
    def test_correct_prediction_analysis(self, tmp_path):
        db = _bootstrap(tmp_path)
        from trading_platform.polymarket.trade_hypotheses import (
            ensure_schema, persist_hypothesis, mark_resolved, build_hypothesis,
        )
        _seed_alpha(db)
        ensure_schema(db)
        h = build_hypothesis(
            db, wallet="0xtest", category="politics",
            signal_type="whale_entry", market_slug="",
            market_question="Test?", direction="YES",
            entry_price=0.50,
        )
        persist_hypothesis(db, h, trade_id=1)
        result = mark_resolved(db, trade_id=1, outcome="win", realized_pnl=25.0)
        assert result is True

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT hypothesis_correct, post_analysis_json FROM trade_hypotheses WHERE trade_id=1"
        ).fetchone()
        conn.close()
        assert row[0] == 1
        analysis = json.loads(row[1])
        assert analysis["prediction_correct"] is True

    def test_pnl_prediction_error(self, tmp_path):
        db = _bootstrap(tmp_path)
        from trading_platform.polymarket.trade_hypotheses import (
            ensure_schema, persist_hypothesis, mark_resolved, build_hypothesis,
        )
        _seed_alpha(db, wr=0.70, avg_win=100, avg_loss=-50)
        ensure_schema(db)
        h = build_hypothesis(
            db, wallet="0xtest", category="politics",
            signal_type="whale_entry", market_slug="",
            market_question="Test?", direction="YES",
            entry_price=0.50,
        )
        persist_hypothesis(db, h, trade_id=2)
        mark_resolved(db, trade_id=2, outcome="win", realized_pnl=80.0)

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT post_analysis_json FROM trade_hypotheses WHERE trade_id=2"
        ).fetchone()
        conn.close()
        analysis = json.loads(row[0])
        assert "pnl_prediction_error" in analysis
        assert "expected_pnl" in analysis


# ── Position Monitoring ──────────────────────────────────────────────────────


class TestPositionMonitoring:
    def test_snapshot_created(self, tmp_path):
        db = _bootstrap(tmp_path)
        conn = sqlite3.connect(db)
        import time
        now = int(time.time())
        conn.execute(
            """INSERT INTO polymarket_paper_trades
               (condition_id, question, category, side, entry_price,
                size_usd, signal_type, confidence, wallet, entry_ts, archived)
               VALUES ('cid1', 'Test?', 'politics', 'YES', 0.50,
                       100, 'whale_entry', 0.7, '0xtest', ?, 0)""",
            (now - 3600,),
        )
        # Add a mock price
        conn.execute(
            """INSERT INTO market_signals
               (condition_id, price, fired_at, signal_type)
               VALUES ('cid1', 0.60, ?, 'whale_entry')""",
            (now,),
        )
        conn.commit()
        conn.close()

        from trading_platform.polymarket.position_monitor import mark_to_market
        result = mark_to_market(db)
        assert result["open_positions"] == 1
        assert result["snapshots_created"] == 1
        assert result["total_unrealized"] > 0  # price went up

    def test_unrealized_pnl_correct(self, tmp_path):
        db = _bootstrap(tmp_path)
        conn = sqlite3.connect(db)
        import time
        now = int(time.time())
        conn.execute(
            """INSERT INTO polymarket_paper_trades
               (condition_id, question, category, side, entry_price,
                size_usd, signal_type, confidence, wallet, entry_ts, archived)
               VALUES ('cid2', 'Test?', 'politics', 'YES', 0.40,
                       100, 'whale_entry', 0.7, '0xtest', ?, 0)""",
            (now - 7200,),
        )
        conn.execute(
            """INSERT INTO market_signals
               (condition_id, price, fired_at, signal_type)
               VALUES ('cid2', 0.60, ?, 'whale_entry')""",
            (now,),
        )
        conn.commit()
        conn.close()

        from trading_platform.polymarket.position_monitor import mark_to_market
        result = mark_to_market(db)
        # Entry 0.40, current 0.60 → +50% → on $100 stake = +$50
        assert result["total_unrealized"] > 0

        conn = sqlite3.connect(db)
        snap = conn.execute(
            "SELECT unrealized_pnl_pct FROM position_snapshots WHERE trade_id=1"
        ).fetchone()
        conn.close()
        assert snap[0] > 0.4  # should be ~0.50


# ── Calibration ──────────────────────────────────────────────────────────────


class TestCalibration:
    def test_insufficient_data(self, tmp_path):
        db = _bootstrap(tmp_path)
        from trading_platform.polymarket.trade_hypotheses import (
            ensure_schema, get_calibration_data,
        )
        ensure_schema(db)
        result = get_calibration_data(db)
        assert result["sufficient_data"] is False
        assert result["resolved_count"] == 0


# ── Streaks ──────────────────────────────────────────────────────────────────


class TestStreaks:
    def test_compute_streaks(self):
        from trading_platform.polymarket.alpha_scores import _compute_streaks
        cur, max_w, max_l = _compute_streaks(["W", "W", "W", "L", "W", "W"])
        assert cur == 2  # ends on W2
        assert max_w == 3  # first 3 wins
        assert max_l == 1  # single loss

    def test_empty_outcomes(self):
        from trading_platform.polymarket.alpha_scores import _compute_streaks
        assert _compute_streaks([]) == (0, 0, 0)

    def test_all_losses(self):
        from trading_platform.polymarket.alpha_scores import _compute_streaks
        cur, max_w, max_l = _compute_streaks(["L", "L", "L"])
        assert cur == -3
        assert max_w == 0
        assert max_l == 3
