"""
Tests for trade_hypotheses generation + persistence.

Bootstraps a tiny isolated DB with the minimum schema (wallet_alpha_scores,
wallet_trades) so build_hypothesis has data to read.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from trading_platform.polymarket.trade_hypotheses import (
    TradeHypothesis,
    build_hypothesis,
    persist_hypothesis,
    get_recent_hypotheses,
    ensure_schema,
)


def _bootstrap(tmp_path: Path) -> Path:
    db = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE wallet_alpha_scores (
            wallet TEXT NOT NULL, category TEXT NOT NULL,
            resolved_trades INTEGER, win_rate REAL, win_rate_30d REAL,
            avg_pnl REAL, total_pnl REAL, profit_factor REAL, avg_bet_size REAL,
            recency_score REAL, copyability REAL, is_copyable INTEGER,
            last_trade_at INTEGER, computed_at INTEGER,
            avg_win_pnl REAL, avg_loss_pnl REAL, median_pnl REAL,
            pnl_stddev REAL, max_win REAL, max_loss REAL,
            avg_entry_price REAL, sharpe_ratio REAL, avg_hold_days REAL,
            kelly_fraction REAL, streak_current INTEGER,
            streak_max_win INTEGER, streak_max_loss INTEGER,
            PRIMARY KEY (wallet, category)
        );
        CREATE TABLE wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, category TEXT, side TEXT, size REAL, price REAL,
            timestamp INTEGER, pnl REAL, pnl_reliable INTEGER DEFAULT 1,
            slug TEXT
        );
    """)
    conn.commit()
    conn.close()
    return db


def _seed_alpha(db: Path, wallet: str, category: str, *,
                wr: float, n: int, avg_pnl: float, copyability: float = 0.85,
                wr_30d: float | None = None) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        """INSERT INTO wallet_alpha_scores
           (wallet, category, resolved_trades, win_rate, win_rate_30d,
            avg_pnl, total_pnl, recency_score, copyability, is_copyable,
            last_trade_at, computed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (wallet, category, n, wr, wr_30d if wr_30d is not None else wr,
         avg_pnl, avg_pnl * n, 1.0, copyability, 1, int(time.time()), int(time.time())),
    )
    conn.commit()
    conn.close()


def _seed_recent_trades(db: Path, wallet: str, category: str, *,
                        wins: int, losses: int) -> None:
    conn = sqlite3.connect(str(db))
    base_ts = int(time.time())
    for i in range(wins):
        conn.execute(
            "INSERT INTO wallet_trades (wallet, category, pnl, pnl_reliable, timestamp) VALUES (?, ?, ?, 1, ?)",
            (wallet, category, 50.0, base_ts - i),
        )
    for i in range(losses):
        conn.execute(
            "INSERT INTO wallet_trades (wallet, category, pnl, pnl_reliable, timestamp) VALUES (?, ?, ?, 1, ?)",
            (wallet, category, -25.0, base_ts - 100 - i),
        )
    conn.commit()
    conn.close()


# ── build_hypothesis ────────────────────────────────────────────────────────


class TestBuildHypothesis:
    def test_minimal_hypothesis_no_data(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        h = build_hypothesis(
            str(db),
            wallet="0xnoone",
            category="politics",
            signal_type="whale_entry",
            market_slug="will-x-happen",
            market_question="Will X happen?",
            direction="BUY",
            entry_price=0.42,
        )
        assert isinstance(h, TradeHypothesis)
        assert h.alpha_score is None
        assert "0xnoone" in h.thesis
        assert "Will X happen" in h.thesis
        assert "No alpha score yet" in h.thesis or "Speculative" in h.thesis

    def test_high_conviction_thesis(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        _seed_alpha(db, "0xpro", "politics", wr=0.85, n=100, avg_pnl=120.0, copyability=0.92)
        _seed_recent_trades(db, "0xpro", "politics", wins=4, losses=1)
        h = build_hypothesis(
            str(db),
            wallet="0xpro",
            category="politics",
            signal_type="whale_entry",
            market_slug="will-fed-cut",
            market_question="Will the Fed cut rates in May?",
            direction="BUY",
            entry_price=0.62,
        )
        assert h.alpha_score == 0.92
        assert h.wallet_wr == 0.85
        assert h.wallet_resolved == 100
        assert "85%" in h.thesis
        assert "100" in h.thesis
        assert "High-conviction" in h.thesis

    def test_convergence_called_out(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        _seed_alpha(db, "0xpro", "politics", wr=0.7, n=30, avg_pnl=50.0, copyability=0.7)
        h = build_hypothesis(
            str(db),
            wallet="0xpro",
            category="politics",
            signal_type="convergence",
            market_slug="m",
            market_question="m?",
            direction="BUY",
            entry_price=0.5,
            convergence_count=4,
        )
        assert h.convergence_count == 4
        assert "4 watched wallets" in h.thesis or "convergence" in h.thesis.lower()

    def test_negative_avg_pnl_warning(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        _seed_alpha(db, "0xpro", "sports", wr=0.6, n=20, avg_pnl=-10.0, copyability=0.55)
        h = build_hypothesis(
            str(db), wallet="0xpro", category="sports",
            signal_type="whale_entry", market_slug="m",
            market_question="m?", direction="BUY", entry_price=0.5,
        )
        assert "negative expectancy" in h.thesis or "small bet" in h.thesis or "⚠" in h.thesis

    def test_confidence_factors_populated(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        _seed_alpha(db, "0xpro", "politics", wr=0.85, n=100, avg_pnl=120.0, copyability=0.92, wr_30d=0.88)
        h = build_hypothesis(
            str(db), wallet="0xpro", category="politics",
            signal_type="whale_entry", market_slug="m",
            market_question="m?", direction="BUY", entry_price=0.5,
            convergence_count=3,
        )
        kinds = {f["factor"] for f in h.confidence_factors}
        assert "alpha_score" in kinds
        assert "lifetime_wr" in kinds
        assert "wr_30d" in kinds
        assert "convergence" in kinds
        for f in h.confidence_factors:
            assert f["weight"] > 0


# ── persistence ─────────────────────────────────────────────────────────────


class TestPersistAndQuery:
    def test_persist_and_retrieve(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        _seed_alpha(db, "0xpro", "politics", wr=0.8, n=50, avg_pnl=100.0)
        h = build_hypothesis(
            str(db), wallet="0xpro", category="politics",
            signal_type="whale_entry", market_slug="m",
            market_question="Will X?", direction="BUY", entry_price=0.4,
        )
        row_id = persist_hypothesis(str(db), h, trade_id=42)
        assert row_id is not None and row_id > 0
        recent = get_recent_hypotheses(str(db), limit=10)
        assert len(recent) == 1
        assert recent[0]["trade_id"] == 42
        assert recent[0]["wallet"] == "0xpro"
        assert isinstance(recent[0]["confidence_factors"], list)

    def test_persist_multiple_returns_all(self, tmp_path):
        db = _bootstrap(tmp_path)
        ensure_schema(str(db))
        for i in range(3):
            h = build_hypothesis(
                str(db), wallet=f"0x{i}", category="politics",
                signal_type="whale_entry", market_slug="m",
                market_question=f"Q{i}", direction="BUY", entry_price=0.5,
            )
            persist_hypothesis(str(db), h, trade_id=i)
        recent = get_recent_hypotheses(str(db), limit=5)
        assert len(recent) == 3
        # Ordering within the same second isn't guaranteed because the
        # ORDER BY is on created_at (epoch seconds). All three trade_ids
        # must be present, but the within-second order is unspecified.
        assert {r["trade_id"] for r in recent} == {0, 1, 2}
