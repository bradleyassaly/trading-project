"""
Tests for execution gates (spread, depth, staleness, exposure, drawdown).
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from trading_platform.polymarket.execution_gates import ExecutionGates


def _bootstrap(tmp_path: Path) -> str:
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE polymarket_paper_trades (
            id INTEGER PRIMARY KEY, condition_id TEXT, question TEXT,
            category TEXT, side TEXT, entry_price REAL, size_usd REAL,
            signal_type TEXT, confidence REAL, wallet TEXT,
            entry_ts INTEGER, exit_ts INTEGER, archived INTEGER DEFAULT 0,
            source_wallet TEXT
        );
    """)
    conn.commit()
    conn.close()
    return db


# ── Drawdown Gate (no external calls) ───────────────────────────────────────


class TestDrawdownGate:
    def test_no_drawdown_full_size(self):
        passed, reason, data = ExecutionGates.check_drawdown(100_000, 100_000)
        assert passed is True
        assert data["size_multiplier"] == 1.0
        assert data["drawdown"] == 0

    def test_moderate_drawdown_half(self):
        passed, reason, data = ExecutionGates.check_drawdown(88_000, 100_000)
        assert passed is True
        assert data["size_multiplier"] == 0.5
        assert "moderate" in reason

    def test_severe_drawdown_quarter(self):
        passed, reason, data = ExecutionGates.check_drawdown(83_000, 100_000)
        assert passed is True
        assert data["size_multiplier"] == 0.25
        assert "severe" in reason

    def test_circuit_breaker_stop(self):
        passed, reason, data = ExecutionGates.check_drawdown(78_000, 100_000)
        assert passed is False
        assert data["size_multiplier"] == 0.0
        assert "circuit_breaker" in reason

    def test_no_baseline(self):
        passed, _, data = ExecutionGates.check_drawdown(50_000, 0)
        assert passed is True
        assert data["size_multiplier"] == 1.0


# ── Exposure Gate (DB-backed) ────────────────────────────────────────────────


class TestExposureGate:
    def test_within_limits_passes(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        passed, reason, data = gates.check_exposure("politics", 50, 100_000)
        assert passed is True
        assert "exposure_ok" in reason

    def test_category_limit_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        conn = sqlite3.connect(db)
        # Insert open trades that total $35K in politics
        for i in range(10):
            conn.execute(
                "INSERT INTO polymarket_paper_trades "
                "(condition_id, side, size_usd, category, entry_ts, archived) "
                "VALUES (?, 'YES', 3500, 'politics', ?, 0)",
                (f"cid{i}", int(time.time())),
            )
        conn.commit()
        conn.close()

        gates = ExecutionGates(db_path=db)
        passed, reason, data = gates.check_exposure("politics", 1000, 100_000)
        assert passed is False
        assert "category_limit" in reason

    def test_max_positions_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        conn = sqlite3.connect(db)
        for i in range(10):
            conn.execute(
                "INSERT INTO polymarket_paper_trades "
                "(condition_id, side, size_usd, category, entry_ts, archived) "
                "VALUES (?, 'YES', 100, 'other', ?, 0)",
                (f"cid{i}", int(time.time())),
            )
        conn.commit()
        conn.close()

        gates = ExecutionGates(db_path=db)
        passed, reason, data = gates.check_exposure("crypto", 50, 100_000)
        assert passed is False
        assert "max_positions" in reason


# ── Spread Gate (mocked API) ────────────────────────────────────────────────


class TestSpreadGate:
    def test_high_ev_passes(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.48", "size": "1000"}],
            "asks": [{"price": "0.52", "size": "1000"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_spread_edge("token1", 0.30)
        assert passed is True
        assert "spread_ok" in reason

    def test_thin_edge_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.45", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "1000"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_spread_edge("token1", 0.02)
        assert passed is False
        assert "edge_too_thin" in reason

    def test_api_failure_passes(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        with patch("requests.get", side_effect=Exception("timeout")):
            passed, reason, data = gates.check_spread_edge("token1", 0.10)
        assert passed is True  # fail-open
        assert "failed" in reason

    def test_no_liquidity_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"bids": [], "asks": []}
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_spread_edge("token1", 0.10)
        assert passed is False
        assert "no_liquidity" in reason


# ── Depth Gate (mocked API) ──────────────────────────────────────────────────


class TestDepthGate:
    def test_sufficient_depth_passes(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "asks": [{"price": "0.50", "size": "1000"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_depth("token1", 50)
        assert passed is True

    def test_insufficient_depth_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "asks": [{"price": "0.50", "size": "10"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_depth("token1", 50)
        assert passed is False
        assert "insufficient_depth" in reason


# ── Staleness Gate (mocked API) ──────────────────────────────────────────────


class TestStalenessGate:
    def test_fresh_price_passes(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.495"}],
            "asks": [{"price": "0.505"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_price_staleness(
                "token1", 0.50, int(time.time()) - 30,
            )
        assert passed is True
        assert "price_fresh" in reason

    def test_stale_price_fails(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.54"}],
            "asks": [{"price": "0.56"}],
        }
        with patch("requests.get", return_value=mock_resp):
            passed, reason, data = gates.check_price_staleness(
                "token1", 0.48, int(time.time()) - 300,
            )
        assert passed is False
        assert "price_moved" in reason

    def test_no_whale_ts_skips(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        passed, reason, data = gates.check_price_staleness("token1", None, None)
        assert passed is True


# ── All Gates ────────────────────────────────────────────────────────────────


class TestAllGates:
    def test_all_pass(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.48", "size": "5000"}],
            "asks": [{"price": "0.52", "size": "5000"}],
        }
        with patch("requests.get", return_value=mock_resp):
            should_trade, results = gates.run_all_gates(
                token_id="t1", expected_ev=0.30, stake=50,
                category="politics", bankroll=100_000,
                starting_bankroll=100_000,
            )
        assert should_trade is True
        assert results["size_multiplier"] == 1.0
        assert results["adjusted_stake"] == 50

    def test_drawdown_reduces_stake(self, tmp_path):
        db = _bootstrap(tmp_path)
        gates = ExecutionGates(db_path=db)
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "bids": [{"price": "0.48", "size": "5000"}],
            "asks": [{"price": "0.52", "size": "5000"}],
        }
        with patch("requests.get", return_value=mock_resp):
            should_trade, results = gates.run_all_gates(
                token_id="t1", expected_ev=0.30, stake=50,
                category="politics", bankroll=88_000,
                starting_bankroll=100_000,  # 12% drawdown
            )
        assert should_trade is True
        assert results["size_multiplier"] == 0.5
        assert results["adjusted_stake"] == 25


# ── Whale Exit Detection ─────────────────────────────────────────────────────


class TestWhaleExit:
    def test_exit_detected(self, tmp_path):
        db = str(tmp_path / "test.db")
        conn = sqlite3.connect(db)
        conn.executescript("""
            CREATE TABLE polymarket_paper_trades (
                id INTEGER PRIMARY KEY, condition_id TEXT,
                side TEXT, entry_price REAL, size_usd REAL,
                wallet TEXT, source_wallet TEXT, exit_ts INTEGER,
                archived INTEGER DEFAULT 0, question TEXT, category TEXT,
                signal_type TEXT, confidence REAL, entry_ts INTEGER
            );
            CREATE TABLE wallet_poll_state (
                wallet TEXT PRIMARY KEY, last_checked_ts INTEGER DEFAULT 0,
                last_trade_hash TEXT, trades_found INTEGER DEFAULT 0,
                updated_at INTEGER DEFAULT 0
            );
            CREATE TABLE whale_exit_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER, wallet TEXT, condition_id TEXT,
                exit_price REAL, exit_side TEXT, exit_ts INTEGER,
                our_entry_price REAL, unrealized_at_exit REAL,
                action_taken TEXT DEFAULT 'HOLD', detected_at INTEGER
            );
            CREATE TABLE wallet_alpha_scores (
                wallet TEXT, category TEXT, is_copyable INTEGER,
                resolved_trades INTEGER, win_rate REAL, computed_at INTEGER,
                PRIMARY KEY (wallet, category)
            );
        """)
        # Insert an open paper trade from wallet 0xwhale
        conn.execute(
            "INSERT INTO polymarket_paper_trades "
            "(id, condition_id, side, entry_price, size_usd, wallet, "
            " source_wallet, archived, entry_ts) "
            "VALUES (1, 'cid1', 'YES', 0.50, 100, '0xwhale', '0xwhale', 0, ?)",
            (int(time.time()),),
        )
        conn.commit()
        conn.close()

        from trading_platform.polymarket.wallet_trade_poller import (
            WalletTradePoller, PolledTrade,
        )
        poller = WalletTradePoller(db_path=db, dry_run=True)

        # Simulate whale selling
        pt = PolledTrade(
            wallet="0xwhale", condition_id="cid1", side="SELL",
            price=0.65, size=100, outcome="YES", title="Test",
            category="politics", timestamp=int(time.time()),
            tx_hash="0xhash", slug="test",
        )
        poller._check_whale_exit(pt)

        conn = sqlite3.connect(db)
        exits = conn.execute("SELECT * FROM whale_exit_signals").fetchall()
        conn.close()
        assert len(exits) == 1
        assert exits[0][7] is not None  # our_entry_price
        assert exits[0][9] == "HOLD"  # action_taken


# ── Order Execution Strategy ─────────────────────────────────────────────────


class TestOrderExecution:
    def test_limit_price_buy(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        strategy = OrderExecutionStrategy()
        book = {
            "bids": [{"price": "0.48"}],
            "asks": [{"price": "0.52"}],
        }
        price = strategy.compute_limit_price("BUY", book, urgency=0.5)
        assert 0.48 < price < 0.52  # between bid and ask

    def test_fill_quality(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        strategy = OrderExecutionStrategy()
        book = {
            "asks": [
                {"price": "0.50", "size": "200"},
                {"price": "0.51", "size": "300"},
            ],
        }
        result = strategy.estimate_fill_quality("BUY", 50, book)
        assert result["fillable"] is True
        assert result["fill_pct"] > 0

    def test_no_liquidity(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        strategy = OrderExecutionStrategy()
        result = strategy.estimate_fill_quality("BUY", 50, {"asks": []})
        assert result["fillable"] is False
