"""
Tests for order lifecycle manager + gap detection + dry-run mode.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trading_platform.polymarket.order_manager import OrderManager, OrderState


def _bootstrap(tmp_path: Path) -> str:
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE trade_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER, order_id TEXT, order_type TEXT,
            intended_price REAL, actual_fill_price REAL,
            intended_size REAL, actual_fill_size REAL,
            slippage REAL, slippage_pct REAL,
            fill_time_ms INTEGER, order_status TEXT,
            raw_response TEXT, created_at INTEGER
        );
        CREATE TABLE stale_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT, condition_id TEXT, market_title TEXT,
            side TEXT, price REAL, size REAL,
            trade_ts INTEGER, detected_at INTEGER,
            age_seconds INTEGER, reason TEXT, created_at INTEGER
        );
        CREATE TABLE dry_run_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_wallet TEXT, signal_type TEXT, token_id TEXT,
            market_title TEXT, category TEXT, side TEXT,
            intended_price REAL, intended_size REAL,
            intended_stake REAL, alpha_score REAL,
            resolution_price REAL, would_have_pnl REAL,
            spread_at_signal REAL, created_at INTEGER
        );
    """)
    conn.commit()
    conn.close()
    return db


def _mock_executor(fill_on_attempt=1, fill_status="filled"):
    """Create a mock executor that fills on the Nth attempt."""
    ex = MagicMock()
    call_count = {"n": 0}

    def place_order(token_id, side, price, size):
        call_count["n"] += 1
        if call_count["n"] >= fill_on_attempt:
            return {"success": True, "order_id": f"order_{call_count['n']}",
                    "intended_price": price, "intended_size": size}
        return {"success": False, "error": "temporary failure"}

    def get_order_status(order_id):
        return {"status": fill_status, "filled_size": 10.0, "price": 0.50}

    ex.place_order = MagicMock(side_effect=place_order)
    ex.get_order_status = MagicMock(side_effect=get_order_status)
    ex.cancel_order = MagicMock(return_value=True)
    return ex


# ── Order Lifecycle ──────────────────────────────────────────────────────────


class TestOrderLifecycle:
    def test_successful_order(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = _mock_executor(fill_on_attempt=1)
        mgr = OrderManager(ex, db_path=db)
        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0, trade_id=1)
        assert result["final_status"] == OrderState.FILLED
        assert result["total_filled"] > 0
        assert result["avg_fill_price"] is not None
        # Fill recorded in DB
        conn = sqlite3.connect(db)
        fills = conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0]
        conn.close()
        assert fills >= 1

    def test_rejected_order(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = MagicMock()
        ex.place_order = MagicMock(return_value={"success": False, "error": "insufficient balance"})
        mgr = OrderManager(ex, db_path=db)
        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0)
        assert result["final_status"] == OrderState.REJECTED
        assert result["total_filled"] == 0

    def test_insufficient_balance_no_retry(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = MagicMock()
        ex.place_order = MagicMock(return_value={"success": False, "error": "insufficient balance"})
        mgr = OrderManager(ex, db_path=db)
        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0)
        # Should NOT retry on balance errors
        assert ex.place_order.call_count == 1
        assert len(result["attempts"]) == 1

    def test_retry_on_failure(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = _mock_executor(fill_on_attempt=2)  # fail first, succeed second
        mgr = OrderManager(ex, db_path=db)
        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0)
        assert result["final_status"] == OrderState.FILLED
        assert len(result["attempts"]) == 2
        assert result["attempts"][0]["status"] == OrderState.REJECTED
        assert result["attempts"][1]["status"] == OrderState.FILLED

    def test_max_retries_exceeded(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = MagicMock()
        ex.place_order = MagicMock(return_value={"success": False, "error": "server error"})
        mgr = OrderManager(ex, db_path=db)
        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0)
        assert ex.place_order.call_count == 3  # initial + 2 retries
        assert result["final_status"] == OrderState.REJECTED

    def test_timeout_cancels(self, tmp_path):
        db = _bootstrap(tmp_path)
        ex = MagicMock()
        ex.place_order = MagicMock(return_value={
            "success": True, "order_id": "order_1",
            "intended_price": 0.50, "intended_size": 10,
        })
        ex.get_order_status = MagicMock(return_value={"status": "open"})
        ex.cancel_order = MagicMock(return_value=True)

        mgr = OrderManager(ex, db_path=db)
        mgr.ORDER_TIMEOUT_SECONDS = 0.1  # very short timeout for test
        mgr.POLL_INTERVAL_SECONDS = 0.05

        result = mgr.execute_with_monitoring("token1", "BUY", 0.50, 10.0)
        assert result["attempts"][0]["status"] == OrderState.EXPIRED
        ex.cancel_order.assert_called()  # called on each timed-out attempt


# ── Gap Detection ────────────────────────────────────────────────────────────


class TestGapDetection:
    def test_stale_signal_not_traded(self, tmp_path):
        """Signals older than threshold should be logged, not traded."""
        db = _bootstrap(tmp_path)
        # Add poll state tables
        conn = sqlite3.connect(db)
        conn.executescript("""
            CREATE TABLE wallet_poll_state (
                wallet TEXT PRIMARY KEY, last_checked_ts INTEGER DEFAULT 0,
                last_trade_hash TEXT, trades_found INTEGER DEFAULT 0,
                updated_at INTEGER DEFAULT 0
            );
            CREATE TABLE wallet_alpha_scores (
                wallet TEXT, category TEXT, is_copyable INTEGER,
                resolved_trades INTEGER, win_rate REAL, computed_at INTEGER,
                PRIMARY KEY (wallet, category)
            );
            INSERT INTO wallet_alpha_scores VALUES ('0xtest', 'politics', 1, 50, 0.7, 1000);
        """)
        conn.commit()
        conn.close()

        from trading_platform.polymarket.wallet_trade_poller import (
            WalletTradePoller, PolledTrade, STALE_SIGNAL_THRESHOLD,
        )
        poller = WalletTradePoller(db_path=db, dry_run=True)

        # Create a trade that's older than the threshold
        old_ts = int(time.time()) - STALE_SIGNAL_THRESHOLD - 60
        pt = PolledTrade(
            wallet="0xtest", condition_id="cid1", side="BUY",
            price=0.50, size=100, outcome="YES", title="Old trade",
            category="politics", timestamp=old_ts, tx_hash="0xold", slug="old",
        )
        poller._record_stale(pt, STALE_SIGNAL_THRESHOLD + 60, "gap_catchup")

        conn = sqlite3.connect(db)
        stale = conn.execute("SELECT COUNT(*) FROM stale_signals").fetchone()[0]
        conn.close()
        assert stale == 1

    def test_fresh_signal_not_stale(self):
        """Signals within threshold should pass."""
        from trading_platform.polymarket.wallet_trade_poller import STALE_SIGNAL_THRESHOLD
        fresh_age = STALE_SIGNAL_THRESHOLD - 10
        assert fresh_age < STALE_SIGNAL_THRESHOLD


# ── Order Execution Strategy ─────────────────────────────────────────────────


class TestOrderExecutionStrategy:
    def test_market_order_tight_spread(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        assert OrderExecutionStrategy.should_use_market_order(0.9, 0.01) is True

    def test_limit_order_wide_spread(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        assert OrderExecutionStrategy.should_use_market_order(0.9, 0.05) is False

    def test_limit_order_low_urgency(self):
        from trading_platform.polymarket.order_execution import OrderExecutionStrategy
        assert OrderExecutionStrategy.should_use_market_order(0.3, 0.01) is False
