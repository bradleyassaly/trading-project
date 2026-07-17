"""
Tests for the cumulative-drawdown CircuitBreaker.

Uses tmp_path-scoped DBs so they run in isolation. Mocks the
TelegramAlerter via the autouse env-scrubbing conftest fixture so no
real messages are sent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from trading_platform.polymarket.circuit_breaker import (
    CircuitBreaker,
    DEFAULT_DAILY_LOSS_LIMIT,
    DEFAULT_MAX_DRAWDOWN_PCT,
    WARNING_DRAWDOWN_PCT,
)


@pytest.fixture(autouse=True)
def _force_sqlite_backend(monkeypatch):
    """Pin these tests to the tmp-path SQLite DBs they bootstrap.

    Under DB_BACKEND=postgres (the post-cutover default) connect_wallet_db()
    IGNORES the explicit db path and routes to the shared Postgres database —
    observed 2026-07-16: this suite ran record_trade()/configure() against the
    PRODUCTION circuit_breaker_state row from pytest. Never remove this.
    """
    monkeypatch.setenv("DB_BACKEND", "sqlite")


def _bootstrap_db(tmp_path: Path) -> Path:
    db = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE circuit_breaker_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            starting_capital REAL NOT NULL,
            peak_equity REAL NOT NULL,
            current_equity REAL NOT NULL,
            max_drawdown_pct REAL NOT NULL DEFAULT 0.20,
            current_drawdown_pct REAL NOT NULL DEFAULT 0.0,
            is_halted INTEGER NOT NULL DEFAULT 0,
            halted_at INTEGER,
            halted_reason TEXT,
            last_reset_at INTEGER,
            last_reset_by TEXT,
            last_updated INTEGER,
            daily_pnl REAL DEFAULT 0.0,
            daily_loss_limit REAL DEFAULT 0.05,
            daily_halted INTEGER DEFAULT 0,
            daily_reset_at INTEGER
        );
        CREATE TABLE circuit_breaker_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            equity_before REAL,
            equity_after REAL,
            drawdown_pct REAL,
            daily_pnl REAL,
            details TEXT,
            created_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()
    return db


# ── Init ────────────────────────────────────────────────────────────────────


class TestInitialize:
    def test_init_sets_peak_equal_to_start(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        s = cb.initialize(1000)
        assert s["peak_equity"] == 1000
        assert s["current_equity"] == 1000
        assert s["is_halted"] is False

    def test_can_trade_initially_true(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        assert cb.can_trade() == (True, "ok")

    def test_can_trade_false_when_uninitialized(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        allowed, reason = cb.can_trade()
        assert allowed is False
        assert "not initialized" in reason

    def test_init_idempotent(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        # Try to re-init with a different value — should be ignored
        cb.initialize(5000)
        s = cb.get_status()
        assert s["starting_capital"] == 1000
        assert s["peak_equity"] == 1000


# ── Drawdown tracking ──────────────────────────────────────────────────────


class TestDrawdownTracking:
    def test_winning_trade_raises_peak(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.record_trade(100)
        s = cb.get_status()
        assert s["peak_equity"] == 1100
        assert s["current_equity"] == 1100
        assert s["current_drawdown_pct"] == 0

    def test_losing_trade_raises_drawdown(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.record_trade(100)  # peak now 1100
        cb.record_trade(-200)  # equity 900, dd from peak 1100
        s = cb.get_status()
        assert s["current_equity"] == 900
        assert s["peak_equity"] == 1100
        assert s["current_drawdown_pct"] == pytest.approx((1100 - 900) / 1100, abs=1e-4)

    def test_drawdown_math(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.record_trade(200)  # peak 1200
        cb.record_trade(-200)  # back to 1000, dd = 200/1200 ≈ 16.67%
        s = cb.get_status()
        assert s["current_drawdown_pct"] == pytest.approx(200 / 1200, abs=1e-4)

    def test_halt_at_threshold(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        # Wide daily limit so we isolate the drawdown gate
        cb.initialize(1000, max_drawdown_pct=0.20, daily_loss_limit=0.95)
        cb.record_trade(-199)  # 19.9% — NOT halted (need >= 20%)
        assert cb.can_trade()[0] is True
        s = cb.get_status()
        assert s["is_halted"] is False
        cb.record_trade(-2)  # 20.1% — halted
        s = cb.get_status()
        assert s["is_halted"] is True
        assert "drawdown" in (s.get("halted_reason") or "").lower()

    def test_halt_just_under_threshold(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        # Lose 19.9% — should NOT halt
        cb.record_trade(-199)
        s = cb.get_status()
        assert s["is_halted"] is False
        assert s["current_drawdown_pct"] == pytest.approx(0.199, abs=1e-3)

    def test_halted_blocks_trading(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        cb.record_trade(-300)
        allowed, reason = cb.can_trade()
        assert allowed is False
        assert "halted" in reason.lower()


# ── Live equity sync (update_equity — the production feed) ────────────────


class TestUpdateEquity:
    def test_seeds_state_when_uninitialized(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        s = cb.update_equity(250.0)
        assert s["starting_capital"] == 250.0
        assert s["peak_equity"] == 250.0
        assert s["current_equity"] == 250.0
        assert cb.can_trade() == (True, "ok")

    def test_level_based_tracking(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, daily_loss_limit=0.95)
        cb.update_equity(1100)
        assert cb.get_status()["peak_equity"] == 1100
        cb.update_equity(900)
        s = cb.get_status()
        assert s["current_equity"] == 900
        assert s["current_drawdown_pct"] == pytest.approx(200 / 1100, abs=1e-4)

    def test_halts_on_live_drawdown(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20, daily_loss_limit=0.95)
        cb.update_equity(750)  # 25% below peak
        s = cb.get_status()
        assert s["is_halted"] is True
        assert cb.can_trade()[0] is False

    def test_noop_sync_touches_state_without_logging(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        n_before = len(cb.get_log(limit=100))
        s = cb.update_equity(1000.0)  # unchanged level
        assert len(cb.get_log(limit=100)) == n_before
        assert s["current_equity"] == 1000

    def test_sync_logged_as_equity_sync(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, daily_loss_limit=0.95)
        cb.update_equity(950)
        events = cb.get_log(limit=10)
        assert any(e["event_type"] == "equity_sync" for e in events)

    def test_mark_to_market_daily_loss_halts(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.50, daily_loss_limit=0.05)
        cb.update_equity(1000)  # prime the sync window (no-op delta)
        cb.update_equity(930)   # -7% intraday mark move, prior sync today
        s = cb.get_status()
        assert s["daily_halted"] is True
        assert s["is_halted"] is False

    def test_first_sync_delta_not_booked_as_todays_loss(self, tmp_path):
        """A first-ever sync carries losses accumulated over prior days
        (prod 2026-07-16: -14.7% since the July-8 rebase). It must move
        equity/drawdown but NOT daily_pnl — booking it as today's loss
        would fire a false daily halt (the 2026-07-02 stale-loss outage
        mode). Subsequent same-day syncs DO count."""
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.50, daily_loss_limit=0.05)
        cb.update_equity(853)  # first sync: -14.7%, like the real backlog
        s = cb.get_status()
        assert s["current_equity"] == pytest.approx(853)
        assert s["current_drawdown_pct"] == pytest.approx(0.147, abs=1e-3)
        assert s["daily_pnl"] == 0.0
        assert s["daily_halted"] is False
        assert s["is_halted"] is False
        cb.update_equity(800)  # second sync, same day: counts
        s = cb.get_status()
        assert s["daily_pnl"] == pytest.approx(-53)
        assert s["daily_halted"] is True  # 53/853 ≈ 6.2% >= 5%


# ── Daily limits ───────────────────────────────────────────────────────────


class TestDailyLimits:
    def test_daily_loss_triggers_halt(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.50, daily_loss_limit=0.05)
        # Lose 6% of starting equity (above 5% daily limit)
        cb.record_trade(-60)
        s = cb.get_status()
        assert s["daily_halted"] is True

    def test_daily_reset_clears_daily_halt(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.50, daily_loss_limit=0.05)
        cb.record_trade(-60)
        cb.reset_daily()
        s = cb.get_status()
        assert s["daily_halted"] is False
        assert s["daily_pnl"] == 0.0

    def test_daily_reset_does_not_clear_drawdown_halt(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.10, daily_loss_limit=0.05)
        cb.record_trade(-150)  # past both daily and drawdown limits
        s = cb.get_status()
        assert s["is_halted"] is True
        assert s["daily_halted"] is True
        cb.reset_daily()
        s = cb.get_status()
        assert s["is_halted"] is True  # max drawdown halt persists
        assert s["daily_halted"] is False


# ── Reset ──────────────────────────────────────────────────────────────────


class TestReset:
    def test_reset_clears_halt(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        cb.record_trade(-300)
        assert cb.get_status()["is_halted"] is True
        cb.reset_drawdown(reset_by="test", reset_peak=True)
        s = cb.get_status()
        assert s["is_halted"] is False
        assert s["last_reset_by"] == "test"

    def test_reset_without_peak_can_rehalt(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20, daily_loss_limit=0.50)
        cb.record_trade(-300)  # halted at 30% drawdown
        cb.reset_drawdown(reset_by="test", reset_peak=False)
        # Peak still 1000, equity 700; another tiny loss should re-halt
        cb.record_trade(-50)  # equity 650, dd = 350/1000 = 35%
        s = cb.get_status()
        assert s["is_halted"] is True

    def test_reset_with_peak_starts_fresh(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20, daily_loss_limit=0.50)
        cb.record_trade(-300)
        cb.reset_drawdown(reset_by="test", reset_peak=True)
        s = cb.get_status()
        assert s["peak_equity"] == 700
        assert s["current_drawdown_pct"] == 0
        assert s["is_halted"] is False

    def test_configure_refused_while_halted(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        cb.record_trade(-300)
        result = cb.configure(max_drawdown_pct=0.50)
        assert "error" in result
        assert "halted" in result["error"].lower()

    def test_configure_works_when_not_halted(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.configure(max_drawdown_pct=0.30, daily_loss_limit=0.10)
        s = cb.get_status()
        assert s["max_drawdown_pct"] == 0.30
        assert s["daily_loss_limit"] == 0.10


# ── Logging ────────────────────────────────────────────────────────────────


class TestLogging:
    def test_every_trade_logged(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.record_trade(100)
        cb.record_trade(-50)
        cb.record_trade(25)
        events = cb.get_log()
        # init + 3 trades = 4 entries minimum
        assert len(events) >= 4

    def test_halt_event_logged(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        cb.record_trade(-300)
        events = cb.get_log()
        assert any(e["event_type"] == "halt" for e in events)

    def test_reset_event_logged(self, tmp_path):
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000)
        cb.record_trade(-300)
        cb.reset_drawdown(reset_by="test", reset_peak=True)
        events = cb.get_log()
        assert any(e["event_type"] == "reset" for e in events)


# ── Lifecycle integration ──────────────────────────────────────────────────


class TestLifecycle:
    def test_full_lifecycle(self, tmp_path):
        """init(500) → win(50) → peak=550 → lose(120) → drawdown=21.8% →
        HALTED → reset(reset_peak=True) → can_trade=True → peak=430"""
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(500, max_drawdown_pct=0.20, daily_loss_limit=0.50)
        cb.record_trade(50)
        s = cb.get_status()
        assert s["peak_equity"] == 550

        cb.record_trade(-120)
        s = cb.get_status()
        # 120/550 ≈ 21.8% > 20%
        assert s["is_halted"] is True
        assert s["current_equity"] == 430
        assert s["current_drawdown_pct"] == pytest.approx(120 / 550, abs=1e-3)
        assert cb.can_trade()[0] is False

        # Reset with peak adjustment
        cb.reset_drawdown(reset_by="test", reset_peak=True)
        s = cb.get_status()
        assert s["is_halted"] is False
        assert s["peak_equity"] == 430
        assert cb.can_trade()[0] is True

    def test_warning_band_does_not_halt(self, tmp_path):
        """Drawdown between WARNING_DRAWDOWN_PCT and max should not halt."""
        cb = CircuitBreaker(str(_bootstrap_db(tmp_path)))
        cb.initialize(1000, max_drawdown_pct=0.20)
        cb.record_trade(-150)  # 15% — warning band
        s = cb.get_status()
        assert s["current_drawdown_pct"] == 0.15
        assert s["is_halted"] is False
