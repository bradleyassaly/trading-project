"""
Tests for the centralized AlertManager.

The Telegram transport is patched at the module level so no real HTTP
calls are made. Each test resets the shared state via the fixture.
"""
from __future__ import annotations

import time

import pytest

from trading_platform.polymarket import alert_manager as am_mod
from trading_platform.polymarket.alert_manager import (
    AlertManager,
    Tier,
    DEDUP_WINDOW_SECONDS,
    MAX_PER_HOUR,
    TASK_FAILURE_THRESHOLD,
    _AlertManagerState,
)


class _FakeAlerter:
    """Stand-in for TelegramAlerter — records sends."""

    enabled = True

    def __init__(self):
        self.sent = []

    def _send(self, message: str, disable_notification: bool = False) -> bool:
        self.sent.append((message, disable_notification))
        return True


@pytest.fixture
def am(monkeypatch):
    # Fresh module-level state per test
    monkeypatch.setattr(am_mod, "_state", _AlertManagerState())
    monkeypatch.setattr(am_mod, "_alert_manager", None)
    monkeypatch.setattr(
        "trading_platform.polymarket.alert_manager.AlertManager.__init__",
        lambda self: None,
    )
    inst = AlertManager()
    inst._alerter = _FakeAlerter()
    inst._state = am_mod._state
    return inst


# ── Dedup ───────────────────────────────────────────────────────────────────


class TestDeduplication:
    def test_duplicate_within_window_suppressed(self, am):
        am._dispatch("hello world", tier=Tier.INFO)
        am._dispatch("hello world", tier=Tier.INFO)
        assert len(am._alerter.sent) == 1

    def test_different_messages_both_sent(self, am):
        am._dispatch("first", tier=Tier.INFO)
        am._dispatch("second", tier=Tier.INFO)
        assert len(am._alerter.sent) == 2

    def test_dedup_expires_after_window(self, am):
        am._dispatch("hello", tier=Tier.INFO)
        # Manually advance the cache timestamp past the window
        am._state.dedup_cache["hello"] = time.time() - DEDUP_WINDOW_SECONDS - 60
        am._dispatch("hello", tier=Tier.INFO)
        assert len(am._alerter.sent) == 2


# ── Rate limiting ───────────────────────────────────────────────────────────


class TestRateLimit:
    def test_blocks_at_max(self, am):
        for i in range(MAX_PER_HOUR + 5):
            am._dispatch(f"msg-{i}", tier=Tier.INFO)
        assert len(am._alerter.sent) == MAX_PER_HOUR

    def test_critical_bypasses_rate_limit(self, am):
        for i in range(MAX_PER_HOUR + 5):
            am._dispatch(f"info-{i}", tier=Tier.INFO)
        assert len(am._alerter.sent) == MAX_PER_HOUR
        am._dispatch("CRITICAL", tier=Tier.CRITICAL)
        assert len(am._alerter.sent) == MAX_PER_HOUR + 1


# ── Critical alerts ─────────────────────────────────────────────────────────


class TestCriticalAlerts:
    def test_critical_bypasses_dedup(self, am):
        am._dispatch("same critical", tier=Tier.CRITICAL)
        am._dispatch("same critical", tier=Tier.CRITICAL)
        assert len(am._alerter.sent) == 2

    def test_circuit_breaker_format(self, am):
        am.alert_circuit_breaker_triggered(
            equity=395, peak=500, drawdown_pct=0.21,
            reason="Max drawdown exceeded",
        )
        msg, silent = am._alerter.sent[0]
        assert "CIRCUIT BREAKER TRIGGERED" in msg
        assert "$395" in msg
        assert "21.0%" in msg
        assert "Max drawdown exceeded" in msg
        # Critical alerts are loud → disable_notification=False
        assert silent is False


# ── Service down / recovery ─────────────────────────────────────────────────


class TestServiceState:
    def test_service_down_fires_once(self, am):
        am.alert_service_down("api", "Connection refused")
        am.alert_service_down("api", "Connection refused")
        assert len(am._alerter.sent) == 1

    def test_recovery_after_down(self, am):
        am.alert_service_down("api", "down")
        am.alert_service_recovered("api")
        assert len(am._alerter.sent) == 2
        recovery_msg = am._alerter.sent[1][0]
        assert "RECOVERED" in recovery_msg

    def test_recovery_without_prior_down_is_noop(self, am):
        am.alert_service_recovered("api")
        assert len(am._alerter.sent) == 0


# ── Trade events ────────────────────────────────────────────────────────────


class TestTradeEvents:
    def test_trade_placed_format(self, am):
        am.alert_trade_placed(
            signal_type="whale_entry",
            wallet="0x1234567890abcdef",
            market="Will Iran ceasefire hold?",
            direction="YES",
            stake=50.0,
            entry_price=0.34,
            fusion_score=0.72,
            wallet_tier="tier1h",
        )
        msg, silent = am._alerter.sent[0]
        assert "TRADE PLACED" in msg
        assert "whale_entry" in msg
        assert "Iran ceasefire" in msg
        assert "$50" in msg
        assert "0.340" in msg or "0.34" in msg
        assert "tier1h" in msg
        # Trade events are loud → disable_notification=False
        assert silent is False
        assert am._state.trades_placed_today == 1

    def test_trade_resolved_win(self, am):
        am.alert_trade_resolved(
            signal_type="whale_entry",
            market="Will Iran ceasefire hold?",
            direction="YES",
            entry_price=0.34,
            exit_price=1.0,
            pnl=47.0,
            cumulative_pnl=147.0,
            win_rate=0.58,
            resolved_count=21,
        )
        msg, _ = am._alerter.sent[0]
        assert "TRADE WON" in msg
        assert "+$47" in msg
        assert "$+147" in msg or "+147" in msg
        assert "58%" in msg
        assert am._state.trades_won_today == 1

    def test_trade_resolved_loss(self, am):
        am.alert_trade_resolved(
            signal_type="convergence",
            market="Test", direction="NO",
            entry_price=0.40, exit_price=0.0,
            pnl=-30.0, cumulative_pnl=-30.0,
            win_rate=0.0, resolved_count=1,
        )
        msg, _ = am._alerter.sent[0]
        assert "TRADE LOST" in msg
        assert am._state.trades_lost_today == 1


# ── Skip accumulation ───────────────────────────────────────────────────────


class TestSkipAccumulation:
    def test_record_skipped_does_not_send(self, am):
        for _ in range(10):
            am.record_signal_skipped("fusion_too_low")
        assert len(am._alerter.sent) == 0
        assert am._state.skipped_signals_today["fusion_too_low"] == 10


# ── Consecutive task failures ───────────────────────────────────────────────


class TestConsecutiveTaskFailures:
    def test_single_failure_no_alert(self, am):
        am.alert_consecutive_task_failure("data_api_fetch", "timeout")
        assert len(am._alerter.sent) == 0

    def test_threshold_triggers_alert(self, am):
        for i in range(TASK_FAILURE_THRESHOLD):
            am.alert_consecutive_task_failure("data_api_fetch", "timeout")
        assert len(am._alerter.sent) == 1

    def test_recovery_resets_counter(self, am):
        for _ in range(TASK_FAILURE_THRESHOLD - 1):
            am.alert_consecutive_task_failure("foo", "err")
        am.task_recovered("foo")
        # Single failure after recovery should not trigger
        am.alert_consecutive_task_failure("foo", "err")
        assert len(am._alerter.sent) == 0


# ── Daily digest ────────────────────────────────────────────────────────────


class TestDailyDigest:
    def test_digest_includes_sections(self, am, monkeypatch):
        # Pre-populate state
        am._state.trades_placed_today = 3
        am._state.trades_resolved_today = 2
        am._state.trades_won_today = 1
        am._state.trades_lost_today = 1
        am._state.pnl_today = 23.0
        # Stub the _fetch_json helper so the digest doesn't hit the network
        monkeypatch.setattr(
            "trading_platform.polymarket.alert_manager.AlertManager._fetch_json",
            staticmethod(lambda path: {
                "/api/paper/bankroll": {
                    "current_equity": 10147,
                    "starting_capital": 10000,
                    "total_pnl": 147,
                },
                "/api/circuit-breaker/status": {
                    "peak_equity": 10200,
                    "current_drawdown_pct": 0.005,
                    "is_halted": False,
                },
                "/api/calibration/status": {
                    "rows": [{"signal_type": "whale_entry", "status": "live",
                              "bayesian_wr": 0.62, "sample_size": 18}],
                },
                "/api/system/scheduler-status": {
                    "tasks": [{"last_status": "ok"}, {"last_status": "ok"}],
                },
                "/api/smart-money/universe-stats": {
                    "tier1h": 158, "tier1": 31, "tier2": 46,
                },
            }.get(path, {})),
        )
        am.send_daily_digest()
        msg = am._alerter.sent[0][0]
        assert "DAILY DIGEST" in msg
        assert "TRADES" in msg
        assert "PORTFOLIO" in msg
        assert "SIGNALS" in msg
        assert "Placed: 3" in msg
        assert "1W / 1L" in msg

    def test_digest_resets_per_day_counters(self, am, monkeypatch):
        am._state.trades_placed_today = 5
        am._state.pnl_today = 100.0
        monkeypatch.setattr(
            "trading_platform.polymarket.alert_manager.AlertManager._fetch_json",
            staticmethod(lambda path: {}),
        )
        am.send_daily_digest()
        assert am._state.trades_placed_today == 0
        assert am._state.pnl_today == 0.0
