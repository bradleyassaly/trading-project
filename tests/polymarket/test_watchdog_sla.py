"""N1: dead-man's-switch SLA checks — reconcile-age logic from state.json."""
import importlib
import json
import os
import sys
import time
import types

import pytest

SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")


@pytest.fixture
def hw(tmp_path, monkeypatch):
    if SCRIPTS not in sys.path:
        sys.path.insert(0, SCRIPTS)
    mod = importlib.import_module("health_watchdog")
    importlib.reload(mod)
    # redirect the state file to a temp path
    monkeypatch.setattr(mod, "SCHEDULER_STATE", tmp_path / "state.json")
    return mod


def _write_state(hw, tasks):
    (hw.SCHEDULER_STATE).write_text(json.dumps({"tasks": tasks}), encoding="utf-8")


def test_reconcile_clean_recent_is_healthy(hw):
    _write_state(hw, [{"name": "reconcile_polymarket_truth",
                       "last_run_at": time.time() - 3600, "last_status": "ok"}])
    s = hw.check_reconcile_age()
    assert s.healthy


def test_reconcile_drift_outstanding_unhealthy(hw):
    # 2026-07-23: the reconciler's designed drift-exit (code 2) is now recorded
    # as last_status='drift', distinct from a crash ('failed'). Drift is still
    # unhealthy and still says "drift".
    _write_state(hw, [{"name": "reconcile_polymarket_truth",
                       "last_run_at": time.time() - 3600, "last_status": "drift"}])
    s = hw.check_reconcile_age()
    assert not s.healthy and "drift" in s.detail.lower()


def test_reconcile_crash_reads_as_crash_not_drift(hw):
    # A real crash (exit 1 / timeout) is 'failed' and must read as a crash,
    # not be mislabelled as drift.
    _write_state(hw, [{"name": "reconcile_polymarket_truth",
                       "last_run_at": time.time() - 3600, "last_status": "failed"}])
    s = hw.check_reconcile_age()
    assert not s.healthy and "crash" in s.detail.lower()


def test_reconcile_too_old_unhealthy(hw):
    _write_state(hw, [{"name": "reconcile_polymarket_truth",
                       "last_run_at": time.time() - 40 * 3600, "last_status": "ok"}])
    s = hw.check_reconcile_age()
    assert not s.healthy


def test_reconcile_never_run_unhealthy(hw):
    _write_state(hw, [{"name": "reconcile_polymarket_truth",
                       "last_run_at": None, "last_status": None}])
    s = hw.check_reconcile_age()
    assert not s.healthy


def test_only_balance_staleness_is_auto_halt(hw):
    # Guard the safety decision: reconcile drift must NOT auto-halt (would
    # wrongly halt for hours on a transient drift); only balance staleness does.
    assert "balance_staleness" in hw._SEVERE_HALT_COMPONENTS
    assert "reconcile_age" not in hw._SEVERE_HALT_COMPONENTS


# ── balance_staleness false-positive regressions (2026-07-27) ──────────────
# A stop-loss exit filled on-chain at 11:10:27, the hourly snapshot at
# 11:10:32 already contained its proceeds, the executor booked exit_ts at
# 11:10:35 — and when that snapshot became the OLDEST of the 8-window, the
# check read "1 fills/exits DESPITE frozen balance" and tripped a 5-hour
# emergency stop on a perfectly healthy system.

class _FakeConn:
    """Stands in for the psycopg connection: canned snapshot rows + moved
    count, records every (sql, params) call for predicate assertions."""

    def __init__(self, snap_rows, moved_count):
        self._snap_rows = snap_rows
        self._moved_count = moved_count
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        rows = (self._snap_rows if "live_equity_snapshots" in sql
                else (self._moved_count,))

        class _Cur:
            def fetchall(_c):
                return rows

            def fetchone(_c):
                return rows
        return _Cur()


def _frozen_snaps(now, n=8, val=214.2772):
    # DESC order, hourly, all the same balance.
    return [(int(now - i * 3600), val) for i in range(n)]


def test_frozen_balance_no_movement_is_healthy(hw, monkeypatch):
    now = time.time()
    conn = _FakeConn(_frozen_snaps(now), moved_count=0)
    monkeypatch.setattr(hw, "_watchdog_pg", lambda: conn)
    s = hw.check_balance_staleness()
    assert s.healthy
    assert "expected" in s.detail


def test_moved_query_applies_boundary_grace_and_excludes_bookkeeping(hw, monkeypatch):
    now = time.time()
    conn = _FakeConn(_frozen_snaps(now), moved_count=0)
    monkeypatch.setattr(hw, "_watchdog_pg", lambda: conn)
    hw.check_balance_staleness()
    moved_sql, moved_params = conn.calls[1]
    oldest_ts = int(now - 7 * 3600)
    # Grace: an exit booked seconds after the boundary snapshot (whose
    # balance already includes the proceeds) must NOT count as movement.
    assert moved_params == (oldest_ts + hw.BALANCE_MOVE_GRACE_S,) * 2
    assert hw.BALANCE_MOVE_GRACE_S >= 60
    # Bookkeeping closes carry exit_ts = booking time for cash that moved
    # long before — they never imply fresh USDC movement.
    for reason in ("reconciled_redeem", "resolved_databook",
                   "resolved_zero_balance", "expired"):
        assert reason in moved_sql


def test_frozen_balance_with_real_movement_still_trips(hw, monkeypatch):
    # The detector must still catch the documented real failure: hours of
    # fills while the balance API reports a frozen number.
    now = time.time()
    conn = _FakeConn(_frozen_snaps(now), moved_count=3)
    monkeypatch.setattr(hw, "_watchdog_pg", lambda: conn)
    s = hw.check_balance_staleness()
    assert not s.healthy
    assert "DESPITE 3" in s.detail


def test_moving_balance_is_healthy(hw, monkeypatch):
    now = time.time()
    snaps = [(int(now - i * 3600), 214.0 + i) for i in range(8)]
    conn = _FakeConn(snaps, moved_count=0)
    monkeypatch.setattr(hw, "_watchdog_pg", lambda: conn)
    s = hw.check_balance_staleness()
    assert s.healthy


def test_auto_clear_sized_in_time_not_cycles(hw):
    # The old hardcoded 30 cycles assumed a 60s poll (~30 min); at the real
    # 300s poll it silently meant 2.5 HOURS of blocked entries. The default
    # must track the poll interval and stay near the ~30 min design intent.
    assert hw.AUTO_CLEAR_CONSECUTIVE >= 3
    assert (hw.AUTO_CLEAR_CONSECUTIVE * hw.POLL_INTERVAL_SECONDS
            <= 45 * 60)
