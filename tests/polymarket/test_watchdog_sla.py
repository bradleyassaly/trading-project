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
