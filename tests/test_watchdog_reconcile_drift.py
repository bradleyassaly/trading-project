"""Watchdog reports a designed reconciler drift-exit ('drift') distinctly from
a real crash ('failed'), and never pages the crash-counter check for a drift.

Companion to test_scheduler_drift_exit.py — the scheduler records the state,
the watchdog interprets it. Both must agree that drift != crash.
"""
import importlib.util
import json
import os
import sys
import time

_SPEC = importlib.util.spec_from_file_location(
    "health_watchdog",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "health_watchdog.py"),
)
hw = importlib.util.module_from_spec(_SPEC)
sys.modules["health_watchdog"] = hw
_SPEC.loader.exec_module(hw)


def _write_state(tmp_path, monkeypatch, tasks):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"updated_at": int(time.time()), "tasks": tasks}),
                 encoding="utf-8")
    monkeypatch.setattr(hw, "SCHEDULER_STATE", p)
    return p


def _recon(status, age_s=3600):
    return {"name": "reconcile_polymarket_truth", "enabled": True,
            "interval_seconds": 4 * 3600, "last_run_at": time.time() - age_s,
            "last_status": status, "consecutive_failures": 0}


def test_drift_status_is_unhealthy_and_labelled_drift(tmp_path, monkeypatch):
    _write_state(tmp_path, monkeypatch, [_recon("drift")])
    st = hw.check_reconcile_age()
    assert st.healthy is False
    assert "DRIFT" in st.detail.upper()
    assert "CRASH" not in st.detail.upper()


def test_failed_status_reads_as_crash_not_drift(tmp_path, monkeypatch):
    _write_state(tmp_path, monkeypatch, [_recon("failed")])
    st = hw.check_reconcile_age()
    assert st.healthy is False
    assert "CRASH" in st.detail.upper()


def test_clean_recent_reconcile_is_healthy(tmp_path, monkeypatch):
    _write_state(tmp_path, monkeypatch, [_recon("ok", age_s=3600)])
    st = hw.check_reconcile_age()
    assert st.healthy is True
    assert "clean" in st.detail.lower()


def test_drift_task_does_not_trip_consecutive_failures_check(tmp_path, monkeypatch):
    # Even if a stale pre-fix counter is present, a drift-status task must not
    # be counted as a sustained crash.
    task = _recon("drift")
    task["consecutive_failures"] = 12
    _write_state(tmp_path, monkeypatch, [task])
    st = hw.check_scheduler_consecutive_failures()
    assert st.healthy is True


def test_genuine_crash_streak_still_trips_consecutive_failures_check(tmp_path, monkeypatch):
    task = _recon("failed")
    task["consecutive_failures"] = 7
    _write_state(tmp_path, monkeypatch, [task])
    st = hw.check_scheduler_consecutive_failures()
    assert st.healthy is False
    assert "reconcile_polymarket_truth" in st.detail
