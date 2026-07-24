"""End-to-end: a REAL subprocess that exits 2 (a synthetic reconciler drift)
flows through the scheduler as 'drift' and the watchdog raises a drift alert —
while a real exit 1 (crash) flows through as 'failed'. This is the live proof
of the requirement "reconciler green, and a synthetic drift still alerts":
the drift path stays loud, it just stops being mis-counted as a crash.
"""
import importlib.util
import json
import os
import sys
import time
import types

_HERE = os.path.dirname(__file__)


def _load(mod_name, rel):
    spec = importlib.util.spec_from_file_location(
        mod_name, os.path.join(_HERE, "..", "scripts", rel))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


ts = _load("task_scheduler", "task_scheduler.py")
hw = _load("health_watchdog", "health_watchdog.py")


def _synthetic_script(tmp_path, exit_code, body='print("DRIFT — synthetic")'):
    p = tmp_path / f"synth_{exit_code}.py"
    p.write_text(f"import sys\n{body}\nsys.exit({exit_code})\n", encoding="utf-8")
    return p


def _quiet_alerts(monkeypatch):
    fired = {"failure": 0, "recovery": 0}
    monkeypatch.setattr(ts, "_send_failure_alert",
                        lambda *a, **k: fired.__setitem__("failure", fired["failure"] + 1))
    monkeypatch.setattr(ts, "_send_recovery_alert",
                        lambda *a, **k: fired.__setitem__("recovery", fired["recovery"] + 1))
    return fired


def test_synthetic_drift_flows_to_watchdog_alert(tmp_path, monkeypatch):
    monkeypatch.setattr(ts, "LOG_DIR", tmp_path)
    fired = _quiet_alerts(monkeypatch)
    script = _synthetic_script(tmp_path, 2)

    task = ts.Task(name="reconcile_polymarket_truth",
                   cmd=f"{sys.executable} {script}",
                   interval_seconds=4 * 3600, description="", drift_exit_code=2)
    ts._run_task(task)                      # REAL subprocess, real exit 2

    # scheduler: recorded as drift, not a crash
    assert task.last_status == "drift"
    assert task.consecutive_failures == 0
    assert fired["failure"] == 0

    # persist -> watchdog reads it -> raises a drift alert (unhealthy)
    state = tmp_path / "state.json"
    monkeypatch.setattr(ts, "STATE_PATH", state)
    ts._persist_state([task])
    monkeypatch.setattr(hw, "SCHEDULER_STATE", state)

    recon = hw.check_reconcile_age()
    assert recon.healthy is False
    assert "drift" in recon.detail.lower()

    # and the crash-counter check does NOT page for this drift
    sched = hw.check_scheduler_consecutive_failures()
    assert sched.healthy is True


def test_synthetic_crash_still_flows_to_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(ts, "LOG_DIR", tmp_path)
    fired = _quiet_alerts(monkeypatch)
    script = _synthetic_script(tmp_path, 1, body='print("boom")')

    task = ts.Task(name="reconcile_polymarket_truth",
                   cmd=f"{sys.executable} {script}",
                   interval_seconds=4 * 3600, description="", drift_exit_code=2)
    ts._run_task(task)                      # REAL subprocess, real exit 1

    assert task.last_status == "failed"
    assert task.consecutive_failures == 1
    assert fired["failure"] == 1
