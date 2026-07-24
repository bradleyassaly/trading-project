"""A designed non-zero exit (the reconciler's exit 2 = 'drift detected') is
recorded as its own 'drift' state and must NOT be counted as a crash.

Regression for 2026-07-23: reconcile_polymarket_truth exits 2 by design when it
finds DB-vs-on-chain drift (errors:0). The scheduler folded every non-zero exit
into 'failed', so ~12 consecutive drift-exits inflated consecutive_failures and
fired the SUSTAINED-FAILURE ladder for 63+h while the reconciler was healthy.
"""
import importlib.util
import os
import sys
import types

_SPEC = importlib.util.spec_from_file_location(
    "task_scheduler",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "task_scheduler.py"),
)
ts = importlib.util.module_from_spec(_SPEC)
sys.modules["task_scheduler"] = ts
_SPEC.loader.exec_module(ts)
Task = ts.Task


def _run_with_exit(task, code, monkeypatch, tmp_path, stdout="output\n"):
    """Drive _run_task with a mocked subprocess result of the given exit code,
    capturing whether the crash / recovery alert paths fire."""
    monkeypatch.setattr(ts, "LOG_DIR", tmp_path)
    fired = {"failure": 0, "recovery": 0}
    monkeypatch.setattr(ts, "_send_failure_alert",
                        lambda *a, **k: fired.__setitem__("failure", fired["failure"] + 1))
    monkeypatch.setattr(ts, "_send_recovery_alert",
                        lambda *a, **k: fired.__setitem__("recovery", fired["recovery"] + 1))

    def fake_run(*a, **k):
        return types.SimpleNamespace(returncode=code, stdout=stdout)

    monkeypatch.setattr(ts.subprocess, "run", fake_run)
    ts._run_task(task)
    return fired


def _reconciler_task():
    return Task(name="reconcile_polymarket_truth", cmd="echo",
                interval_seconds=4 * 3600, description="", drift_exit_code=2)


def test_drift_exit_records_drift_not_failure(monkeypatch, tmp_path):
    t = _reconciler_task()
    fired = _run_with_exit(t, 2, monkeypatch, tmp_path)
    assert t.last_status == "drift"
    assert t.consecutive_failures == 0       # crash counter untouched
    assert t.first_failure_at is None
    assert t.consecutive_drifts == 1
    assert t.last_drift_at is not None
    assert fired["failure"] == 0             # NO crash alert on a designed drift


def test_repeated_drift_never_inflates_crash_counter(monkeypatch, tmp_path):
    t = _reconciler_task()
    for _ in range(12):                      # the 63h/12-run scenario
        _run_with_exit(t, 2, monkeypatch, tmp_path)
    assert t.consecutive_failures == 0
    assert t.consecutive_drifts == 12
    assert t.last_status == "drift"


def test_real_crash_exit1_still_counts_as_failure(monkeypatch, tmp_path):
    t = _reconciler_task()
    fired = _run_with_exit(t, 1, monkeypatch, tmp_path)
    assert t.last_status == "failed"
    assert t.consecutive_failures == 1
    assert t.first_failure_at is not None
    assert fired["failure"] == 1             # a real crash DOES alert


def test_clean_exit_clears_drift_state(monkeypatch, tmp_path):
    t = _reconciler_task()
    _run_with_exit(t, 2, monkeypatch, tmp_path)   # drift
    _run_with_exit(t, 0, monkeypatch, tmp_path)   # then green
    assert t.last_status == "ok"
    assert t.consecutive_drifts == 0
    assert t.last_drift_at is None


def test_exit2_without_drift_code_is_still_a_failure(monkeypatch, tmp_path):
    # A task that did NOT opt in to drift semantics must treat exit 2 as a crash.
    t = Task(name="ordinary", cmd="echo", interval_seconds=900, description="")
    fired = _run_with_exit(t, 2, monkeypatch, tmp_path)
    assert t.last_status == "failed"
    assert t.consecutive_failures == 1
    assert fired["failure"] == 1


def test_drift_after_crash_fires_one_recovery_and_resets(monkeypatch, tmp_path):
    t = _reconciler_task()
    for _ in range(6):                            # sustained crash
        _run_with_exit(t, 1, monkeypatch, tmp_path)
    assert t.consecutive_failures == 6
    fired = _run_with_exit(t, 2, monkeypatch, tmp_path)   # now merely drifts
    assert t.last_status == "drift"
    assert t.consecutive_failures == 0            # crash streak cleared
    assert fired["recovery"] == 1                 # one crash-recovery ping


def test_real_schedule_reconciler_opts_in_to_drift_code():
    task = next(t for t in ts.SCHEDULE if t.name == "reconcile_polymarket_truth")
    assert task.drift_exit_code == 2
