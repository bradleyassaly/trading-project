"""Scheduler runs trading-critical tasks FIRST on a busy/recovery pass.

After an outage the whole backlog is overdue; the loop runs due tasks
synchronously in one pass, so without priority the live money loop
(resolution_decay_signal, live_position_monitor) sits behind slow batch jobs.
_due_tasks must order critical tasks first, most-overdue-first within a group,
and exclude not-due / disabled tasks.
"""
import importlib.util
import os
import sys

_SPEC = importlib.util.spec_from_file_location(
    "task_scheduler",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "task_scheduler.py"),
)
ts = importlib.util.module_from_spec(_SPEC)
# Register before exec so the Task dataclass (PEP 563 string annotations) can
# resolve its own module via sys.modules during instantiation.
sys.modules["task_scheduler"] = ts
_SPEC.loader.exec_module(ts)
Task = ts.Task


def _task(name, *, critical=False, last_run_at=None, interval=900, enabled=True):
    return Task(name=name, cmd="echo", interval_seconds=interval,
                description="", last_run_at=last_run_at, enabled=enabled,
                critical=critical)


NOW = 1_000_000.0


def test_critical_runs_before_more_overdue_batch():
    crit = _task("signal", critical=True, last_run_at=NOW - 1000)      # 100s overdue
    batch = _task("backtest", critical=False, last_run_at=NOW - 9000)  # far more overdue
    order = ts._due_tasks([batch, crit], NOW)
    assert [t.name for t in order] == ["signal", "backtest"]


def test_excludes_not_due_and_disabled():
    due_crit = _task("exit_monitor", critical=True, last_run_at=NOW - 1000)
    not_due = _task("fresh", last_run_at=NOW)                    # next run in the future
    disabled = _task("off", critical=True, last_run_at=NOW - 9999, enabled=False)
    order = ts._due_tasks([not_due, disabled, due_crit], NOW)
    assert [t.name for t in order] == ["exit_monitor"]


def test_most_overdue_first_within_group():
    a = _task("a", last_run_at=NOW - 2000)
    b = _task("b", last_run_at=NOW - 8000)   # more overdue -> first
    c = _task("c", last_run_at=NOW - 5000)
    order = ts._due_tasks([a, b, c], NOW)
    assert [t.name for t in order] == ["b", "c", "a"]


def test_never_run_task_is_due_immediately():
    # last_run_at=None -> next_run_at returns `now` -> due on first pass
    fresh = _task("new", last_run_at=None)
    assert [t.name for t in ts._due_tasks([fresh], NOW)] == ["new"]


def test_real_schedule_marks_money_loop_critical():
    # 2026-07-23: live_position_monitor / live_equity_snapshot were EXTRACTED
    # from SCHEDULE into the critical-loops container (commit 1d7a716) so the
    # money loop survives dispatch-loop death. In SCHEDULE the remaining
    # critical task is the entry signal; the monitors must NOT quietly
    # reappear here — that would put live exits back behind batch jobs.
    names = {t.name for t in ts.SCHEDULE}
    crit = {t.name for t in ts.SCHEDULE if getattr(t, "critical", False)}
    assert "resolution_decay_signal" in crit
    assert "live_position_monitor" not in names
    assert "live_equity_snapshot" not in names
