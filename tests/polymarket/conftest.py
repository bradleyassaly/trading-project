"""
Polymarket test fixtures — production-file write guard.

2026-07-09: unit tests constructing RealtimeMonitor (and anything else
that instantiates AlertLog() bare) had appended 1294 synthetic fixture
rows to the production data/polymarket/alerts.jsonl and 1454 lines to
logs/monitor.log. The tests now pass explicit tmp paths, but this
autouse fixture is defense-in-depth: it redirects the *defaults* to
tmp_path so no future test can write the real files by accident.
"""
from __future__ import annotations

import pytest

from trading_platform.polymarket import alert_log as _alert_log_mod
from trading_platform.polymarket import realtime_monitor as _rt_mod


@pytest.fixture(autouse=True)
def _redirect_default_output_paths(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Point AlertLog/RealtimeMonitor default output paths at tmp_path.

    Two patches per default are needed for AlertLog: the module global
    ``_DEFAULT_PATH`` is only cosmetic once the module is imported —
    Python binds default argument values at ``def`` time, so
    ``AlertLog.__init__.__defaults__`` must be patched as well.
    """
    tmp_alerts = str(tmp_path / "default_alerts.jsonl")
    monkeypatch.setattr(_alert_log_mod, "_DEFAULT_PATH", tmp_alerts)
    monkeypatch.setattr(
        _alert_log_mod.AlertLog.__init__, "__defaults__", (tmp_alerts,)
    )

    # RealtimeMonitor's monitor_log_path is keyword-only, so its default
    # lives in __kwdefaults__ (a dict — setitem restores on teardown).
    tmp_monitor_log = str(tmp_path / "default_monitor.log")
    monkeypatch.setattr(_rt_mod, "_DEFAULT_MONITOR_LOG", tmp_monitor_log)
    monkeypatch.setitem(
        _rt_mod.RealtimeMonitor.__init__.__kwdefaults__,
        "monitor_log_path",
        tmp_monitor_log,
    )
