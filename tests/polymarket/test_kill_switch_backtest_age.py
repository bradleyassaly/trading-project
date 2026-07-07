"""Locks the F5 fix: a stale/backtest-only signal cannot reach full stake.

Audit finding (2026-07-07): with no recency filter and no zero-live cap, a
months-old backtest (wallet_reversal n=192 from May) yielded effective_n=96,
skipped probation, and authorized a full MAX_TRADE_USD trade on replay alone.
"""
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket.kill_switch import KillSwitch


def _seed(db_path):
    c = sqlite3.connect(db_path)
    c.execute(
        """CREATE TABLE polymarket_paper_trades (
             id INTEGER PRIMARY KEY, signal_type TEXT, archived INT DEFAULT 0,
             exit_ts INT, realized_pnl REAL, size_usd REAL, outcome TEXT)"""
    )
    c.execute(
        """CREATE TABLE signal_backtest_results (
             signal_type TEXT, resolved INT, ev REAL, wr REAL, run_ts INT)"""
    )
    c.execute("CREATE TABLE signal_health (signal_type TEXT, ic_14d REAL, decay_flag INT)")
    c.commit()
    return c


def test_stale_backtest_only_stays_probation(monkeypatch):
    monkeypatch.setenv("POLYMARKET_LIVE_ENABLED", "1")
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    c = _seed(path)
    # A big backtest resolved 400 days ago; zero live/paper rows.
    import time
    c.execute(
        "INSERT INTO signal_backtest_results VALUES (?,?,?,?,?)",
        ("stale_sig", 200, 0.20, 0.80, int(time.time()) - 400 * 86400),
    )
    c.commit()
    c.close()

    ks = KillSwitch(path, bankroll=300.0)
    res = ks.check("stale_sig", trade_size_usd=5.0, confidence=0.7)
    # The stale backtest must NOT authorize a full-stake trade. Either blocked
    # outright (no usable evidence) or allowed only with a probation cap.
    assert (not res.allowed) or (res.probation_cap is not None), (
        f"stale backtest authorized full stake: allowed={res.allowed} "
        f"cap={res.probation_cap}"
    )
    os.unlink(path)
