"""A3: per-slice exit-policy overrides from the hold-vs-exit counterfactual.

v1 semantics under test: only stop_loss can ever be exempted, only at
n>=30 and delta > $10; the monitor read is fail-safe-CLOSED (missing table,
stale row, exception — stops stay ON); the profile injection makes
stop_loss unreachable while trailing/TP/time-decay still fire.
"""
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import exit_counterfactual as xc


NOW = 1_783_000_000


@pytest.fixture()
def xdb(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)
    yield path
    os.unlink(path)


def _seed_overrides(path, rows):
    """rows: (signal_type, slice, reason, action, n, updated_at)."""
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE exit_policy_overrides (
                   signal_type TEXT, category_slice TEXT, exit_reason TEXT,
                   action TEXT, n INT, actual_pnl REAL, hold_pnl REAL,
                   delta REAL, window_days INT, updated_at INT)""")
    for sig, sl, reason, action, n, ts in rows:
        c.execute("INSERT INTO exit_policy_overrides VALUES (?,?,?,?,?,0,0,0,60,?)",
                  (sig, sl, reason, action, n, ts))
    c.commit()
    c.close()


# ---------------------------------------------------------------------------
# stop_loss_exempt truth table (fail-safe-CLOSED)
# ---------------------------------------------------------------------------

def test_exempt_fresh_and_big_n(xdb):
    _seed_overrides(xdb, [("resolution_decay", "sports", "stop_loss",
                           "exempt", 30, int(time.time()))])
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is True


def test_not_exempt_below_n_floor(xdb):
    _seed_overrides(xdb, [("resolution_decay", "sports", "stop_loss",
                           "exempt", 29, int(time.time()))])
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is False


def test_not_exempt_when_stale(xdb):
    _seed_overrides(xdb, [("resolution_decay", "sports", "stop_loss",
                           "exempt", 40, int(time.time()) - 8 * 86400)])
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is False


def test_missing_table_fails_closed(xdb):
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is False


def test_action_none_not_exempt(xdb):
    _seed_overrides(xdb, [("resolution_decay", "sports", "stop_loss",
                           "none", 50, int(time.time()))])
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is False


def test_slice_mapping(xdb):
    _seed_overrides(xdb, [("resolution_decay", "other", "stop_loss",
                           "exempt", 40, int(time.time()))])
    # a politics trade maps to slice 'other'
    assert xc.stop_loss_exempt("resolution_decay", "politics", db_path=xdb) is True
    # sports has no row → False
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is False


# ---------------------------------------------------------------------------
# Writer thresholds
# ---------------------------------------------------------------------------

def _seed_exits(path, n, actual_each, hold_each, reason="stop_loss",
                cat="sports", sig="resolution_decay"):
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE IF NOT EXISTS live_trades (
                   id INTEGER PRIMARY KEY, signal_type TEXT, category TEXT,
                   direction TEXT, exit_reason TEXT, fill_price REAL,
                   entry_price REAL, shares REAL, size_usd REAL,
                   realized_pnl REAL, token_id TEXT, condition_id TEXT,
                   dry_run INT, exit_ts INT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS markets (
                   condition_id TEXT, closed INT, outcome_prices TEXT,
                   yes_token_id TEXT, no_token_id TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS wallet_trades (
                   condition_id TEXT, market_resolved INT, market_outcome TEXT)""")
    for i in range(n):
        cid, tok = f"c{i}", f"yes{i}"
        # hold counterfactual: shares*payout - cost. Build a YES win where
        # cost = shares*fill = 5, payout total = hold_each + 5.
        shares = hold_each + 5.0
        fill = 5.0 / shares
        c.execute("INSERT INTO live_trades (signal_type, category, direction, "
                  "exit_reason, fill_price, shares, size_usd, realized_pnl, "
                  "token_id, condition_id, dry_run, exit_ts) "
                  "VALUES (?,?,?,?,?,?,?,?,?,?,0,?)",
                  (sig, cat, "BUY", reason, fill, shares, 5.0,
                   actual_each, tok, cid, NOW - 100))
        c.execute("INSERT INTO markets VALUES (?,?,?,?,?)",
                  (cid, 1, '["1.0","0.0"]', tok, f"no{i}"))
    c.commit()
    c.close()


def test_writer_exempts_only_over_thresholds(xdb):
    # 31 stop-loss exits, each: actual -$1, hold +$0 → per-slice delta ≈ $31
    _seed_exits(xdb, 31, actual_each=-1.0, hold_each=0.0)
    out = xc.update_exit_policy(db_path=xdb)
    assert out["exempted"] == 1
    assert xc.stop_loss_exempt("resolution_decay", "sports", db_path=xdb) is True


def test_writer_small_delta_is_none(xdb):
    # delta ≈ $9.3 total < $10 floor → action 'none'
    _seed_exits(xdb, 31, actual_each=-0.3, hold_each=0.0)
    out = xc.update_exit_policy(db_path=xdb)
    assert out["exempted"] == 0


def test_writer_small_n_is_none(xdb):
    _seed_exits(xdb, 29, actual_each=-5.0, hold_each=0.0)
    out = xc.update_exit_policy(db_path=xdb)
    assert out["exempted"] == 0


def test_writer_never_exempts_other_reasons(xdb):
    _seed_exits(xdb, 40, actual_each=-5.0, hold_each=0.0, reason="take_profit")
    out = xc.update_exit_policy(db_path=xdb)
    assert out["exempted"] == 0
    assert out["evaluated"] >= 1  # still audited as 'none'


def test_dry_run_writes_nothing(xdb):
    _seed_exits(xdb, 31, actual_each=-1.0, hold_each=0.0)
    out = xc.update_exit_policy(db_path=xdb, dry_run=True)
    assert out["exempted"] == 1 and out["dry_run"] is True
    c = sqlite3.connect(xdb)
    try:
        n = c.execute("SELECT COUNT(*) FROM exit_policy_overrides").fetchone()[0]
    except sqlite3.OperationalError:
        n = 0
    c.close()
    assert n == 0


# ---------------------------------------------------------------------------
# Profile injection: SL unreachable, other exits intact
# ---------------------------------------------------------------------------

def test_sl_injection_disables_only_stop_loss():
    from trading_platform.polymarket.live_position_monitor import (
        _decide_exit, _exit_profile,
    )
    prof = _exit_profile("resolution_decay", "BUY")
    exempt_prof = {**prof, "sl": -10.0}
    # Entry 0.50 → current 0.20 is a -60% drawdown: the normal profile
    # stops out; the exempt profile must not (SL threshold unreachable).
    normal = _decide_exit(side="BUY", entry=0.50, current=0.20,
                          mfe_pct=0.0, age_days=0.5, profile=prof)
    exempt = _decide_exit(side="BUY", entry=0.50, current=0.20,
                          mfe_pct=0.0, age_days=0.5, profile=exempt_prof)
    assert normal == "stop_loss"
    assert exempt != "stop_loss"
    # Take-profit still fires on the exempt profile (fresh peak so the
    # trailing ladder can't trigger first).
    tp_gain = prof["tp"] + 0.05
    current_tp = min(0.99, 0.50 * (1 + tp_gain))
    tp = _decide_exit(side="BUY", entry=0.50, current=current_tp,
                      mfe_pct=tp_gain, age_days=0.5, profile=exempt_prof)
    assert tp == "take_profit"
