"""X1 (2026-07-27): per-trade persistence in the exit counterfactual.

The A3 aggregate (exit_policy_overrides) evaporated history on every
refresh; exit_counterfactuals keeps one immutable row per closed trade so
exit-policy debates run on standing evidence. Covers the source-1 payout
path (markets snapshot: closed + outcome_prices) — DB-free of the
canonical resolutions table, so it runs on a bare sqlite fixture.
"""
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import exit_counterfactual as ecf


@pytest.fixture()
def db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE live_trades (
                   id INTEGER PRIMARY KEY, signal_type TEXT, category TEXT,
                   direction TEXT, exit_reason TEXT, fill_price REAL,
                   entry_price REAL, shares REAL, size_usd REAL,
                   realized_pnl REAL, token_id TEXT, condition_id TEXT,
                   dry_run INT, exit_ts INT, is_probe INT DEFAULT 0)""")
    c.execute("""CREATE TABLE markets (
                   condition_id TEXT, closed INT, outcome_prices TEXT,
                   yes_token_id TEXT, no_token_id TEXT)""")
    c.execute("CREATE TABLE wallet_trades (condition_id TEXT, "
              "market_resolved INT, market_outcome TEXT)")
    now = int(time.time())
    # YES trade stopped out at −$2.00; market later resolved YES ($1/share)
    # → holding would have made +$7 on 10 shares @ 0.30 cost: alpha −9.
    c.execute("INSERT INTO live_trades VALUES (1,'resolution_decay','sports',"
              "'BUY','stop_loss',0.30,0.30,10.0,3.0,-2.0,'ytok','c1',0,?,0)",
              (now - 3600,))
    c.execute("INSERT INTO markets VALUES ('c1',1,'[\"1.0\",\"0.0\"]',"
              "'ytok','ntok')")
    c.commit(); c.close()
    yield path
    os.unlink(path)


def test_per_trade_row_persisted(db):
    out = ecf.update_exit_policy(db_path=db, dry_run=False)
    assert out["evaluated"] >= 1
    c = sqlite3.connect(db)
    row = c.execute("SELECT trade_id, exit_reason, realized_pnl, payout, "
                    "pnl_if_held, exit_alpha FROM exit_counterfactuals "
                    "WHERE trade_id = 1").fetchone()
    c.close()
    assert row is not None
    tid, reason, realized, payout, hold, alpha = row
    assert reason == "stop_loss"
    assert payout == 1.0
    assert hold == pytest.approx(10.0 * 1.0 - 3.0)   # +7.00
    assert alpha == pytest.approx(-2.0 - 7.0)        # exit lost $9 vs hold


def test_dry_run_does_not_persist(db):
    ecf.update_exit_policy(db_path=db, dry_run=True)
    c = sqlite3.connect(db)
    n = c.execute("SELECT COUNT(*) FROM exit_counterfactuals").fetchone()[0]
    c.close()
    assert n == 0


def test_rerun_is_idempotent(db):
    ecf.update_exit_policy(db_path=db, dry_run=False)
    ecf.update_exit_policy(db_path=db, dry_run=False)
    c = sqlite3.connect(db)
    n = c.execute("SELECT COUNT(*) FROM exit_counterfactuals").fetchone()[0]
    c.close()
    assert n == 1
