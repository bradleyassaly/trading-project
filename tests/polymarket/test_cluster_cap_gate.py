"""R2: the cluster dollar cap, finally enforced pre-trade.

portfolio_risk.assess() computed cluster exposure but nothing read it at
trade time — one football match carried 6 correlated positions on 7/6.
check_cluster_cap blocks the trade that would CREATE a >20%-of-bankroll
cluster (marginal-delta), with a 60s cache (2-4s fast lane), burst
visibility via record_open, and a fail-OPEN-with-alert degraded circuit.
"""
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import portfolio_risk as pr


def _seed(path, positions, equity=315.0, snap_ts=None):
    """positions: (direction, size_usd, question, status)."""
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE live_trades (
                   id INTEGER PRIMARY KEY, signal_type TEXT, direction TEXT,
                   size_usd REAL, question TEXT, condition_id TEXT,
                   dry_run INT, status TEXT, realized_pnl REAL, exit_ts INT)""")
    c.execute("""CREATE TABLE live_equity_snapshots (
                   ts INT, usdc_balance REAL, total_equity REAL,
                   open_cost_basis REAL, open_count INT)""")
    for d, s, q, st in positions:
        c.execute("INSERT INTO live_trades (signal_type, direction, size_usd, "
                  "question, condition_id, dry_run, status, realized_pnl) "
                  "VALUES ('t',?,?,?,'c',0,?,NULL)", (d, s, q, st))
    c.execute("INSERT INTO live_equity_snapshots VALUES (?,?,?,?,?)",
              (snap_ts or int(time.time()), equity, equity, 50.0, len(positions)))
    c.commit()
    c.close()


# _topic_stem = first 6 words after date/number stripping — these two share
# the stem "will belgium beat spain in the".
QUESTION = "Will Belgium beat Spain in the semifinal on Friday?"
SAME_CLUSTER = "Will Belgium beat Spain in the final match tonight?"


@pytest.fixture()
def gate_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)
    pr.invalidate_cluster_cache()
    yield path
    pr.invalidate_cluster_cache()
    os.unlink(path)


def test_blocks_breach_and_allows_under_cap(gate_db):
    # $50 existing in the cluster on $315 equity; cap = $63.
    _seed(gate_db, [("BUY", 50.0, QUESTION, "matched")])
    out = pr.check_cluster_cap(SAME_CLUSTER, 15.0, db_path=gate_db)
    assert out["allowed"] is False and "cluster_cap" in out["reason"]
    out2 = pr.check_cluster_cap(SAME_CLUSTER, 10.0, db_path=gate_db)
    assert out2["allowed"] is True


def test_marginal_delta_blocks_the_creating_trade(gate_db):
    # Cluster at 19% (below cap — assess()'s existing-breach flag is silent)
    # but the candidate pushes it to 21%: THIS trade must block.
    _seed(gate_db, [("BUY", 60.0, QUESTION, "matched")], equity=315.0)
    out = pr.check_cluster_cap(SAME_CLUSTER, 7.0, db_path=gate_db)
    assert out["allowed"] is False


def test_counts_submitted_and_both_directions(gate_db):
    # submitted rows are committed capital; SELL on the same event is the
    # same directional bet, not a hedge.
    _seed(gate_db, [("BUY", 30.0, QUESTION, "submitted"),
                    ("SELL", 30.0, SAME_CLUSTER, "live")])
    out = pr.check_cluster_cap(SAME_CLUSTER, 10.0, db_path=gate_db)
    assert out["cluster_exposure"] == 60.0
    assert out["allowed"] is False


def test_ignores_exited_and_dry_run(gate_db):
    _seed(gate_db, [("BUY", 500.0, QUESTION, "matched")])
    c = sqlite3.connect(gate_db)
    c.execute("UPDATE live_trades SET realized_pnl = 1.0")  # exited
    c.execute("INSERT INTO live_trades (signal_type, direction, size_usd, "
              "question, condition_id, dry_run, status, realized_pnl) "
              "VALUES ('t','BUY',500,?, 'c',1,'matched',NULL)", (QUESTION,))
    c.commit(); c.close()
    out = pr.check_cluster_cap(SAME_CLUSTER, 5.0, db_path=gate_db)
    assert out["allowed"] is True and out["cluster_exposure"] == 0.0


def test_short_stem_passes(gate_db):
    _seed(gate_db, [])
    out = pr.check_cluster_cap("Up?", 5.0, db_path=gate_db)
    assert out["allowed"] is True and out["reason"] == "no_stem"


def test_ttl_cache_no_requery_within_window(gate_db, monkeypatch):
    _seed(gate_db, [("BUY", 10.0, QUESTION, "matched")])
    pr.check_cluster_cap(SAME_CLUSTER, 5.0, db_path=gate_db)
    calls = []
    real_gc = pr.get_connection
    monkeypatch.setattr(pr, "get_connection",
                        lambda *a, **k: (calls.append(1), real_gc(*a, **k))[1])
    pr.check_cluster_cap(SAME_CLUSTER, 5.0, db_path=gate_db)
    assert calls == []  # served from cache within TTL


def test_record_open_visible_before_refresh(gate_db):
    # A burst: first trade submits $55; the cached snapshot must reflect it
    # so the second signal in the same minute blocks.
    _seed(gate_db, [])
    assert pr.check_cluster_cap(QUESTION, 55.0, db_path=gate_db)["allowed"]
    pr.record_open(QUESTION, 55.0)
    out = pr.check_cluster_cap(SAME_CLUSTER, 10.0, db_path=gate_db)
    assert out["allowed"] is False


def test_degraded_on_connection_failure(gate_db, monkeypatch):
    pr.invalidate_cluster_cache()
    def _boom(*a, **k): raise OSError("db down")
    monkeypatch.setattr(pr, "get_connection", _boom)
    out = pr.check_cluster_cap(QUESTION, 5.0, db_path=gate_db)
    assert out["allowed"] is True and out["degraded"] is True


def test_degraded_on_phantom_equity(gate_db):
    # Stale-balance defense: equity $5.43 while real positions held $60+.
    _seed(gate_db, [("BUY", 4.0, QUESTION, "matched")], equity=5.43)
    out = pr.check_cluster_cap(SAME_CLUSTER, 5.0, db_path=gate_db)
    assert out["allowed"] is True and out["degraded"] is True
    assert "stale-balance" in out["reason"]


def test_assess_keys_unchanged_and_counts_submitted(gate_db):
    _seed(gate_db, [("BUY", 30.0, QUESTION, "submitted")])
    from trading_platform.polymarket import db_connection as dbc
    out = pr.assess()
    for k in ("ts", "equity", "n_open", "buy_exposure", "sell_exposure",
              "net_short", "var5_dollars", "var5_frac", "cluster_flags"):
        assert k in out
    assert out["buy_exposure"] == 30.0  # submitted row counted
