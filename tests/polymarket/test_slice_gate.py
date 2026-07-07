"""P5: slice demoter — Wilson-bound decisions, gate table, fail-safe reader.

The promoter could only ADD stake (boost rows; its 'demotion' compared the
WR stored at promote time). slice_gate derives the negative blocklist the
live executor previously hardcoded (_CAT_BLOCKS), with kill/demote split and
a staleness beacon so an empty table is distinguishable from a dead job.
"""
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import slice_multiplier_promoter as smp


# ---------------------------------------------------------------------------
# Pure decision function
# ---------------------------------------------------------------------------

def test_kill_when_confidently_negative():
    # WR 20% at n=40 buying ~0.60 favorites, deep negative pnl: even the
    # optimistic Wilson bound can't pay the entry.
    status, why = smp._slice_gate_decision(40, 8, 0.60, -55.0, protected=False)
    assert status == "killed"
    assert "wilson_ub" in why


def test_demote_when_both_channels_negative():
    # WR 40% at n=25 buying 0.55 — EV lower bound < 0 and pnl <= 0.
    status, _ = smp._slice_gate_decision(25, 10, 0.55, -3.0, protected=False)
    assert status == "demoted"


def test_lottery_slice_not_killed_by_wr_alone():
    # The confluence_2plus×crypto shape: WR 22% but strongly POSITIVE pnl
    # (rare huge wins). The pnl conjunct must protect it.
    status, _ = smp._slice_gate_decision(50, 11, 0.30, 2110.0, protected=False)
    assert status is None


def test_protected_signals_higher_floor():
    # n=25 would demote an ordinary signal; a protected one needs n>=30.
    args = (25, 10, 0.55, -3.0)
    assert smp._slice_gate_decision(*args, protected=False)[0] == "demoted"
    assert smp._slice_gate_decision(*args, protected=True)[0] is None
    assert smp._slice_gate_decision(35, 14, 0.55, -3.0, protected=True)[0] == "demoted"


def test_healthy_slice_passes():
    status, why = smp._slice_gate_decision(60, 40, 0.50, 80.0, protected=False)
    assert status is None and why.startswith("ok")


def test_no_data_guard():
    assert smp._slice_gate_decision(0, 0, 0.5, 0, protected=False)[0] is None
    assert smp._slice_gate_decision(30, 10, 0, -5, protected=False)[0] is None


def test_wilson_bounds_bracket_phat():
    lb = smp._wilson_lower_bound(30, 60)
    ub = smp._wilson_upper_bound(30, 60)
    assert lb < 0.5 < ub
    assert 0 < lb and ub < 1


# ---------------------------------------------------------------------------
# Reader: staleness fail-safe, prefix matching, cache
# ---------------------------------------------------------------------------

def _seed_gate(path, rows, last_run_at):
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE slice_gate (
                   signal_type TEXT, category TEXT, status TEXT,
                   n_resolved INT, wins INT, wr REAL, avg_entry REAL,
                   wilson_lb REAL, ev_lb REAL, sum_pnl REAL, reason TEXT,
                   computed_at INT)""")
    c.execute("CREATE TABLE slice_gate_meta (id INT PRIMARY KEY, last_run_at INT, "
              "candidates_tested INT, n_demoted INT, n_killed INT)")
    for sig, cat, status in rows:
        c.execute("INSERT INTO slice_gate VALUES (?,?,?,0,0,0,0,0,0,0,'',0)",
                  (sig, cat, status))
    c.execute("INSERT INTO slice_gate_meta VALUES (1,?,0,0,0)", (last_run_at,))
    c.commit()
    c.close()


@pytest.fixture()
def gate_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    # bust the module cache between tests
    monkeypatch.setattr(smp, "_GATE_CACHE", {"at": 0.0, "rows": {}, "fresh": False})
    yield path
    os.unlink(path)


def test_reader_exact_match(gate_db):
    _seed_gate(gate_db, [("whale_entry_filtered", "entertainment", "demoted")],
               int(time.time()))
    assert smp.get_slice_gate("whale_entry_filtered", "entertainment",
                              db_path=gate_db) == ("demoted", True)
    assert smp.get_slice_gate("whale_entry_filtered", "politics",
                              db_path=gate_db) == (None, True)


def test_reader_stale_table_fails_safe(gate_db):
    _seed_gate(gate_db, [("x", "sports", "killed")],
               int(time.time()) - 3 * 86400)  # older than 48h
    status, fresh = smp.get_slice_gate("x", "sports", db_path=gate_db)
    assert (status, fresh) == (None, False)   # caller falls back to hardcoded


def test_reader_missing_table_fails_safe(gate_db):
    status, fresh = smp.get_slice_gate("x", "sports", db_path=gate_db)
    assert (status, fresh) == (None, False)


def test_reader_prefix_bridging(gate_db):
    # promoter keys on markets.subcategory ('science/weather'); executor cat
    # is classifier output ('science') — both directions must match.
    _seed_gate(gate_db, [("cascade", "science/weather", "demoted"),
                         ("oversized_bet", "sports", "killed")],
               int(time.time()))
    assert smp.get_slice_gate("cascade", "science", db_path=gate_db) == ("demoted", True)
    assert smp.get_slice_gate("oversized_bet", "sports/nba",
                              db_path=gate_db) == ("killed", True)


# ---------------------------------------------------------------------------
# run_promoter pass 3 end-to-end on sqlite
# ---------------------------------------------------------------------------

def _seed_paper(path, slices):
    """slices: (signal_type, category, n, wins, entry_price, pnl_per_trade)."""
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE polymarket_paper_trades (
                   signal_type TEXT, category TEXT, condition_id TEXT,
                   archived INT DEFAULT 0, exit_ts INT, entry_ts INT,
                   outcome TEXT, realized_pnl REAL, entry_price REAL)""")
    c.execute("CREATE TABLE markets (condition_id TEXT, subcategory TEXT)")
    now = int(time.time())
    for sig, cat, n, wins, ep, ppt in slices:
        for i in range(n):
            won = i < wins
            c.execute(
                "INSERT INTO polymarket_paper_trades VALUES (?,?,?,0,?,?,?,?,?)",
                (sig, cat, f"{sig}-{cat}-{i}", now - 100, now - 200,
                 "win" if won else "loss", ppt if won else -abs(ppt), ep))
    c.commit()
    c.close()


def test_run_promoter_populates_gate_and_meta(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        monkeypatch.setenv("DB_BACKEND", "sqlite")
        from trading_platform.polymarket import db_connection as dbc
        monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
        monkeypatch.setattr(smp, "_GATE_CACHE",
                            {"at": 0.0, "rows": {}, "fresh": False})
        _seed_paper(path, [
            ("bad_sig", "entertainment", 40, 8, 0.60, 3.0),   # → killed
            ("meh_sig", "sports", 25, 10, 0.55, 2.0),          # → demoted
            ("good_sig", "politics", 60, 40, 0.50, 2.0),       # → clean
        ])
        out = smp.run_promoter(db_path=path)
        assert out["gate_killed"] == 1 and out["gate_demoted"] == 1, out
        c = sqlite3.connect(path)
        rows = {(r[0], r[1]): r[2] for r in
                c.execute("SELECT signal_type, category, status FROM slice_gate")}
        meta = c.execute("SELECT candidates_tested FROM slice_gate_meta").fetchone()
        c.close()
        assert rows[("bad_sig", "entertainment")] == "killed"
        assert rows[("meh_sig", "sports")] == "demoted"
        assert ("good_sig", "politics") not in rows
        assert meta[0] == 3  # G2 denominator recorded
        # reader sees it fresh
        assert smp.get_slice_gate("bad_sig", "entertainment",
                                  db_path=path) == ("killed", True)
    finally:
        os.unlink(path)


def test_gate_deletes_stake_boost_of_demoted_slice(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        monkeypatch.setenv("DB_BACKEND", "sqlite")
        from trading_platform.polymarket import db_connection as dbc
        monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
        monkeypatch.setattr(smp, "_GATE_CACHE",
                            {"at": 0.0, "rows": {}, "fresh": False})
        _seed_paper(path, [("meh_sig", "sports", 25, 10, 0.55, 2.0)])
        # pre-existing stale BOOST row for the now-negative slice
        c = sqlite3.connect(path)
        c.execute("""CREATE TABLE stake_multiplier_overrides (
                       signal_type TEXT, subdomain TEXT, multiplier REAL,
                       n_resolved INT, wr REAL, pnl REAL, promoted_at INT,
                       expires_at INT, PRIMARY KEY (signal_type, subdomain))""")
        c.execute("INSERT INTO stake_multiplier_overrides VALUES "
                  "('meh_sig','sports',1.5,30,0.6,50,1,NULL)")
        c.commit(); c.close()
        smp.run_promoter(db_path=path)
        c = sqlite3.connect(path)
        n = c.execute("SELECT COUNT(*) FROM stake_multiplier_overrides "
                      "WHERE signal_type='meh_sig'").fetchone()[0]
        c.close()
        assert n == 0  # the boost row cannot survive a demotion
    finally:
        os.unlink(path)
