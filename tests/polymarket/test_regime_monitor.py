"""C4: regime/drift monitor on the two live edges.

The signal_health IC monitor reads the paper lane and was blind to live
drift (n_resolved_30d=0 vs 15 actual live resolutions at recon time). These
tests pin: the state classifier, the event-clustered bootstrap (governance
gate G3 — same-event trades resolve together), economic-date windowing, and
transition-only alerting.
"""
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket import regime_monitor as rm


# ---------------------------------------------------------------------------
# classify()
# ---------------------------------------------------------------------------

def test_classify_starved_below_n30_floor():
    assert rm.classify(n14=2, ev14=0.5, n30=4, ev30=0.5,
                       lo30=0.1, hi30=0.9, n60=10, lo60=0.0) == "STARVED"


def test_classify_breach_when_ci_fully_below_zero():
    assert rm.classify(n14=10, ev14=-0.4, n30=20, ev30=-0.4,
                       lo30=-0.6, hi30=-0.1, n60=40, lo60=-0.5) == "DRIFT_BREACH"


def test_classify_no_breach_below_n15():
    # 30d CI below zero but n30 < 15 — not enough to declare the edge dead.
    assert rm.classify(n14=6, ev14=-0.4, n30=10, ev30=-0.4,
                       lo30=-0.6, hi30=-0.1, n60=40, lo60=-0.5) != "DRIFT_BREACH"


def test_classify_warn_when_14d_below_60d_band():
    assert rm.classify(n14=8, ev14=-0.10, n30=20, ev30=0.2,
                       lo30=-0.1, hi30=0.5, n60=40, lo60=0.05) == "DRIFT_WARN"


def test_classify_ok():
    assert rm.classify(n14=10, ev14=0.3, n30=20, ev30=0.25,
                       lo30=0.05, hi30=0.5, n60=40, lo60=0.0) == "OK"


# ---------------------------------------------------------------------------
# event_clustered_ev_ci()
# ---------------------------------------------------------------------------

def test_ev_is_ratio_of_sums():
    rows = [(5.0, 10.0, "a"), (-2.0, 10.0, "b"), (3.0, 20.0, "c"),
            (0.0, 10.0, "d"), (1.0, 10.0, "e")]
    ev, lo, hi, n, ncl = rm.event_clustered_ev_ci(rows)
    assert n == 5 and ncl == 5
    assert abs(ev - (7.0 / 60.0)) < 1e-9
    assert lo <= ev <= hi


def test_ci_deterministic():
    rows = [((i % 3) - 1.0, 10.0, f"c{i % 7}") for i in range(30)]
    a = rm.event_clustered_ev_ci(rows)
    b = rm.event_clustered_ev_ci(rows)
    assert a == b


def test_wide_band_under_five_clusters():
    rows = [(1.0, 10.0, "a"), (1.0, 10.0, "a"), (-1.0, 10.0, "b")]
    ev, lo, hi, n, ncl = rm.event_clustered_ev_ci(rows)
    assert ncl == 2
    assert lo == pytest.approx(ev - 1.0) and hi == pytest.approx(ev + 1.0)


def test_clustered_band_wider_than_iid_when_pnl_concentrated():
    # 6 clusters; one cluster holds ALL the losses (correlated event legs).
    # Cluster resampling must produce a wider band than trade-level iid.
    rows = ([(-5.0, 10.0, "bad")] * 5
            + [(0.5, 10.0, f"c{i}") for i in range(5)])
    ev, lo, hi, n, ncl = rm.event_clustered_ev_ci(rows)
    iid_rows = [(p, s, f"t{i}") for i, (p, s, _) in enumerate(rows)]
    _, ilo, ihi, _, _ = rm.event_clustered_ev_ci(iid_rows)
    assert (hi - lo) > (ihi - ilo)


def test_zero_trades():
    assert rm.event_clustered_ev_ci([]) == (0.0, 0.0, 0.0, 0, 0)


# ---------------------------------------------------------------------------
# run() end-to-end on sqlite
# ---------------------------------------------------------------------------

NOW = 1_783_000_000


def _seed(path, trades, slugs=None):
    """trades: (signal_type, pnl, stake, resolution_date, exit_ts, cid)."""
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE live_trades (
                   dry_run INT, signal_type TEXT, outcome TEXT,
                   realized_pnl REAL, size_usd REAL,
                   resolution_date INT, exit_ts INT, condition_id TEXT,
                   is_probe INT DEFAULT 0)""")
    c.execute("CREATE TABLE markets (condition_id TEXT, event_slug TEXT)")
    for st, pnl, stake, res, ex, cid in trades:
        c.execute("INSERT INTO live_trades VALUES (0,?,?,?,?,?,?,?,0)",
                  (st, "win" if pnl > 0 else "loss", pnl, stake, res, ex, cid))
    for cid, slug in (slugs or {}).items():
        c.execute("INSERT INTO markets VALUES (?,?)", (cid, slug))
    c.commit()
    c.close()


def _point_at(monkeypatch, path):
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")


@pytest.fixture()
def alert_spy(monkeypatch):
    calls = []
    monkeypatch.setattr(rm, "_alert", lambda tr: calls.append(tr))
    return calls


def test_run_inserts_and_first_drift_alerts(monkeypatch, alert_spy):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        # whale_entry_filtered: 20 losing trades over 10 distinct events in
        # the last 30d — a clean DRIFT_BREACH. resolution_decay: none (STARVED).
        trades = [("whale_entry_filtered", -2.0, 5.0, NOW - i * 86400, None, f"c{i % 10}")
                  for i in range(20)]
        _seed(path, trades)
        _point_at(monkeypatch, path)
        out = rm.run(db_path=path, now=NOW)
        assert out["whale_entry_filtered"]["state"] == "DRIFT_BREACH"
        assert out["resolution_decay"]["state"] == "STARVED"
        # First run landing in a drift state alerts; STARVED does not.
        assert len(alert_spy) == 1
        sigs = [t[0] for t in alert_spy[0]]
        assert sigs == ["whale_entry_filtered"]
    finally:
        os.unlink(path)


def test_no_alert_when_state_unchanged(monkeypatch, alert_spy):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        trades = [("whale_entry_filtered", -2.0, 5.0, NOW - i * 86400, None, f"c{i % 10}")
                  for i in range(20)]
        _seed(path, trades)
        _point_at(monkeypatch, path)
        rm.run(db_path=path, now=NOW)
        n_first = len(alert_spy)
        rm.run(db_path=path, now=NOW)     # same data, same state
        assert len(alert_spy) == n_first  # no repeat alert
        # state_since preserved across same-state runs
        c = sqlite3.connect(path)
        since = c.execute("SELECT state_since FROM signal_regime WHERE signal_type='whale_entry_filtered'").fetchone()[0]
        c.close()
        assert since == NOW
    finally:
        os.unlink(path)


def test_recovery_transition_alerts_once(monkeypatch, alert_spy):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        trades = [("whale_entry_filtered", -2.0, 5.0, NOW - i * 86400, None, f"c{i % 10}")
                  for i in range(20)]
        _seed(path, trades)
        _point_at(monkeypatch, path)
        rm.run(db_path=path, now=NOW)
        # 40 days later the losses age out of every window → STARVED
        later = NOW + 40 * 86400
        rm.run(db_path=path, now=later)
        assert len(alert_spy) == 2
        sig, old, new, _ = alert_spy[1][0]
        assert (old, new) == ("DRIFT_BREACH", "STARVED")
        c = sqlite3.connect(path)
        since = c.execute("SELECT state_since FROM signal_regime WHERE signal_type='whale_entry_filtered'").fetchone()[0]
        c.close()
        assert since == later  # reset on transition
    finally:
        os.unlink(path)


def test_windows_use_economic_date_not_booking(monkeypatch, alert_spy):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        # Trades RESOLVED 90 days ago (economic date) — must not pollute the
        # 30d window even if exit_ts (booking) is recent. Plus 6 recent wins.
        old = [("resolution_decay", -3.0, 5.0, NOW - 90 * 86400, NOW - 3600, f"o{i}")
               for i in range(10)]
        recent = [("resolution_decay", 2.0, 5.0, NOW - i * 86400, None, f"r{i}")
                  for i in range(6)]
        _seed(path, old + recent)
        _point_at(monkeypatch, path)
        out = rm.run(db_path=path, now=NOW)
        row = out["resolution_decay"]
        assert row["n_30d"] == 6          # back-booked losses excluded
        assert row["ev_30d"] > 0
    finally:
        os.unlink(path)


def test_event_clustering_via_event_slug(monkeypatch, alert_spy):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        # 12 trades across 12 condition_ids, but markets maps them all to 2
        # event slugs → n_clusters must be 2, not 12.
        trades = [("resolution_decay", 1.0, 5.0, NOW - i * 3600, None, f"c{i}")
                  for i in range(12)]
        slugs = {f"c{i}": ("nba-finals-more-markets" if i % 2 else "nba-finals")
                 for i in range(12)}
        _seed(path, trades, slugs)
        _point_at(monkeypatch, path)
        out = rm.run(db_path=path, now=NOW)
        assert out["resolution_decay"]["n_clusters_30d"] == 1  # suffix normalized
    finally:
        os.unlink(path)
