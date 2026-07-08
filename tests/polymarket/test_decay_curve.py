"""A2: learned decay curves for resolution_decay (champion/challenger).

The hand-coded confidence encodes no market evidence; the fitted lookup
must shrink tiny per-bucket samples toward the parent level, count DISTINCT
markets per cell (G3), and fail back to the formula on any doubt.
"""
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import decay_curve as dc


NOW = int(time.time())


def test_shrink_math():
    # n=0 → prior; huge n → empirical; k midpoint blends.
    assert dc.shrink(0, 0, 0.3, k=20) == pytest.approx(0.3)
    assert dc.shrink(900, 1000, 0.3, k=20) == pytest.approx(900.3 * 0 + (900 + 20 * 0.3) / 1020)
    mid = dc.shrink(10, 20, 0.0, k=20)
    assert 0.2 < mid < 0.3  # (10+0)/(40) = 0.25


def test_buckets():
    assert dc._bucket_hours(2) == "01-03h"
    assert dc._bucket_hours(30) == "24-48h"
    assert dc._bucket_hours(60) is None
    assert dc._bucket_price(0.15) == "0.10-0.20"
    assert dc._bucket_price(0.5) is None


@pytest.fixture()
def ddb(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)
    from trading_platform.polymarket import resolutions as rs
    monkeypatch.setattr(rs, "_SCHEMA_READY", set())
    yield path
    os.unlink(path)


def _seed(path, obs, labels):
    """obs: (cid, fired_at, resolution_date, signal_price, category, subcat).
    labels: {cid: resolves_yes}."""
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE live_trades (
                   condition_id TEXT, signal_type TEXT, signal_fired_at BIGINT,
                   resolution_date BIGINT, signal_price REAL, category TEXT,
                   dry_run INT)""")
    c.execute("CREATE TABLE markets (condition_id TEXT, subcategory TEXT)")
    for cid, fired, res, px, cat, sub in obs:
        c.execute("INSERT INTO live_trades VALUES (?,?,?,?,?,?,1)",
                  (cid, "resolution_decay", fired, res, px, cat))
        c.execute("INSERT OR IGNORE INTO markets VALUES (?,?)", (cid, sub))
    c.commit()
    c.close()
    from trading_platform.polymarket import resolutions as rs
    rs.ensure_schema(path)
    for cid, yes in labels.items():
        rs.record_resolution(cid, source="uma_gamma", resolves_yes=yes,
                             db_path=path)


def test_fit_counts_distinct_markets(ddb):
    # One market firing 5x inside a cell counts ONCE (G3).
    obs = [("c1", NOW - 3600 * 2, NOW, 0.15, "sports", None)] * 5
    obs += [(f"x{i}", NOW - 3600 * 2, NOW, 0.15, "sports", None)
            for i in range(3)]
    _seed(ddb, obs, {"c1": 1, **{f"x{i}": 0 for i in range(3)}})
    out = dc.fit_decay_lookup(db_path=ddb)
    c = sqlite3.connect(ddb)
    n = c.execute("SELECT n_markets FROM decay_curve_lookup WHERE "
                  "slice_key='sports' AND hours_bucket='01-03h' "
                  "AND price_bucket='0.10-0.20'").fetchone()[0]
    c.close()
    assert n == 4  # c1 once + 3 x's


def test_lookup_waterfall_and_floor(ddb):
    # 40 sports markets in one cell (passes the n>=30 floor); a lone
    # 'sports/nba' subcategory with n=1 must NOT be used — falls to category.
    obs = [(f"s{i}", NOW - 3600 * 2, NOW, 0.15, "sports",
            "sports/nba" if i == 0 else None) for i in range(40)]
    _seed(ddb, obs, {f"s{i}": (1 if i < 8 else 0) for i in range(40)})
    dc.fit_decay_lookup(db_path=ddb)
    p = dc.lookup_probability("sports/nba", "sports", 2.0, 0.15, db_path=ddb)
    assert p is not None
    # ~8/40 = 0.20 empirical, shrunk toward the global prior (also 0.20 here)
    assert 0.10 < p < 0.35
    # unknown buckets → None (caller keeps formula)
    assert dc.lookup_probability(None, "sports", 60.0, 0.15, db_path=ddb) is None
    assert dc.lookup_probability(None, "sports", 2.0, 0.60, db_path=ddb) is None


def test_stale_table_returns_none(ddb):
    obs = [(f"s{i}", NOW - 3600 * 2, NOW, 0.15, "sports", None)
           for i in range(35)]
    _seed(ddb, obs, {f"s{i}": 1 for i in range(35)})
    dc.fit_decay_lookup(db_path=ddb)
    c = sqlite3.connect(ddb)
    c.execute("UPDATE decay_curve_lookup SET updated_at = ?",
              (NOW - 8 * 86400,))
    c.commit(); c.close()
    assert dc.lookup_probability(None, "sports", 2.0, 0.15, db_path=ddb) is None


def test_empty_table_returns_none(ddb):
    _seed(ddb, [], {})
    dc.fit_decay_lookup(db_path=ddb)
    assert dc.lookup_probability(None, "sports", 2.0, 0.15, db_path=ddb) is None


# ---------------------------------------------------------------------------
# Signal wiring (champion/challenger semantics)
# ---------------------------------------------------------------------------

def test_confidence_champion_by_default(monkeypatch):
    from trading_platform.polymarket import resolution_decay_signal as rds
    monkeypatch.delenv("DECAY_CURVE_ENFORCE", raising=False)
    monkeypatch.setattr("trading_platform.polymarket.decay_curve.lookup_probability",
                        lambda *a, **k: 0.17)
    conf, lookup = rds._confidence(3.0, 0.15, subcategory=None, category="sports")
    # formula stays champion; the challenger rides along
    assert conf > 0.5
    assert lookup == 0.17


def test_confidence_enforce_flips_to_lookup(monkeypatch):
    from trading_platform.polymarket import resolution_decay_signal as rds
    monkeypatch.setenv("DECAY_CURVE_ENFORCE", "1")
    monkeypatch.setattr("trading_platform.polymarket.decay_curve.lookup_probability",
                        lambda *a, **k: 0.17)
    conf, lookup = rds._confidence(3.0, 0.15, subcategory=None, category="sports")
    assert conf == 0.17
    # fail-safe: lookup None → formula even when enforced
    monkeypatch.setattr("trading_platform.polymarket.decay_curve.lookup_probability",
                        lambda *a, **k: None)
    conf2, lookup2 = rds._confidence(3.0, 0.15)
    assert conf2 > 0.5 and lookup2 is None


def test_market_signals_insert_uses_real_columns():
    import inspect
    from trading_platform.polymarket import resolution_decay_signal as rds
    src = inspect.getsource(rds)
    # the phantom columns that made the insert silently fail forever
    assert "entry_price, confidence, wallet, wallet_tier" not in src
    assert "decay_lookup_p" in src  # challenger rides in the payload


def test_live_decay_veto_is_veto_only_and_failsafe():
    # The veto uses the honest empirical P to BLOCK a -wedge fire but never
    # to authorize/up-size one; fail-safe when decay_lookup_p is absent;
    # LIVE only (paper keeps collecting the full distribution).
    import inspect
    from trading_platform.polymarket import polymarket_live_executor as ple
    src = inspect.getsource(ple)
    assert "LIVE_DECAY_VETO" in src
    assert "DECAY_CURVE_VETO" in src           # flag, default on
    assert 'signal.get("decay_lookup_p")' in src
    # veto only exists inside the resolution_decay BUY branch and returns
    # self._block — it can only reduce trades, never permit new ones
    assert "decay-curve veto" in src
    # it is NOT in the paper executor (paper must keep collecting)
    from trading_platform.polymarket import polymarket_paper_executor as ppe
    assert "LIVE_DECAY_VETO" not in inspect.getsource(ppe)


def test_scan_aperture_widened_to_48h():
    import inspect
    from trading_platform.polymarket import resolution_decay_signal as rds
    src = inspect.getsource(rds)
    # SQL cap now matches MAX_HOURS_TO_RESOLVE=48 (was a 24h starve)
    assert "interval '48 hours'" in src
    assert rds.MAX_HOURS_TO_RESOLVE == 48.0
