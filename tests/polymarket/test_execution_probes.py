"""2026-07-09: bounded $1 execution-probe program.

Probes exist to populate the P6 execution dataset (fill rate, spread,
slippage) — data paper cannot produce — NOT to learn outcome EV (paper +
harness do that at 1000x speed). Bounds under test: uncertainty band only,
daily budget, min interval, flat $1 stake (never Kelly/multiplier-lifted),
is_probe tag, and exclusion from every EV evidence stream.
"""
import inspect
import os
import sqlite3
import tempfile
import time

import pytest

from trading_platform.polymarket import polymarket_live_executor as ple
from trading_platform.polymarket.polymarket_live_executor import (
    PolymarketLiveExecutor,
)


def _ex(path, probes_on=True, monkeypatch=None):
    ex = object.__new__(PolymarketLiveExecutor)
    ex._db_path = path
    if monkeypatch is not None:
        monkeypatch.setattr(ple, "EXPLORATION_PROBES", probes_on)
    return ex


@pytest.fixture()
def pdb(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    c = sqlite3.connect(path)
    c.execute("""CREATE TABLE live_trades (
                   id INTEGER PRIMARY KEY, dry_run INT, is_probe INT,
                   status TEXT, size_usd REAL, attempted_at INT)""")
    c.commit(); c.close()
    yield path
    os.unlink(path)


def _seed_probe(path, size, ago_s, status="matched"):
    c = sqlite3.connect(path)
    c.execute("INSERT INTO live_trades (dry_run, is_probe, status, size_usd, "
              "attempted_at) VALUES (0,1,?,?,?)",
              (status, size, int(time.time()) - ago_s))
    c.commit(); c.close()


def test_probe_fires_in_uncertainty_band(pdb, monkeypatch):
    ex = _ex(pdb, monkeypatch=monkeypatch)
    sig = {}
    # unmeasured cell (decay_p None) → eligible
    assert ex._maybe_probe(sig, 0.15, None) is True
    # near-boundary (within PROBE_BAND=0.05) → eligible
    assert ex._maybe_probe(sig, 0.15, 0.12) is True


def test_probe_refuses_confident_negative(pdb, monkeypatch):
    ex = _ex(pdb, monkeypatch=monkeypatch)
    # decay_p far below price: measured -EV, not uncertainty → no probe
    assert ex._maybe_probe({}, 0.30, 0.10) is False


def test_probe_respects_daily_budget(pdb, monkeypatch):
    ex = _ex(pdb, monkeypatch=monkeypatch)
    for _ in range(5):  # $5 budget consumed today
        _seed_probe(pdb, 1.0, ago_s=7200)
    assert ex._maybe_probe({}, 0.15, None) is False


def test_probe_respects_min_interval(pdb, monkeypatch):
    ex = _ex(pdb, monkeypatch=monkeypatch)
    _seed_probe(pdb, 1.0, ago_s=600)  # 10 min ago < 1h interval
    assert ex._maybe_probe({}, 0.15, None) is False
    # but a probe from 2h ago does not block
    c = sqlite3.connect(pdb); c.execute("DELETE FROM live_trades"); c.commit(); c.close()
    _seed_probe(pdb, 1.0, ago_s=7200)
    assert ex._maybe_probe({}, 0.15, None) is True


def test_blocked_probes_do_not_consume_budget(pdb, monkeypatch):
    ex = _ex(pdb, monkeypatch=monkeypatch)
    for _ in range(10):  # blocked rows are free
        _seed_probe(pdb, 1.0, ago_s=7200, status="blocked")
    assert ex._maybe_probe({}, 0.15, None) is True


def test_flag_off_no_probe(pdb, monkeypatch):
    ex = _ex(pdb, probes_on=False, monkeypatch=monkeypatch)
    assert ex._maybe_probe({}, 0.15, None) is False


def test_db_error_fails_safe(monkeypatch):
    ex = _ex("/nonexistent/nope.db", monkeypatch=monkeypatch)
    assert ex._maybe_probe({}, 0.15, None) is False


# ---------------------------------------------------------------------------
# Wiring: probes bypass ONLY the two EV gates; stake pinned; streams excluded
# ---------------------------------------------------------------------------

def test_wiring_invariants():
    src = inspect.getsource(ple)
    # probe escape exists at both EV-gate block sites, plus the slice-gate
    # demote conversion (2026-07-27: a demoted slice converts EV-PASSING
    # entries to $1 probes so re-measurement runs on the slice's best
    # candidates, not only its EV-gate rejects)
    assert src.count('signal["is_probe"] = 1') == 3
    # probe stake pinned, never Kelly-lifted or multiplier-scaled
    assert "size_usd = PROBE_STAKE_USD" in src
    assert 'not signal.get("is_probe")' in src  # kelly stash guard
    assert '_mult_fresh and not signal.get("is_probe")' in src
    # recorded
    assert '1 if signal.get("is_probe") else 0' in src
    assert '"is_probe INTEGER"' in src


def test_probe_waives_performance_ks_blocks_only():
    """2026-07-09: the KS sits downstream of probe tagging, so a signal-
    level WR halt silently starved the whole probe program (709 eligible,
    0 fired). Probes now waive performance-class KS blocks (EV/WR) only;
    safety-class blocks (ENV/EMERGENCY/DISABLED/MIN_RESOLVED/UNKNOWN)
    must still bind."""
    src = inspect.getsource(ple)
    # the waiver exists and is scoped to probe-tagged signals + EV/WR codes
    assert 'signal.get("is_probe") and code in ("EV", "WR")' in src
    # no wider waiver ever sneaks in
    assert '"EMERGENCY")' not in src.split('code in ("EV", "WR")')[1][:200]
    # non-probe path still records the block (else-branch preserved)
    waiver_idx = src.index('signal.get("is_probe") and code in ("EV", "WR")')
    tail = src[waiver_idx:waiver_idx + 1500]
    assert 'status="blocked"' in tail


def test_evidence_streams_exclude_probes():
    from trading_platform.polymarket import regime_monitor as rm
    from trading_platform.polymarket import exit_counterfactual as xc
    assert "COALESCE(lt.is_probe, 0) = 0" in inspect.getsource(rm)
    assert "COALESCE(lt.is_probe, 0) = 0" in inspect.getsource(xc)
    src = open("scripts/update_sizing_multipliers.py", encoding="utf-8").read()
    assert "COALESCE(is_probe, 0) = 0" in src
