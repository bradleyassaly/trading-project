"""Governance state machine for wallet_overrides (auto-demote + recovery).

2026-07-16: the DEMOTED tier was a one-way ratchet — apply_demotions only
ever upserted DEMOTED and nothing anywhere cleared it, so the 07-13
auto-demote of the phase_b_resolution_decay pseudo-wallet silently
blocked all live trading for 3 days. These tests pin the full lifecycle
so it can't regress:

    DEMOTED -> PROBATION (criteria no longer met) -> cleared (breakeven+
    on enough n at probation stake), plus re-demotion out of PROBATION.
"""
from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "wallet_attribution.py"
_spec = importlib.util.spec_from_file_location("wallet_attribution", _SCRIPT)
wa = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wa)


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    yield c
    c.close()


def _attr(wallet: str, n: int, pnl: float) -> dict:
    return {"wallet": wallet, "n": n, "pnl": pnl, "wins": 0, "losses": 0,
            "avg": 0.0, "sd": None, "sharpe": None, "wr": 0.0}


def _tier(conn, wallet: str):
    row = conn.execute(
        "SELECT tier_override FROM wallet_overrides WHERE wallet = ?",
        (wallet,),
    ).fetchone()
    return row[0] if row else None


def test_candidate_gets_demoted_and_stays(conn):
    cand = _attr("phase_b_resolution_decay", 51, -28.72)
    n, newly = wa.apply_demotions(conn, [cand])
    assert n == 1
    assert [w["wallet"] for w in newly] == ["phase_b_resolution_decay"]
    assert _tier(conn, "phase_b_resolution_decay") == "DEMOTED"
    # Still a candidate -> recovery plan must NOT touch it (the current
    # resolution_decay demote is correct; no blind auto-clear).
    plan = wa.plan_recoveries(conn, [cand], [cand])
    assert plan == {"to_probation": [], "to_clear": []}
    # Nightly re-stamp isn't "new" -> no repeat strategy-kill alert.
    n2, newly2 = wa.apply_demotions(conn, [cand])
    assert n2 == 1 and newly2 == []


def test_demoted_flips_to_probation_when_pnl_recovers(conn):
    wa.apply_demotions(conn, [_attr("w1", 10, -20.0)])
    recovered = _attr("w1", 12, -3.0)  # above threshold, enough n
    plan = wa.plan_recoveries(conn, [recovered], [])
    assert [t["wallet"] for t in plan["to_probation"]] == ["w1"]
    assert "recovered" in plan["to_probation"][0]["why"]
    wa.apply_recoveries(conn, plan)
    assert _tier(conn, "w1") == "PROBATION"


def test_demoted_flips_to_probation_when_evidence_expires(conn):
    wa.apply_demotions(conn, [_attr("w2", 8, -15.0)])
    # 30d window slid past all resolved trades -> wallet absent from attr.
    plan = wa.plan_recoveries(conn, [], [])
    assert [t["wallet"] for t in plan["to_probation"]] == ["w2"]
    assert "expired" in plan["to_probation"][0]["why"]


def test_probation_clears_only_at_breakeven_on_enough_n(conn):
    wa.apply_demotions(conn, [_attr("w3", 8, -15.0)])
    wa.apply_recoveries(conn, wa.plan_recoveries(conn, [], []))
    assert _tier(conn, "w3") == "PROBATION"
    # Still slightly negative -> stays capped (no silent full-size return).
    plan = wa.plan_recoveries(conn, [_attr("w3", 6, -3.0)], [])
    assert plan == {"to_probation": [], "to_clear": []}
    # Not enough n -> stays too.
    plan = wa.plan_recoveries(conn, [_attr("w3", 3, 2.0)], [])
    assert plan == {"to_probation": [], "to_clear": []}
    # Breakeven-or-better on n>=5 -> row deleted, full size resumes.
    plan = wa.plan_recoveries(conn, [_attr("w3", 6, 1.5)], [])
    assert [t["wallet"] for t in plan["to_clear"]] == ["w3"]
    wa.apply_recoveries(conn, plan)
    assert _tier(conn, "w3") is None


def test_probation_redemotes_when_criteria_trip_again(conn):
    wa.apply_demotions(conn, [_attr("w4", 8, -15.0)])
    wa.apply_recoveries(conn, wa.plan_recoveries(conn, [], []))
    assert _tier(conn, "w4") == "PROBATION"
    relapse = _attr("w4", 7, -12.0)
    # A candidate again -> recovery must skip it...
    plan = wa.plan_recoveries(conn, [relapse], [relapse])
    assert plan == {"to_probation": [], "to_clear": []}
    # ...and the demote counts as NEW (probation failed -> alert again).
    n, newly = wa.apply_demotions(conn, [relapse])
    assert n == 1
    assert [w["wallet"] for w in newly] == ["w4"]
    assert _tier(conn, "w4") == "DEMOTED"
