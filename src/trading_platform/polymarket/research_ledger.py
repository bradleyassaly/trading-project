"""Research ledger — the G2/FDR governance substrate (roadmap A1).

Every harness run is RECORDED, pass or fail. Without this, a replay harness
is an industrial p-hacking machine: test 200 predicates, ship the 10 that
look good, and the book fills with survivorship noise wearing a lab coat.

Governance rules enforced here:
  * Hard family cap: registering a NEW hypothesis_id in a family whose
    distinct-hypothesis count is already at FDR_MAX_FAMILY raises — the
    family budget must be chosen BEFORE searching (pre-registration:
    reports/research_fdr_families.md). Re-running an existing id is free.
  * Benjamini-Hochberg across the family: any promotion of a
    harness-discovered predicate must cite a ledger row whose family
    bh_fdr_report q-value is <= 0.10. Procedural, like the mirror-copy
    kill rule — the convention IS the control.

(Table named research_hypotheses; the existing trade_hypotheses table is a
per-trade rationale journal, not a research ledger.)
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

FDR_MAX_FAMILY = int(os.environ.get("FDR_MAX_FAMILY", "20"))
FDR_Q = 0.10

_SCHEMA = """
CREATE TABLE IF NOT EXISTS research_hypotheses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hypothesis_id TEXT NOT NULL,
    family TEXT NOT NULL,
    description TEXT,
    params_json TEXT NOT NULL,
    registered_at INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    window_days INTEGER,
    lag_s INTEGER,
    code_version TEXT,
    n_trades INTEGER,
    n_events INTEGER,
    n_wallets INTEGER,
    ev_net REAL,
    ev_gross REAL,
    ex_top3_ev REAL,
    wr REAL,
    ci_lo REAL,
    ci_hi REAL,
    fold_evs_json TEXT,
    p_shuffle REAL,
    verdict TEXT,
    results_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_rh_family ON research_hypotheses(family, run_at DESC);
CREATE INDEX IF NOT EXISTS idx_rh_hid ON research_hypotheses(hypothesis_id);
"""


def ensure_schema(conn=None, db_path: str | None = None) -> None:
    own = conn is None
    if own:
        conn = get_connection(db_path) if db_path else get_connection()
    try:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                try: conn.execute(stmt)
                except Exception: pass
        conn.commit()
    finally:
        if own:
            try: conn.close()
            except Exception: pass


def family_size(family: str, conn=None, db_path: str | None = None) -> int:
    own = conn is None
    if own:
        conn = get_connection(db_path) if db_path else get_connection()
    try:
        ensure_schema(conn)
        r = conn.execute(
            "SELECT COUNT(DISTINCT hypothesis_id) FROM research_hypotheses "
            "WHERE family = ?", (family,)).fetchone()
        return int(r[0] or 0)
    finally:
        if own:
            try: conn.close()
            except Exception: pass


def register_run(*, hypothesis_id: str, family: str, description: str,
                 params: dict, window_days: int, lag_s: int,
                 results: dict, verdict: str,
                 code_version: str = "", db_path: str | None = None) -> int:
    """Record one run. Raises RuntimeError when a NEW hypothesis_id would
    exceed the family's pre-registered budget (the G2 hard cap)."""
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        ensure_schema(conn)
        exists = conn.execute(
            "SELECT 1 FROM research_hypotheses WHERE hypothesis_id = ? LIMIT 1",
            (hypothesis_id,)).fetchone()
        if not exists and family_size(family, conn=conn) >= FDR_MAX_FAMILY:
            raise RuntimeError(
                f"FDR family '{family}' is at its budget "
                f"({FDR_MAX_FAMILY} distinct hypotheses). Pre-register a new "
                f"family (reports/research_fdr_families.md) before searching "
                f"further — this cap is the point of G2.")
        now = int(time.time())
        cur = conn.execute(
            """INSERT INTO research_hypotheses
                 (hypothesis_id, family, description, params_json,
                  registered_at, run_at, window_days, lag_s, code_version,
                  n_trades, n_events, n_wallets, ev_net, ev_gross,
                  ex_top3_ev, wr, ci_lo, ci_hi, fold_evs_json, p_shuffle,
                  verdict, results_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               RETURNING id""",
            (hypothesis_id, family, description,
             json.dumps(params, default=str), now, now, window_days, lag_s,
             code_version,
             results.get("n_trades"), results.get("n_events"),
             results.get("n_wallets"), results.get("ev_net"),
             results.get("ev_gross"), results.get("ex_top3_ev"),
             results.get("wr"), results.get("ci_lo"), results.get("ci_hi"),
             json.dumps(results.get("fold_evs") or []),
             results.get("p_shuffle"), verdict,
             json.dumps(results, default=str)[:4000]),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row[0]) if row else -1
    finally:
        try: conn.close()
        except Exception: pass


def bh_fdr_report(family: str, alpha: float = FDR_Q,
                  db_path: str | None = None) -> dict[str, Any]:
    """Benjamini-Hochberg over the LATEST run of each hypothesis in the
    family. p = p_shuffle; fallback normal-approx from the event-clustered
    CI when a run predates the shuffle test."""
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        ensure_schema(conn)
        rows = conn.execute(
            """SELECT hypothesis_id, ev_net, ci_lo, ci_hi, p_shuffle, verdict
                 FROM research_hypotheses rh
                WHERE family = ? AND run_at = (
                      SELECT MAX(run_at) FROM research_hypotheses
                       WHERE hypothesis_id = rh.hypothesis_id)""",
            (family,)).fetchall()
    finally:
        try: conn.close()
        except Exception: pass
    entries = []
    for hid, ev, lo, hi, p, verdict in rows:
        if p is None:
            # normal approx: sd from the 95% CI half-width
            if ev is None or lo is None or hi is None or hi <= lo:
                continue
            sd = (hi - lo) / 3.92
            z = ev / sd if sd > 0 else 0.0
            p = 0.5 * math.erfc(z / math.sqrt(2))
        entries.append({"hypothesis_id": hid, "ev_net": ev,
                        "p": float(p), "verdict": verdict})
    entries.sort(key=lambda e: e["p"])
    m = len(entries)
    passing_cut = 0
    for i, e in enumerate(entries, start=1):
        if e["p"] <= alpha * i / m:
            passing_cut = i
    for i, e in enumerate(entries, start=1):
        e["q_rank"] = i
        e["bh_threshold"] = round(alpha * i / m, 5) if m else None
        e["fdr_pass"] = i <= passing_cut
    return {"family": family, "alpha": alpha, "tested": m,
            "fdr_passing": passing_cut, "entries": entries}
