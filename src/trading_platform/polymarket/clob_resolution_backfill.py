"""Backfill canonical resolution truth from CLOB winner flags.

Post-mortem 2026-07-14: Gamma delists a market once it resolves, so the
uma_gamma / gamma_bulk ingestion paths never capture it — ~50% of concluded
markets we traded had no row in market_resolutions, starving every exit-policy
counterfactual and net-of-cost EV audit that keys on resolution truth.

The CLOB endpoint https://clob.polymarket.com/markets/{condition_id} keeps the
settled market with per-token `winner` flags long after Gamma delists it, so it
is the durable source of resolution truth. We record it under source
'clob_winner' (rank 80, see resolutions.SOURCE_RANK). record_resolution is
monotonic write-once-wins, so re-runs are idempotent and never downgrade a
higher-rank (uma_gamma / manual) row.

Standalone + scheduled (task 'clob_resolution_backfill'). The parse step is
pure and unit-tested; the fetch/backfill layer needs network + the primary DB.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.resolutions import (
    get_resolutions_bulk,
    record_resolution,
)

logger = logging.getLogger(__name__)

_CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{cid}"


# ── Pure parse (unit-tested) ────────────────────────────────────────────────

def parse_clob_resolution(market: dict | None) -> dict | None:
    """CLOB /markets payload -> resolution kwargs, or None if not cleanly settled.

    Requires closed=True, a Yes and a No token, and EXACTLY one winner token
    (a clean binary settlement). Returns kwargs for record_resolution:
    resolves_yes, payout_yes, winning_outcome, yes_token_id, no_token_id, question.
    """
    if not market or not market.get("closed"):
        return None
    tokens = market.get("tokens") or []
    yes_tok = no_tok = None
    for t in tokens:
        outcome = (t.get("outcome") or "").strip().lower()
        if outcome == "yes":
            yes_tok = t
        elif outcome == "no":
            no_tok = t
    if not yes_tok or not no_tok:
        return None
    winners = [t for t in tokens if t.get("winner") is True]
    if len(winners) != 1:
        return None  # unsettled or ambiguous (0 or 2 winners)
    resolves_yes = 1 if yes_tok.get("winner") is True else 0

    def _tok(t):
        tid = t.get("token_id")
        return str(tid) if tid not in (None, "") else None

    return {
        "resolves_yes": resolves_yes,
        "payout_yes": 1.0 if resolves_yes else 0.0,
        "winning_outcome": "Yes" if resolves_yes else "No",
        "yes_token_id": _tok(yes_tok),
        "no_token_id": _tok(no_tok),
        "question": market.get("question"),
    }


# ── Fetch + record (network / DB) ───────────────────────────────────────────

def fetch_clob_market(condition_id: str, timeout: float = 12.0) -> dict | None:
    try:
        req = urllib.request.Request(
            _CLOB_MARKET_URL.format(cid=condition_id),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as exc:  # noqa: BLE001 — a delisted/unknown cid 404s; skip it
        logger.debug("clob market fetch failed for %s: %s", condition_id[:12], exc)
        return None


def record_clob_resolution(condition_id: str, db_path: str | None = None) -> str:
    """Fetch CLOB winner flags for one market and record it. Returns the
    record_resolution status ('inserted'|'upgraded'|'kept'|'conflict') or
    'skip' when the market isn't cleanly settled / not fetchable."""
    parsed = parse_clob_resolution(fetch_clob_market(condition_id))
    if not parsed:
        return "skip"
    return record_resolution(
        condition_id, source="clob_winner", details={"src": "clob_markets"},
        db_path=db_path, **parsed,
    )


def discover_missing(conn, limit: int | None = None) -> list[str]:
    """Concluded condition_ids we traded/evaluated that are ABSENT from
    market_resolutions — the coverage gap this backfill closes."""
    from datetime import datetime, timezone
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    cids: list[str] = []
    seen: set[str] = set()
    queries = [
        # markets we actually resolved a live trade on
        ("SELECT DISTINCT condition_id FROM live_trades "
         "WHERE realized_pnl IS NOT NULL AND condition_id IS NOT NULL", ()),
        # signal outcomes that resolved
        ("SELECT DISTINCT condition_id FROM signal_outcomes "
         "WHERE resolved_at IS NOT NULL AND condition_id IS NOT NULL", ()),
        # 2026-07-28: OPEN paper positions whose market already ended.
        # These were invisible to the two queries above (no live trade, no
        # resolved outcome yet) — and Gamma delists concluded markets, so
        # NOTHING ever resolved them: 311/313 open paper positions sat on
        # ended markets (309 with no resolution row), paper_resolutions
        # swept "checked 313 / resolved 0" for days, the 300-position
        # paper cap jammed, and smoke_tests' paper_position_cap failed
        # 18x with new paper signals — the slice gate's evidence stream —
        # about to be blocked. Paper books are first-class backfill
        # citizens now.
        ("SELECT DISTINCT pt.condition_id FROM polymarket_paper_trades pt "
         "JOIN markets m ON m.condition_id = pt.condition_id "
         "WHERE pt.exit_ts IS NULL AND pt.archived = 0 "
         "AND pt.condition_id IS NOT NULL AND m.end_date_iso < ?", (today,)),
    ]
    for q, params in queries:
        try:
            for r in conn.execute(q, params).fetchall():
                if r and r[0] and r[0] not in seen:
                    seen.add(r[0])
                    cids.append(r[0])
        except Exception as exc:
            logger.debug("discover query skipped (%s): %s", q[:40], exc)
    # keep only those not already in market_resolutions
    have = get_resolutions_bulk(cids)
    missing = [c for c in cids if (c or "").lower() not in have]
    return missing[:limit] if limit else missing


def backfill(limit: int | None = 500, sleep_s: float = 0.1) -> dict:
    """Discover missing concluded markets and record their CLOB resolutions."""
    conn = get_connection()
    try:
        missing = discover_missing(conn, limit=limit)
    finally:
        try: conn.close()
        except Exception: pass
    logger.info("[clob_backfill] %d concluded cids missing from market_resolutions",
                len(missing))
    counts: dict[str, int] = {}
    for cid in missing:
        status = record_clob_resolution(cid)
        counts[status] = counts.get(status, 0) + 1
        if sleep_s:
            time.sleep(sleep_s)  # be polite to the CLOB
    logger.info("[clob_backfill] done: %s", counts)
    return {"missing": len(missing), "counts": counts}


def main() -> int:
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description="Backfill market_resolutions from CLOB")
    p.add_argument("--cid", help="record a single condition_id (validation)")
    p.add_argument("--limit", type=int, default=500)
    args = p.parse_args()
    if args.cid:
        print(record_clob_resolution(args.cid))
    else:
        print(json.dumps(backfill(limit=args.limit), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
