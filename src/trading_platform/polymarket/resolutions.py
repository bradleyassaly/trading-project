"""Canonical market resolutions (roadmap P1).

Resolution truth was scattered across 6 disagreeing paths (rolling-window
CSV rewritten daily, per-condition UMA checks whose results were thrown
into that CSV, wallet_trades outcomes with 6,619 internally-contradictory
condition_ids, our own data-api positions, on-chain dust triggers, and a
frozen legacy CSV). This module is the ONE table they all feed, with
monotonic write-once-wins precedence: a higher-confidence source can
upgrade a row; a lower-confidence source can never downgrade it; an
equal-or-higher-rank DISAGREEMENT is logged to market_resolution_conflicts
and does not overwrite.

resolution_decay — the one live edge — lives or dies on near-resolution
truth; this table is its ground.

Canonical price space: payout_yes is the YES-token payout (1.0/0.0).
resolve_token() answers "did the token I hold pay out?" for either token,
replacing ResolutionResolver's inverted condition_id fallback.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)

# Precedence. manual: operator correction, never auto-overwritten.
# uma_gamma: Gamma with closed AND umaResolutionStatus=='resolved' and
#   clamped 0/1 prices — the highest-quality automated source.
# data_api_positions: Polymarket's own settled numbers for wallets we track
#   (redeemable + curPrice in {0,1}), token role resolved via markets table.
# gamma_bulk: the >=0.95 outcomePrices heuristic WITHOUT a UMA check.
# wallet_trades_vote: bootstrap import from unanimous wallet_trades rows.
# csv_legacy: rows imported from historical CSVs, provenance unknown.
SOURCE_RANK = {
    "manual": 100,
    "uma_gamma": 90,
    # chain_condition_resolution: the ConditionalTokens ConditionResolution
    # event decoded live off the wallet-stream WS (wallet_stream.py) —
    # payoutNumerators straight from the oracle's reportPayouts(), zero
    # inference, immune to Gamma delisting. YES side mapped via
    # markets.yes_token_id (= clobTokenIds[0] ↔ outcome slot 0; order
    # validated empirically against high-rank rows 2026-07-28, see
    # scripts/validate_condition_resolutions.py). Above clob_winner — it IS
    # the report the CLOB winner flags derive from — but below uma_gamma
    # until live coexistence proves it (initial validation in the script).
    "chain_condition_resolution": 85,
    # clob_winner: CLOB /markets/{cid} per-token winner flags. Gamma DELISTS a
    # market once it resolves, so uma_gamma/gamma_bulk miss it (~50% of concluded
    # markets we traded had no row here); the CLOB keeps the settled market with
    # winner=True/False long after. Ranks above the gamma_bulk >=0.95 heuristic
    # and data_api_positions (settled token truth, not a price heuristic), below
    # uma_gamma's explicit UMA-resolved check.
    "clob_winner": 80,
    "data_api_positions": 70,
    # chain_redeem: winner inferred from on-chain PayoutRedemption cash flow
    # (wallet_ctf_events) matched against each redeemer's net token position
    # (redeem pays $1 per winning share, so redeemed USDC ≈ winning-side net
    # shares). The CASH is ground truth; the side attribution is an
    # inference, hence below the direct flag reads (clob_winner,
    # data_api_positions) and above the gamma_bulk price heuristic.
    # Validated 2026-07-28 against existing high-rank rows before enabling
    # (see chain_resolutions.py --validate). Immune to Gamma delisting.
    "chain_redeem": 65,
    "gamma_bulk": 60,
    "wallet_trades_vote": 30,
    "csv_legacy": 20,
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_resolutions (
    condition_id    TEXT PRIMARY KEY,
    resolves_yes    INTEGER,
    payout_yes      REAL,
    winning_outcome TEXT,
    yes_token_id    TEXT,
    no_token_id     TEXT,
    source          TEXT NOT NULL,
    source_rank     INTEGER NOT NULL,
    resolved_at     INTEGER,
    recorded_at     INTEGER NOT NULL,
    question        TEXT,
    details_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_mr_resolved_at ON market_resolutions(resolved_at);
CREATE TABLE IF NOT EXISTS market_resolution_conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    existing_source TEXT,
    existing_resolves_yes INTEGER,
    new_source TEXT,
    new_resolves_yes INTEGER,
    recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mrc_cid ON market_resolution_conflicts(condition_id);
"""


_SCHEMA_READY: set[str] = set()


def ensure_schema(db_path: str | None = None) -> None:
    key = str(db_path or "__default__")
    if key in _SCHEMA_READY:
        return
    with db(db_path) as c:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                c.execute(stmt)
    _SCHEMA_READY.add(key)


def record_resolution(
    condition_id: str,
    *,
    source: str,
    resolves_yes: int | None = None,
    payout_yes: float | None = None,
    winning_outcome: str | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
    resolved_at: int | None = None,
    question: str | None = None,
    details: dict | None = None,
    db_path: str | None = None,
) -> str:
    """Single writer. Returns 'inserted' | 'upgraded' | 'kept' | 'conflict'.

    Monotonic: only a strictly higher source_rank overwrites. An equal-or-
    higher-rank row with a DIFFERENT resolves_yes logs a conflict row and
    keeps the existing value (write-once-wins).
    """
    rank = SOURCE_RANK.get(source)
    if rank is None:
        raise ValueError(f"unknown resolution source: {source!r}")
    cid = (condition_id or "").lower()
    if not cid:
        return "kept"
    ensure_schema(db_path)
    if payout_yes is None and resolves_yes is not None:
        payout_yes = 1.0 if resolves_yes else 0.0
    now = int(time.time())
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        existing = conn.execute(
            "SELECT source, source_rank, resolves_yes FROM market_resolutions "
            "WHERE condition_id = ?",
            (cid,),
        ).fetchone()
        if existing is not None:
            ex_source, ex_rank, ex_yes = existing[0], int(existing[1] or 0), existing[2]
            if rank <= ex_rank:
                if (resolves_yes is not None and ex_yes is not None
                        and int(resolves_yes) != int(ex_yes)):
                    conn.execute(
                        """INSERT INTO market_resolution_conflicts
                             (condition_id, existing_source, existing_resolves_yes,
                              new_source, new_resolves_yes, recorded_at)
                           VALUES (?,?,?,?,?,?)""",
                        (cid, ex_source, ex_yes, source,
                         int(resolves_yes), now),
                    )
                    conn.commit()
                    return "conflict"
                return "kept"
        conn.execute(
            """INSERT INTO market_resolutions
                 (condition_id, resolves_yes, payout_yes, winning_outcome,
                  yes_token_id, no_token_id, source, source_rank,
                  resolved_at, recorded_at, question, details_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT (condition_id) DO UPDATE SET
                 resolves_yes = EXCLUDED.resolves_yes,
                 payout_yes = EXCLUDED.payout_yes,
                 winning_outcome = EXCLUDED.winning_outcome,
                 yes_token_id = COALESCE(EXCLUDED.yes_token_id, market_resolutions.yes_token_id),
                 no_token_id = COALESCE(EXCLUDED.no_token_id, market_resolutions.no_token_id),
                 source = EXCLUDED.source,
                 source_rank = EXCLUDED.source_rank,
                 resolved_at = COALESCE(EXCLUDED.resolved_at, market_resolutions.resolved_at),
                 recorded_at = EXCLUDED.recorded_at,
                 question = COALESCE(EXCLUDED.question, market_resolutions.question),
                 details_json = EXCLUDED.details_json
               WHERE EXCLUDED.source_rank > market_resolutions.source_rank""",
            (cid,
             int(resolves_yes) if resolves_yes is not None else None,
             payout_yes, winning_outcome, yes_token_id, no_token_id,
             source, rank, resolved_at, now,
             (question or "")[:300] or None,
             json.dumps(details, default=str)[:1000] if details else None),
        )
        conn.commit()
        return "upgraded" if existing is not None else "inserted"
    finally:
        try: conn.close()
        except Exception: pass


def get_resolution(condition_id: str, db_path: str | None = None) -> dict | None:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        r = conn.execute(
            """SELECT condition_id, resolves_yes, payout_yes, winning_outcome,
                      yes_token_id, no_token_id, source, source_rank, resolved_at
                 FROM market_resolutions WHERE condition_id = ?""",
            ((condition_id or "").lower(),),
        ).fetchone()
    finally:
        try: conn.close()
        except Exception: pass
    if not r:
        return None
    return {"condition_id": r[0], "resolves_yes": r[1], "payout_yes": r[2],
            "winning_outcome": r[3], "yes_token_id": r[4], "no_token_id": r[5],
            "source": r[6], "source_rank": r[7], "resolved_at": r[8]}


def get_resolutions_bulk(condition_ids: list[str],
                         db_path: str | None = None) -> dict[str, dict]:
    """Batch lookup for resolver Pass 0 — one query, no HTTP."""
    if not condition_ids:
        return {}
    cids = [(c or "").lower() for c in condition_ids]
    out: dict[str, dict] = {}
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        # chunk to keep parameter lists sane
        for i in range(0, len(cids), 500):
            chunk = cids[i:i + 500]
            ph = ",".join("?" for _ in chunk)
            for r in conn.execute(
                    f"""SELECT condition_id, resolves_yes, payout_yes, source,
                               source_rank, resolved_at
                          FROM market_resolutions
                         WHERE condition_id IN ({ph})""", chunk).fetchall():
                out[r[0]] = {"condition_id": r[0], "resolves_yes": r[1],
                             "payout_yes": r[2], "source": r[3],
                             "source_rank": r[4], "resolved_at": r[5]}
    finally:
        try: conn.close()
        except Exception: pass
    return out


def resolve_token(token_id: str, db_path: str | None = None) -> bool | None:
    """Did the token pay out 1.0? None if unknown/non-binary.

    Replaces ResolutionResolver's condition_id fallback, whose last-row-wins
    CSV read returned the NO-token payout for YES-token queries.
    """
    tid = str(token_id)
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        r = conn.execute(
            """SELECT resolves_yes, yes_token_id, no_token_id
                 FROM market_resolutions
                WHERE yes_token_id = ? OR no_token_id = ? LIMIT 1""",
            (tid, tid),
        ).fetchone()
    finally:
        try: conn.close()
        except Exception: pass
    if not r or r[0] is None:
        return None
    resolves_yes, yes_tok, no_tok = int(r[0]), r[1], r[2]
    if str(yes_tok) == tid:
        return bool(resolves_yes)
    if str(no_tok) == tid:
        return not bool(resolves_yes)
    return None


def stats(db_path: str | None = None) -> dict[str, Any]:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM market_resolutions").fetchone()[0]
        by_source = dict(conn.execute(
            "SELECT source, COUNT(*) FROM market_resolutions GROUP BY source"
        ).fetchall())
        conflicts = conn.execute(
            "SELECT COUNT(*) FROM market_resolution_conflicts").fetchone()[0]
        return {"total": int(total or 0), "by_source": by_source,
                "conflicts": int(conflicts or 0)}
    finally:
        try: conn.close()
        except Exception: pass
