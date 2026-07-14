"""Exit-policy counterfactual + per-slice exit overrides (roadmap A3).

For every closed real-money trade sold BEFORE resolution, compare booked
realized_pnl against the hold-to-resolution counterfactual (shares × payout
− cost). The 7/6 origin: in-play football stops took −$8.58 while stopped
markets were still coin-flips. delta = hold − actual; delta > 0 means exits
are LOSING money vs the signal's own hold-to-resolution evidence frame.

The verdict feeds `exit_policy_overrides` — a daily-refreshed lookup table
the exit monitors read (fail-safe-CLOSED: any doubt means stops stay ON).
v1 only ever exempts stop_loss, only at n>=30 and delta > $10, and never
touches kill-switch hard stops, dust settlement, or the fp>=0.90 SELL skip.

Run: python -m trading_platform.polymarket.exit_counterfactual [--apply|--dry-run]
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

PRE_RES_EXITS = ("stop_loss", "take_profit", "trailing_stop",
                 "whale_mirror_exit", "time_decay",
                 "reconciled_dataapi_sell", "market_life_expired")

EXEMPT_MIN_N = 30
EXEMPT_MIN_DELTA_USD = 10.0
OVERRIDE_MAX_AGE_SEC = 7 * 86400

_SCHEMA = """
CREATE TABLE IF NOT EXISTS exit_policy_overrides (
    signal_type    TEXT NOT NULL,
    category_slice TEXT NOT NULL,
    exit_reason    TEXT NOT NULL,
    action         TEXT NOT NULL,
    n              INTEGER NOT NULL,
    actual_pnl     REAL NOT NULL,
    hold_pnl       REAL NOT NULL,
    delta          REAL NOT NULL,
    window_days    INTEGER NOT NULL,
    updated_at     BIGINT NOT NULL,
    PRIMARY KEY (signal_type, category_slice, exit_reason)
);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.split(";"):
        if stmt.strip():
            try: conn.execute(stmt)
            except Exception: pass


def compute_exit_counterfactual(conn, now_ts: int, window_days: int = 60,
                                ) -> tuple[dict[tuple, dict], int]:
    """{(signal_type, slice, exit_reason): {n, actual, hold}}, n_unresolved.

    slice = 'sports' | 'other' (categories are too sparse for finer cuts at
    this book's n). Payout waterfall: canonical market_resolutions (P1)
    → markets snapshot → gamma CSV per-token → wallet_trades outcome.
    """
    cutoff = now_ts - window_days * 86400
    rows = conn.execute(
        """SELECT lt.signal_type, COALESCE(lt.category,'unknown'),
                  lt.direction, lt.exit_reason,
                  COALESCE(lt.fill_price, lt.entry_price), lt.shares,
                  lt.size_usd, lt.realized_pnl, lt.token_id,
                  m.closed, m.outcome_prices, m.no_token_id, m.yes_token_id,
                  (SELECT wt.market_outcome FROM wallet_trades wt
                    WHERE wt.condition_id = lt.condition_id
                      AND wt.market_resolved = 1
                      AND wt.market_outcome IN ('YES','NO')
                    LIMIT 1) AS wt_outcome,
                  lt.condition_id
             FROM live_trades lt
             LEFT JOIN markets m ON m.condition_id = lt.condition_id
            WHERE lt.dry_run = 0 AND lt.realized_pnl IS NOT NULL
              AND lt.exit_ts > ? AND lt.token_id IS NOT NULL
              AND COALESCE(lt.is_probe, 0) = 0
              AND lt.exit_reason IN ({})""".format(
                  ",".join("?" * len(PRE_RES_EXITS))),
        (cutoff, *PRE_RES_EXITS),
    ).fetchall()
    if not rows:
        return {}, 0

    resolver = None
    try:
        from trading_platform.polymarket.resolution_resolver import ResolutionResolver
        _csv = Path("data/polymarket/gamma_resolution.csv")
        resolver = ResolutionResolver(str(_csv))
    except Exception:
        resolver = None

    # Source 0 (P1): canonical table, one bulk read.
    canonical: dict[str, int] = {}
    try:
        from trading_platform.polymarket.resolutions import get_resolutions_bulk
        cids = list({r[14].lower() for r in rows if r[14]})
        for cid, rr in get_resolutions_bulk(cids).items():
            if rr.get("resolves_yes") is not None:
                canonical[cid] = int(rr["resolves_yes"])
    except Exception as exc:
        logger.debug("canonical bulk read failed: %s", exc)

    per_key: dict[tuple, dict] = {}
    unresolved = 0
    for (sig, cat, direction, exit_reason, fp, sh, usd, actual, token_id,
         m_closed, m_prices, m_no_tok, m_yes_tok, wt_outcome, cid) in rows:
        payout = None
        # Source 0: canonical market_resolutions
        rj = canonical.get((cid or "").lower())
        if rj is not None and (m_yes_tok or m_no_tok):
            if m_yes_tok and str(token_id) == str(m_yes_tok):
                payout = 1.0 if rj else 0.0
            elif m_no_tok and str(token_id) == str(m_no_tok):
                payout = 0.0 if rj else 1.0
        # Source 1: markets table snapshot
        if payout is None and m_closed and m_prices:
            try:
                prices = [float(x) for x in json.loads(m_prices)]
                idx = 1 if str(token_id) == str(m_no_tok) else 0
                px = prices[idx]
                if px >= 0.99:
                    payout = 1.0
                elif px <= 0.01:
                    payout = 0.0
            except (ValueError, TypeError, IndexError):
                payout = None
        # Source 2: per-token resolution file
        if payout is None and resolver is not None:
            try:
                price = resolver.resolve(str(token_id))
            except Exception:
                price = None
            if price is not None:
                payout = 1.0 if price >= 99.0 else (0.0 if price <= 1.0 else None)
        # Source 3: wallet_trades market_outcome (needs token role)
        if payout is None and wt_outcome in ("YES", "NO"):
            if m_yes_tok and str(token_id) == str(m_yes_tok):
                payout = 1.0 if wt_outcome == "YES" else 0.0
            elif m_no_tok and str(token_id) == str(m_no_tok):
                payout = 1.0 if wt_outcome == "NO" else 0.0
        if payout is None:
            unresolved += 1
            continue
        fp = float(fp or 0)
        # Direction-aware shares fallback: mirror the cost line's price space so
        # a null-shares SELL row (NO priced 1-fp) doesn't divide the stake by the
        # BUY-space price and understate shares ~ (1-fp)/fp x.
        _is_buy = (direction or "BUY").upper() == "BUY"
        _entry_px = max(fp, 0.01) if _is_buy else max(1.0 - fp, 0.001)
        shares = float(sh or 0) or (float(usd or 0) / _entry_px)
        if shares <= 0 or fp <= 0:
            unresolved += 1
            continue
        cost = shares * (fp if _is_buy else max(1.0 - fp, 0.001))
        hold = shares * payout - cost
        key = (sig, "sports" if (cat or "").lower() == "sports" else "other",
               exit_reason or "unknown")
        agg = per_key.setdefault(key, {"n": 0, "actual": 0.0, "hold": 0.0})
        agg["n"] += 1
        agg["actual"] += float(actual)
        agg["hold"] += hold
    return per_key, unresolved


def update_exit_policy(db_path: str | None = None, dry_run: bool = False,
                       window_days: int = 60) -> dict[str, Any]:
    """Refresh exit_policy_overrides. Full refresh; 'exempt' ONLY for
    stop_loss at n>=EXEMPT_MIN_N and delta > EXEMPT_MIN_DELTA_USD; every
    other evaluated row is written as 'none' for the audit trail."""
    now = int(time.time())
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        per_key, unresolved = compute_exit_counterfactual(conn, now, window_days)
        n_exempt = 0
        rows_out = []
        for (sig, sl, reason), a in per_key.items():
            delta = a["hold"] - a["actual"]
            action = "none"
            if (reason == "stop_loss" and a["n"] >= EXEMPT_MIN_N
                    and delta > EXEMPT_MIN_DELTA_USD):
                action = "exempt"
                n_exempt += 1
            rows_out.append((sig, sl, reason, action, a["n"],
                             round(a["actual"], 2), round(a["hold"], 2),
                             round(delta, 2), window_days, now))
        if not dry_run:
            conn.execute("DELETE FROM exit_policy_overrides")
            for r in rows_out:
                conn.execute(
                    """INSERT INTO exit_policy_overrides
                         (signal_type, category_slice, exit_reason, action,
                          n, actual_pnl, hold_pnl, delta, window_days,
                          updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""", r)
            conn.commit()
        return {"evaluated": len(rows_out), "exempted": n_exempt,
                "unresolved": unresolved, "dry_run": dry_run,
                "rows": sorted(rows_out, key=lambda r: -r[7])[:10]}
    finally:
        try: conn.close()
        except Exception: pass


def stop_loss_exempt(signal_type: str, category: str | None,
                     db_path: str | None = None) -> bool:
    """Fail-safe-CLOSED read: True ONLY when a fresh (7d) override row says
    stop_loss exits on this (signal × slice) lose > $10 vs hold at n>=30.
    Missing table, stale row, any exception → False (stops stay ON)."""
    try:
        sl = "sports" if (category or "").lower() == "sports" else "other"
        conn = get_connection(db_path) if db_path else get_connection()
        try:
            r = conn.execute(
                """SELECT action, n, updated_at FROM exit_policy_overrides
                    WHERE signal_type = ? AND category_slice = ?
                      AND exit_reason = 'stop_loss'""",
                (signal_type, sl),
            ).fetchone()
        finally:
            try: conn.close()
            except Exception: pass
        if not r:
            return False
        action, n, updated_at = r[0], int(r[1] or 0), int(r[2] or 0)
        return (action == "exempt" and n >= EXEMPT_MIN_N
                and (int(time.time()) - updated_at) <= OVERRIDE_MAX_AGE_SEC)
    except Exception:
        return False


def main() -> int:
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    out = update_exit_policy(dry_run=not args.apply or args.dry_run)
    print(f"evaluated={out['evaluated']} exempted={out['exempted']} "
          f"unresolved={out['unresolved']} dry_run={out['dry_run']}")
    for r in out["rows"]:
        print(f"  {r[0]:<22}{r[1]:<8}{r[2]:<22}{r[3]:<8}n={r[4]:>3} "
              f"delta=${r[7]:+.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
