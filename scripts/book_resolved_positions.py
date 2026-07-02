"""Book resolved-but-unbooked live positions from Polymarket data-api truth.

Closes the gap that left 11 positions open in the DB on 2026-06-17 while
Polymarket's data-api already showed them resolved. Root cause: the live
position monitor only books a loss when the on-chain token balance is *dust*.
When a market resolves AGAINST a SELL (we hold the now-worthless NO token), the
balance is non-dust (we still hold 5-29 worthless tokens) so the dust branch
never fires, the SELL-exit can't fill (resolved market has no orderbook), and
the position sits open forever — its loss never booked, equity overstated, WR
and calibration stats missing the outcome.

The data-api /positions endpoint is the Gamma-independent source of truth: it
returns `redeemable` (market resolved) + `curPrice` + `cashPnl` even after
Gamma delists the market. For each open live_trade we match the data-api
position by token_id/condition_id; if it is `redeemable`, we book it using
Polymarket's own `cashPnl` as realized_pnl.

DRY-RUN BY DEFAULT — prints the proposed bookings and totals; writes nothing.
Pass --apply to commit the exit rows.

Usage:
    python scripts/book_resolved_positions.py            # dry-run
    python scripts/book_resolved_positions.py --apply    # commit
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.position_fetcher import PositionFetcher


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Commit the exit bookings (default: dry-run, no writes)")
    args = ap.parse_args()

    wallet = (os.environ.get("POLYMARKET_FUNDER_ADDRESS")
              or os.environ.get("POLYMARKET_WALLET_ADDRESS"))
    if not wallet:
        print("No POLYMARKET_FUNDER_ADDRESS / POLYMARKET_WALLET_ADDRESS in env")
        return 1

    positions = PositionFetcher().fetch_wallet_positions(wallet)
    by_tok = {str(p.get("asset")): p for p in positions}
    by_cid = {str(p.get("conditionId")): p for p in positions}
    print(f"data-api returned {len(positions)} live positions for our wallet\n")

    conn = get_connection()
    rows = conn.execute(
        """SELECT id, token_id, condition_id, direction, size_usd, entry_price,
                  LEFT(question, 36) q
             FROM live_trades
            WHERE dry_run=0 AND exit_ts IS NULL
              AND status IN ('submitted','live','matched')
            ORDER BY id"""
    ).fetchall()

    now = int(time.time())
    to_book: list[tuple] = []
    skipped_open = 0
    skipped_missing = 0

    print(f"{'#id':<7}{'dir':<5}{'action':<10}{'outcome':<8}{'realPnl':<9}{'q'}")
    for r in rows:
        tid, tok, cid, direction, sz, ep, q = r
        p = by_tok.get(str(tok)) or by_cid.get(str(cid))
        if not p:
            skipped_missing += 1
            print(f"#{tid:<6}{str(direction):<5}{'SKIP':<10}{'-':<8}{'-':<9}"
                  f"{q}  (not in data-api — verify manually)")
            continue
        redeemable = bool(p.get("redeemable"))
        if not redeemable:
            skipped_open += 1
            cur = p.get("curPrice")
            print(f"#{tid:<6}{str(direction):<5}{'open':<10}{'-':<8}{'-':<9}"
                  f"{q}  (unresolved, curPx={cur})")
            continue
        # Resolved. Use Polymarket's own cashPnl as realized_pnl, curPrice as
        # exit_price. cashPnl = currentValue - initialValue; for a resolved
        # loser currentValue=0 so cashPnl = -cost basis, for a winner it is
        # payout - cost. Outcome follows the sign.
        cash_pnl = p.get("cashPnl")
        cur_price = p.get("curPrice")
        realized = round(float(cash_pnl), 2) if cash_pnl is not None else 0.0
        outcome = "win" if realized > 0 else "loss"
        to_book.append((tid, outcome, realized,
                        float(cur_price) if cur_price is not None else 0.0))
        print(f"#{tid:<6}{str(direction):<5}{'BOOK':<10}{outcome:<8}"
              f"{realized:<+9.2f}{q}")

    total = sum(b[2] for b in to_book)
    print(f"\n{'='*60}")
    print(f"  To book: {len(to_book)} positions  |  net realized: ${total:+.2f}")
    print(f"  Left open (unresolved): {skipped_open}")
    print(f"  Missing from data-api (manual): {skipped_missing}")
    print(f"{'='*60}")

    if not args.apply:
        print("\nDRY-RUN — no writes. Re-run with --apply to commit.")
        conn.close()
        return 0

    booked = 0
    for tid, outcome, realized, cur_price in to_book:
        conn.execute(
            """UPDATE live_trades
                  SET exit_ts=%s, exit_price=%s, outcome=%s,
                      realized_pnl=%s, exit_reason='resolved_databook'
                WHERE id=%s AND exit_ts IS NULL""",
            (now, cur_price, outcome, realized, tid),
        )
        booked += 1
    conn.commit()
    conn.close()
    print(f"\nCOMMITTED {booked} bookings (exit_reason='resolved_databook').")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
