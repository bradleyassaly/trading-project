"""Find + fix prematurely-closed live trades.

A trade is "prematurely closed" when:
  - DB has exit_ts set + realized_pnl recorded
  - But on-chain CONDITIONAL balance > 0 (we still hold the tokens)

Caused by the bug fixed 2026-05-28 in live_position_monitor: place_market_order
returned success=True for status='live' (resting order, not filled), and we
recorded fictional realized_pnl.

This script:
  1. Iterates all live trades closed in past 7 days
  2. Queries on-chain balance for each token_id
  3. If balance >= 1 share, re-open the trade (clear exit_ts, realized_pnl,
     outcome, exit_reason) so the position monitor will retry

Default: dry-run. Use --apply to commit.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    from py_clob_client_v2 import ClobClient, ApiCreds
    from py_clob_client_v2.constants import POLYGON
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
    pk      = os.environ["POLYMARKET_PRIVATE_KEY"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    secret  = os.environ["POLYMARKET_API_SECRET"]
    api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                or os.environ.get("POLYMARKET_PASSPHRASE"))
    funder  = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
    sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
    client = ClobClient(
        host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
        signature_type=sig_type, funder=funder or None,
    )

    conn = get_connection()
    try:
        cutoff = f"EXTRACT(EPOCH FROM NOW())::BIGINT - {args.days * 86400}"
        rows = conn.execute(f"""
            SELECT id, signal_type, direction, token_id, exit_reason,
                   realized_pnl, exit_price
              FROM live_trades
             WHERE dry_run=0 AND exit_ts IS NOT NULL
               AND exit_ts >= {cutoff}
               AND token_id IS NOT NULL
               AND exit_reason IN ('take_profit','trailing_stop','stop_loss','whale_mirror_exit')
        """).fetchall()

        print(f"Checking {len(rows)} closed trades from last {args.days} days...\n")
        fictitious = []
        for r in rows:
            lid, sig, di, tid, er, pnl, ep = r
            try:
                bal = client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
                )
                on_chain = int(bal.get("balance", 0)) / 1_000_000
            except Exception as exc:
                print(f"  #{lid} balance fetch failed: {exc}")
                continue
            if on_chain >= 1.0:
                print(f"  #{lid} {sig:<22} {di} exit={er} pnl=${pnl}")
                print(f"     >>> ON-CHAIN STILL HAS {on_chain} SHARES — fictitious close")
                fictitious.append(lid)

        print(f"\n{len(fictitious)} fictitious closes found")

        if not args.apply:
            print("\nDry-run. Re-run with --apply to re-open these trades.")
            return 0

        for lid in fictitious:
            conn.execute("""
                UPDATE live_trades
                   SET exit_ts = NULL,
                       exit_price = NULL,
                       outcome = NULL,
                       realized_pnl = NULL,
                       exit_reason = NULL
                 WHERE id = %s
            """, (lid,))
        conn.commit()
        print(f"\nRe-opened {len(fictitious)} positions. Position monitor will retry.")
    finally:
        try: conn.close()
        except Exception: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
