"""Backfill live_trades.shares from on-chain CONDITIONAL balance.

Why: orders that partial-fill in thin books leave live_trades.shares
recorded as the INTENDED quantity (size_usd / no_price) instead of the
actual filled quantity. On 2026-05-22, the reconciler found 3 positions
with shares 50-7x off from on-chain (worst: #9974 db=500 vs on-chain=10).

This script:
  1. Iterates all open live trades with token_id
  2. Queries CONDITIONAL balance via py_clob_client_v2
  3. If on-chain > 0 and differs from DB by > 5% (or > 5 shares),
     updates DB.shares to on-chain
  4. Reports what changed

Safe to re-run. Default is --dry-run; use --apply to commit.
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


def _make_clob():
    from py_clob_client_v2 import ClobClient, ApiCreds
    from py_clob_client_v2.constants import POLYGON
    pk      = os.environ["POLYMARKET_PRIVATE_KEY"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    secret  = os.environ["POLYMARKET_API_SECRET"]
    api_pass = (os.environ.get("POLYMARKET_API_PASSPHRASE")
                or os.environ.get("POLYMARKET_PASSPHRASE"))
    funder  = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
    sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
    return ClobClient(
        host="https://clob.polymarket.com", chain_id=POLYGON, key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=secret, api_passphrase=api_pass),
        signature_type=sig_type, funder=funder or None,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually update DB. Default is dry-run.")
    args = ap.parse_args()

    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    conn = get_connection()
    rows = conn.execute("""
        SELECT id, signal_type, direction, token_id, shares, size_usd
          FROM live_trades
         WHERE dry_run=0 AND exit_ts IS NULL
           AND status IN ('matched','live') AND token_id IS NOT NULL
    """).fetchall()

    print(f"Checking {len(rows)} open positions\n")
    client = _make_clob()

    updates = []
    for r in rows:
        lid, sig, di, tid, sh_db, sz = r
        try:
            result = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
            )
            real = int(result.get("balance", 0)) / 1_000_000
        except Exception as exc:
            print(f"  #{lid}: balance fetch failed — {exc}")
            continue

        db_sh = float(sh_db) if sh_db is not None else None
        # 2026-07-06: never sync a ZERO balance onto recorded shares. A
        # zero CONDITIONAL balance means the market resolved (loss, or
        # win auto-redeemed) — that's the dust-settlement path's job.
        # Overwriting shares with 0 here destroyed the cost basis before
        # settlement ran: 6 resolved wins on 7/6 booked at pnl=0 because
        # _settle_dust_close saw db_shares=0 → "no capital committed".
        # The docstring always said "If on-chain > 0"; the code didn't.
        if real <= 0:
            continue
        # Compute drift; flag if > 5 shares OR > 5% of db value
        threshold = max(5.0, (db_sh or 0) * 0.05)
        if db_sh is None or abs(real - db_sh) > threshold:
            db_disp = f"{db_sh:.2f}" if db_sh is not None else "NULL"
            print(f"  #{lid} {sig:<22} {di:<5} db={db_disp:>10}  on-chain={real:>10.2f}  diff={real - (db_sh or 0):+.2f}")
            updates.append((real, lid))

    print(f"\n{len(updates)} positions to update")

    if not args.apply:
        print("Dry-run. Re-run with --apply to commit.")
        conn.close()
        return 0

    for real, lid in updates:
        conn.execute(
            "UPDATE live_trades SET shares = %s WHERE id = %s",
            (real, lid),
        )
    conn.commit()
    conn.close()
    print(f"Updated {len(updates)} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
