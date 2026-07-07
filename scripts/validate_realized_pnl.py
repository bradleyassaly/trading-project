"""Validate historical realized_pnl against on-chain reality.

For every closed live trade, check:
  - On-chain CONDITIONAL balance for token_id (should be 0 if truly closed)
  - If balance > 1 share: the close was fictitious — recorded realized_pnl
    is unreliable, outcome flag is unreliable too

Distinct from fix_premature_closes.py (which only covers 7d). This walks
ALL closed trades and reports the full extent of contamination.

Output: count + sum of fictitious closes, optional --apply to re-open them.

Why this matters: today's discovery (11 fictitious closes worth ~$60 of
fake P&L) was from 7d. If the bug has been quietly happening for weeks,
the realized_pnl_cumulative + signal WR metrics that feed the calibration
analyzer + sizing decisions may be biased high.
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
    ap.add_argument("--apply", action="store_true",
                    help="Re-open fictitious closes (clear exit_ts, realized_pnl, outcome).")
    ap.add_argument("--days", type=int, default=90,
                    help="Window in days. Default 90 (all-time-ish for our system).")
    args = ap.parse_args()

    # Quarantine (2026-07-07 audit F6): --apply nulls exit fields on a bare
    # balance check with no re-entry guard (same hazard as
    # fix_premature_closes). Require explicit opt-in for the mutating path.
    if getattr(args, "apply", False) and os.environ.get("ALLOW_LEDGER_REPAIR") != "1":
        raise SystemExit(
            "QUARANTINED: validate_realized_pnl.py --apply can corrupt the "
            "ledger (audit F6). Set ALLOW_LEDGER_REPAIR=1 to override, or run "
            "without --apply for a dry-run."
        )

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
        cutoff_sql = f"EXTRACT(EPOCH FROM NOW())::BIGINT - {args.days * 86400}"
        # Restrict to closes that PLACED an exit order (action-triggered).
        # resolved_zero_balance + market resolution don't have this bug.
        rows = conn.execute(f"""
            SELECT id, signal_type, direction, token_id, exit_reason,
                   realized_pnl, outcome, exit_ts
              FROM live_trades
             WHERE dry_run=0 AND exit_ts IS NOT NULL
               AND exit_ts >= {cutoff_sql}
               AND token_id IS NOT NULL
               AND realized_pnl IS NOT NULL
               AND exit_reason IN ('take_profit','trailing_stop','stop_loss',
                                   'whale_mirror_exit','time_decay')
             ORDER BY exit_ts DESC
        """).fetchall()

        print(f"Auditing {len(rows)} closed trades from last {args.days} days...\n")

        fictitious = []
        sum_fictitious_pnl = 0.0
        for r in rows:
            lid, sig, di, tid, er, pnl, out, ets = r
            try:
                bal = client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
                )
                on_chain = int(bal.get("balance", 0)) / 1_000_000
            except Exception:
                continue
            if on_chain >= 1.0:
                fictitious.append((lid, sig, di, er, float(pnl or 0), out, on_chain))
                sum_fictitious_pnl += float(pnl or 0)

        if not fictitious:
            print("OK — no fictitious closes found. Realized_pnl history is clean.")
            return 0

        print(f"FOUND {len(fictitious)} fictitious closes:\n")
        print(f"  {'id':<7} {'signal':<22} {'dir':<5} {'exit_reason':<18} {'recorded_pnl':>13} {'outcome':<8} {'on_chain':>8}")
        for lid, sig, di, er, pnl, out, bal in fictitious:
            print(f"  #{lid:<6d} {sig:<22} {di:<5} {er:<18} ${pnl:>+10.2f}  {str(out):<8} {bal:>8.1f}")

        print(f"\nTotal fictitious recorded P&L: ${sum_fictitious_pnl:+.2f}")
        # How many "wins" vs "losses" — relevant to per-signal WR calibration
        wins = sum(1 for f in fictitious if f[5] == 'win')
        losses = sum(1 for f in fictitious if f[5] == 'loss')
        print(f"Fictitious outcome distribution: {wins} wins / {losses} losses")
        print("  >>> These inflate signal WR metrics that feed the calibration analyzer.")

        if not args.apply:
            print("\nDry-run. Re-run with --apply to re-open these trades.")
            return 0

        for lid, *_ in fictitious:
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
        print(f"\nRe-opened {len(fictitious)} positions. ")
        print("Next live_position_monitor cycle will retry the exits with the fixed code.")
        return 0
    finally:
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    raise SystemExit(main())
