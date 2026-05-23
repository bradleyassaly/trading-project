"""Per-wallet revenue attribution and auto-tier feedback.

For every closed live_trade with a known signal_wallet, attribute the
realized PnL back to that wallet. Roll up over 30d (and 7d) into:
  - n_trades, wins, losses
  - total realized PnL
  - avg PnL/trade
  - Sharpe-like ratio (mean / stddev) when n>=5

Auto-tier downgrade: if a wallet's 30d realized PnL < AUTO_DEMOTE_THRESHOLD
on n >= AUTO_DEMOTE_N trades, the script flags it (default: print only;
pass --apply to actually update wallet tier).

Why: we record signal_wallet on every live trade but never roll up
"wallet X contributed $Y in 30d". Likely 20-30% of signals come from
unmeasured wallets (no live evidence either way). Without attribution
we can't auto-promote/demote — we're flying on inherited leaderboard
tiers from historical paper data.

Usage:
    python scripts/wallet_attribution.py          # report only
    python scripts/wallet_attribution.py --apply  # commit tier changes
    python scripts/wallet_attribution.py --json   # machine output
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection


AUTO_DEMOTE_PNL = -10.0     # Demote wallets with 30d realized < -$10
AUTO_DEMOTE_N = 5            # ... if they have at least 5 resolved trades
TOP_N = 10                   # How many wallets to show in top/bottom


def gather_attribution(conn, now_ts: int, window_days: int) -> list[dict]:
    """Roll up realized PnL by signal_wallet over `window_days`."""
    cutoff = now_ts - window_days * 86400
    rows = conn.execute("""
        SELECT signal_wallet,
               COUNT(*) n,
               SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
               SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END) losses,
               ROUND(SUM(COALESCE(realized_pnl, 0))::numeric, 2) pnl,
               ROUND(AVG(COALESCE(realized_pnl, 0))::numeric, 3) avg_pnl,
               ROUND(STDDEV_SAMP(COALESCE(realized_pnl, 0))::numeric, 3) sd_pnl
          FROM live_trades
         WHERE dry_run=0 AND exit_ts >= %s AND realized_pnl IS NOT NULL
           AND signal_wallet IS NOT NULL AND signal_wallet != ''
         GROUP BY signal_wallet
        HAVING COUNT(*) >= 1
         ORDER BY pnl DESC
    """, (cutoff,)).fetchall()
    out = []
    for r in rows:
        wallet, n, wins, losses, pnl, avg, sd = r
        n = int(n or 0)
        # Sharpe-ish: avg / sd when sd > 0 and n >= 5
        sharpe = None
        if n >= 5 and sd is not None and float(sd) > 0:
            sharpe = round(float(avg) / float(sd) * math.sqrt(n), 2)
        out.append({
            "wallet": wallet,
            "n": n,
            "wins": int(wins or 0),
            "losses": int(losses or 0),
            "pnl": float(pnl or 0),
            "avg": float(avg or 0),
            "sd": float(sd) if sd is not None else None,
            "sharpe": sharpe,
            "wr": (int(wins or 0) / n) if n else 0,
        })
    return out


def print_human(attr_30d: list[dict], attr_7d: list[dict]) -> None:
    print("\n" + "=" * 72)
    print("  PER-WALLET ATTRIBUTION — 30d realized PnL")
    print("=" * 72)
    if not attr_30d:
        print("  No wallets with attributable trades.")
        return

    total_pnl = sum(w["pnl"] for w in attr_30d)
    print(f"  Total tracked wallets: {len(attr_30d)}, sum PnL: ${total_pnl:+.2f}\n")

    print(f"  TOP {TOP_N} (drove revenue):")
    print(f"  {'wallet':<44s} {'n':>4} {'WR':>4} {'pnl':>9} {'avg':>7} {'sharpe':>7}")
    for w in attr_30d[:TOP_N]:
        sh = f"{w['sharpe']:.2f}" if w["sharpe"] is not None else "  -  "
        print(f"  {w['wallet'][:42]:<44s} {w['n']:>4d} {w['wr']*100:>3.0f}% "
              f"${w['pnl']:>+7.2f} ${w['avg']:>+5.2f}  {sh}")

    if len(attr_30d) > TOP_N:
        print(f"\n  BOTTOM {TOP_N} (cost revenue):")
        print(f"  {'wallet':<44s} {'n':>4} {'WR':>4} {'pnl':>9} {'avg':>7} {'sharpe':>7}")
        for w in attr_30d[-TOP_N:]:
            sh = f"{w['sharpe']:.2f}" if w["sharpe"] is not None else "  -  "
            print(f"  {w['wallet'][:42]:<44s} {w['n']:>4d} {w['wr']*100:>3.0f}% "
                  f"${w['pnl']:>+7.2f} ${w['avg']:>+5.2f}  {sh}")

    # Auto-demote candidates
    candidates = [w for w in attr_30d
                  if w["pnl"] <= AUTO_DEMOTE_PNL and w["n"] >= AUTO_DEMOTE_N]
    if candidates:
        print(f"\n  AUTO-DEMOTE candidates ({len(candidates)}): "
              f"30d PnL <= ${AUTO_DEMOTE_PNL} AND n >= {AUTO_DEMOTE_N}")
        for w in candidates:
            print(f"    {w['wallet'][:50]} n={w['n']} pnl=${w['pnl']:+.2f}")
    else:
        print("\n  No auto-demote candidates.")

    # 7d trend (anyone trending bad?)
    print(f"\n  7d snapshot (last week's drivers):")
    for w in attr_7d[:5]:
        print(f"    {w['wallet'][:44]:<46s} n={w['n']:>2d} pnl=${w['pnl']:>+6.2f}")


def apply_demotions(conn, candidates: list[dict]) -> int:
    """Apply tier downgrade. Returns count of wallets affected.

    The exact column/table for wallet tier varies; we update a generic
    'wallet_overrides' table (created if missing) so downstream logic
    can read it without us having to know which authoritative table
    holds tier. Conservative — we don't modify the leaderboard.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_overrides (
            wallet TEXT PRIMARY KEY,
            tier_override TEXT,
            reason TEXT,
            applied_at BIGINT
        )
    """)
    now = int(time.time())
    n = 0
    for w in candidates:
        conn.execute(
            """INSERT INTO wallet_overrides (wallet, tier_override, reason, applied_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (wallet) DO UPDATE SET
                 tier_override = EXCLUDED.tier_override,
                 reason = EXCLUDED.reason,
                 applied_at = EXCLUDED.applied_at""",
            (w["wallet"], "DEMOTED",
             f"30d pnl ${w['pnl']:+.2f} on n={w['n']} (auto)",
             now),
        )
        n += 1
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Apply tier demotions. Default: report only.")
    ap.add_argument("--json", action="store_true",
                    help="JSON output for machine consumption.")
    args = ap.parse_args()

    now = int(time.time())
    conn = get_connection()
    try:
        attr_30d = gather_attribution(conn, now, 30)
        attr_7d = gather_attribution(conn, now, 7)

        if args.json:
            print(json.dumps({"attr_30d": attr_30d, "attr_7d": attr_7d}, default=str))
            return 0

        print_human(attr_30d, attr_7d)

        if args.apply:
            candidates = [w for w in attr_30d
                          if w["pnl"] <= AUTO_DEMOTE_PNL and w["n"] >= AUTO_DEMOTE_N]
            if candidates:
                n = apply_demotions(conn, candidates)
                print(f"\nApplied {n} demotions to wallet_overrides table.")
            else:
                print("\nNothing to apply.")
    finally:
        try: conn.close()
        except Exception: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
