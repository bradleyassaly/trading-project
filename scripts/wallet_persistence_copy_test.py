"""Does INDIVIDUAL-wallet skill persist out-of-sample, and is it copyable?

The user's question: instead of group/cohort signals, can we identify individual
wallets with real alpha to copy (like copy-trading competitors)? Prerequisite test:
if a wallet's PAST performance doesn't predict its FUTURE performance, the "skill" is
luck and copy is dead before we even get to execution costs.

Design (split-sample, the standard persistence test):
  P1 = [split_days .. lookback_days] ago, P2 = [0 .. split_days] ago.
  For wallets with >= MIN_N resolved trades in BOTH halves, compute per-$ mean PnL in
  each half. Report Spearman rank correlation (skill persistence) AND the actionable
  test: do P1 TOP-quartile wallets earn positive per-$ PnL in P2 — at the wallet's OWN
  entry prices (an upper bound; a follower does strictly worse).

Read-only. Uses wallet_trades (pnl, pnl_reliable, market_resolved).
"""
from __future__ import annotations

import argparse
import time

from trading_platform.polymarket.db_connection import get_connection


def half_stats(conn, lo_ts, hi_ts, min_n):
    rows = conn.execute(
        """SELECT wallet,
                  COUNT(*) n,
                  SUM(pnl) tot_pnl,
                  SUM(ABS(size)) tot_size
           FROM wallet_trades
           WHERE market_resolved = 1 AND pnl_reliable = 1 AND pnl IS NOT NULL
             AND size IS NOT NULL AND size > 0
             AND timestamp >= ? AND timestamp < ?
           GROUP BY wallet
           HAVING COUNT(*) >= ?""",
        (lo_ts, hi_ts, min_n),
    ).fetchall()
    out = {}
    for w, n, tot_pnl, tot_size in rows:
        if tot_size and tot_size > 0:
            out[w] = {"n": n, "per_dollar": tot_pnl / tot_size, "pnl": tot_pnl}
    return out


def spearman(pairs):
    """pairs: list of (x, y). Returns rank-correlation rho."""
    n = len(pairs)
    if n < 3:
        return None
    xs = sorted(range(n), key=lambda i: pairs[i][0])
    ys = sorted(range(n), key=lambda i: pairs[i][1])
    rx = [0] * n
    ry = [0] * n
    for rank, i in enumerate(xs):
        rx[i] = rank
    for rank, i in enumerate(ys):
        ry[i] = rank
    d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    return 1 - (6 * d2) / (n * (n * n - 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--split-days", type=int, default=45)
    ap.add_argument("--min-n", type=int, default=15)
    args = ap.parse_args()

    now = int(time.time())
    p1_lo = now - args.lookback_days * 86400
    p1_hi = now - args.split_days * 86400
    p2_lo = p1_hi
    p2_hi = now

    conn = get_connection()
    p1 = half_stats(conn, p1_lo, p1_hi, args.min_n)
    p2 = half_stats(conn, p2_lo, p2_hi, args.min_n)
    both = sorted(set(p1) & set(p2))

    print(f"\nWallet persistence test — P1={args.lookback_days}-{args.split_days}d ago, "
          f"P2={args.split_days}-0d ago, min_n={args.min_n}/half")
    print(f"  wallets qualifying in P1: {len(p1)}, P2: {len(p2)}, BOTH: {len(both)}\n")
    if len(both) < 10:
        print("  Too few wallets in both halves for a persistence read.")
        return

    pairs = [(p1[w]["per_dollar"], p2[w]["per_dollar"]) for w in both]
    rho = spearman(pairs)
    print(f"  Spearman rho(P1 per-$ PnL, P2 per-$ PnL) = {rho:+.3f}   "
          f"({'skill persists' if rho and rho > 0.2 else 'weak/none — looks like luck'})")

    # Actionable test: P1 top-quartile -> P2 forward performance (own prices).
    ranked = sorted(both, key=lambda w: p1[w]["per_dollar"], reverse=True)
    q = max(1, len(ranked) // 4)
    top = ranked[:q]
    bot = ranked[-q:]
    def avg(ws, half): return sum(half[w]["per_dollar"] for w in ws) / len(ws)
    print(f"\n  P1 TOP-quartile ({q} wallets): P1 per-$ {avg(top,p1):+.3f} -> "
          f"P2 per-$ {avg(top,p2):+.3f}   <-- the number that matters")
    print(f"  P1 BOT-quartile ({q} wallets): P1 per-$ {avg(bot,p1):+.3f} -> "
          f"P2 per-$ {avg(bot,p2):+.3f}")
    p2_top = avg(top, p2)
    print("\n  READ: P1 top-quartile forward (P2) per-$ PnL is the ceiling a copier could")
    print("  earn AT THE WALLET'S OWN PRICES (a real follower enters later/worse, pays")
    print("  spread+slippage, so subtract execution). If this is not comfortably")
    print(f"  positive, copy is dead before costs. Here it is {p2_top:+.3f}/$.\n")
    conn.close()


if __name__ == "__main__":
    main()
