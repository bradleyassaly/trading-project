"""Fade-the-losers backtest: if bottom-quartile wallets persistently lose,
is taking the OPPOSITE side of their trades +EV after costs?

User hypothesis (2026-07-16): loser-persistence (rho +0.566, losers stay
losers) should be monetizable by betting against them — and doing so would
confirm they're negative-EV.

Design (split-sample, no selection leak):
  P1 = [lookback..split]d ago: rank wallets by per-$ realized pnl (BUY rows,
       resolved, reliable, n >= min-n). Bottom quartile = "faders' targets".
  P2 = [split..0]d ago: for each target's BUY trades that resolved, simulate
       the mirror: buy the COMPLEMENT token at (1-p) at their trade time, hold
       to resolution. Binary markets are zero-sum per share pair, so
       gross fade pnl = -(their pnl). Fade stake = size*(1-p) (NOT their
       stake size*p) — this asymmetry is the whole story for longshot faders.
  Costs: entry only (settlement redemption is free) via the honest CostModel
       (price-aware half-spread + size slippage) charged per share on the
       complement price.

Read-only. BUY rows only (SELL pnl is basis-dependent, not mirrorable).

Caveat: runs on wallet_trades as of now — the 2026-07-16 backfill may still be
in flight, so coverage of loser wallets may be partial. Selection ranks and
per-trade EV are computed on the same (possibly partial) sample; treat output
as a first-pass estimate and re-run post-backfill.

!! VERDICT (2026-07-16, do NOT trust this script's headline number) !!
The naive run printed fade NET +0.46/$ and the strict-directional variant
+0.72/$ — both ARTIFACTS, verified against lb-api ground truth:
  1. One-leg accounting: BUY-side pnl misclassifies MMs/arbitrageurs (91/327
     conditions had the same "loser" on BOTH outcome tokens; SELL legs +$252k
     offset one "loser's" -$707k; SPLIT-acquired inventory is invisible to
     /activity TRADE rows). One flagged "loser" (0x73c5...) actually made
     +$186k/30d per Polymarket.
  2. Unachievable fills: the big per-$ losses are wrong-price buys on
     effectively-decided markets. The complement was never available at
     (1-p) to a FOLLOWER — the edge accrued to the RESTING counterparty at
     fill time. Follow-fading buys the complement at the post-trade book
     (~fair) and captures ~nothing.
Real losers DO exist (lb-api confirms -$157k and -$245k/30d wallets), but the
monetizable "other side of dumb flow" is being the resting maker they cross
into — i.e. the taker->maker pivot — not follow-fading as a taker.
"""
from __future__ import annotations

import argparse
import time

from trading_platform.polymarket.cost_model import CostModel
from trading_platform.polymarket.db_connection import get_connection


def wallet_ranks(conn, lo, hi, min_n):
    rows = conn.execute(
        """SELECT wallet, COUNT(*) n, SUM(pnl) tot, SUM(ABS(size*price)) stake
           FROM wallet_trades
           WHERE side='BUY' AND market_resolved=1 AND pnl_reliable=1
             AND pnl IS NOT NULL AND size > 0 AND price BETWEEN 0.02 AND 0.98
             AND timestamp >= ? AND timestamp < ?
           GROUP BY wallet HAVING COUNT(*) >= ?""",
        (lo, hi, min_n),
    ).fetchall()
    return {w: {"n": n, "per_dollar": t / s} for w, n, t, s in rows if s and s > 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--split-days", type=int, default=45)
    ap.add_argument("--min-n", type=int, default=15)
    args = ap.parse_args()

    now = int(time.time())
    p1_lo, p1_hi = now - args.lookback_days * 86400, now - args.split_days * 86400
    p2_lo, p2_hi = p1_hi, now

    conn = get_connection()
    cm = CostModel()

    p1 = wallet_ranks(conn, p1_lo, p1_hi, args.min_n)
    ranked = sorted(p1, key=lambda w: p1[w]["per_dollar"])
    q = max(1, len(ranked) // 4)
    losers = ranked[:q]
    print(f"\nP1 wallets ranked: {len(ranked)}; bottom quartile (fade targets): {len(losers)}")
    print(f"  their P1 per-$: avg {sum(p1[w]['per_dollar'] for w in losers)/len(losers):+.3f}")

    # P2 trades of the fade targets
    ph = ",".join("?" * len(losers))
    rows = conn.execute(
        f"""SELECT wallet, price, size, pnl
            FROM wallet_trades
            WHERE side='BUY' AND market_resolved=1 AND pnl_reliable=1
              AND pnl IS NOT NULL AND size > 0 AND price BETWEEN 0.02 AND 0.98
              AND timestamp >= ? AND timestamp < ?
              AND wallet IN ({ph})""",
        (p2_lo, p2_hi, *losers),
    ).fetchall()
    if not rows:
        print("  No P2 resolved BUY trades for fade targets — insufficient data.")
        return

    # simulate the fade, bucketed by THEIR entry price
    buckets: dict[str, dict] = {}
    tot = {"n": 0, "their_stake": 0.0, "their_pnl": 0.0,
           "fade_stake": 0.0, "fade_gross": 0.0, "fade_cost": 0.0}
    for _, p, size, their_pnl in rows:
        p = float(p); size = float(size); their_pnl = float(their_pnl)
        comp = 1.0 - p
        fade_stake = size * comp
        # entry cost on the complement token, per honest cost model
        est = cm.entry_cost(comp, "BUY", fade_stake)
        cost = (est.effective_price - comp) * size
        gross = -their_pnl
        b = f"{0.1*int(p*10):.1f}"
        d = buckets.setdefault(b, {"n": 0, "their_stake": 0.0, "their_pnl": 0.0,
                                   "fade_stake": 0.0, "fade_gross": 0.0, "fade_cost": 0.0})
        for dd in (d, tot):
            dd["n"] += 1
            dd["their_stake"] += size * p
            dd["their_pnl"] += their_pnl
            dd["fade_stake"] += fade_stake
            dd["fade_gross"] += gross
            dd["fade_cost"] += cost

    print(f"\nP2 fade simulation over {tot['n']} resolved BUY trades of P1 bottom-quartile wallets")
    print(f"\n{'their px':>8} {'n':>6} {'their $/$':>10} {'fade gross/$':>13} {'fade NET/$':>11} {'capital mult':>13}")
    print("-" * 68)
    for b in sorted(buckets):
        d = buckets[b]
        if d["their_stake"] <= 0 or d["fade_stake"] <= 0:
            continue
        their_pd = d["their_pnl"] / d["their_stake"]
        fg = d["fade_gross"] / d["fade_stake"]
        fn = (d["fade_gross"] - d["fade_cost"]) / d["fade_stake"]
        mult = d["fade_stake"] / d["their_stake"]
        print(f"{b:>8} {d['n']:>6} {their_pd:>+10.3f} {fg:>+13.3f} {fn:>+11.3f} {mult:>12.1f}x")
    their_pd = tot["their_pnl"] / tot["their_stake"]
    fg = tot["fade_gross"] / tot["fade_stake"]
    fn = (tot["fade_gross"] - tot["fade_cost"]) / tot["fade_stake"]
    print("-" * 68)
    print(f"{'TOTAL':>8} {tot['n']:>6} {their_pd:>+10.3f} {fg:>+13.3f} {fn:>+11.3f} "
          f"{tot['fade_stake']/tot['their_stake']:>12.1f}x")
    print(f"\n  their total P2 loss: ${-tot['their_pnl']:,.0f} on ${tot['their_stake']:,.0f} staked")
    print(f"  fade capital required: ${tot['fade_stake']:,.0f}  "
          f"gross ${tot['fade_gross']:,.0f}  costs ${tot['fade_cost']:,.0f}  "
          f"NET ${tot['fade_gross']-tot['fade_cost']:,.0f}")
    conn.close()


if __name__ == "__main__":
    main()
