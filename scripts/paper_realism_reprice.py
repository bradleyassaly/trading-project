"""Read-only re-pricing audit: is any paper "alpha" robust to REALISTIC exit costs?

The paper lane books early exits (take_profit etc.) at the transient mid tick with
a flat 2% spread and a $50-floor slippage schedule (cost_model.py) — avg modeled exit
cost ~$0.01 on a $16 position. On thin longshot books that is fantasy: a momentary
0.77 mid is not a fillable price for the whole size. Ground truth: on the SAME signal,
real money captured only ~42% of paper's favorable move (live winners exit ~0.43 vs
paper ~0.73 on ~0.21 entries), implying a ~58% haircut on spike-exits.

This script does NOT modify anything. It re-prices each closed paper trade's EARLY
exit under a transparent, parameterized cost model and reports corrected per-signal EV
across a sensitivity band (optimistic / base / live-anchored). Resolution exits (price
pinned to ~0 or ~1) are kept as-is: those ARE realizable at settlement.

Model (BUY/YES; SELL is mirrored):
  favorable early exit (mid > entry):
      eff_exit = entry + capture * (mid - entry) - half_spread_abs
  adverse early exit (mid <= entry):
      eff_exit = mid - half_spread_abs
  resolution exit (mid<=RES_LO or mid>=RES_HI): eff_exit = mid   (no haircut)
  pnl = (eff_exit - entry) * size / max(entry, 0.01)      [BUY]
        (entry - eff_exit) * size / max(1-entry, 0.01)    [SELL]

capture = 1 - impact_frac.  Live-anchored impact ~0.58 (capture ~0.42).
"""
from __future__ import annotations

import argparse

from trading_platform.polymarket.cost_model import CostModel
from trading_platform.polymarket.db_connection import get_connection

# Scenarios vary the spike-impact (unrealizable share of a favorable move). Each uses
# the SAME CostModel the live paper path now uses, so the audit and the executor agree.
# live-anchored (0.58) matches the ~58% haircut observed on real fills.
SCENARIOS = [
    ("optimistic",    0.20),
    ("base",          0.50),
    ("live-anchored", 0.58),
]


def reprice(raw_entry, raw_exit, side, size, spike_impact):
    """Full round-trip re-price from RAW (pre-cost) prices, entry AND exit, using
    the production CostModel so the honest number matches go-forward paper."""
    if raw_entry is None or raw_entry <= 0 or raw_exit is None:
        return None
    cm = CostModel(spike_impact=spike_impact)
    eff_entry = cm.entry_cost(float(raw_entry), side, float(size)).effective_price
    eff_exit = cm.exit_cost(float(raw_exit), side, float(size),
                            entry_price=eff_entry).effective_price
    if side.upper() in ("YES", "BUY"):
        return (eff_exit - eff_entry) * size / max(eff_entry, 0.01)
    return (eff_entry - eff_exit) * size / max(1 - eff_entry, 0.01)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-n", type=int, default=20)
    args = ap.parse_args()

    conn = get_connection()
    # Re-price from RAW (pre-cost) prices where available, falling back to the
    # cost-adjusted columns when raw is null (older rows).
    rows = conn.execute(
        """SELECT signal_type,
                  COALESCE(raw_entry_price, entry_price) AS raw_entry,
                  COALESCE(raw_exit_price, exit_price)  AS raw_exit,
                  side, size_usd, realized_pnl, exit_reason
           FROM polymarket_paper_trades
           WHERE exit_ts IS NOT NULL
             AND entry_ts > (strftime('%s','now') - ? * 86400)""",
        (args.days,),
    ).fetchall()

    # group by signal
    by_sig: dict[str, list] = {}
    for r in rows:
        by_sig.setdefault(r[0], []).append(r)

    hdr = f"{'signal':<22} {'n':>4} {'booked_EV':>10}"
    for label, _ in SCENARIOS:
        hdr += f" {label[:12]:>13}"
    print(f"\nPaper re-pricing audit — last {args.days}d, signals with n>={args.min_n}\n")
    print(hdr)
    print("-" * len(hdr))

    results = []
    for sig, rs in by_sig.items():
        if len(rs) < args.min_n:
            continue
        n = len(rs)
        booked = sum((r[5] or 0) for r in rs) / n
        line = f"{sig:<22} {n:>4} {booked:>+10.2f}"
        scen_ev = {}
        for label, imp in SCENARIOS:
            tot = 0.0
            for r in rs:
                _, raw_entry, raw_exit, side, size, _, _ = r
                pnl = reprice(raw_entry, raw_exit, side or "YES", float(size or 0), imp)
                tot += pnl if pnl is not None else 0.0
            scen_ev[label] = tot / n
            line += f" {scen_ev[label]:>+13.2f}"
        print(line)
        results.append((sig, n, booked, scen_ev))

    # verdict
    print("\nVERDICT (live-anchored EV/trade — the honest number):")
    for sig, n, booked, scen in sorted(results, key=lambda x: -x[3]["live-anchored"]):
        ev = scen["live-anchored"]
        tag = "POSITIVE — investigate" if ev > 0.10 else (
              "~breakeven" if ev > -0.10 else "NEGATIVE")
        print(f"  {sig:<22} n={n:>4}  booked {booked:>+7.2f} -> live-anchored {ev:>+7.2f}  [{tag}]")
    conn.close()


if __name__ == "__main__":
    main()
