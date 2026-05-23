"""Per-(signal_type × category) calibration analyzer.

Fits an isotonic regression on live outcomes for each (signal × category)
slice with sufficient sample size. Reports Brier-before vs Brier-after
and the calibration shape per slice. Output is READ-ONLY — no curves
are persisted yet because rolling out per-slice calibration mid-trading
risks worse sizing than the current multipliers band-aid.

Once we have 30+ days of post-fix data showing per-slice Brier < 0.25
on all major slices, the next step is to wire these curves into
isotonic_calibration.apply_calibration (already supports
per-category — extend to (signal × category) precedence) and retire
the multipliers table.

Why this matters: current isotonic curve fits on raw alpha_score, but
the executor uses final post-pipeline confidence. They diverge.
Brier=0.30-0.58 on 6 of 8 signals proves it. Per-slice fit on the
actual final confidence vs realized outcome would tighten Brier.

Usage:
    python scripts/calibration_per_slice.py        # full report
    python scripts/calibration_per_slice.py --min-n 15
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.isotonic_calibration import isotonic_regression


def brier(xs: list[float], ys: list[int]) -> float:
    return sum((xs[i] - ys[i]) ** 2 for i in range(len(xs))) / max(len(xs), 1)


def apply_curve(breakpoints: list[tuple[float, float]], x: float) -> float:
    import bisect
    bp_x = [b[0] for b in breakpoints]
    bp_y = [b[1] for b in breakpoints]
    if not bp_x:
        return x
    i = bisect.bisect_right(bp_x, x) - 1
    if i < 0:
        return bp_y[0]
    return bp_y[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60,
                    help="Window in days. Default 60.")
    ap.add_argument("--min-n", type=int, default=15,
                    help="Minimum resolved trades per slice. Default 15.")
    args = ap.parse_args()

    cutoff = int(time.time()) - args.days * 86400
    conn = get_connection()
    try:
        # Use FINAL confidence (what the executor actually saw) vs outcome.
        # That's what we want to calibrate, not the raw alpha_score.
        rows = conn.execute("""
            SELECT signal_type, category, direction,
                   confidence,
                   CASE WHEN outcome='win' THEN 1
                        WHEN outcome='loss' THEN 0
                        ELSE NULL END as y
              FROM live_trades
             WHERE dry_run=0 AND exit_ts >= %s
               AND outcome IN ('win','loss')
               AND confidence IS NOT NULL
               AND signal_type IS NOT NULL
        """, (cutoff,)).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    print(f"Loaded {len(rows)} resolved live trades from last {args.days}d\n")

    # Group by (signal, category, direction)
    slices: dict = {}
    for r in rows:
        sig, cat, di, conf, y = r
        key = (sig, cat or "(none)", di)
        slices.setdefault(key, []).append((float(conf), int(y)))

    # Also group by (signal × direction) — collapsing category
    sig_dir_slices: dict = {}
    for (sig, cat, di), pts in slices.items():
        sig_dir_slices.setdefault((sig, di), []).extend(pts)

    # And by signal alone
    sig_slices: dict = {}
    for (sig, cat, di), pts in slices.items():
        sig_slices.setdefault(sig, []).extend(pts)

    print("=" * 88)
    print(f"  CALIBRATION ANALYSIS — by (signal × direction)  min n={args.min_n}")
    print("=" * 88)
    print(f"  {'signal':<22} {'dir':<5} {'n':>4} {'WR':>5} {'mean_conf':>10} "
          f"{'Brier_before':>13} {'Brier_after':>12} {'improve':>8}")

    candidates = []
    for key, pts in sorted(sig_dir_slices.items(),
                           key=lambda kv: -len(kv[1])):
        sig, di = key
        n = len(pts)
        if n < args.min_n:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        wr = sum(ys) / n
        mean_conf = sum(xs) / n
        bp = isotonic_regression(xs, ys)
        b_before = brier(xs, ys)
        b_after = sum((apply_curve(bp, xs[i]) - ys[i]) ** 2 for i in range(n)) / n
        improve = b_before - b_after
        bp_y = [b[1] for b in bp]
        bp_range = (max(bp_y) - min(bp_y)) if bp_y else 0
        flag = "  *FLAT*" if bp_range < 0.30 else ""
        print(f"  {sig:<22} {di:<5} {n:>4d} {wr:>4.0%}  {mean_conf:>9.3f}  "
              f"{b_before:>12.3f}  {b_after:>11.3f}  {improve:>+7.3f}{flag}")
        candidates.append({
            "signal": sig, "direction": di, "n": n,
            "wr": wr, "mean_conf": mean_conf,
            "brier_before": b_before, "brier_after": b_after,
            "improvement": improve,
            "bp_range": bp_range,
        })

    # Summary
    print()
    n_improved = sum(1 for c in candidates if c["improvement"] > 0.02)
    n_brier_under_25 = sum(1 for c in candidates if c["brier_after"] < 0.25)
    print(f"  {len(candidates)} slices analyzed (n>={args.min_n})")
    print(f"  {n_improved} would improve Brier by >0.02 with per-slice fit")
    print(f"  {n_brier_under_25} would achieve Brier < 0.25 (scaling-ready bar)")

    print()
    print("  NEXT STEPS (not done today — high-risk to rollout mid-trading):")
    print("  1. Add (signal_type, direction) columns to alpha_calibration_curve")
    print("  2. Update fit_calibration() to accept (signal, direction) params")
    print("  3. Update apply_calibration() precedence:")
    print("       (signal × direction × category) → (signal × direction) → (category) → global")
    print("  4. Shadow-run for 14 days (compute both old + new, log divergence)")
    print("  5. Once Brier verified <0.25 and no regression in realized PnL,")
    print("     retire signal_sizing_multipliers table.")


if __name__ == "__main__":
    raise SystemExit(main())
