"""Scheduled per-slice calibration refit.

For each (signal_type × direction) with sufficient live samples, fit
an isotonic curve and persist. Designed to run weekly — Brier changes
slowly and live data accumulates at ~6 trades/day.

The fitted curves are persisted to alpha_calibration_curve with
signal_type + direction columns populated. apply_calibration() picks
them up ONLY if ENABLE_PER_SLICE_CALIB=1 — by default this is a
shadow operation: data accumulates, sizing unchanged.

After 14 days of shadow accumulation showing no regression vs the
existing per-category curves, flip the env var to swap in per-slice
lookups for Kelly sizing.

Usage:
    python scripts/refit_per_slice_calibration.py        # fit + print
    python scripts/refit_per_slice_calibration.py --days 90
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.isotonic_calibration import (
    fit_calibration_per_slice,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-n", type=int, default=15)
    args = ap.parse_args()

    # Enumerate the (signal, direction) slices that have enough data
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT signal_type, direction, COUNT(*) n
              FROM live_trades
             WHERE dry_run=0
               AND exit_ts >= EXTRACT(EPOCH FROM NOW())::BIGINT - %s * 86400
               AND outcome IN ('win','loss')
               AND confidence IS NOT NULL
               AND signal_type IS NOT NULL AND direction IS NOT NULL
             GROUP BY 1, 2
            HAVING COUNT(*) >= %s
             ORDER BY 3 DESC
        """, (args.days, args.min_n)).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    print(f"=== Per-slice calibration refit — {args.days}d window, n>={args.min_n} ===\n")
    print(f"Slices to fit: {len(rows)}\n")

    results = []
    for r in rows:
        sig, di, n = r
        result = fit_calibration_per_slice(sig, di, window_days=args.days)
        results.append(result)
        if "skipped" in result:
            print(f"  {sig:<22} {di:<5} n={n:>3d}  SKIP: {result['skipped']}")
        elif "error" in result:
            print(f"  {sig:<22} {di:<5} ERR: {result['error']}")
        else:
            print(f"  {sig:<22} {di:<5} n={result['n']:>3d}  "
                  f"Brier {result['brier_before']} → {result['brier_after']}  "
                  f"({result['improvement_pct']:+.1f}%)")

    fitted = sum(1 for r in results if "brier_after" in r and "skipped" not in r)
    print(f"\nFitted + persisted: {fitted} curves")
    print("ENABLE_PER_SLICE_CALIB=1 to activate; currently shadow mode.")


if __name__ == "__main__":
    raise SystemExit(main())
