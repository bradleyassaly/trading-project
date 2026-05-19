"""Live confidence calibration check.

Measures how well the executor's `confidence` predicts live win rate.
Bins resolved live_trades by confidence, computes empirical WR + EV per
bin, reports calibration error (predicted - observed).

Run this weekly. If overall calibration error > 0.10, the upstream
isotonic curve is probably stale and should be refit on fresh data.

Usage:
    python scripts/calibration_check.py [--days 30] [--min-n 5]
"""
import sys
import os
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30,
                    help="Window of resolved trades to evaluate")
    ap.add_argument("--min-n", type=int, default=5,
                    help="Skip bins with fewer than this many trades")
    ap.add_argument("--direction", default=None, choices=["BUY", "SELL"],
                    help="Filter by direction")
    args = ap.parse_args()

    conn = get_connection()
    since = int(time.time()) - args.days * 86400

    # Pull resolved live trades with confidence + outcome
    q = """SELECT signal_type, direction, category, confidence,
                  outcome, realized_pnl, size_usd
             FROM live_trades
            WHERE dry_run=0 AND attempted_at > %s
              AND confidence IS NOT NULL
              AND outcome IN ('win','loss')"""
    params = [since]
    if args.direction:
        q += " AND direction = %s"
        params.append(args.direction)

    rows = conn.execute(q, params).fetchall()
    if not rows:
        print(f"No resolved trades in last {args.days} days")
        return

    bins = [
        (0.0, 0.50, "0-50pct"),
        (0.50, 0.55, "50-55pct"),
        (0.55, 0.60, "55-60pct"),
        (0.60, 0.65, "60-65pct"),
        (0.65, 0.70, "65-70pct"),
        (0.70, 0.80, "70-80pct"),
        (0.80, 1.01, ">=80pct"),
    ]

    print(f"\n=== Calibration: confidence -> win rate ({args.days}d, n={len(rows)}) ===")
    print(f"{'Conf bin':<12} {'midpoint':>8} {'n':>4} {'WR':>7} {'EV':>9} "
          f"{'pred-obs':>10}")
    print("-" * 56)

    total_brier = 0.0
    total_calib_err = 0.0
    valid_bins = 0
    for lo, hi, label in bins:
        bucket = [r for r in rows if lo <= float(r[3] or 0) < hi]
        if len(bucket) < args.min_n:
            continue
        wins = sum(1 for r in bucket if r[4] == "win")
        wr = wins / len(bucket)
        ev = sum(float(r[5] or 0) for r in bucket) / len(bucket)
        midpoint = (lo + hi) / 2
        gap = midpoint - wr
        total_calib_err += abs(gap) * len(bucket)
        for r in bucket:
            p = float(r[3])
            y = 1 if r[4] == "win" else 0
            total_brier += (p - y) ** 2
        valid_bins += 1
        flag = "  OK" if abs(gap) < 0.10 else " HIGH" if abs(gap) < 0.20 else "CRIT"
        print(f"{label:<12} {midpoint:>8.2f} {len(bucket):>4} "
              f"{wr:>6.1%} ${ev:>+7.4f} {gap:>+9.3f} {flag}")

    if valid_bins == 0:
        print("\nNot enough data in any bin (need >= --min-n per bin)")
        return

    print()
    n = len(rows)
    print(f"Brier score:               {total_brier/n:.4f}  (lower is better)")
    print(f"Weighted calib. error:     {total_calib_err/n:.4f}  (mean |pred-obs|)")
    print()
    if total_calib_err/n > 0.10:
        print("CALIB ERR > 0.10 — refit isotonic curve recommended.")
        print("Run: python -c 'from trading_platform.polymarket.isotonic_calibration "
              "import fit_calibration; print(fit_calibration(window_days=30))'")
    else:
        print("Calibration acceptable (err < 0.10).")

    # By signal type — which signals are most miscalibrated?
    print(f"\n=== Per-signal calibration ===")
    print(f"{'Signal':<28} {'n':>4} {'mean_conf':>10} {'WR':>7} {'gap':>8}")
    by_sig = {}
    for r in rows:
        sig = r[0] or "unknown"
        by_sig.setdefault(sig, []).append(r)
    for sig, bucket in sorted(by_sig.items(), key=lambda x: -len(x[1])):
        if len(bucket) < args.min_n:
            continue
        mean_conf = sum(float(r[3]) for r in bucket) / len(bucket)
        wr = sum(1 for r in bucket if r[4] == "win") / len(bucket)
        gap = mean_conf - wr
        print(f"{sig:<28} {len(bucket):>4} {mean_conf:>10.3f} "
              f"{wr:>6.1%} {gap:>+8.3f}")

    conn.close()


if __name__ == "__main__":
    main()
