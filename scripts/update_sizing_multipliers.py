"""Update per-(signal, direction) sizing multipliers from live trade data.

The executor's `confidence` is severely miscalibrated against live outcomes
(Brier 0.32, calib err 0.23 measured 2026-05-12). Rather than refit the
upstream model, this script computes a direct empirical correction:

    multiplier = clamp(actual_WR / mean_confidence, [MIN_MULT, MAX_MULT])

Applied post-Kelly so the order size lines up with realized edge instead
of the (broken) confidence signal.

Safety:
- Only writes multipliers when n_resolved >= MIN_N (sample stability)
- Clamps to [0.25, 2.0] so a single hot streak can't 5x stakes
- Direction-aware (SELL miscalibration is opposite of BUY in this system)
- Records mean_confidence + actual_wr so the rationale is auditable

Run daily via cron. Read by executor at signal evaluation time.
"""
import sys
import os
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection

# Tuning knobs
MIN_N = 15            # minimum resolved trades before we'll write a multiplier
MIN_MULT = 0.25       # floor — even very over-confident signals get $1.25 floor
MAX_MULT = 2.0        # ceiling — under-confident signals scale up but capped
DEFAULT_WINDOW = 30   # days of live data to compute from


def compute_multiplier(actual_wr: float, mean_conf: float,
                       actual_ev: float | None = None) -> float:
    """Empirical correction: scale size by actual_WR / predicted_WR.

    EV guard (2026-07-14): NEVER up-size a net-losing slice. A signal can
    have WR > mean_confidence — which lifts raw above 1.0 — yet still be
    net-EV-negative once win/loss SIZES and costs are counted. resolution_decay
    was getting 1.49x at actual_ev=-$0.36/trade (realized -$24.90/30d), so the
    multiplier was amplifying the bleed on the only live signal. When realized
    EV per trade is <= 0, clamp the multiplier to at most 1.0. Down-corrections
    (over-confident signals, raw < 1.0) are unaffected — we still shrink those.
    """
    if mean_conf <= 0:
        return 1.0
    raw = actual_wr / mean_conf
    mult = max(MIN_MULT, min(MAX_MULT, raw))
    if actual_ev is not None and actual_ev <= 0:
        mult = min(mult, 1.0)
    return mult


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=DEFAULT_WINDOW)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print computed multipliers without writing to DB")
    args = ap.parse_args()

    conn = get_connection()
    since = int(time.time()) - args.days * 86400

    rows = conn.execute("""
        SELECT signal_type, direction,
               COUNT(*) n,
               AVG(CASE WHEN outcome='win' THEN 1.0 ELSE 0.0 END) wr,
               AVG(confidence) mean_conf,
               AVG(realized_pnl) ev
          FROM live_trades
         WHERE dry_run=0 AND attempted_at > %s
           AND outcome IN ('win','loss')
           AND confidence IS NOT NULL
           AND COALESCE(is_probe, 0) = 0
         GROUP BY signal_type, direction
         HAVING COUNT(*) >= %s
    """, (since, MIN_N)).fetchall()

    if not rows:
        print(f"No (signal, direction) slices with n >= {MIN_N} in last {args.days}d")
        return

    now_ts = int(time.time())
    print(f"\n=== Computed multipliers (last {args.days}d, min n={MIN_N}) ===")
    print(f"{'Signal':<28} {'Dir':<5} {'n':>4} {'WR':>7} {'conf':>7} {'mult':>6}  notes")
    print("-" * 80)

    updates = []
    for r in rows:
        sig, dire, n, wr, conf, ev = r
        n = int(n)
        wr = float(wr)
        conf = float(conf or 0)
        ev = float(ev or 0)
        mult = compute_multiplier(wr, conf, ev)
        # Annotate the direction of the correction for review
        if ev <= 0 and conf > 0 and (wr / conf) > 1.0 and mult <= 1.0:
            notes = "EV<=0 — up-size BLOCKED (net-losing slice)"
        elif mult >= 1.5:
            notes = "scale up — under-confident"
        elif mult <= 0.5:
            notes = "scale down — over-confident"
        elif mult <= 0.75:
            notes = "mild scale down"
        elif mult >= 1.25:
            notes = "mild scale up"
        else:
            notes = "no correction needed"
        print(f"{sig:<28} {dire:<5} {n:>4} {wr:>6.1%} {conf:>6.3f} {mult:>5.2f}x  {notes}")
        updates.append((sig, dire, mult, n, wr, conf, ev, args.days, now_ts, notes))

    if args.dry_run:
        print("\nDRY RUN — no writes")
        return

    # UPSERT
    for u in updates:
        conn.execute("""
            INSERT INTO signal_sizing_multipliers
                (signal_type, direction, multiplier, n_resolved,
                 actual_wr, mean_confidence, actual_ev, window_days,
                 updated_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (signal_type, direction) DO UPDATE SET
                multiplier      = EXCLUDED.multiplier,
                n_resolved      = EXCLUDED.n_resolved,
                actual_wr       = EXCLUDED.actual_wr,
                mean_confidence = EXCLUDED.mean_confidence,
                actual_ev       = EXCLUDED.actual_ev,
                window_days     = EXCLUDED.window_days,
                updated_at      = EXCLUDED.updated_at,
                notes           = EXCLUDED.notes
        """, u)
    conn.commit()
    print(f"\nUpdated {len(updates)} (signal, direction) multipliers in signal_sizing_multipliers")

    conn.close()


if __name__ == "__main__":
    main()
