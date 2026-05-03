"""Temporal EV slicers: time-of-day + resolution-window.

Two new slicing dimensions on top of (signal, category, wallet_tier):

A) Time-of-day buckets (UTC):
   - asia_pm    (00:00-06:00 UTC = afternoon Asia)
   - europe_am  (06:00-12:00 UTC = morning Europe)
   - us_am      (12:00-18:00 UTC = morning US)
   - us_pm      (18:00-24:00 UTC = evening US, peak PM volume)

B) Resolution-window buckets:
   - fast    (<=72h to resolve)
   - medium  (72h-7d)
   - slow    (>7d)

Each table is its own EV slice. Together with category/tier slicing,
we get a much sharper read on which conditions produce edge.

Example finding (hypothetical until data accumulates):
  wallet_reversal × politics × us_pm × fast = +60% EV
  wallet_reversal × politics × asia_pm × slow = -10% EV

Same signal+category, opposite EV. Time + horizon are real drivers.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


MIN_N_PER_SLICE = 5
WINDOW_DAYS = 30


_SCHEMA_TIME = """
CREATE TABLE IF NOT EXISTS signal_time_ev (
    signal_type   TEXT NOT NULL,
    time_bucket   TEXT NOT NULL,
    n_resolved    BIGINT NOT NULL,
    win_rate      DOUBLE PRECISION,
    avg_ev        DOUBLE PRECISION,
    sum_pnl       DOUBLE PRECISION,
    computed_at   BIGINT NOT NULL,
    PRIMARY KEY (signal_type, time_bucket)
);
"""

_SCHEMA_HORIZON = """
CREATE TABLE IF NOT EXISTS signal_horizon_ev (
    signal_type     TEXT NOT NULL,
    horizon_bucket  TEXT NOT NULL,
    n_resolved      BIGINT NOT NULL,
    win_rate        DOUBLE PRECISION,
    avg_ev          DOUBLE PRECISION,
    sum_pnl         DOUBLE PRECISION,
    computed_at     BIGINT NOT NULL,
    PRIMARY KEY (signal_type, horizon_bucket)
);
"""


def _ensure_schemas(conn) -> None:
    for s in [_SCHEMA_TIME, _SCHEMA_HORIZON]:
        try: conn.execute(s.strip())
        except Exception: pass


def _time_bucket_sql() -> str:
    """SQL CASE expression returning bucket from entry_ts (UTC hour)."""
    return (
        "CASE "
        "WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(pt.entry_ts)) < 6 THEN 'asia_pm' "
        "WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(pt.entry_ts)) < 12 THEN 'europe_am' "
        "WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(pt.entry_ts)) < 18 THEN 'us_am' "
        "ELSE 'us_pm' END"
    )


def _horizon_bucket_sql() -> str:
    """SQL CASE expression returning bucket from (exit_ts - entry_ts) hours.
    Uses actual hold time as proxy for resolution window since that's
    typically <= the time-to-resolve at entry."""
    return (
        "CASE "
        "WHEN (pt.exit_ts - pt.entry_ts) <= 72 * 3600 THEN 'fast' "
        "WHEN (pt.exit_ts - pt.entry_ts) <= 7 * 86400 THEN 'medium' "
        "ELSE 'slow' END"
    )


def run_temporal_slicer() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    try:
        _ensure_schemas(conn)
        cutoff = int(time.time()) - WINDOW_DAYS * 86400
        now = int(time.time())

        # Time-of-day slicer
        time_rows = conn.execute(
            f"""SELECT pt.signal_type,
                       {_time_bucket_sql()} AS bucket,
                       COUNT(*),
                       SUM(CASE WHEN pt.outcome='win' THEN 1 ELSE 0 END),
                       AVG(pt.realized_pnl / NULLIF(pt.size_usd, 0)),
                       SUM(pt.realized_pnl)
                  FROM polymarket_paper_trades pt
                 WHERE pt.archived = 0 AND pt.exit_ts IS NOT NULL
                   AND pt.entry_ts > ?
                   AND pt.outcome IN ('win','loss')
                   AND pt.realized_pnl IS NOT NULL AND pt.size_usd > 0
                 GROUP BY pt.signal_type, {_time_bucket_sql()}""",
            (cutoff,),
        ).fetchall()
        n_time = 0
        for r in time_rows:
            sig, bucket, n, wins, avg_ev, sum_pnl = r
            n = int(n or 0)
            if n < MIN_N_PER_SLICE:
                continue
            wr = (int(wins or 0) / n) if n > 0 else 0.0
            try:
                conn.execute(
                    """INSERT INTO signal_time_ev
                         (signal_type, time_bucket, n_resolved, win_rate,
                          avg_ev, sum_pnl, computed_at)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type, time_bucket) DO UPDATE SET
                         n_resolved=EXCLUDED.n_resolved,
                         win_rate=EXCLUDED.win_rate,
                         avg_ev=EXCLUDED.avg_ev,
                         sum_pnl=EXCLUDED.sum_pnl,
                         computed_at=EXCLUDED.computed_at""",
                    (sig, bucket, n, round(wr, 4),
                     round(float(avg_ev or 0), 4),
                     round(float(sum_pnl or 0), 2), now),
                )
                n_time += 1
            except Exception:
                pass

        # Horizon (resolution-window) slicer
        horizon_rows = conn.execute(
            f"""SELECT pt.signal_type,
                       {_horizon_bucket_sql()} AS bucket,
                       COUNT(*),
                       SUM(CASE WHEN pt.outcome='win' THEN 1 ELSE 0 END),
                       AVG(pt.realized_pnl / NULLIF(pt.size_usd, 0)),
                       SUM(pt.realized_pnl)
                  FROM polymarket_paper_trades pt
                 WHERE pt.archived = 0 AND pt.exit_ts IS NOT NULL
                   AND pt.entry_ts > ?
                   AND pt.outcome IN ('win','loss')
                   AND pt.realized_pnl IS NOT NULL AND pt.size_usd > 0
                 GROUP BY pt.signal_type, {_horizon_bucket_sql()}""",
            (cutoff,),
        ).fetchall()
        n_horizon = 0
        for r in horizon_rows:
            sig, bucket, n, wins, avg_ev, sum_pnl = r
            n = int(n or 0)
            if n < MIN_N_PER_SLICE:
                continue
            wr = (int(wins or 0) / n) if n > 0 else 0.0
            try:
                conn.execute(
                    """INSERT INTO signal_horizon_ev
                         (signal_type, horizon_bucket, n_resolved, win_rate,
                          avg_ev, sum_pnl, computed_at)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type, horizon_bucket) DO UPDATE SET
                         n_resolved=EXCLUDED.n_resolved,
                         win_rate=EXCLUDED.win_rate,
                         avg_ev=EXCLUDED.avg_ev,
                         sum_pnl=EXCLUDED.sum_pnl,
                         computed_at=EXCLUDED.computed_at""",
                    (sig, bucket, n, round(wr, 4),
                     round(float(avg_ev or 0), 4),
                     round(float(sum_pnl or 0), 2), now),
                )
                n_horizon += 1
            except Exception:
                pass

        conn.commit()
        # Top time slices
        top_t = conn.execute(
            "SELECT signal_type, time_bucket, n_resolved, win_rate, avg_ev "
            "FROM signal_time_ev WHERE n_resolved >= 5 "
            "ORDER BY avg_ev DESC LIMIT 8"
        ).fetchall()
        top_h = conn.execute(
            "SELECT signal_type, horizon_bucket, n_resolved, win_rate, avg_ev "
            "FROM signal_horizon_ev WHERE n_resolved >= 5 "
            "ORDER BY avg_ev DESC LIMIT 8"
        ).fetchall()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "time_slices": n_time,
            "horizon_slices": n_horizon,
            "top_time": [{"signal": r[0], "bucket": r[1], "n": r[2],
                          "wr": float(r[3]), "ev": float(r[4])} for r in top_t],
            "top_horizon": [{"signal": r[0], "bucket": r[1], "n": r[2],
                             "wr": float(r[3]), "ev": float(r[4])} for r in top_h],
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_temporal_slicer()
    print(f"time_slices={out['time_slices']} horizon_slices={out['horizon_slices']}")
    print("\nTop time-of-day slices:")
    for r in out["top_time"]:
        print(f"  {r['signal']:25s} × {r['bucket']:10s} n={r['n']:>3d} wr={r['wr']*100:.0f}% ev={r['ev']*100:+.0f}%")
    print("\nTop horizon slices:")
    for r in out["top_horizon"]:
        print(f"  {r['signal']:25s} × {r['bucket']:8s} n={r['n']:>3d} wr={r['wr']*100:.0f}% ev={r['ev']*100:+.0f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
