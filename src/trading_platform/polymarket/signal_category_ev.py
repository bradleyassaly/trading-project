"""Per-(signal_type × category) EV slicer.

The signal_engine_backtest produces aggregate EV per signal_type
(e.g. wallet_reversal +10.1%). But within a signal, EV varies wildly
by category — wallet_reversal might be +25% in politics and -2% in
crypto. Aggregate masks the dispersion.

This module computes EV per (signal_type, subcategory) tuple from
polymarket_paper_trades. Output:
  signal_category_ev table:
    signal_type, category, n_resolved, wins, win_rate, avg_ev,
    sum_pnl, computed_at

The live executor and tier-promotion logic can then read this table
to make sharper allowlist decisions: instead of "wallet_reversal is
allowed live", it becomes "wallet_reversal × politics is allowed live;
wallet_reversal × entertainment requires more samples".

Run via scheduler at 6h cadence — fast enough to react to regime
shifts, slow enough not to overfit on noise.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# Minimum n per slice to record. Below this, the EV is too noisy.
MIN_N_PER_SLICE = 5

# Window — match signal_health cadence
WINDOW_DAYS = 30


_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_category_ev (
    signal_type   TEXT NOT NULL,
    category      TEXT NOT NULL,
    n_resolved    BIGINT NOT NULL,
    wins          BIGINT NOT NULL,
    win_rate      DOUBLE PRECISION,
    avg_ev        DOUBLE PRECISION,
    sum_pnl       DOUBLE PRECISION,
    computed_at   BIGINT NOT NULL,
    PRIMARY KEY (signal_type, category)
);
CREATE INDEX IF NOT EXISTS idx_sce_avg_ev ON signal_category_ev(avg_ev DESC);
CREATE INDEX IF NOT EXISTS idx_sce_signal ON signal_category_ev(signal_type, avg_ev DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def run_slicer() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    n_slices = 0
    n_skipped = 0
    try:
        _ensure_schema(conn)
        # Compute fresh slices from paper trades
        cutoff = int(time.time()) - WINDOW_DAYS * 86400
        rows = conn.execute(
            """SELECT pt.signal_type,
                      COALESCE(m.subcategory, pt.category, 'unknown') AS slice_cat,
                      COUNT(*) AS n,
                      SUM(CASE WHEN pt.outcome='win' THEN 1 ELSE 0 END) AS wins,
                      AVG(pt.realized_pnl / NULLIF(pt.size_usd, 0)) AS avg_ev,
                      SUM(pt.realized_pnl) AS sum_pnl
                 FROM polymarket_paper_trades pt
                 LEFT JOIN markets m ON m.condition_id = pt.condition_id
                WHERE pt.archived = 0
                  AND pt.exit_ts IS NOT NULL
                  AND pt.entry_ts > ?
                  AND pt.outcome IN ('win','loss')
                  AND pt.realized_pnl IS NOT NULL
                  AND pt.size_usd > 0
                GROUP BY pt.signal_type, COALESCE(m.subcategory, pt.category, 'unknown')""",
            (cutoff,),
        ).fetchall()
        now = int(time.time())
        for r in rows:
            sig, cat, n, wins, avg_ev, sum_pnl = r
            n = int(n or 0)
            if n < MIN_N_PER_SLICE:
                n_skipped += 1
                continue
            wr = (int(wins or 0) / n) if n > 0 else 0.0
            try:
                conn.execute(
                    """INSERT INTO signal_category_ev
                         (signal_type, category, n_resolved, wins, win_rate,
                          avg_ev, sum_pnl, computed_at)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type, category) DO UPDATE SET
                         n_resolved = EXCLUDED.n_resolved,
                         wins = EXCLUDED.wins,
                         win_rate = EXCLUDED.win_rate,
                         avg_ev = EXCLUDED.avg_ev,
                         sum_pnl = EXCLUDED.sum_pnl,
                         computed_at = EXCLUDED.computed_at""",
                    (sig, cat, n, int(wins or 0), round(wr, 4),
                     round(float(avg_ev or 0), 4),
                     round(float(sum_pnl or 0), 2), now),
                )
                n_slices += 1
            except Exception as exc:
                logger.debug("slice upsert failed %s/%s: %s", sig, cat, exc)
        conn.commit()
        # Top winners
        top = conn.execute(
            """SELECT signal_type, category, n_resolved, win_rate, avg_ev, sum_pnl
                 FROM signal_category_ev
                WHERE n_resolved >= 10
                ORDER BY avg_ev DESC LIMIT 10"""
        ).fetchall()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "slices_computed": n_slices,
            "slices_skipped_low_n": n_skipped,
            "top_slices": [
                {"signal": r[0], "category": r[1], "n": r[2],
                 "wr": float(r[3]), "ev": float(r[4]), "pnl": float(r[5])}
                for r in top
            ],
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_slicer()
    print(f"computed {out['slices_computed']} slices in {out['elapsed_seconds']}s")
    print("Top 10 (signal × category) by EV (n>=10):")
    for r in out.get("top_slices", []):
        print(f"  {r['signal']:25s} × {r['category']:20s} n={r['n']:>3d} wr={r['wr']*100:.0f}% ev={r['ev']*100:+.1f}% pnl=${r['pnl']:+.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
