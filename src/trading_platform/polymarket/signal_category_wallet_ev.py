"""3-dimensional EV slicer: signal × category × wallet_tier.

Extends signal_category_ev (2-dim) with wallet quality. Hypothesis:
the same (signal, category) slice has very different EV depending on
the source wallet's tier. e.g.:
  wallet_reversal × politics × tier1h: maybe +35% EV (insider)
  wallet_reversal × politics × tier2:  maybe -5% EV (noise)
Aggregating across tiers obscures this.

Output: signal_category_wallet_ev table. Each row =
  (signal_type, category, wallet_tier, n, wr, avg_ev, sum_pnl).

Read by paper_executor + live_executor for sharper allowlists.
Daily cadence (matches alpha-score refresh).
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


MIN_N_PER_SLICE = 5
WINDOW_DAYS = 30


_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_category_wallet_ev (
    signal_type   TEXT NOT NULL,
    category      TEXT NOT NULL,
    wallet_tier   TEXT NOT NULL,
    n_resolved    BIGINT NOT NULL,
    wins          BIGINT NOT NULL,
    win_rate      DOUBLE PRECISION,
    avg_ev        DOUBLE PRECISION,
    sum_pnl       DOUBLE PRECISION,
    computed_at   BIGINT NOT NULL,
    PRIMARY KEY (signal_type, category, wallet_tier)
);
CREATE INDEX IF NOT EXISTS idx_scwe_avg_ev ON signal_category_wallet_ev(avg_ev DESC);
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
        cutoff = int(time.time()) - WINDOW_DAYS * 86400
        rows = conn.execute(
            """SELECT pt.signal_type,
                      COALESCE(m.subcategory, pt.category, 'unknown') AS slice_cat,
                      COALESCE(pt.wallet_tier_at_fire, 'unknown') AS slice_tier,
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
                GROUP BY pt.signal_type,
                         COALESCE(m.subcategory, pt.category, 'unknown'),
                         COALESCE(pt.wallet_tier_at_fire, 'unknown')""",
            (cutoff,),
        ).fetchall()
        now = int(time.time())
        for r in rows:
            sig, cat, tier, n, wins, avg_ev, sum_pnl = r
            n = int(n or 0)
            if n < MIN_N_PER_SLICE:
                n_skipped += 1
                continue
            wr = (int(wins or 0) / n) if n > 0 else 0.0
            try:
                conn.execute(
                    """INSERT INTO signal_category_wallet_ev
                         (signal_type, category, wallet_tier, n_resolved, wins,
                          win_rate, avg_ev, sum_pnl, computed_at)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type, category, wallet_tier) DO UPDATE SET
                         n_resolved = EXCLUDED.n_resolved,
                         wins = EXCLUDED.wins,
                         win_rate = EXCLUDED.win_rate,
                         avg_ev = EXCLUDED.avg_ev,
                         sum_pnl = EXCLUDED.sum_pnl,
                         computed_at = EXCLUDED.computed_at""",
                    (sig, cat, tier, n, int(wins or 0), round(wr, 4),
                     round(float(avg_ev or 0), 4),
                     round(float(sum_pnl or 0), 2), now),
                )
                n_slices += 1
            except Exception as exc:
                logger.debug("slice upsert failed %s/%s/%s: %s", sig, cat, tier, exc)
        conn.commit()
        top = conn.execute(
            """SELECT signal_type, category, wallet_tier, n_resolved, win_rate, avg_ev, sum_pnl
                 FROM signal_category_wallet_ev
                WHERE n_resolved >= 5
                ORDER BY avg_ev DESC LIMIT 12"""
        ).fetchall()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "slices_computed": n_slices,
            "slices_skipped_low_n": n_skipped,
            "top_slices": [
                {"signal": r[0], "category": r[1], "tier": r[2],
                 "n": r[3], "wr": float(r[4]), "ev": float(r[5]), "pnl": float(r[6])}
                for r in top
            ],
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_slicer()
    print(f"computed {out['slices_computed']} 3D slices in {out['elapsed_seconds']}s")
    print("Top 12 (signal × category × tier) by EV (n>=5):")
    for r in out.get("top_slices", []):
        print(f"  {r['signal']:25s} × {r['category']:18s} × {r['tier']:8s} n={r['n']:>3d} wr={r['wr']*100:.0f}% ev={r['ev']*100:+.1f}% pnl=${r['pnl']:+.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
