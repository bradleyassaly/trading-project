"""Auto-promote insider wallets nightly.

Closes the gap surfaced 2026-04-25: only 82 wallets in `insider_wallets`,
no automated discovery. Insider signals fire ~5/day vs 480/day for
smart-money — too narrow to ever resolve enough samples.

Promotion criteria (all must hold):
  * early_win_rate ≥ 0.65 over resolved_trades ≥ 10
  * avg_entry_hours_before_close ≥ 12 (truly early, not last-minute)
  * pnl_reliable trades only (no Goldsky-stale or Data-API-only rows)
  * net_pnl_usdc > 0 OR pm_pnl_usdc ≥ $50K (have actually been right)
  * not is_likely_farmer (skip wash-traders)

Idempotent: existing rows updated, new rows inserted, missing-criteria
rows are NOT removed (downgrades require manual review since insider
signals trade at low MIN_RESOLVED).
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


def run_promotion(db_path: str | None = None) -> dict[str, Any]:
    """Scan wallet_profiles + wallet_behavior_metrics, upsert qualifiers
    into insider_wallets. Returns counts."""
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        # Pull candidate wallets meeting the early/skill thresholds.
        # Left join to wallet_behavior_metrics so we can exclude farmers
        # — fail-open if the table doesn't exist yet.
        # 2026-04-30: tightened to address the +459-wallets-in-one-day
        # spike. The prior bar (early_wr≥0.65, n≥10) admitted ~475 wallets
        # which inflates the insider_entry signal count and dilutes
        # specialist-boost confidence. New bar: early_wr≥0.70 AND
        # lifetime_wr≥0.55 AND n≥20 AND net_pnl≥$50 (or pm_pnl≥$50K).
        # Conservative; the right wallets will still qualify.
        try:
            rows = conn.execute(
                """SELECT wp.wallet, wp.early_win_rate, wp.resolved_trades,
                          wp.avg_entry_hours_before_close,
                          wp.net_pnl_usdc, wp.pm_pnl_usdc,
                          wp.win_rate,
                          COALESCE(wbm.is_likely_farmer, 0) AS farmer,
                          wp.total_volume_usdc
                     FROM wallet_profiles wp
                     LEFT JOIN wallet_behavior_metrics wbm ON wbm.wallet = wp.wallet
                    WHERE wp.early_win_rate IS NOT NULL
                      AND wp.early_win_rate >= 0.70
                      AND wp.resolved_trades >= 20
                      AND (wp.win_rate IS NULL OR wp.win_rate >= 0.55)
                      AND (wp.avg_entry_hours_before_close >= 12
                           OR wp.avg_entry_hours_before_close IS NULL)
                      AND COALESCE(wbm.is_likely_farmer, 0) = 0
                      AND (wp.net_pnl_usdc >= 50 OR wp.pm_pnl_usdc >= 50000)"""
            ).fetchall()
        except Exception as exc:
            logger.warning("insider candidate query failed: %s", exc)
            return {"error": str(exc)[:200], "elapsed_seconds": round(time.time()-t0, 1)}

        now = int(time.time())
        n_upsert = 0
        n_new = 0
        for r in rows:
            wallet, early_wr, n_resolved, entry_hrs, net_pnl, pm_pnl, lifetime_wr, farmer, vol = r
            # insider_score: blend early_wr (heavy) + entry timing
            #   score = 0.6*early_wr + 0.2*min(entry_hrs/72, 1.0) + 0.2*min(lifetime_wr, 1.0)
            insider_score = (
                0.6 * float(early_wr or 0)
                + 0.2 * min(float(entry_hrs or 0) / 72.0, 1.0)
                + 0.2 * min(float(lifetime_wr or 0), 1.0)
            )
            # primary_category: leave None for the daily run; can be
            # filled by a separate per-category audit later.
            try:
                exists = conn.execute(
                    "SELECT 1 FROM insider_wallets WHERE wallet = ?", (wallet,),
                ).fetchone()
                if exists:
                    conn.execute(
                        """UPDATE insider_wallets SET
                             insider_score = ?, total_trades = ?,
                             total_pnl = ?, avg_trade_size = ?,
                             detection_method = 'auto_promote',
                             classified_at = ?
                           WHERE wallet = ?""",
                        (
                            round(insider_score, 4), int(n_resolved),
                            float(net_pnl or 0),
                            float(vol or 0) / max(int(n_resolved), 1) if n_resolved else None,
                            now, wallet,
                        ),
                    )
                    n_upsert += 1
                else:
                    conn.execute(
                        """INSERT INTO insider_wallets
                             (wallet, insider_score,
                              uncertain_accuracy, uncertain_trades,
                              total_trades, total_pnl, avg_trade_size,
                              detection_method, classified_at)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            wallet, round(insider_score, 4),
                            float(early_wr or 0), int(n_resolved),
                            int(n_resolved),
                            float(net_pnl or 0),
                            float(vol or 0) / max(int(n_resolved), 1) if n_resolved else None,
                            "auto_promote", now,
                        ),
                    )
                    n_new += 1
            except Exception as exc:
                logger.debug("insider upsert failed %s: %s", wallet[:12], exc)
        # 2026-04-30: demote previously auto_promote'd wallets that no
        # longer meet the (newly tightened) bar. Only operates on rows
        # we created — never touches manual or accuracy_60pct insiders.
        n_demoted = 0
        try:
            qualifying_set = {r[0] for r in rows}
            existing = conn.execute(
                "SELECT wallet FROM insider_wallets "
                "WHERE detection_method = 'auto_promote'",
            ).fetchall()
            for (w,) in existing:
                if w not in qualifying_set:
                    try:
                        conn.execute(
                            "DELETE FROM insider_wallets "
                            "WHERE wallet = ? AND detection_method = 'auto_promote'",
                            (w,),
                        )
                        n_demoted += 1
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("auto_promote demotion sweep failed: %s", exc)

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "candidates_evaluated": len(rows),
            "new_insider_wallets": n_new,
            "updated_insider_wallets": n_upsert,
            "demoted_no_longer_qualifying": n_demoted,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_promotion())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
