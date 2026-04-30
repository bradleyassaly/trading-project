"""Wallet discovery from signal_outcomes.

The PM leaderboard sync pulls top-500 by lifetime PnL. That's a slow
discovery channel for wallets whose alpha is recent or sub-domain
specific (sport specialists may have small lifetime PnL but high
recent alpha).

This module mines `signal_outcomes` directly: any wallet whose
fired-and-resolved signals ended profitably (is_win=1) at least
DISCOVERY_MIN_WINS times in the past LOOKBACK_DAYS gets evaluated
for promotion to either the watched-wallet set or directly to
insider_wallets if criteria are met.

Output:
  - wallet_discovery_candidates table — full evaluation history
  - Auto-promotes high-conviction discoveries into insider_wallets
    (re-using the existing insider_promote schema)

Daily cadence; chained after signal_resolver.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_discovery_candidates (
    wallet              TEXT PRIMARY KEY,
    n_resolved_signals  BIGINT,
    wins                BIGINT,
    sub_wr              DOUBLE PRECISION,
    sum_paper_pnl       DOUBLE PRECISION,
    first_signal_at     BIGINT,
    last_signal_at      BIGINT,
    promoted_to_insider BIGINT DEFAULT 0,
    last_evaluated_at   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wdc_promoted ON wallet_discovery_candidates(promoted_to_insider);
"""

LOOKBACK_DAYS = 30
# 2026-04-30: tightened thresholds. Prior settings (3W/5R/55%) caused
# +459 wallets to be promoted to insider_wallets in a single run on
# 2026-04-29 (16 → 475). At n=5, a 60% WR is one heads-flip away from
# 50% — pure noise gets through. Raising the bar both stops noise and
# preserves the goal (find genuinely-profitable wallets that lifetime-
# PnL leaderboard scan misses).
DISCOVERY_MIN_WINS = 6              # at least 6 winning signals
DISCOVERY_MIN_RESOLVED = 10         # over at least 10 resolved
DISCOVERY_MIN_WR = 0.60             # WR ≥ 60% (Wilson-95 lower bound on
                                    # 6/10 ≈ 30%, so this still allows
                                    # noise; future: switch to Wilson)
DISCOVERY_MIN_PNL = 5.0             # net +$5 paper PnL minimum


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def run_discovery(db_path: str | None = None) -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        cutoff = int(time.time()) - LOOKBACK_DAYS * 86400
        try:
            rows = conn.execute(
                """SELECT wallet,
                          COUNT(*) AS n,
                          SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END) AS wins,
                          ROUND(SUM(COALESCE(paper_pnl, 0))::numeric, 2) AS sum_pnl,
                          MIN(fired_at) AS first_at,
                          MAX(fired_at) AS last_at
                     FROM signal_outcomes
                    WHERE wallet IS NOT NULL AND wallet != ''
                      AND wallet NOT IN ('velocity_detector', 'order_book_monitor',
                                         'phase_b_resolution_decay', 'synthetic_test')
                      AND resolved_at IS NOT NULL
                      AND resolved_at > ?
                      AND is_win IN (0, 1)
                    GROUP BY wallet
                   HAVING COUNT(*) >= ?""",
                (cutoff, DISCOVERY_MIN_RESOLVED),
            ).fetchall()
        except Exception as exc:
            return {"error": str(exc)[:200]}

        n_evaluated = len(rows)
        n_qualifying = 0
        n_promoted = 0
        now = int(time.time())

        for r in rows:
            wallet, n, wins, sum_pnl, first_at, last_at = (
                r[0], int(r[1]), int(r[2] or 0), float(r[3] or 0),
                int(r[4] or 0), int(r[5] or 0),
            )
            wr = wins / n if n else 0
            qualifies = (
                wins >= DISCOVERY_MIN_WINS
                and wr >= DISCOVERY_MIN_WR
                and sum_pnl >= DISCOVERY_MIN_PNL
            )
            if qualifies:
                n_qualifying += 1

            try:
                conn.execute(
                    """INSERT INTO wallet_discovery_candidates
                         (wallet, n_resolved_signals, wins, sub_wr,
                          sum_paper_pnl, first_signal_at, last_signal_at,
                          last_evaluated_at)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT (wallet) DO UPDATE SET
                         n_resolved_signals = EXCLUDED.n_resolved_signals,
                         wins = EXCLUDED.wins,
                         sub_wr = EXCLUDED.sub_wr,
                         sum_paper_pnl = EXCLUDED.sum_paper_pnl,
                         last_signal_at = EXCLUDED.last_signal_at,
                         last_evaluated_at = EXCLUDED.last_evaluated_at""",
                    (wallet, n, wins, round(wr, 4), round(sum_pnl, 2),
                     first_at, last_at, now),
                )
            except Exception as exc:
                logger.debug("candidate upsert failed %s: %s", wallet[:12], exc)
                continue

            # Auto-promote to insider_wallets (re-use existing schema)
            if qualifies:
                try:
                    exists = conn.execute(
                        "SELECT 1 FROM insider_wallets WHERE wallet = ?",
                        (wallet,),
                    ).fetchone()
                    if not exists:
                        # Use sub_wr as both insider_score base + uncertain_accuracy
                        conn.execute(
                            """INSERT INTO insider_wallets
                                 (wallet, insider_score,
                                  uncertain_accuracy, uncertain_trades,
                                  total_trades, total_pnl, avg_trade_size,
                                  detection_method, classified_at)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            (
                                wallet, round(wr, 4),
                                wr, n, n,
                                round(sum_pnl, 2), None,
                                "signal_outcome_discovery", now,
                            ),
                        )
                        n_promoted += 1
                        # Mark in our table
                        conn.execute(
                            "UPDATE wallet_discovery_candidates "
                            "SET promoted_to_insider = 1 WHERE wallet = ?",
                            (wallet,),
                        )
                except Exception as exc:
                    logger.debug("insider promote failed %s: %s", wallet[:12], exc)

        # 2026-04-30: demote previously-promoted wallets that no longer
        # qualify under the current (tightened) thresholds. Closes the
        # discovery loop self-correctingly: yesterday's 459-spike rows
        # get demoted on the next run when they fail the new bar.
        # Only operates on rows we created — never touches insiders
        # detected by other methods (accuracy_60pct etc.).
        n_demoted = 0
        try:
            promoted_rows = conn.execute(
                "SELECT wallet FROM insider_wallets "
                "WHERE detection_method = 'signal_outcome_discovery'",
            ).fetchall()
            qualifying_set = {
                r[0] for r in rows
                if (
                    int(r[2] or 0) >= DISCOVERY_MIN_WINS
                    and (int(r[2] or 0) / int(r[1])) >= DISCOVERY_MIN_WR
                    and float(r[3] or 0) >= DISCOVERY_MIN_PNL
                )
            }
            for (w,) in promoted_rows:
                if w not in qualifying_set:
                    try:
                        conn.execute(
                            "DELETE FROM insider_wallets "
                            "WHERE wallet = ? AND detection_method = 'signal_outcome_discovery'",
                            (w,),
                        )
                        conn.execute(
                            "UPDATE wallet_discovery_candidates "
                            "SET promoted_to_insider = 0 WHERE wallet = ?",
                            (w,),
                        )
                        n_demoted += 1
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("demotion sweep failed: %s", exc)

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "wallets_evaluated": n_evaluated,
            "qualifying": n_qualifying,
            "newly_promoted_to_insider": n_promoted,
            "demoted_no_longer_qualifying": n_demoted,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_discovery())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
