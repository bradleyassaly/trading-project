"""Rolling-window wallet quality score.

Replaces the alpha_score + lifetime PnL ranking with a forward-looking
composite based on recent behavior only.

Score components (all per-wallet):
  wr_60d        — win rate on resolved trades in last 60 days
  n_60d         — number of resolved trades in same window (sample size)
  size_cv       — coefficient of variation of position size (stability)
  recency_days  — days since last trade

Composite:
  wq_score = wr_60d
           * min(1.0, n_60d / 30)               # shrink toward 0 below n=30
           * exp(-recency_days / 14)            # half-life ~10 days
           / (1 + size_cv)                      # penalize erratic sizing

Range roughly 0–0.8 (a wallet at WR=70% / n=100 / fresh / CV=0.5 ≈ 0.32).
Use a hard floor of 0.10 for cohort inclusion.

Runs as a scheduled job; reads wallet_trades, writes wq_* on wallet_profiles.
"""
from __future__ import annotations

import logging
import math
import time

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

WINDOW_DAYS = 60
MIN_N_TARGET = 30  # below this we shrink the score


def recompute_all() -> dict[str, int]:
    """Recompute wq_* for every wallet with at least 1 trade in the window.

    Returns counters for observability.
    """
    cutoff = int(time.time()) - WINDOW_DAYS * 86400
    now_ts = int(time.time())

    conn = get_connection()
    try:
        # Aggregate per wallet in one pass — cheaper than per-wallet queries.
        # size is SHARES; bet-size stats use size*price (USDC notional).
        rows = conn.execute(
            """
            SELECT wallet,
                   COUNT(*) FILTER (WHERE pnl IS NOT NULL) AS n,
                   COUNT(*) FILTER (WHERE pnl > 0) AS wins,
                   AVG(size * price) FILTER (WHERE pnl IS NOT NULL) AS mean_bet_usd,
                   STDDEV(size * price) FILTER (WHERE pnl IS NOT NULL) AS std_bet_usd,
                   MAX(timestamp) AS last_ts
              FROM wallet_trades
             WHERE timestamp >= %s AND size > 0
             GROUP BY wallet
            HAVING COUNT(*) FILTER (WHERE pnl IS NOT NULL) > 0
            """,
            (cutoff,),
        ).fetchall()

        updated = 0
        skipped_no_profile = 0
        for r in rows:
            wallet = r[0]
            n = int(r[1] or 0)
            wins = int(r[2] or 0)
            mean_bet_usd = float(r[3] or 0)
            std_bet_usd = float(r[4] or 0)
            last_ts = int(r[5] or 0)

            if n == 0 or mean_bet_usd <= 0 or last_ts == 0:
                continue

            wr = wins / n
            size_cv = std_bet_usd / mean_bet_usd if mean_bet_usd > 0 else 1.0
            recency_days = (now_ts - last_ts) / 86400.0
            sample_factor = min(1.0, n / MIN_N_TARGET)
            recency_factor = math.exp(-recency_days / 14.0)
            cv_factor = 1.0 / (1.0 + size_cv)
            score = wr * sample_factor * recency_factor * cv_factor

            res = conn.execute(
                """
                UPDATE wallet_profiles
                   SET wq_wr_60d = %s,
                       wq_n_60d = %s,
                       wq_size_cv = %s,
                       wq_recency_days = %s,
                       wq_score = %s,
                       wq_updated_at = %s
                 WHERE wallet = %s
                """,
                (
                    round(wr, 4), n, round(size_cv, 4),
                    round(recency_days, 2), round(score, 6), now_ts, wallet,
                ),
            )
            try:
                rc = res.rowcount  # may be None on some adapters
            except Exception:
                rc = None
            if rc == 0:
                skipped_no_profile += 1
            else:
                updated += 1

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info(
        "[wallet_quality] wallets updated=%d skipped(no_profile)=%d window=%dd",
        updated, skipped_no_profile, WINDOW_DAYS,
    )
    return {"updated": updated, "skipped_no_profile": skipped_no_profile}


def top_cohort(min_score: float = 0.10, min_n: int = 30, min_wr: float = 0.55,
               max_recency_days: float = 30.0, limit: int = 50) -> list[str]:
    """Return the cohort of wallets passing all quality gates, ranked by wq_score."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT wallet FROM wallet_profiles
             WHERE wq_score >= %s
               AND wq_n_60d >= %s
               AND wq_wr_60d >= %s
               AND wq_recency_days <= %s
             ORDER BY wq_score DESC NULLS LAST
             LIMIT %s
            """,
            (min_score, min_n, min_wr, max_recency_days, limit),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    stats = recompute_all()
    print(f"[wallet_quality] recomputed: updated={stats['updated']}, "
          f"skipped_no_profile={stats['skipped_no_profile']}")
    cohort = top_cohort(limit=10)
    print(f"[wallet_quality] top-10 cohort (preview): {cohort}")


if __name__ == "__main__":
    main()
