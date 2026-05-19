"""
Wallet auto-discovery job.

Mines wallet_trades for high-performing wallets that are NOT yet in the
leaderboard and adds them automatically. Runs daily.

Criteria to qualify for auto-discovery:
- resolved_trades >= MIN_RESOLVED (30 by default)
- win_rate >= MIN_WR (0.55)
- total_pnl >= MIN_PNL (50_000)
- Not already in leaderboard

Tier assignment:
- tier1h: WR >= 0.70 and total_pnl >= 100_000
- tier1:  WR >= 0.60 or total_pnl >= 100_000
- tier2:  everyone else that qualifies

Usage::
    python -m trading_platform.polymarket.wallet_auto_discovery
    python -m trading_platform.polymarket.wallet_auto_discovery --dry-run
    python -m trading_platform.polymarket.wallet_auto_discovery --min-pnl 25000
"""
from __future__ import annotations

import argparse
import logging
import math
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

MIN_RESOLVED = 30
MIN_WR = 0.55
MIN_PNL = 50_000.0


def _wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    spread = (z / denom) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, centre - spread)


def _assign_tier(wr: float, pnl: float) -> str:
    if wr >= 0.70 and pnl >= 100_000:
        return "tier1h"
    if wr >= 0.60 or pnl >= 100_000:
        return "tier1"
    return "tier2"


def discover(
    min_resolved: int = MIN_RESOLVED,
    min_wr: float = MIN_WR,
    min_pnl: float = MIN_PNL,
    dry_run: bool = False,
) -> dict:
    from trading_platform.polymarket.db_connection import get_connection

    conn = get_connection()
    try:
        # Find high-performing wallets not in leaderboard
        candidates = conn.execute("""
            SELECT
                wt.wallet,
                COUNT(DISTINCT wt.condition_id)                                      AS markets,
                SUM(CASE WHEN wt.market_resolved=1 AND wt.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN wt.market_resolved=1 THEN 1 ELSE 0 END)                AS resolved,
                ROUND(SUM(COALESCE(wt.pnl, 0))::numeric, 2)                         AS total_pnl,
                ROUND(AVG(wt.price)::numeric, 3)                                     AS avg_entry,
                ROUND(AVG(wt.size)::numeric, 2)                                      AS avg_size,
                MAX(wt.timestamp)                                                     AS last_ts,
                MODE() WITHIN GROUP (ORDER BY LOWER(wt.category))                    AS primary_cat
            FROM wallet_trades wt
            LEFT JOIN leaderboard l ON wt.wallet = l.wallet
            WHERE l.wallet IS NULL
              AND wt.pnl IS NOT NULL
            GROUP BY wt.wallet
            HAVING SUM(CASE WHEN wt.market_resolved=1 THEN 1 ELSE 0 END) >= %s
               AND SUM(COALESCE(wt.pnl, 0)) >= %s
               AND (
                   SUM(CASE WHEN wt.market_resolved=1 AND wt.pnl > 0 THEN 1 ELSE 0 END)::float /
                   NULLIF(SUM(CASE WHEN wt.market_resolved=1 THEN 1 ELSE 0 END), 0)
               ) >= %s
            ORDER BY total_pnl DESC
        """, (min_resolved, min_pnl, min_wr)).fetchall()
    finally:
        conn.close()

    if not candidates:
        logger.info("No new wallets qualify for auto-discovery")
        return {"discovered": 0, "added": 0}

    logger.info("Found %d qualifying wallets not in leaderboard", len(candidates))

    if dry_run:
        print(f"DRY RUN — {len(candidates)} wallets would be added:")
        print(f"  {'Wallet':<16} {'WR':>5} {'n':>6} {'PnL':>12}  {'Tier':<8} {'Category'}")
        print(f"  {'-'*75}")
        for row in candidates:
            wallet, markets, wins, resolved, pnl, avg_e, avg_s, last_ts, cat = row
            wr = wins / resolved if resolved else 0
            tier = _assign_tier(wr, float(pnl))
            print(f"  {wallet[:14]}  {wr:>4.0%}  {resolved:>6}  ${float(pnl):>10,.0f}  {tier:<8} {cat or 'n/a'}")
        return {"discovered": len(candidates), "added": 0, "dry_run": True}

    now_ts = int(time.time())
    added = 0
    conn = get_connection()
    try:
        for row in candidates:
            wallet, markets, wins, resolved, pnl, avg_e, avg_s, last_ts, cat = row
            wr = wins / resolved if resolved else 0.0
            wilson = _wilson_lower(int(wins), int(resolved))
            tier = _assign_tier(wr, float(pnl))

            conn.execute("""
                INSERT INTO leaderboard (
                    wallet, tier, directional_win_rate, net_pnl_usdc,
                    avg_position_size_usdc, primary_category,
                    resolved_trades, last_trade_ts,
                    leaderboard_source, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'auto_discovery', %s)
                ON CONFLICT (wallet) DO NOTHING
            """, (
                wallet, tier, round(wr, 3), float(pnl),
                float(avg_s or 0), cat,
                int(resolved), int(last_ts or 0),
                now_ts,
            ))

            conn.execute("""
                INSERT INTO wallet_poll_state (wallet, last_checked_ts, trades_found, updated_at)
                VALUES (%s, 0, 0, %s)
                ON CONFLICT (wallet) DO NOTHING
            """, (wallet, now_ts))

            added += 1
            logger.info(
                "Added wallet %s: tier=%s WR=%.1f%% n=%d pnl=%.0f cat=%s",
                wallet[:16], tier, wr * 100, resolved, float(pnl), cat,
            )

        conn.commit()
    finally:
        conn.close()

    print(f"Auto-discovery: added {added} new wallets to leaderboard and polling")
    return {"discovered": len(candidates), "added": added}


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="Auto-discover high-performing wallets")
    p.add_argument("--dry-run", action="store_true", help="Show what would be added without writing")
    p.add_argument("--min-resolved", type=int, default=MIN_RESOLVED)
    p.add_argument("--min-wr", type=float, default=MIN_WR)
    p.add_argument("--min-pnl", type=float, default=MIN_PNL)
    args = p.parse_args()

    result = discover(
        min_resolved=args.min_resolved,
        min_wr=args.min_wr,
        min_pnl=args.min_pnl,
        dry_run=args.dry_run,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
