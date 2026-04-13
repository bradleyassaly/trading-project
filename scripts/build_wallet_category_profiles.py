"""
Build wallet_category_profiles from resolved wallet_trades.

Background: the wallet_category_profiles table was a stub (2 rows) while the
rich data lived in wallet_trades (410k rows, 62k with reliable PnL). This
script aggregates resolved + pnl-reliable trades per (wallet, category) and
assigns tier letters S/A/B/C/D based on win rate, resolved count, and
total PnL. It's designed to be run offline after category backfill, not
from the hot signal path.

Usage: .venv/Scripts/python.exe scripts/build_wallet_category_profiles.py
"""
from __future__ import annotations

import math
import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = Path("data/polymarket/wallet_intelligence.db")

# Tier thresholds. Tuned to put the clear winners in S/A, the middle in B/C,
# and the losers in D. Win rate is Laplace-smoothed so tiny samples don't
# pop to 1.0 on a single lucky fill.
TIER_RULES = [
    # (tier, min_win_rate, min_resolved, min_total_pnl)
    ("S", 0.65, 30, 50_000),
    ("A", 0.58, 15, 10_000),
    ("B", 0.53,  8,  1_000),
    ("C", 0.45,  3,      0),
]
# Anything that falls through the rules is D.


def smoothed_win_rate(wins: int, losses: int, prior: float = 0.5, strength: int = 4) -> float:
    n = wins + losses
    if n == 0:
        return 0.0
    return (wins + prior * strength) / (n + strength)


def tier_for(win_rate: float, resolved: int, total_pnl: float) -> str:
    for t, wr_floor, res_floor, pnl_floor in TIER_RULES:
        if win_rate >= wr_floor and resolved >= res_floor and total_pnl >= pnl_floor:
            return t
    return "D"


def main() -> int:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")

    print("[BUILD] aggregating resolved trades by (wallet, category)...")
    rows = conn.execute("""
        SELECT wallet,
               category,
               COUNT(*)                                              AS resolved,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)              AS wins,
               SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END)             AS losses,
               SUM(COALESCE(pnl, 0))                                 AS total_pnl,
               AVG(size)                                             AS avg_bet_size,
               MAX(timestamp)                                        AS last_trade_at,
               SUM(CASE
                   WHEN timestamp > unixepoch('now', '-30 days') AND pnl > 0 THEN 1
                   ELSE 0
               END)                                                  AS wins_30d,
               SUM(CASE
                   WHEN timestamp > unixepoch('now', '-30 days') THEN 1
                   ELSE 0
               END)                                                  AS resolved_30d,
               SUM(CASE
                   WHEN timestamp > unixepoch('now', '-90 days') AND pnl > 0 THEN 1
                   ELSE 0
               END)                                                  AS wins_90d,
               SUM(CASE
                   WHEN timestamp > unixepoch('now', '-90 days') THEN 1
                   ELSE 0
               END)                                                  AS resolved_90d,
               SUM(CASE
                   WHEN timestamp > unixepoch('now', '-30 days') THEN COALESCE(pnl, 0)
                   ELSE 0
               END)                                                  AS monthly_pnl
        FROM wallet_trades
        WHERE market_resolved = 1
          AND pnl IS NOT NULL
          AND pnl_reliable = 1
          AND category IS NOT NULL
          AND category != ''
        GROUP BY wallet, category
        HAVING resolved >= 3
    """).fetchall()
    print(f"[BUILD] {len(rows)} (wallet, category) pairs with >=3 resolved trades")

    # Category purity: fraction of that wallet's resolved trades in this category.
    print("[BUILD] computing category purity (per-wallet total resolved)...")
    totals = dict(conn.execute("""
        SELECT wallet, COUNT(*)
        FROM wallet_trades
        WHERE market_resolved = 1 AND pnl IS NOT NULL AND pnl_reliable = 1
        GROUP BY wallet
    """).fetchall())

    now_ts = int(time.time())
    records = []
    tier_counts: dict[str, int] = {}
    for (wallet, category, resolved, wins, losses, total_pnl,
         avg_bet_size, last_trade_at, wins_30d, resolved_30d,
         wins_90d, resolved_90d, monthly_pnl) in rows:

        wr = smoothed_win_rate(wins or 0, losses or 0)
        wr_raw = (wins or 0) / max(resolved, 1)
        wr_30 = smoothed_win_rate(wins_30d or 0, (resolved_30d or 0) - (wins_30d or 0))
        wr_90 = smoothed_win_rate(wins_90d or 0, (resolved_90d or 0) - (wins_90d or 0))

        total_for_wallet = totals.get(wallet, resolved)
        purity = resolved / max(total_for_wallet, 1)

        # Consistency: how close is the 30d WR to the 90d WR to the all-time WR?
        # Low variance => consistent. Use stddev of the three smoothed rates.
        series = [wr, wr_30, wr_90]
        mean = sum(series) / 3
        variance = sum((x - mean) ** 2 for x in series) / 3
        consistency = max(0.0, 1.0 - math.sqrt(variance) * 3)  # 0 = unstable, 1 = rock solid

        # Tier score: blend of smoothed WR, resolved count, total PnL, consistency.
        # Monotonic in each so it's useful for sorting within a tier.
        score = (
            wr * 40
            + min(resolved / 50, 1.0) * 20
            + min(max(total_pnl, 0) / 100_000, 1.0) * 20
            + consistency * 10
            + purity * 10
        )

        tier = tier_for(wr_raw, resolved, total_pnl)
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

        records.append((
            wallet, category, tier, round(score, 2),
            round(wr_raw, 4), round(wr_30, 4), round(wr_90, 4),
            resolved, round(avg_bet_size or 0, 2), round(monthly_pnl or 0, 2),
            round(consistency, 4), round(purity, 4), 0.0,
            last_trade_at or 0, now_ts, 0, now_ts,
        ))

    print(f"[BUILD] tier distribution: {tier_counts}")

    print("[BUILD] replacing wallet_category_profiles...")
    conn.execute("DELETE FROM wallet_category_profiles")
    conn.executemany(
        """INSERT INTO wallet_category_profiles (
            wallet, category, tier, tier_score,
            win_rate, win_rate_30d, win_rate_90d,
            resolved_trades, avg_bet_size, monthly_pnl,
            consistency_score, category_purity, trend_score,
            last_trade_at, last_evaluated, resolved_since_last_change, last_change_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    print(f"[BUILD] wrote {len(records)} profiles")

    # Report
    print("\n=== tier x category distribution ===")
    for row in conn.execute("""
        SELECT category, tier, COUNT(*) AS n,
               ROUND(AVG(win_rate), 3)     AS avg_wr,
               ROUND(SUM(monthly_pnl), 0)  AS sum_30d_pnl
        FROM wallet_category_profiles
        GROUP BY category, tier
        ORDER BY category,
                 CASE tier WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3
                           WHEN 'C' THEN 4 ELSE 5 END
    """).fetchall():
        print("  ", row)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
