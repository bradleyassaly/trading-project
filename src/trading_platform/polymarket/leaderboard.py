"""
Build the wallet leaderboard from profiles and trade data.

Ranks wallets by conviction score, assigns tier1/tier2, applies
rolling WR demotion, and writes atomically to the leaderboard table.

Usage::

    from trading_platform.polymarket.leaderboard import build_leaderboard
    result = build_leaderboard()
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)

TIER1_MIN_WR = 0.58
TIER1_MIN_RESOLVED = 10
TIER1_MIN_VOLUME = 5_000
TIER2_MIN_WR = 0.53
TIER2_MIN_RESOLVED = 5
TIER2_MIN_VOLUME = 1_000
DEMOTION_ROLLING_WR = 0.45


def build_leaderboard(db: WalletDB | None = None) -> dict[str, Any]:
    """Rebuild leaderboard from wallet_profiles + wallet_trades.

    Only commits if at least 1 wallet qualifies — never overwrites
    a valid leaderboard with an empty one.
    """
    db = db or WalletDB()
    t0 = time.time()
    version = db.get_leaderboard_version() + 1
    db.begin_leaderboard_rebuild(version)

    conn = sqlite3.connect(str(db._path))
    rows = conn.execute("""
        SELECT wallet, directional_win_rate, conviction_score,
               resolved_trades, total_volume_usdc, wallet_type,
               last_trade_ts
        FROM wallet_profiles
        WHERE directional_win_rate IS NOT NULL
    """).fetchall()
    conn.close()

    tier1_list: list[str] = []
    tier2_list: list[str] = []
    demoted = 0
    insufficient = 0
    leaderboard_rows: list[dict[str, Any]] = []

    for row in rows:
        wallet, wr, conv, resolved, volume, wtype, last_ts = row
        resolved = resolved or 0
        volume = volume or 0

        # Demotion check via rolling window
        rolling = db.compute_rolling_wr(wallet, n=20)
        if rolling is not None and rolling < DEMOTION_ROLLING_WR:
            demoted += 1
            continue

        # Tier assignment
        if (wr >= TIER1_MIN_WR
                and resolved >= TIER1_MIN_RESOLVED
                and volume >= TIER1_MIN_VOLUME):
            tier = "tier1"
            tier1_list.append(wallet)
        elif (wr >= TIER2_MIN_WR
              and resolved >= TIER2_MIN_RESOLVED
              and volume >= TIER2_MIN_VOLUME):
            tier = "tier2"
            tier2_list.append(wallet)
        else:
            insufficient += 1
            continue

        leaderboard_rows.append({
            "wallet": wallet,
            "tier": tier,
            "directional_win_rate": wr,
            "rolling_20_wr": rolling,
            "conviction_score": conv or 0.0,
            "resolved_trades": resolved,
            "total_volume_usdc": volume,
            "wallet_type": wtype,
            "last_trade_ts": last_ts,
            "version": version,
            "updated_at": int(time.time()),
        })

    if not leaderboard_rows:
        logger.warning("Leaderboard build produced 0 wallets — keeping previous version")
        print("[WARNING] 0 wallets qualified — previous leaderboard preserved.")
        return {
            "version": version - 1,
            "tier1": 0,
            "tier2": 0,
            "demoted": demoted,
            "insufficient": insufficient,
            "elapsed_seconds": round(time.time() - t0, 1),
            "committed": False,
        }

    # Rank within tier by conviction_score
    leaderboard_rows.sort(key=lambda r: (0 if r["tier"] == "tier1" else 1, -(r["conviction_score"] or 0)))
    for i, r in enumerate(leaderboard_rows):
        r["rank"] = i + 1

    # Write to DB
    conn = sqlite3.connect(str(db._path))
    conn.execute("DELETE FROM leaderboard")
    for r in leaderboard_rows:
        conn.execute("""
            INSERT OR REPLACE INTO leaderboard
                (wallet, tier, directional_win_rate, rolling_20_wr,
                 conviction_score, resolved_trades, total_volume_usdc,
                 wallet_type, last_trade_ts, rank, version, updated_at)
            VALUES
                (:wallet, :tier, :directional_win_rate, :rolling_20_wr,
                 :conviction_score, :resolved_trades, :total_volume_usdc,
                 :wallet_type, :last_trade_ts, :rank, :version, :updated_at)
        """, r)
    conn.commit()
    conn.close()

    db.commit_leaderboard(version, len(tier1_list), len(tier2_list))

    elapsed = round(time.time() - t0, 1)
    print(
        f"[LEADERBOARD v{version}] tier1={len(tier1_list)} tier2={len(tier2_list)} "
        f"demoted={demoted} insufficient={insufficient} elapsed={elapsed}s"
    )

    top5 = [r for r in leaderboard_rows if r["tier"] == "tier1"][:5]
    for r in top5:
        print(
            f"  {r['wallet'][:12]}... "
            f"WR={r['directional_win_rate']:.1%} "
            f"conv={r['conviction_score']:.3f} "
            f"resolved={r['resolved_trades']}"
        )

    return {
        "version": version,
        "tier1": len(tier1_list),
        "tier2": len(tier2_list),
        "demoted": demoted,
        "insufficient": insufficient,
        "elapsed_seconds": elapsed,
        "committed": True,
    }
