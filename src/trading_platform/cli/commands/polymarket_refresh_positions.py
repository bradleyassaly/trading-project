"""
CLI command: trading-cli data polymarket refresh-positions

Fetches current open positions for tier1h (and optionally tier1/tier2)
wallets from Polymarket positions API and stores in wallet_positions.
"""
from __future__ import annotations

import argparse
import sqlite3


def cmd_polymarket_refresh_positions(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.position_fetcher import PositionFetcher
    from trading_platform.polymarket.wallet_db import WalletDB

    tier = getattr(args, "tier", "tier1h")
    limit = getattr(args, "limit", 50)

    db = WalletDB()
    conn = sqlite3.connect(str(db._path))

    if tier == "all":
        rows = conn.execute(
            "SELECT wallet FROM leaderboard WHERE tier IN ('tier1h','tier1','tier2') LIMIT ?",
            (limit,),
        ).fetchall()
    elif tier in ("tier1h", "tier1", "tier2"):
        rows = conn.execute(
            "SELECT wallet FROM leaderboard WHERE tier = ? LIMIT ?",
            (tier, limit),
        ).fetchall()
    else:
        print(f"[ERROR] invalid tier: {tier}")
        return

    conn.close()
    wallets = [r[0] for r in rows]
    if not wallets:
        print(f"[INFO] No wallets found for tier={tier}")
        return

    print(f"Refreshing positions for {len(wallets)} wallets (tier={tier})...")
    fetcher = PositionFetcher()
    results = fetcher.fetch_batch(wallets, str(db._path), sleep_between=0.3)

    total = sum(results.values())
    print()
    print(f"[DONE] Refreshed {len(results)} wallets, {total} positions stored")
