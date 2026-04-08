"""
CLI command: trading-cli data polymarket ingest-top-wallets

Fetches Polymarket's official leaderboard and backfills missing wallets.
"""
from __future__ import annotations

import argparse


def cmd_polymarket_ingest_top_wallets(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor

    limit = getattr(args, "limit", 200)
    window = getattr(args, "window", "all")
    by_volume = getattr(args, "by_volume", False)
    dry_run = getattr(args, "dry_run", False)

    ing = PolymarketLeaderboardIngestor()
    result = ing.ingest_top_wallets(limit=limit, window=window, by_volume=by_volume, dry_run=dry_run)

    print()
    print(f"[DONE] Leaderboard ingest complete in {result['elapsed_seconds']}s")
    print(f"  Leaderboard wallets : {result['leaderboard_wallets_found']}")
    print(f"  Already in DB       : {result['wallets_already_in_db']}")
    print(f"  Missing             : {result['wallets_missing']}")
    print(f"  Trades fetched      : {result['trades_fetched']}")
    if dry_run:
        print("  (DRY RUN — no data written)")


def cmd_polymarket_backfill_wallet(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor

    addr = getattr(args, "address", "")
    if not addr:
        print("[ERROR] --address required")
        return

    ing = PolymarketLeaderboardIngestor()
    result = ing.backfill_wallet(addr)
    print()
    print(f"[DONE] Backfilled {result['wallet'][:20]}...")
    print(f"  Trades fetched : {result['trades_fetched']}")
    print(f"  Elapsed        : {result['elapsed_seconds']}s")


def cmd_polymarket_discover_market_wallets(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor

    markets = getattr(args, "markets", 50)
    traders = getattr(args, "traders", 100)
    days_back = getattr(args, "days_back", 30)
    dry_run = getattr(args, "dry_run", False)

    ing = PolymarketLeaderboardIngestor()
    result = ing.discover_wallets_from_markets(
        markets_limit=markets,
        traders_per_market=traders,
        days_back=days_back,
        dry_run=dry_run,
    )

    print()
    print(f"[DONE] Market discovery complete in {result['elapsed_seconds']}s")
    print(f"  Markets scanned       : {result['markets_scanned']}")
    print(f"  Unique wallets found  : {result['unique_wallets_found']}")
    print(f"  Already in DB         : {result['wallets_already_in_db']}")
    print(f"  New wallets           : {result['wallets_new']}")
    print(f"  Trades fetched        : {result['trades_fetched']}")
    if dry_run:
        print("  (DRY RUN — no data written)")


def cmd_polymarket_backfill_top_wallets(args: argparse.Namespace) -> None:
    """Backfill top 50 wallets from leaderboard table by net_pnl_usdc."""
    from trading_platform.polymarket.wallet_db import WalletDB
    from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor

    limit = getattr(args, "limit", 50)
    db = WalletDB()
    with db._lock:
        rows = db._conn.execute(
            "SELECT wallet, net_pnl_usdc FROM leaderboard ORDER BY net_pnl_usdc DESC LIMIT ?",
            (limit,),
        ).fetchall()

    if not rows:
        print("[ERROR] No wallets in leaderboard. Run build-leaderboard first.")
        return

    ing = PolymarketLeaderboardIngestor(db=db)
    print(f"Backfilling {len(rows)} top wallets...")
    total = 0
    for i, (wallet, pnl) in enumerate(rows, 1):
        print(f"  [{i}/{len(rows)}] {wallet[:16]}... (PnL: ${(pnl or 0):,.0f})", end=" ")
        result = ing.backfill_wallet(wallet)
        total += result["trades_fetched"]
        print(f"{result['trades_fetched']} trades")

    print()
    print(f"[DONE] Total trades fetched: {total}")
