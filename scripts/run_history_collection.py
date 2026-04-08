#!/usr/bin/env python
"""
Bootstrap historical data for the backtesting system.

Steps:
  1. Pick all watched wallets from leaderboard
  2. Backfill 6 months of trades per wallet
  3. Mark resolved trades via gamma-api
  4. Fetch market price history for traded markets
  5. Reconstruct wallet_market_positions

Usage:
  python scripts/run_history_collection.py
  python scripts/run_history_collection.py --wallets 100
  python scripts/run_history_collection.py --skip-trades --skip-prices
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from trading_platform.polymarket.leaderboard_ingest import PolymarketLeaderboardIngestor
from trading_platform.polymarket.price_history_fetcher import MarketPriceHistoryFetcher
from trading_platform.polymarket.wallet_db import WalletDB
from trading_platform.polymarket.wallet_position_reconstructor import WalletPositionReconstructor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("history_collection")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect historical data for backtesting")
    parser.add_argument("--wallets", type=int, default=100,
                        help="Top N watched wallets to backfill (default: 100)")
    parser.add_argument("--days-back", type=int, default=180,
                        help="Days of history to fetch (default: 180)")
    parser.add_argument("--max-markets", type=int, default=500,
                        help="Max markets to fetch price history for (default: 500)")
    parser.add_argument("--skip-trades", "--skip-backfill", dest="skip_trades",
                        action="store_true",
                        help="Skip trade backfill (assume already done)")
    parser.add_argument("--skip-resolution", action="store_true",
                        help="Skip resolution check via gamma-api")
    parser.add_argument("--skip-prices", action="store_true",
                        help="Skip market price history fetch")
    parser.add_argument("--skip-reconstruct", action="store_true",
                        help="Skip wallet position reconstruction")
    parser.add_argument("--only-reconstruct", action="store_true",
                        help="Run only step 5 (position reconstruction)")
    parser.add_argument("--step", type=int, choices=[2, 3, 4, 5], default=None,
                        help="Run only one specific step (2=trades, 3=resolution, 4=prices, 5=reconstruct)")
    args = parser.parse_args()

    # --only-reconstruct shortcut: skip 2/3/4
    if args.only_reconstruct:
        args.skip_trades = True
        args.skip_resolution = True
        args.skip_prices = True
    # --step N: skip everything else
    if args.step is not None:
        args.skip_trades = args.step != 2
        args.skip_resolution = args.step != 3
        args.skip_prices = args.step != 4
        args.skip_reconstruct = args.step != 5

    db = WalletDB()
    db_path = str(db._path)
    conn = sqlite3.connect(db_path)
    wallets = [
        r[0] for r in conn.execute(
            """SELECT wallet FROM leaderboard
               WHERE tier IN ('tier1h','tier1','tier2')
               ORDER BY CASE tier
                   WHEN 'tier1h' THEN 1
                   WHEN 'tier1' THEN 2
                   WHEN 'tier2' THEN 3
               END, COALESCE(net_pnl_usdc, 0) DESC
               LIMIT ?""",
            (args.wallets,),
        ).fetchall()
    ]
    conn.close()
    logger.info("STEP 1: %d wallets to process", len(wallets))

    ingestor = PolymarketLeaderboardIngestor(db=db)

    # STEP 2: Backfill trades
    if not args.skip_trades:
        logger.info("STEP 2: Backfilling trade history (%d days)...", args.days_back)
        for i, wallet in enumerate(wallets):
            try:
                n = ingestor.fetch_wallet_history(wallet)
                logger.info("[%d/%d] %s ... +%d trades", i + 1, len(wallets), wallet[:16], n)
            except Exception as exc:
                logger.warning("[%d/%d] %s failed: %s", i + 1, len(wallets), wallet[:16], exc)
            time.sleep(0.5)
    else:
        logger.info("STEP 2: SKIPPED")

    # STEP 3: Mark resolved trades
    if not args.skip_resolution:
        logger.info("STEP 3: Marking resolved trades via Gamma API...")
        top = wallets[:30]
        for i, wallet in enumerate(top):
            try:
                n = ingestor.fetch_resolved_trades(wallet, db_path)
                logger.info("[%d/%d] %s ... %d trades resolved", i + 1, len(top), wallet[:16], n)
            except Exception as exc:
                logger.warning("[%d/%d] %s failed: %s", i + 1, len(top), wallet[:16], exc)
            time.sleep(1.0)
    else:
        logger.info("STEP 3: SKIPPED")

    # STEP 4: Price history
    # Prioritize markets the backtester actually needs:
    #   1. Resolved positions in wallet_market_positions (highest priority — backtester input)
    #   2. Resolved trades in wallet_trades (next priority)
    #   3. Any remaining traded markets (fill remaining quota)
    if not args.skip_prices:
        logger.info("STEP 4: Collecting market price history...")
        conn = sqlite3.connect(db_path)
        ph = ",".join("?" * len(wallets)) if wallets else "''"

        seen: set[str] = set()
        ordered_cids: list[str] = []

        def _add(rows: list[tuple[str]]) -> None:
            for (cid,) in rows:
                if cid and cid not in seen:
                    seen.add(cid)
                    ordered_cids.append(cid)
                    if len(ordered_cids) >= args.max_markets:
                        return

        # 1. Resolved positions in wallet_market_positions for our wallets
        if wallets:
            _add(conn.execute(
                f"""SELECT DISTINCT condition_id FROM wallet_market_positions
                       WHERE market_resolved = 1 AND wallet IN ({ph})""",
                tuple(wallets),
            ).fetchall())
        # 2. Resolved trades in wallet_trades for our wallets
        if len(ordered_cids) < args.max_markets and wallets:
            _add(conn.execute(
                f"""SELECT DISTINCT condition_id FROM wallet_trades
                       WHERE market_resolved = 1
                         AND condition_id IS NOT NULL AND condition_id != ''
                         AND wallet IN ({ph})""",
                tuple(wallets),
            ).fetchall())
        # 3. Any traded markets for our wallets (fill remainder)
        if len(ordered_cids) < args.max_markets and wallets:
            _add(conn.execute(
                f"""SELECT DISTINCT condition_id FROM wallet_trades
                       WHERE condition_id IS NOT NULL AND condition_id != ''
                         AND wallet IN ({ph})""",
                tuple(wallets),
            ).fetchall())

        cids = ordered_cids[: args.max_markets]
        conn.close()
        logger.info("  %d unique markets to fetch (resolved-first ordering)", len(cids))
        fetcher = MarketPriceHistoryFetcher()
        results = fetcher.batch_fetch(cids, db_path, sleep_between=0.4)
        success = sum(1 for v in results.values() if v > 0)
        logger.info("  Price history: %d/%d markets fetched", success, len(cids))
    else:
        logger.info("STEP 4: SKIPPED")

    # STEP 5: Reconstruct positions
    if not args.skip_reconstruct:
        logger.info("STEP 5: Reconstructing wallet market positions...")
        reconstructor = WalletPositionReconstructor()
        total = 0
        for i, wallet in enumerate(wallets):
            try:
                n = reconstructor.reconstruct_wallet(wallet, db_path)
                total += n
                if (i + 1) % 10 == 0 or i == len(wallets) - 1:
                    logger.info("  [%d/%d] %s: %d positions", i + 1, len(wallets), wallet[:16], n)
            except Exception as exc:
                logger.warning("  reconstruct failed for %s: %s", wallet[:16], exc)
        logger.info("  Total positions reconstructed: %d", total)
    else:
        logger.info("STEP 5: SKIPPED")

    logger.info("History collection complete.")


if __name__ == "__main__":
    main()
