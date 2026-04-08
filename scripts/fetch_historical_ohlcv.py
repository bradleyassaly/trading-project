#!/usr/bin/env python
"""
Fetch historical fills from the Polymarket data-api and store them as
``market_ticks`` rows so the candle synthesizer can produce dense
intra-day OHLCV charts for tracked markets.

The data-api ``trades`` endpoint is paginated; we walk through each
market until we exhaust pages or hit the user-supplied page cap.

Usage::

    python scripts/fetch_historical_ohlcv.py --markets 200 --max-pages 20
    python scripts/fetch_historical_ohlcv.py --markets 10 --dry-run
    python scripts/fetch_historical_ohlcv.py --cid 0xabc... --max-pages 50
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

import requests

from trading_platform.polymarket.wallet_db import WalletDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("ohlcv_fetcher")

DATA_API_TRADES = "https://data-api.polymarket.com/trades"
PAGE_SIZE = 500


def fetch_market_fills(condition_id: str, max_pages: int = 20) -> list[dict[str, Any]]:
    """Paginate through all fills for a market, oldest-first."""
    all_fills: list[dict[str, Any]] = []
    offset = 0
    for _ in range(max_pages):
        try:
            r = requests.get(
                DATA_API_TRADES,
                params={
                    "market": condition_id,
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "sortBy": "timestamp",
                    "order": "asc",
                },
                timeout=15,
            )
        except Exception as exc:
            logger.warning("fetch failed for %s offset=%d: %s", condition_id[:16], offset, exc)
            break
        if r.status_code != 200:
            logger.debug("%s offset=%d -> %d", condition_id[:16], offset, r.status_code)
            break
        try:
            data = r.json()
        except Exception:
            break
        fills = data if isinstance(data, list) else data.get("data", [])
        if not fills:
            break
        all_fills.extend(fills)
        if len(fills) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)
    return all_fills


def store_fills_as_ticks(condition_id: str, fills: list[dict[str, Any]], conn: sqlite3.Connection) -> int:
    """Insert fills into ``market_ticks`` (idempotent — UNIQUE on cid+ts)."""
    rows: list[tuple[str, int, float]] = []
    seen_ts: set[int] = set()
    for f in fills:
        ts = f.get("timestamp")
        price = f.get("price")
        if ts is None or price is None:
            continue
        try:
            ts_i = int(ts)
            price_f = float(price)
        except (TypeError, ValueError):
            continue
        # Drop within-batch dupes so executemany matches UNIQUE constraint
        if ts_i in seen_ts:
            continue
        seen_ts.add(ts_i)
        rows.append((condition_id, ts_i, price_f))
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR IGNORE INTO market_ticks (condition_id, timestamp, price)
           VALUES (?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill market_ticks from Polymarket data-api")
    parser.add_argument("--markets", type=int, default=200,
                        help="Top N markets (by mph density) to fetch. Default: 200")
    parser.add_argument("--max-pages", type=int, default=20,
                        help="Max paginated requests per market. Default: 20")
    parser.add_argument("--cid", type=str, default=None,
                        help="Fetch a single market by condition_id (overrides --markets)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List candidate markets without fetching")
    args = parser.parse_args()

    db = WalletDB()
    db_path = str(db._path)
    conn = sqlite3.connect(db_path)

    if args.cid:
        cids = [args.cid]
    else:
        cids = [
            r[0] for r in conn.execute(
                """SELECT condition_id FROM market_price_history
                   GROUP BY condition_id
                   ORDER BY COUNT(*) DESC LIMIT ?""",
                (args.markets,),
            ).fetchall()
        ]

    logger.info("Markets to process: %d", len(cids))
    if args.dry_run:
        for c in cids[:10]:
            print(f"  {c}")
        if len(cids) > 10:
            print(f"  ... and {len(cids) - 10} more")
        print(f"Estimated requests: up to {len(cids) * args.max_pages}")
        return

    t0 = time.time()
    total_fills = 0
    total_inserted = 0
    for i, cid in enumerate(cids, 1):
        try:
            fills = fetch_market_fills(cid, max_pages=args.max_pages)
            inserted = store_fills_as_ticks(cid, fills, conn)
            total_fills += len(fills)
            total_inserted += inserted
            logger.info("[%d/%d] %s: %d fills (%d new ticks)",
                        i, len(cids), cid[:18], len(fills), inserted)
        except Exception as exc:
            logger.warning("[%d/%d] %s failed: %s", i, len(cids), cid[:18], exc)
        time.sleep(0.2)

    conn.close()
    logger.info(
        "Done in %.0fs. Fills fetched: %d. New ticks stored: %d.",
        time.time() - t0, total_fills, total_inserted,
    )


if __name__ == "__main__":
    main()
