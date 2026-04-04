"""
Polymarket CLOB trade history fetcher.

Fetches trade history from the CLOB REST API for active markets.
Output matches the poly-trade-scan CSV schema for wallet profiler
compatibility.

Usage::

    from trading_platform.polymarket.clob_trades_fetcher import ClobTradesFetcher
    fetcher = ClobTradesFetcher()
    fetcher.fetch_market_trades("104173...", "data/polymarket/clob_trades")
"""
from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

_CLOB_BASE = "https://clob.polymarket.com"
_DEFAULT_SLEEP = 0.2


class ClobTradesFetcher:
    """Fetch trade history from the Polymarket CLOB API."""

    def __init__(self, *, base_url: str = _CLOB_BASE, sleep_sec: float = _DEFAULT_SLEEP) -> None:
        self._base = base_url.rstrip("/")
        self._sleep = sleep_sec
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def fetch_market_trades(
        self,
        token_id: str,
        output_dir: str | Path,
        *,
        max_pages: int = 100,
    ) -> int:
        """Fetch all trades for a token_id and write to CSV. Returns row count."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"{token_id[:32]}.csv"

        fieldnames = ["timestamp", "wallet", "token_id", "side", "price", "total_usdc"]
        rows_written = 0

        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()

            cursor: str | None = None
            for page in range(max_pages):
                params: dict[str, Any] = {
                    "asset_id": token_id,
                    "limit": 500,
                }
                if cursor:
                    params["next_cursor"] = cursor

                try:
                    time.sleep(self._sleep)
                    resp = self._session.get(f"{self._base}/trades", params=params, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logger.warning("Trade fetch failed for %s page %d: %s", token_id[:16], page, exc)
                    break

                trades = data if isinstance(data, list) else data.get("data", data.get("trades", []))
                if not trades:
                    break

                for trade in trades:
                    try:
                        writer.writerow({
                            "timestamp": trade.get("match_time") or trade.get("timestamp") or "",
                            "wallet": trade.get("maker_address") or trade.get("taker_address") or "",
                            "token_id": token_id,
                            "side": trade.get("side", ""),
                            "price": trade.get("price", ""),
                            "total_usdc": str(float(trade.get("size", 0)) * float(trade.get("price", 0))),
                        })
                        rows_written += 1
                    except (TypeError, ValueError):
                        continue

                cursor = data.get("next_cursor") if isinstance(data, dict) else None
                if not cursor:
                    break

        logger.info("Fetched %d trades for %s", rows_written, token_id[:16])
        return rows_written

    def fetch_all_active_markets(
        self,
        output_dir: str | Path,
        *,
        metadata_db_path: str | Path | None = None,
        hours_back: int = 168,
    ) -> dict[str, int]:
        """Fetch trades for all active markets from the live collector DB.

        Returns dict of token_id → row count.
        """
        output_dir = Path(output_dir)
        db_path = Path(metadata_db_path) if metadata_db_path else Path("data/polymarket/live/prices.db")

        if not db_path.exists():
            logger.warning("No metadata DB at %s", db_path)
            return {}

        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        rows = conn.execute("SELECT yes_token_id FROM markets WHERE yes_token_id IS NOT NULL").fetchall()
        conn.close()

        token_ids = [r[0] for r in rows if r[0]]
        logger.info("Fetching trades for %d active markets", len(token_ids))

        results: dict[str, int] = {}
        for token_id in token_ids:
            count = self.fetch_market_trades(token_id, output_dir)
            results[token_id] = count

        return results
