"""
Polymarket Data API trade fetcher.

Fetches trade history from the free, unauthenticated Data API at
``https://data-api.polymarket.com/trades``. Supports filtering by
market (conditionId) or wallet (proxyWallet).

Output CSV is compatible with both the blockchain ingest pipeline
and the wallet profiler.

Usage::

    from trading_platform.polymarket.data_api_fetcher import PolymarketDataApiFetcher
    fetcher = PolymarketDataApiFetcher()
    fetcher.fetch_recent_trades(hours_back=168, output_dir="data/polymarket/data_api_trades")
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

_BASE_URL = "https://data-api.polymarket.com"
_DEFAULT_SLEEP = 0.1


class PolymarketDataApiFetcher:
    """Fetch trades from the Polymarket Data API (no auth required)."""

    def __init__(self, *, base_url: str = _BASE_URL, sleep_sec: float = _DEFAULT_SLEEP) -> None:
        self._base = base_url.rstrip("/")
        self._sleep = sleep_sec
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def fetch_recent_trades(
        self,
        output_dir: str | Path,
        *,
        hours_back: int = 168,
        max_pages: int = 200,
    ) -> int:
        """Fetch all recent trades across all markets.

        Paginates until trades are older than ``hours_back`` hours.
        Returns total rows written.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)
        cutoff_ts = int(cutoff.timestamp())

        ts_label = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        csv_path = output_dir / f"recent_{ts_label}.csv"

        fieldnames = _CSV_FIELDS
        rows_written = 0
        hit_cutoff = False

        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()

            offset = 0
            for page in range(max_pages):
                trades = self._fetch_page(params={"limit": 500, "offset": offset})
                if not trades:
                    break

                for trade in trades:
                    ts = _get_timestamp(trade)
                    if ts and ts < cutoff_ts:
                        hit_cutoff = True
                        break
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1

                if hit_cutoff:
                    break
                offset += len(trades)
                time.sleep(self._sleep)

        logger.info("Fetched %d recent trades to %s", rows_written, csv_path)
        return rows_written

    def fetch_market_trades(
        self,
        condition_id: str,
        output_dir: str | Path,
        *,
        max_pages: int = 100,
    ) -> int:
        """Fetch all trades for a specific market. Returns row count."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"{condition_id[:32]}.csv"

        rows_written = 0
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
            writer.writeheader()

            offset = 0
            for page in range(max_pages):
                trades = self._fetch_page(params={
                    "market": condition_id, "limit": 500, "offset": offset,
                })
                if not trades:
                    break
                for trade in trades:
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                offset += len(trades)
                time.sleep(self._sleep)

        return rows_written

    def fetch_wallet_history(
        self,
        wallet: str,
        output_dir: str | Path,
        *,
        max_pages: int = 100,
    ) -> int:
        """Fetch all trades for a specific wallet. Returns row count."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"wallet_{wallet[:16]}.csv"

        rows_written = 0
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
            writer.writeheader()

            offset = 0
            for page in range(max_pages):
                trades = self._fetch_page(params={
                    "user": wallet, "limit": 500, "offset": offset,
                })
                if not trades:
                    break
                for trade in trades:
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                offset += len(trades)
                time.sleep(self._sleep)

        return rows_written

    def _fetch_page(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            resp = self._session.get(f"{self._base}/trades", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else data.get("data", data.get("trades", []))
        except Exception as exc:
            logger.warning("Data API fetch failed: %s", exc)
            return []


# ── Helpers ──────────────────────────────────────────────────────────────────

_CSV_FIELDS = [
    "timestamp", "wallet", "token_id", "condition_id",
    "side", "price", "total_usdc", "outcome", "title",
]


def _get_timestamp(trade: dict[str, Any]) -> int | None:
    ts = trade.get("timestamp")
    if ts is None:
        return None
    try:
        return int(float(ts))
    except (TypeError, ValueError):
        return None


def _trade_to_row(trade: dict[str, Any]) -> dict[str, str] | None:
    try:
        price = float(trade.get("price") or 0)
        size = float(trade.get("size") or 0)
        ts = trade.get("timestamp", "")
        return {
            "timestamp": str(ts),
            "wallet": trade.get("proxyWallet") or trade.get("maker_address") or "",
            "token_id": trade.get("asset") or trade.get("token_id") or "",
            "condition_id": trade.get("conditionId") or "",
            "side": trade.get("side", ""),
            "price": str(price),
            "total_usdc": str(round(size * price, 2)),
            "outcome": trade.get("outcome") or "",
            "title": trade.get("title") or "",
        }
    except (TypeError, ValueError):
        return None
