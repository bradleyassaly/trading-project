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

            # Use timestamp-based pagination (offset API caps at 3500)
            before_ts: int | None = None
            for page in range(max_pages):
                params: dict[str, Any] = {"limit": 500}
                if before_ts is not None:
                    params["before"] = before_ts
                trades = self._fetch_page(params=params)
                if not trades:
                    break

                oldest_ts: int | None = None
                for trade in trades:
                    ts = _get_timestamp(trade)
                    if ts and ts < cutoff_ts:
                        hit_cutoff = True
                        break
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                    if ts is not None and (oldest_ts is None or ts < oldest_ts):
                        oldest_ts = ts

                if hit_cutoff:
                    break
                if oldest_ts is not None:
                    before_ts = oldest_ts
                else:
                    break
                time.sleep(self._sleep)

        logger.info("Fetched %d recent trades to %s", rows_written, csv_path)
        return rows_written

    def fetch_market_trades(
        self,
        market_id: str,
        output_dir: str | Path,
        *,
        max_pages: int = 7,
    ) -> int:
        """Fetch all trades for a specific market. Returns row count.

        Uses offset-based pagination (0, 500, ..., 3000) since per-market
        queries are bounded and offset works within 3500.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"{market_id[:32]}.csv"

        rows_written = 0
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
            writer.writeheader()

            offset = 0
            for page in range(max_pages):
                trades = self._fetch_page(params={
                    "market": market_id, "limit": 500, "offset": offset,
                })
                if not trades:
                    break
                for trade in trades:
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                if len(trades) < 500:
                    break  # last page
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

            before_ts: int | None = None
            for page in range(max_pages):
                params: dict[str, Any] = {"user": wallet, "limit": 500}
                if before_ts is not None:
                    params["before"] = before_ts
                trades = self._fetch_page(params=params)
                if not trades:
                    break
                oldest: int | None = None
                for trade in trades:
                    row = _trade_to_row(trade)
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                    ts = _get_timestamp(trade)
                    if ts is not None and (oldest is None or ts < oldest):
                        oldest = ts
                if oldest is not None:
                    before_ts = oldest
                else:
                    break
                time.sleep(self._sleep)

        return rows_written

    def fetch_trades_for_our_markets(
        self,
        output_dir: str | Path,
        *,
        metadata_db_path: str | Path | None = None,
    ) -> dict[str, int]:
        """Fetch trades per-market from our live DB.

        Uses condition_id if available, falls back to yes_token_id
        (the Data API accepts both as the ``market`` parameter).
        Returns dict of market_key → rows written.
        """
        output_dir = Path(output_dir)
        db_path = Path(metadata_db_path) if metadata_db_path else Path("data/polymarket/live/prices.db")
        if not db_path.exists():
            logger.warning("No metadata DB at %s", db_path)
            return {}

        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        # Use yes_token_id directly — confirmed working with Data API
        try:
            rows = conn.execute(
                "SELECT yes_token_id, market_id, question FROM markets WHERE yes_token_id IS NOT NULL"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        conn.close()

        if not rows:
            logger.warning("No markets found in DB")
            return {}

        logger.info("Fetching trades for %d markets", len(rows))
        results: dict[str, int] = {}
        combined_path = output_dir / f"all_markets_{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}.csv"
        output_dir.mkdir(parents=True, exist_ok=True)

        with combined_path.open("w", newline="", encoding="utf-8") as combined_fh:
            combined_writer = csv.DictWriter(combined_fh, fieldnames=_CSV_FIELDS)
            combined_writer.writeheader()

            for yes_token_id, market_id, question in rows:
                if not yes_token_id:
                    continue
                count = self.fetch_market_trades(yes_token_id, output_dir)
                results[yes_token_id] = count

                # Also append to combined file
                per_market_csv = output_dir / f"{yes_token_id[:32]}.csv"
                if per_market_csv.exists():
                    with per_market_csv.open(newline="", encoding="utf-8") as mf:
                        reader = csv.DictReader(mf)
                        for row in reader:
                            combined_writer.writerow(row)

        total = sum(results.values())
        logger.info("Fetched %d total trades across %d markets", total, len(results))
        return results

    def _fetch_page(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            resp = self._session.get(f"{self._base}/trades", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            try:
                from trading_platform.polymarket.api_health import record_success
                record_success("data_api_trades")
            except Exception: pass
            return data if isinstance(data, list) else data.get("data", data.get("trades", []))
        except Exception as exc:
            logger.warning("Data API fetch failed: %s", exc)
            try:
                from trading_platform.polymarket.api_health import record_error
                record_error("data_api_trades", str(exc))
            except Exception: pass
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
