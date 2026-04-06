"""
Goldsky subgraph client for Polymarket order-level data.

Queries the Goldsky-hosted orderbook subgraph for fill events
and orderbook snapshots. No authentication required.

Usage::

    from trading_platform.polymarket.goldsky_client import GoldskyClient
    client = GoldskyClient()
    fills = client.fetch_fills("token_id_here", since_hours=24)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_ORDERBOOK_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "orderbook-subgraph/0.0.1/gn"
)
_POSITIONS_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "positions-subgraph/0.0.7/gn"
)

_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0


class GoldskyClient:
    """Query the Goldsky Polymarket orderbook subgraph."""

    ORDERBOOK_URL = _ORDERBOOK_URL
    POSITIONS_URL = _POSITIONS_URL

    def __init__(self, *, endpoint_url: str | None = None) -> None:
        self._url = endpoint_url or _ORDERBOOK_URL
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def query(self, gql: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query with retry on 429/503."""
        body: dict[str, Any] = {"query": gql}
        if variables:
            body["variables"] = variables

        for attempt in range(_MAX_RETRIES):
            try:
                resp = self._session.post(self._url, json=body, timeout=15)
                if resp.status_code in (429, 503) and attempt < _MAX_RETRIES - 1:
                    delay = _BACKOFF_BASE * (2 ** attempt)
                    logger.debug("Goldsky %d, retrying in %.1fs", resp.status_code, delay)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    raise ValueError(f"GraphQL errors: {data['errors']}")
                return data
            except requests.exceptions.RequestException as exc:
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(_BACKOFF_BASE * (2 ** attempt))
                    continue
                raise
        return {}

    def fetch_fills(
        self,
        market_token_id: str,
        *,
        since_hours: int = 24,
    ) -> pd.DataFrame:
        """Fetch order fill events for a market token.

        Returns DataFrame with columns: timestamp, maker_wallet, taker_wallet,
        maker_amount, taker_amount, maker_asset_id, taker_asset_id, fee, id.
        """
        since_ts = int((datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)).timestamp())
        all_rows: list[dict[str, Any]] = []
        skip = 0
        batch_size = 1000

        while True:
            gql = """
            query($market: String!, $since: BigInt!, $first: Int!, $skip: Int!) {
              orderFilledEvents(
                where: { takerAssetId: $market, timestamp_gte: $since }
                first: $first
                skip: $skip
                orderBy: timestamp
                orderDirection: desc
              ) {
                id
                timestamp
                maker
                taker
                makerAmountFilled
                takerAmountFilled
                makerAssetId
                takerAssetId
                fee
              }
            }
            """
            variables = {
                "market": market_token_id,
                "since": str(since_ts),
                "first": batch_size,
                "skip": skip,
            }

            try:
                data = self.query(gql, variables)
            except Exception as exc:
                logger.warning("Fill fetch failed for %s: %s", market_token_id[:16], exc)
                break

            events = data.get("data", {}).get("orderFilledEvents", [])
            if not events:
                break

            for ev in events:
                try:
                    ts = int(ev.get("timestamp", 0))
                    all_rows.append({
                        "id": ev.get("id", ""),
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "maker_wallet": ev.get("maker", ""),
                        "taker_wallet": ev.get("taker", ""),
                        "maker_amount": float(ev.get("makerAmountFilled", 0)) / 1e6,
                        "taker_amount": float(ev.get("takerAmountFilled", 0)) / 1e6,
                        "maker_asset_id": ev.get("makerAssetId", ""),
                        "taker_asset_id": ev.get("takerAssetId", ""),
                        "fee": float(ev.get("fee", 0)) / 1e6,
                    })
                except (TypeError, ValueError):
                    continue

            if len(events) < batch_size:
                break
            skip += len(events)

        if not all_rows:
            return pd.DataFrame(columns=[
                "id", "timestamp", "maker_wallet", "taker_wallet",
                "maker_amount", "taker_amount", "maker_asset_id",
                "taker_asset_id", "fee",
            ])

        return pd.DataFrame(all_rows)

    def fetch_fills_by_maker(
        self,
        wallet: str,
        *,
        since_hours: float = 1.5,
    ) -> pd.DataFrame:
        """Fetch fills where the given wallet is the maker."""
        since_ts = int((datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)).timestamp())
        all_rows: list[dict[str, Any]] = []
        skip = 0

        while True:
            gql = """
            query($wallet: String!, $since: BigInt!, $first: Int!, $skip: Int!) {
              orderFilledEvents(
                where: { maker: $wallet, timestamp_gte: $since }
                first: $first
                skip: $skip
                orderBy: timestamp
                orderDirection: desc
              ) {
                id timestamp maker taker
                makerAmountFilled takerAmountFilled
                makerAssetId takerAssetId fee
              }
            }
            """
            try:
                data = self.query(gql, {
                    "wallet": wallet.lower(),
                    "since": str(since_ts),
                    "first": 1000,
                    "skip": skip,
                })
            except Exception as exc:
                logger.debug("Maker fill fetch failed for %s: %s", wallet[:10], exc)
                break

            events = data.get("data", {}).get("orderFilledEvents", [])
            if not events:
                break

            for ev in events:
                try:
                    ts = int(ev.get("timestamp", 0))
                    all_rows.append({
                        "id": ev.get("id", ""),
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "maker_wallet": ev.get("maker", ""),
                        "taker_wallet": ev.get("taker", ""),
                        "maker_amount": float(ev.get("makerAmountFilled", 0)) / 1e6,
                        "taker_amount": float(ev.get("takerAmountFilled", 0)) / 1e6,
                        "maker_asset_id": ev.get("makerAssetId", ""),
                        "taker_asset_id": ev.get("takerAssetId", ""),
                        "fee": float(ev.get("fee", 0)) / 1e6,
                    })
                except (TypeError, ValueError):
                    continue

            if len(events) < 1000:
                break
            skip += len(events)

        if not all_rows:
            return pd.DataFrame(columns=[
                "id", "timestamp", "maker_wallet", "taker_wallet",
                "maker_amount", "taker_amount", "maker_asset_id",
                "taker_asset_id", "fee",
            ])
        return pd.DataFrame(all_rows)

    def fetch_fills_by_makers(
        self,
        wallets: list[str],
        *,
        since_hours: float = 1.0,
        batch_size: int = 10,
        max_fills: int = 1000,
    ) -> pd.DataFrame:
        """Fetch fills for multiple maker wallets in batched queries."""
        since_ts = int((datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)).timestamp())
        all_rows: list[dict[str, Any]] = []

        total_batches = (len(wallets) + batch_size - 1) // batch_size
        for i in range(0, len(wallets), batch_size):
            if len(all_rows) >= max_fills:
                break
            batch_num = i // batch_size + 1
            batch = [w.lower() for w in wallets[i:i + batch_size]]
            logger.debug("Fetching batch %d/%d (%d wallets)", batch_num, total_batches, len(batch))
            skip = 0
            while True:
                gql = """
                query($wallets: [String!]!, $since: BigInt!, $first: Int!, $skip: Int!) {
                  orderFilledEvents(
                    where: { maker_in: $wallets, timestamp_gte: $since }
                    first: $first
                    skip: $skip
                    orderBy: timestamp
                    orderDirection: desc
                  ) {
                    id timestamp maker taker
                    makerAmountFilled takerAmountFilled
                    makerAssetId takerAssetId fee
                  }
                }
                """
                try:
                    data = self.query(gql, {
                        "wallets": batch,
                        "since": str(since_ts),
                        "first": 1000,
                        "skip": skip,
                    })
                except Exception as exc:
                    logger.debug("Batch maker fill fetch failed: %s", exc)
                    break

                events = data.get("data", {}).get("orderFilledEvents", [])
                if not events:
                    break

                for ev in events:
                    try:
                        ts = int(ev.get("timestamp", 0))
                        all_rows.append({
                            "id": ev.get("id", ""),
                            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                            "maker_wallet": ev.get("maker", ""),
                            "taker_wallet": ev.get("taker", ""),
                            "maker_amount": float(ev.get("makerAmountFilled", 0)) / 1e6,
                            "taker_amount": float(ev.get("takerAmountFilled", 0)) / 1e6,
                            "maker_asset_id": ev.get("makerAssetId", ""),
                            "taker_asset_id": ev.get("takerAssetId", ""),
                            "fee": float(ev.get("fee", 0)) / 1e6,
                        })
                    except (TypeError, ValueError):
                        continue

                if len(events) < 1000 or len(all_rows) >= max_fills:
                    break
                skip += len(events)

        if not all_rows:
            return pd.DataFrame(columns=[
                "id", "timestamp", "maker_wallet", "taker_wallet",
                "maker_amount", "taker_amount", "maker_asset_id",
                "taker_asset_id", "fee",
            ])
        return pd.DataFrame(all_rows)

    def fetch_orderbook(self, market_token_id: str) -> dict[str, Any]:
        """Fetch current orderbook for a market token."""
        gql = """
        query($market: String!) {
          orderBooks(where: { id: $market }, first: 1) {
            id
            bids(first: 20, orderBy: price, orderDirection: desc) {
              price
              size
              owner
            }
            asks(first: 20, orderBy: price, orderDirection: asc) {
              price
              size
              owner
            }
          }
        }
        """
        try:
            data = self.query(gql, {"market": market_token_id})
            books = data.get("data", {}).get("orderBooks", [])
            if books:
                return books[0]
        except Exception as exc:
            logger.warning("Orderbook fetch failed for %s: %s", market_token_id[:16], exc)
        return {"bids": [], "asks": []}
