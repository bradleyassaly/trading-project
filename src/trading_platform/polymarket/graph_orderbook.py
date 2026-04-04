"""
Polymarket Goldsky subgraph orderbook fetcher.

Queries the Goldsky-hosted orderbook subgraph for orderbook depth
and imbalance data on active Polymarket markets.

Usage::

    from trading_platform.polymarket.graph_orderbook import GraphOrderbookFetcher
    fetcher = GraphOrderbookFetcher()
    schema = fetcher.get_schema()
    df = fetcher.fetch_orderbooks(["token_id_1", "token_id_2"])
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "polymarket-orderbook/0.0.1/gn"
)


class GraphOrderbookFetcher:
    """Fetch orderbook microstructure from Goldsky subgraph."""

    def __init__(self, *, endpoint_url: str = _GOLDSKY_URL) -> None:
        self._url = endpoint_url
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def get_schema(self) -> list[str]:
        """Introspect the subgraph to discover available query fields."""
        query = '{ __schema { queryType { fields { name description } } } }'
        try:
            resp = self._session.post(self._url, json={"query": query}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            fields = data.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])
            return [f["name"] for f in fields]
        except Exception as exc:
            logger.warning("Schema introspection failed: %s", exc)
            return []

    def fetch_orderbooks(self, token_ids: list[str]) -> pd.DataFrame:
        """Fetch orderbook data for a list of token IDs.

        Returns a DataFrame with columns:
            token_id, best_bid, best_ask, spread, bid_depth, ask_depth,
            orderbook_imbalance, last_trade_price
        """
        if not token_ids:
            return pd.DataFrame()

        # Query in batches of 100
        all_rows: list[dict[str, Any]] = []
        for i in range(0, len(token_ids), 100):
            batch = token_ids[i:i + 100]
            rows = self._fetch_batch(batch)
            all_rows.extend(rows)

        if not all_rows:
            return pd.DataFrame(columns=[
                "token_id", "best_bid", "best_ask", "spread",
                "bid_depth", "ask_depth", "orderbook_imbalance", "last_trade_price",
            ])

        return pd.DataFrame(all_rows)

    def _fetch_batch(self, token_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch orderbook data for a batch of token IDs via GraphQL."""
        ids_str = ", ".join(f'"{tid}"' for tid in token_ids)
        query = f"""
        {{
          tokenOrderbooks(where: {{ tokenId_in: [{ids_str}] }}, first: 100) {{
            tokenId
            bestBid
            bestAsk
            totalBidDepth
            totalAskDepth
            lastTradePrice
          }}
        }}
        """
        try:
            resp = self._session.post(self._url, json={"query": query}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Orderbook fetch failed: %s", exc)
            return []

        items = (
            data.get("data", {}).get("tokenOrderbooks")
            or data.get("data", {}).get("markets")
            or []
        )

        rows: list[dict[str, Any]] = []
        for item in items:
            try:
                bid = float(item.get("bestBid") or 0)
                ask = float(item.get("bestAsk") or 0)
                bid_depth = float(item.get("totalBidDepth") or 0)
                ask_depth = float(item.get("totalAskDepth") or 0)
                spread = ask - bid if ask > 0 and bid > 0 else 0
                total_depth = bid_depth + ask_depth
                imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0

                rows.append({
                    "token_id": item.get("tokenId", ""),
                    "best_bid": bid,
                    "best_ask": ask,
                    "spread": round(spread, 4),
                    "bid_depth": bid_depth,
                    "ask_depth": ask_depth,
                    "orderbook_imbalance": round(imbalance, 4),
                    "last_trade_price": float(item.get("lastTradePrice") or 0),
                })
            except (TypeError, ValueError):
                continue

        return rows
