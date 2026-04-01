"""
Kalshi REST API client.

Wraps all public market data and authenticated portfolio endpoints.
Uses `requests` (already a project dependency) with RSA-PSS auth headers.

Rate limits (Basic tier):  20 reads/sec,  10 writes/sec
We default to ~14 req/sec to stay safely under the read limit.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any
from urllib.parse import urlencode

import requests

from trading_platform.kalshi.auth import KalshiConfig, build_auth_headers
from trading_platform.kalshi.models import (
    KalshiFill,
    KalshiMarket,
    KalshiOrderBook,
    KalshiOrderBookLevel,
    KalshiOrderRequest,
    KalshiOrderStatus,
    KalshiPosition,
    KalshiTrade,
)

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15  # seconds
_READ_SLEEP = 0.072    # ~14 req/sec  (Basic tier limit: 20/sec)
_WRITE_SLEEP = 0.11    # ~9 req/sec   (Basic tier limit: 10/sec)


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_market(raw: dict[str, Any]) -> KalshiMarket:
    return KalshiMarket(
        ticker=raw.get("ticker", ""),
        title=raw.get("title", ""),
        subtitle=raw.get("subtitle"),
        status=raw.get("status", ""),
        yes_bid=raw.get("yes_bid_dollars") or raw.get("yes_bid"),
        yes_ask=raw.get("yes_ask_dollars") or raw.get("yes_ask"),
        no_bid=raw.get("no_bid_dollars") or raw.get("no_bid"),
        no_ask=raw.get("no_ask_dollars") or raw.get("no_ask"),
        volume=raw.get("volume"),
        open_interest=raw.get("open_interest"),
        close_time=raw.get("close_time"),
        event_ticker=raw.get("event_ticker"),
        series_ticker=raw.get("series_ticker"),
        category=raw.get("category"),
        liquidity=raw.get("liquidity_dollars") or raw.get("liquidity"),
        raw=raw,
    )


def _parse_order_status(raw: dict[str, Any]) -> KalshiOrderStatus:
    return KalshiOrderStatus(
        order_id=raw.get("order_id", ""),
        client_order_id=raw.get("client_order_id"),
        ticker=raw.get("ticker", ""),
        side=raw.get("side", ""),
        action=raw.get("action", ""),
        status=raw.get("status", ""),
        order_type=raw.get("type", "limit"),
        yes_price=raw.get("yes_price_dollars") or raw.get("yes_price"),
        no_price=raw.get("no_price_dollars") or raw.get("no_price"),
        count=raw.get("count", 0),
        remaining_count=raw.get("remaining_count", 0),
        amend_count=raw.get("amend_count", 0),
        created_time=raw.get("created_time"),
        close_time=raw.get("close_time"),
        fees=raw.get("fees"),
    )


# ── Client ────────────────────────────────────────────────────────────────────

class KalshiClient:
    """
    Synchronous Kalshi API client.

    Usage::

        config = KalshiConfig.from_env()
        client = KalshiClient(config)

        markets = client.get_all_markets(status="open")
        ob = client.get_orderbook("AAPL-23DEC29-B150", depth=5)
        balance = client.get_balance()
    """

    def __init__(self, config: KalshiConfig) -> None:
        self.config = config
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}{path}"

    def _auth(self, method: str, path: str) -> dict[str, str]:
        return build_auth_headers(self.config, method, path)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        time.sleep(_READ_SLEEP)
        full_path = path
        if params:
            cleaned = {k: v for k, v in params.items() if v is not None}
            if cleaned:
                full_path = f"{path}?{urlencode(cleaned)}"
        headers = self._auth("GET", full_path)
        resp = self._session.get(self._url(full_path), headers=headers, timeout=_DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        time.sleep(_WRITE_SLEEP)
        headers = self._auth("POST", path)
        resp = self._session.post(self._url(path), json=body, headers=headers, timeout=_DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, params: dict[str, Any] | None = None) -> Any:
        time.sleep(_WRITE_SLEEP)
        full_path = path
        if params:
            cleaned = {k: v for k, v in params.items() if v is not None}
            if cleaned:
                full_path = f"{path}?{urlencode(cleaned)}"
        headers = self._auth("DELETE", full_path)
        resp = self._session.delete(self._url(full_path), headers=headers, timeout=_DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    # ── Market Data (no auth required) ───────────────────────────────────────

    def get_markets(
        self,
        status: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[KalshiMarket], str | None]:
        """Fetch one page of markets. Returns (markets, next_cursor)."""
        data = self._get("/markets", {
            "limit": limit,
            "status": status,
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "cursor": cursor,
        })
        return [_parse_market(m) for m in data.get("markets", [])], data.get("cursor")

    def get_all_markets(
        self,
        status: str | None = None,
        series_ticker: str | None = None,
    ) -> list[KalshiMarket]:
        """Paginate through all markets matching the given filters."""
        all_markets: list[KalshiMarket] = []
        cursor: str | None = None
        while True:
            markets, cursor = self.get_markets(
                status=status, series_ticker=series_ticker, limit=200, cursor=cursor
            )
            all_markets.extend(markets)
            if not cursor:
                break
        return all_markets

    def get_market(self, ticker: str) -> KalshiMarket:
        data = self._get(f"/markets/{ticker}")
        return _parse_market(data.get("market", data))

    def get_orderbook(self, ticker: str, depth: int = 10) -> KalshiOrderBook:
        data = self._get(f"/markets/{ticker}/orderbook", {"depth": depth})
        ob = data.get("orderbook", data)
        yes_bids = [
            KalshiOrderBookLevel(price=str(level[0]), quantity=int(level[1]))
            for level in ob.get("yes_dollars", ob.get("yes", []))
        ]
        no_bids = [
            KalshiOrderBookLevel(price=str(level[0]), quantity=int(level[1]))
            for level in ob.get("no_dollars", ob.get("no", []))
        ]
        return KalshiOrderBook(ticker=ticker, yes_bids=yes_bids, no_bids=no_bids)

    def get_trades(
        self,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> tuple[list[KalshiTrade], str | None]:
        """Fetch one page of completed trades. Returns (trades, next_cursor)."""
        data = self._get("/markets/trades", {
            "ticker": ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "limit": limit,
            "cursor": cursor,
        })
        trades = [
            KalshiTrade(
                trade_id=t.get("trade_id", ""),
                ticker=t.get("ticker", ticker or ""),
                side=t.get("taker_side", ""),
                yes_price=str(t.get("yes_price_dollars") or t.get("yes_price", "")),
                no_price=str(t.get("no_price_dollars") or t.get("no_price", "")),
                count=t.get("count", 0),
                created_time=t.get("created_time", ""),
            )
            for t in data.get("trades", [])
        ]
        return trades, data.get("cursor")

    def get_all_trades(
        self,
        ticker: str,
        min_ts: int | None = None,
        max_ts: int | None = None,
    ) -> list[KalshiTrade]:
        """Paginate through all trades for a ticker."""
        all_trades: list[KalshiTrade] = []
        cursor: str | None = None
        while True:
            trades, cursor = self.get_trades(
                ticker=ticker, min_ts=min_ts, max_ts=max_ts, limit=1000, cursor=cursor
            )
            all_trades.extend(trades)
            if not cursor:
                break
        return all_trades

    # ── Portfolio (authenticated) ─────────────────────────────────────────────

    def get_balance(self) -> dict[str, Any]:
        """Raw balance response. Keys: balance (cents), portfolio_value (cents)."""
        return self._get("/portfolio/balance")

    def get_positions(self, ticker: str | None = None) -> list[KalshiPosition]:
        data = self._get("/portfolio/positions", {"ticker": ticker, "limit": 1000})
        return [
            KalshiPosition(
                ticker=p.get("ticker", ""),
                market_exposure=p.get("market_exposure", 0),
                position=p.get("position", 0),
                resting_orders_count=p.get("resting_orders_count", 0),
                total_traded=p.get("total_traded", 0),
                fees_paid=str(p.get("fees_paid_dollars") or p.get("fees_paid", "0")),
                realized_pnl=str(p.get("realized_pnl_dollars")) if p.get("realized_pnl_dollars") is not None else None,
                unrealized_pnl=str(p.get("unrealized_pnl_dollars")) if p.get("unrealized_pnl_dollars") is not None else None,
            )
            for p in data.get("market_positions", [])
        ]

    def get_orders(
        self,
        ticker: str | None = None,
        status: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[KalshiOrderStatus], str | None]:
        data = self._get("/portfolio/orders", {
            "ticker": ticker,
            "status": status,
            "limit": limit,
            "cursor": cursor,
        })
        return [_parse_order_status(o) for o in data.get("orders", [])], data.get("cursor")

    def get_fills(
        self,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[KalshiFill], str | None]:
        data = self._get("/portfolio/fills", {
            "ticker": ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "limit": limit,
            "cursor": cursor,
        })
        fills = [
            KalshiFill(
                fill_id=f.get("fill_id") or f.get("id"),
                order_id=f.get("order_id", ""),
                ticker=f.get("ticker", ""),
                side=f.get("side", ""),
                action=f.get("action", ""),
                count=f.get("count", 0),
                yes_price=str(f.get("yes_price_dollars") or f.get("yes_price", "")),
                no_price=str(f.get("no_price_dollars") or f.get("no_price", "")),
                created_time=f.get("created_time"),
                fees=str(f.get("fees_dollars") or f.get("fees")) if f.get("fees") is not None else None,
                is_taker=f.get("is_taker", False),
            )
            for f in data.get("fills", [])
        ]
        return fills, data.get("cursor")

    # ── Order Management ──────────────────────────────────────────────────────

    def create_order(self, order: KalshiOrderRequest) -> KalshiOrderStatus:
        if order.client_order_id is None:
            object.__setattr__(order, "client_order_id", str(uuid.uuid4()))

        body: dict[str, Any] = {
            "ticker": order.ticker,
            "side": order.side,
            "action": order.action,
            "count": order.count,
            "type": "limit",
            "time_in_force": order.time_in_force,
            "post_only": order.post_only,
            "reduce_only": order.reduce_only,
            "client_order_id": order.client_order_id,
        }
        if order.yes_price is not None:
            body["yes_price_dollars"] = order.yes_price
        if order.no_price is not None:
            body["no_price_dollars"] = order.no_price

        data = self._post("/portfolio/orders", body)
        return _parse_order_status(data.get("order", data))

    def cancel_order(self, order_id: str) -> KalshiOrderStatus:
        data = self._delete(f"/portfolio/orders/{order_id}")
        return _parse_order_status(data.get("order", data))

    def cancel_all_orders(self, ticker: str | None = None) -> list[KalshiOrderStatus]:
        """Cancel all resting orders, optionally scoped to a single ticker."""
        all_resting: list[KalshiOrderStatus] = []
        cursor: str | None = None
        while True:
            orders, cursor = self.get_orders(ticker=ticker, status="resting", limit=200, cursor=cursor)
            all_resting.extend(orders)
            if not cursor:
                break

        canceled: list[KalshiOrderStatus] = []
        for o in all_resting:
            try:
                canceled.append(self.cancel_order(o.order_id))
            except requests.HTTPError as exc:
                logger.warning("Failed to cancel order %s: %s", o.order_id, exc)
        return canceled
