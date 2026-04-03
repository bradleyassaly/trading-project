"""
Kalshi REST API client.

Wraps all public market data and authenticated portfolio endpoints.
Uses `requests` (already a project dependency) with RSA-PSS auth headers.

Rate limits (Basic tier):  20 reads/sec,  10 writes/sec
We default to ~14 req/sec to stay safely under the read limit.
"""
from __future__ import annotations

import random
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
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
_DEFAULT_HIST_SLEEP = 0.1   # 20 req/sec for historical endpoints (free tier read limit)
_PUBLIC_429_MAX_RETRIES = 5
_PUBLIC_429_BACKOFF_BASE_SEC = 0.5
_PUBLIC_429_BACKOFF_MAX_SEC = 8.0
_PUBLIC_429_JITTER_MAX_SEC = 0.25


@dataclass(frozen=True)
class KalshiGetRetryPolicy:
    max_retries: int
    backoff_base_sec: float
    backoff_max_sec: float
    jitter_max_sec: float

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.backoff_base_sec < 0:
            raise ValueError("backoff_base_sec must be >= 0")
        if self.backoff_max_sec < 0:
            raise ValueError("backoff_max_sec must be >= 0")
        if self.jitter_max_sec < 0:
            raise ValueError("jitter_max_sec must be >= 0")


_DEFAULT_AUTHENTICATED_RETRY_POLICY = KalshiGetRetryPolicy(
    max_retries=_PUBLIC_429_MAX_RETRIES,
    backoff_base_sec=_PUBLIC_429_BACKOFF_BASE_SEC,
    backoff_max_sec=_PUBLIC_429_BACKOFF_MAX_SEC,
    jitter_max_sec=_PUBLIC_429_JITTER_MAX_SEC,
)
_DEFAULT_PUBLIC_RETRY_POLICY = KalshiGetRetryPolicy(
    max_retries=_PUBLIC_429_MAX_RETRIES,
    backoff_base_sec=_PUBLIC_429_BACKOFF_BASE_SEC,
    backoff_max_sec=_PUBLIC_429_BACKOFF_MAX_SEC,
    jitter_max_sec=_PUBLIC_429_JITTER_MAX_SEC,
)


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


def _parse_retry_after_seconds(retry_after: str | None) -> float | None:
    if not retry_after:
        return None
    value = retry_after.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


def _market_close_time_in_range(
    market: dict[str, Any],
    *,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
) -> bool:
    if min_close_ts is None and max_close_ts is None:
        return True
    close_time_raw = market.get("close_time")
    if not close_time_raw:
        return False
    try:
        close_time = datetime.fromisoformat(str(close_time_raw).replace("Z", "+00:00"))
    except ValueError:
        return False
    close_ts = int(close_time.timestamp())
    if min_close_ts is not None and close_ts < min_close_ts:
        return False
    if max_close_ts is not None and close_ts > max_close_ts:
        return False
    return True


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

    def __init__(
        self,
        config: KalshiConfig,
        *,
        historical_sleep_sec: float = _DEFAULT_HIST_SLEEP,
        authenticated_sleep_sec: float = _READ_SLEEP,
        authenticated_retry_policy: KalshiGetRetryPolicy | None = None,
        public_retry_policy: KalshiGetRetryPolicy | None = None,
    ) -> None:
        self.config = config
        self.historical_sleep_sec = historical_sleep_sec
        self.authenticated_sleep_sec = authenticated_sleep_sec
        self.authenticated_retry_policy = authenticated_retry_policy or _DEFAULT_AUTHENTICATED_RETRY_POLICY
        self.public_retry_policy = public_retry_policy or _DEFAULT_PUBLIC_RETRY_POLICY
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}{path}"

    def _auth(self, method: str, path: str) -> dict[str, str]:
        return build_auth_headers(self.config, method, path)

    def _raise_http_error(
        self,
        resp: requests.Response,
        *,
        full_path: str,
        retries_exhausted: bool = False,
        max_retries: int | None = None,
    ) -> None:
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            status = getattr(resp, "status_code", "unknown")
            details = f"Kalshi GET {full_path} failed with status {status}"
            if retries_exhausted:
                details += f" after {max_retries if max_retries is not None else _PUBLIC_429_MAX_RETRIES} retries"
            if hasattr(exc, "add_note"):
                exc.add_note(details)
            logger.warning(details)
            raise

    def _build_rate_limit_delay(
        self,
        resp: requests.Response,
        *,
        attempt_number: int,
        retry_policy: KalshiGetRetryPolicy,
    ) -> float:
        retry_after = _parse_retry_after_seconds(resp.headers.get("Retry-After"))
        if retry_after is not None:
            return retry_after
        exponential_delay = min(
            retry_policy.backoff_base_sec * (2 ** max(attempt_number - 1, 0)),
            retry_policy.backoff_max_sec,
        )
        return exponential_delay + random.uniform(0.0, retry_policy.jitter_max_sec)

    def _get_json_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        use_auth: bool,
        sleep_sec: float,
        retry_policy: KalshiGetRetryPolicy,
        rate_limit_label: str,
    ) -> Any:
        time.sleep(sleep_sec)
        full_path = path
        if params:
            cleaned = {k: v for k, v in params.items() if v is not None}
            if cleaned:
                full_path = f"{path}?{urlencode(cleaned)}"
        headers = self._auth("GET", full_path) if use_auth else None
        for attempt_number in range(1, retry_policy.max_retries + 2):
            resp = self._session.get(self._url(full_path), headers=headers, timeout=_DEFAULT_TIMEOUT)
            if resp.status_code != 429:
                self._raise_http_error(
                    resp,
                    full_path=full_path,
                    max_retries=retry_policy.max_retries,
                )
                return resp.json()

            if attempt_number > retry_policy.max_retries:
                self._raise_http_error(
                    resp,
                    full_path=full_path,
                    retries_exhausted=True,
                    max_retries=retry_policy.max_retries,
                )

            retry_after = self._build_rate_limit_delay(
                resp,
                attempt_number=attempt_number,
                retry_policy=retry_policy,
            )
            logger.warning(
                "Kalshi %s GET rate limited for %s; retrying in %.2fs (attempt %d/%d).",
                rate_limit_label,
                full_path,
                retry_after,
                attempt_number,
                retry_policy.max_retries,
            )
            time.sleep(retry_after)
        raise RuntimeError(f"Kalshi GET retry loop ended unexpectedly for {full_path}")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._get_json_with_retry(
            path,
            params,
            use_auth=True,
            sleep_sec=self.authenticated_sleep_sec,
            retry_policy=self.authenticated_retry_policy,
            rate_limit_label="live/authenticated",
        )

    def _get_public(self, path: str, params: dict[str, Any] | None = None, *, sleep: float | None = None) -> Any:
        """GET without auth headers — for public historical endpoints."""
        return self._get_json_with_retry(
            path,
            params,
            use_auth=False,
            sleep_sec=self.historical_sleep_sec if sleep is None else sleep,
            retry_policy=self.public_retry_policy,
            rate_limit_label="public historical",
        )

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
        category: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[KalshiMarket], str | None]:
        """Fetch one page of markets. Returns (markets, next_cursor)."""
        data = self._get("/markets", {
            "limit": limit,
            "status": status,
            "category": category,
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "cursor": cursor,
        })
        return [_parse_market(m) for m in data.get("markets", [])], data.get("cursor")

    def get_markets_raw(
        self,
        status: str | None = None,
        category: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        tickers: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one page of raw market payloads from the live endpoint."""
        data = self._get("/markets", {
            "limit": limit,
            "status": status,
            "category": category,
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "tickers": ",".join(tickers) if tickers else None,
            "cursor": cursor,
        })
        return data.get("markets", []), data.get("cursor")

    def get_events_raw(
        self,
        category: str | None = None,
        status: str | None = None,
        with_nested_markets: bool = False,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one page of raw event payloads from the live endpoint.

        Unlike ``/markets``, the ``/events`` endpoint correctly filters by
        ``category`` server-side, making it the efficient path when a category
        allowlist is configured.

        :param category:             Optional category filter (server-side, exact match).
        :param status:               Optional event status filter.
        :param with_nested_markets:  When True, each event includes a ``markets`` list.
        :param limit:                Page size (max 200).
        :param cursor:               Pagination cursor from a previous call.
        :returns:                    Tuple of (list of raw event dicts, next cursor or None).
        """
        data = self._get("/events", {
            "limit": limit,
            "category": category,
            "status": status,
            "with_nested_markets": "true" if with_nested_markets else "false",
            "cursor": cursor,
        })
        return data.get("events", []), data.get("cursor")

    def get_all_events_raw(
        self,
        category: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all events matching the given filters."""
        all_events: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            events, cursor = self.get_events_raw(
                category=category,
                status=status,
                limit=200,
                cursor=cursor,
            )
            all_events.extend(events)
            if not cursor:
                break
        return all_events

    def get_settled_markets(
        self,
        category: str | None = None,
        series_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one page of settled markets from the live endpoint with optional filters.

        Unlike ``/historical/markets``, the live ``/markets`` endpoint properly
        filters by ``category`` and ``series_ticker`` server-side, making it
        the efficient way to retrieve Economics/Politics markets without
        scanning through millions of sports markets.

        Requires authentication (uses ``_get``).
        """
        data = self._get("/markets", {
            "status": "settled",
            "category": category,
            "series_ticker": series_ticker,
            "limit": limit,
            "cursor": cursor,
        })
        return data.get("markets", []), data.get("cursor")

    def get_all_markets(
        self,
        status: str | None = None,
        category: str | None = None,
        series_ticker: str | None = None,
    ) -> list[KalshiMarket]:
        """Paginate through all markets matching the given filters."""
        all_markets: list[KalshiMarket] = []
        cursor: str | None = None
        while True:
            markets, cursor = self.get_markets(
                status=status, category=category, series_ticker=series_ticker, limit=200, cursor=cursor
            )
            all_markets.extend(markets)
            if not cursor:
                break
        return all_markets

    def get_all_markets_raw(
        self,
        status: str | None = None,
        category: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through raw live markets while preserving settlement fields."""
        all_markets: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            markets, cursor = self.get_markets_raw(
                status=status,
                category=category,
                series_ticker=series_ticker,
                event_ticker=event_ticker,
                limit=limit,
                tickers=tickers,
                cursor=cursor,
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

    def get_trades_raw(
        self,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one page of raw live trades."""
        data = self._get("/markets/trades", {
            "ticker": ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "limit": limit,
            "cursor": cursor,
        })
        return data.get("trades", []), data.get("cursor")

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

    def get_all_trades_raw(
        self,
        ticker: str,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Paginate through raw live trades for a ticker."""
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            trades, cursor = self.get_trades_raw(
                ticker=ticker,
                min_ts=min_ts,
                max_ts=max_ts,
                limit=limit,
                cursor=cursor,
            )
            all_trades.extend(trades)
            if not cursor:
                break
        return all_trades

    # ── Historical Market Data (no auth required) ────────────────────────────

    def get_historical_markets(
        self,
        limit: int = 200,
        cursor: str | None = None,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        sleep: float | None = None,
        series_ticker: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch one page of resolved historical markets.

        Returns raw dicts (not parsed KalshiMarket) so that the ``result``
        field and other historical-only fields are preserved. Kalshi's
        endpoint paginates by cursor but does not expose close-time query
        filters, so ``min_close_ts`` / ``max_close_ts`` are applied
        client-side to the returned page.

        :param limit:         Page size (max 200).
        :param cursor:        Pagination cursor from a previous call.
        :param min_close_ts:  Lower bound on close_time (Unix seconds).
        :param max_close_ts:  Upper bound on close_time (Unix seconds).
        :param series_ticker: When provided, fetch only markets for this series.
        :returns:             Tuple of (list of raw market dicts, next cursor or None).
        """
        params: dict[str, Any] = {
            "limit": limit,
            "cursor": cursor,
        }
        if series_ticker is not None:
            params["series_ticker"] = series_ticker
        data = self._get_public(
            "/historical/markets",
            params,
            sleep=sleep,
        )
        markets = [
            market
            for market in data.get("markets", [])
            if _market_close_time_in_range(
                market,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
            )
        ]
        return markets, data.get("cursor")

    def get_all_historical_markets(
        self,
        limit: int = 200,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        sleep: float | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all resolved historical markets in the given time range."""
        all_markets: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            markets, cursor = self.get_historical_markets(
                limit=limit,
                cursor=cursor,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
                sleep=sleep,
            )
            all_markets.extend(markets)
            if not cursor:
                break
        return all_markets

    def get_historical_cutoff(self) -> dict[str, Any]:
        """Fetch the live/historical boundary timestamps."""
        return self._get_public("/historical/cutoff")

    def get_historical_market(self, ticker: str) -> dict[str, Any]:
        """
        Fetch a single resolved market by ticker.

        The response includes a ``result`` field (``"yes"`` or ``"no"``)
        indicating the market outcome.
        """
        data = self._get_public(f"/historical/markets/{ticker}")
        return data.get("market", data)

    def get_historical_trades(
        self,
        ticker: str,
        limit: int = 1000,
        cursor: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        sleep: float | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch one page of historical trades for a ticker.

        :returns: Tuple of (list of raw trade dicts, next cursor or None).
        """
        data = self._get_public(
            "/historical/trades",
            {
                "ticker": ticker,
                "limit": limit,
                "cursor": cursor,
                "min_ts": min_ts,
                "max_ts": max_ts,
            },
            sleep=sleep,
        )
        return data.get("trades", []), data.get("cursor")

    def get_all_historical_trades(
        self,
        ticker: str,
        limit: int = 1000,
        min_ts: int | None = None,
        max_ts: int | None = None,
        sleep: float | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all historical trades for a ticker."""
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            trades, cursor = self.get_historical_trades(
                ticker=ticker,
                limit=limit,
                cursor=cursor,
                min_ts=min_ts,
                max_ts=max_ts,
                sleep=sleep,
            )
            all_trades.extend(trades)
            if not cursor:
                break
        return all_trades

    def get_market_candlesticks_raw(
        self,
        ticker: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        period_interval: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch raw live candlestick payloads for a market.

        The endpoint is used only for markets that settled after the historical cutoff.
        """
        data = self._get(
            f"/markets/{ticker}/candlesticks",
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )
        return data.get("candlesticks", data.get("market_candlesticks", []))

    def get_historical_market_candlesticks_raw(
        self,
        ticker: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        period_interval: int | None = None,
        sleep: float | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch raw historical candlestick payloads for an archived market."""
        data = self._get_public(
            f"/historical/markets/{ticker}/candlesticks",
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
            sleep=sleep,
        )
        return data.get("candlesticks", data.get("market_candlesticks", []))

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
