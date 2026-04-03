from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)

BINANCE_SPOT_BASE_URL = "https://api.binance.com"
_DEFAULT_TIMEOUT_SEC = 15.0


@dataclass(frozen=True)
class BinanceClientConfig:
    base_url: str = BINANCE_SPOT_BASE_URL
    request_sleep_sec: float = 0.1
    max_retries: int = 5
    backoff_base_sec: float = 0.5
    backoff_max_sec: float = 8.0
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC


@dataclass
class BinanceClientStats:
    request_count: int = 0
    retry_count: int = 0


class BinanceClient:
    def __init__(self, config: BinanceClientConfig | None = None, session: requests.Session | None = None) -> None:
        self.config = config or BinanceClientConfig()
        self.session = session or requests.Session()
        self.stats = BinanceClientStats()

    def _build_delay(self, attempt_number: int) -> float:
        base = min(self.config.backoff_base_sec * (2 ** max(attempt_number - 1, 0)), self.config.backoff_max_sec)
        return base + random.uniform(0.0, min(0.5, self.config.backoff_base_sec))

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        cleaned = {key: value for key, value in (params or {}).items() if value is not None}
        time.sleep(self.config.request_sleep_sec)
        url = f"{self.config.base_url}{path}"
        for attempt_number in range(1, self.config.max_retries + 2):
            self.stats.request_count += 1
            try:
                response = self.session.get(url, params=cleaned, timeout=self.config.timeout_sec)
            except requests.RequestException as exc:
                if attempt_number > self.config.max_retries:
                    raise
                self.stats.retry_count += 1
                delay = self._build_delay(attempt_number)
                logger.warning("Binance GET %s transport failure (%s). Retrying in %.2fs", path, exc, delay)
                time.sleep(delay)
                continue
            if response.status_code in {429, 500, 502, 503, 504} and attempt_number <= self.config.max_retries:
                self.stats.retry_count += 1
                delay = self._build_delay(attempt_number)
                logger.warning(
                    "Binance GET %s returned %s. Retrying in %.2fs",
                    path,
                    response.status_code,
                    delay,
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Binance GET {path} exhausted retries")

    def get_exchange_info(self) -> dict[str, Any]:
        return dict(self._get("/api/v3/exchangeInfo"))

    def get_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int,
    ) -> list[list[Any]]:
        payload = self._get(
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "limit": limit,
            },
        )
        return list(payload or [])

    def get_agg_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        from_id: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            "/api/v3/aggTrades",
            params={
                "symbol": symbol,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "fromId": from_id,
                "limit": limit,
            },
        )
        return [dict(row) for row in (payload or [])]

    def get_book_ticker(self, *, symbol: str) -> dict[str, Any]:
        return dict(self._get("/api/v3/ticker/bookTicker", params={"symbol": symbol}))
