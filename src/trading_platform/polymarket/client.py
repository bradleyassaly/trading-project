from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

_DEFAULT_TIMEOUT = 15
_DEFAULT_SLEEP_SEC = 0.05
_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class PolymarketConfig:
    gamma_base_url: str = _GAMMA_BASE_URL
    request_sleep_sec: float = _DEFAULT_SLEEP_SEC

    def __post_init__(self) -> None:
        if self.request_sleep_sec < 0:
            raise ValueError("request_sleep_sec must be >= 0.")


class PolymarketClient:
    def __init__(self, config: PolymarketConfig | None = None) -> None:
        self.config = config or PolymarketConfig()
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.config.gamma_base_url}{path}"

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        time.sleep(self.config.request_sleep_sec)
        response = self._session.get(self._url(path), params=params, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def list_markets(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        active: bool | None = True,
        closed: bool | None = False,
        archived: bool | None = False,
    ) -> list[dict[str, Any]]:
        params = {
            "limit": limit,
            "offset": offset,
            "active": active,
            "closed": closed,
            "archived": archived,
        }
        payload = self._get("/markets", params=params)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        return []

    def get_all_markets(
        self,
        *,
        limit: int = 500,
        active: bool | None = True,
        closed: bool | None = False,
        archived: bool | None = False,
        max_markets: int | None = None,
    ) -> list[dict[str, Any]]:
        offset = 0
        markets: list[dict[str, Any]] = []
        while True:
            page = self.list_markets(limit=limit, offset=offset, active=active, closed=closed, archived=archived)
            if not page:
                break
            markets.extend(page)
            if max_markets is not None and len(markets) >= max_markets:
                return markets[:max_markets]
            if len(page) < limit:
                break
            offset += limit
        return markets
