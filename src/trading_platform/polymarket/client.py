from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

_DEFAULT_TIMEOUT = 15
_DEFAULT_SLEEP_SEC = 0.05
_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
_CLOB_BASE_URL = "https://clob.polymarket.com"


@dataclass(frozen=True)
class PolymarketConfig:
    gamma_base_url: str = _GAMMA_BASE_URL
    clob_base_url: str = _CLOB_BASE_URL
    request_sleep_sec: float = _DEFAULT_SLEEP_SEC

    def __post_init__(self) -> None:
        if self.request_sleep_sec < 0:
            raise ValueError("request_sleep_sec must be >= 0.")


class PolymarketClient:
    def __init__(self, config: PolymarketConfig | None = None) -> None:
        self.config = config or PolymarketConfig()
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def _gamma_url(self, path: str) -> str:
        return f"{self.config.gamma_base_url}{path}"

    def _clob_url(self, path: str) -> str:
        return f"{self.config.clob_base_url}{path}"

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        time.sleep(self.config.request_sleep_sec)
        response = self._session.get(url, params=params, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    # ── Gamma API ─────────────────────────────────────────────────────────────

    def get_markets(
        self,
        *,
        tag_slug: str | None = None,
        closed: bool = True,
        limit: int = 100,
        offset: int = 0,
        order: str | None = None,
        ascending: bool | None = None,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
        active: bool | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """
        Fetch one page of markets from the Gamma API.

        Returns (markets, next_offset) where next_offset is None when the
        last page has been reached.

        Note: the ``order`` / ``ascending`` params break ``tag_slug`` filtering
        on the Gamma API.  When filtering by tag, prefer ``end_date_min``
        (ISO-8601 date string) to skip old markets instead of sorting.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "closed": "true" if closed else "false",
        }
        if tag_slug:
            params["tag_slug"] = tag_slug
        if order is not None:
            params["order"] = order
        if ascending is not None:
            params["ascending"] = "true" if ascending else "false"
        if end_date_min is not None:
            params["end_date_min"] = end_date_min
        if end_date_max is not None:
            params["end_date_max"] = end_date_max
        if active is not None:
            params["active"] = "true" if active else "false"
        payload = self._get(self._gamma_url("/markets"), params=params)
        markets: list[dict[str, Any]]
        if isinstance(payload, list):
            # Gamma API occasionally returns nested lists — flatten one level
            # so callers always receive [dict, dict, ...].
            flat: list[dict[str, Any]] = []
            for item in payload:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            markets = flat
        elif isinstance(payload, dict):
            markets = payload.get("markets") or payload.get("data") or []
        else:
            markets = []
        next_offset: int | None = offset + len(markets) if len(markets) == limit else None
        return markets, next_offset

    def get_all_markets(
        self,
        *,
        tag_slug: str | None = None,
        closed: bool = True,
        order: str | None = None,
        ascending: bool | None = None,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
        active: bool | None = None,
        _page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Paginate through all markets for a given tag_slug."""
        all_markets: list[dict[str, Any]] = []
        offset = 0
        while True:
            page, next_offset = self.get_markets(
                tag_slug=tag_slug, closed=closed, limit=_page_size, offset=offset,
                order=order, ascending=ascending, end_date_min=end_date_min,
                end_date_max=end_date_max, active=active,
            )
            all_markets.extend(page)
            if next_offset is None:
                break
            offset = next_offset
        return all_markets

    def get_market(self, market_id: str) -> dict[str, Any]:
        """Fetch a single market by ID."""
        return self._get(self._gamma_url(f"/markets/{market_id}"))

    # Backward-compatible alias used by existing cross-market-monitor code
    def list_markets(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        active: bool | None = True,
        closed: bool | None = False,
        archived: bool | None = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if active is not None:
            params["active"] = active
        if closed is not None:
            params["closed"] = closed
        if archived is not None:
            params["archived"] = archived
        payload = self._get(self._gamma_url("/markets"), params=params)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("data") or payload.get("markets") or []
        return []

    # ── CLOB API ──────────────────────────────────────────────────────────────

    def get_price_history(
        self,
        token_id: str,
        *,
        interval: str = "1h",
        fidelity: int = 60,
    ) -> list[dict[str, Any]]:
        """
        Fetch hourly price history from the CLOB prices-history endpoint.

        Returns a list of ``{"t": unix_timestamp, "p": price_0_to_1}`` dicts.
        ``token_id`` is a clobTokenId from the Gamma market object (index 0
        for the YES outcome).
        """
        params: dict[str, Any] = {
            "market": token_id,
            "interval": interval,
            "fidelity": fidelity,
        }
        payload = self._get(self._clob_url("/prices-history"), params=params)
        if isinstance(payload, dict):
            return payload.get("history", [])
        return []
