"""
EOA to proxy wallet address mapping for Polymarket.

Polymarket uses proxy contracts for trading. On-chain data (Goldsky) uses
EOA addresses while the Data API uses proxy addresses. This module maintains
the bidirectional mapping.

Usage::

    from trading_platform.polymarket.address_map import AddressMap
    am = AddressMap()
    proxy = am.get_proxy("0xeoa...")
    eoa = am.get_eoa("0xproxy...")
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "data/polymarket/wallet_address_map.parquet"
_GAMMA_API = "https://gamma-api.polymarket.com"


class AddressMap:
    """Bidirectional EOA <-> proxy wallet mapping."""

    def __init__(self, path: str | Path = _DEFAULT_PATH) -> None:
        self._path = Path(path)
        self._eoa_to_proxy: dict[str, str] = {}
        self._proxy_to_eoa: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            df = pd.read_parquet(self._path)
            for _, row in df.iterrows():
                eoa = str(row.get("eoa_address", "")).lower()
                proxy = str(row.get("proxy_wallet", "")).lower()
                if eoa and proxy and eoa != "nan" and proxy != "nan":
                    self._eoa_to_proxy[eoa] = proxy
                    self._proxy_to_eoa[proxy] = eoa
            logger.info("Loaded %d address mappings", len(self._eoa_to_proxy))
        except Exception as exc:
            logger.warning("Failed to load address map: %s", exc)

    def get_proxy(self, eoa: str) -> str | None:
        return self._eoa_to_proxy.get(eoa.lower())

    def get_eoa(self, proxy: str) -> str | None:
        return self._proxy_to_eoa.get(proxy.lower())

    def resolve_for_data_api(self, wallet: str) -> str:
        """Return the proxy address to use with Data API, or the wallet itself."""
        w = wallet.lower()
        # If it's already a known proxy, use directly
        if w in self._proxy_to_eoa:
            return w
        # If it's an EOA, return its proxy
        proxy = self._eoa_to_proxy.get(w)
        if proxy:
            return proxy
        # Try fetching from Gamma
        proxy = self._fetch_and_store(w)
        return proxy or w

    def _fetch_and_store(self, address: str) -> str | None:
        """Fetch proxy mapping from Gamma API and cache it."""
        try:
            resp = requests.get(
                f"{_GAMMA_API}/profiles",
                params={"address": address},
                timeout=10,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None
            profile = data[0] if isinstance(data, list) else data
            proxy = (profile.get("proxyWallet") or profile.get("proxy_wallet") or "").lower()
            eoa = (profile.get("address") or profile.get("eoa") or address).lower()
            if proxy and eoa:
                self._eoa_to_proxy[eoa] = proxy
                self._proxy_to_eoa[proxy] = eoa
                return proxy
        except Exception:
            pass
        return None

    def build_map(
        self,
        wallets: list[str],
        *,
        sleep_seconds: float = 0.3,
    ) -> int:
        """Fetch proxy mappings for a list of wallets. Returns count of new mappings."""
        new = 0
        for i, w in enumerate(wallets):
            if (i + 1) % 50 == 0 or i == 0:
                print(f"  Mapping addresses {i+1}/{len(wallets)}...")
            wl = w.lower()
            if wl in self._eoa_to_proxy or wl in self._proxy_to_eoa:
                continue
            proxy = self._fetch_and_store(wl)
            if proxy:
                new += 1
            time.sleep(sleep_seconds)
        self._save()
        return new

    def _save(self) -> None:
        rows = []
        for eoa, proxy in self._eoa_to_proxy.items():
            rows.append({"eoa_address": eoa, "proxy_wallet": proxy})
        if rows:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_parquet(self._path, index=False)
            logger.info("Saved %d address mappings to %s", len(rows), self._path)

    @property
    def size(self) -> int:
        return len(self._eoa_to_proxy)
