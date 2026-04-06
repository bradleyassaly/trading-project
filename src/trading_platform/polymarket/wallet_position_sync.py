"""
Sync wallet positions from Polymarket Data API.

Fetches current open positions for each tracked wallet
and stores in the intelligence DB.

Usage::

    from trading_platform.polymarket.wallet_position_sync import sync_wallet_positions
    sync_wallet_positions()
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from trading_platform.polymarket.wallet_db import WalletDB, _now_ts

logger = logging.getLogger(__name__)

_DATA_API = "https://data-api.polymarket.com"
_RATE_LIMIT_SLEEP = 0.5


def sync_wallet_positions(
    *,
    db: WalletDB | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    """Sync positions for all tracked wallets. Returns summary stats."""
    db = db or WalletDB()
    wallets = db.get_all_wallets()
    if top_n:
        wallets = wallets[:top_n]

    total_positions = 0
    errors = 0

    for i, wallet in enumerate(wallets):
        if (i + 1) % 25 == 0 or i == 0:
            print(f"  Syncing positions {i+1}/{len(wallets)}: {wallet[:16]}...")

        try:
            count = _sync_one_wallet(db, wallet)
            total_positions += count
        except Exception as exc:
            errors += 1
            logger.debug("Position sync failed for %s: %s", wallet[:16], exc)

        time.sleep(_RATE_LIMIT_SLEEP)

    return {"wallets_synced": len(wallets), "positions": total_positions, "errors": errors}


def _sync_one_wallet(db: WalletDB, wallet: str) -> int:
    """Fetch and store positions for one wallet. Returns count stored."""
    positions = _fetch_positions(wallet)
    count = 0

    for p in positions:
        size = _safe_float(p.get("size", 0))
        if size < 1.0:
            continue  # filter dust

        asset = p.get("asset", "")
        if not asset:
            continue

        db.upsert_position(
            wallet=wallet,
            asset=asset,
            condition_id=p.get("conditionId"),
            title=p.get("title", ""),
            slug=p.get("slug", ""),
            outcome=p.get("outcome", ""),
            size=size,
            avg_price=_safe_float(p.get("avgPrice")),
            initial_value=_safe_float(p.get("initialValue")),
            current_value=_safe_float(p.get("currentValue")),
            cash_pnl=_safe_float(p.get("cashPnl")),
            percent_pnl=_safe_float(p.get("percentPnl")),
            realized_pnl=_safe_float(p.get("realizedPnl")),
            cur_price=_safe_float(p.get("curPrice")),
            end_date=p.get("endDate", ""),
        )
        count += 1

    return count


def _fetch_positions(wallet: str, limit: int = 500) -> list[dict[str, Any]]:
    try:
        resp = requests.get(
            f"{_DATA_API}/positions",
            params={"user": wallet, "limit": limit},
            timeout=15,
        )
        if resp.status_code == 429:
            time.sleep(5)
            return []
        if resp.status_code != 200:
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("data", data.get("positions", []))
    except Exception:
        return []


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
