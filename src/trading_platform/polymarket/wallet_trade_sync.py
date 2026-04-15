"""
Sync wallet trade history from Polymarket Data API.

Fetches trade history for each wallet in the intelligence DB,
categorizes by market type, and stores for downstream analysis.

Usage::

    from trading_platform.polymarket.wallet_trade_sync import sync_wallet_trades
    sync_wallet_trades(top_n=200)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from trading_platform.polymarket.wallet_db import WalletDB, categorize_slug, _now_ts

logger = logging.getLogger(__name__)

_DATA_API = "https://data-api.polymarket.com"
_RATE_LIMIT_SLEEP = 0.5
_ERROR_LOG = Path("data/polymarket/sync_errors.log")


def sync_wallet_trades(
    *,
    db: WalletDB | None = None,
    top_n: int = 200,
    max_age_hours: float = 2.0,
) -> dict[str, Any]:
    """Sync trades for wallets needing refresh. Returns summary stats."""
    from trading_platform.polymarket.address_map import AddressMap

    db = db or WalletDB()
    addr_map = AddressMap()
    _ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)

    wallets = db.get_profiles_needing_sync(max_age_hours=max_age_hours)
    if top_n:
        wallets = wallets[:top_n]

    total_new = 0
    total_skipped = 0
    errors = 0

    for i, wallet in enumerate(wallets):
        if (i + 1) % 25 == 0 or i == 0:
            print(f"  Syncing trades {i+1}/{len(wallets)}: {wallet[:16]}...")

        try:
            new, skipped = _sync_one_wallet(db, wallet, addr_map)
            total_new += new
            total_skipped += skipped
        except Exception as exc:
            errors += 1
            logger.debug("Trade sync failed for %s: %s", wallet[:16], exc)
            _log_error(wallet, str(exc))

        time.sleep(_RATE_LIMIT_SLEEP)

    return {"wallets_synced": len(wallets), "new_trades": total_new,
            "skipped": total_skipped, "errors": errors}


def _sync_one_wallet(db: WalletDB, wallet: str, addr_map: Any = None) -> tuple[int, int]:
    """Fetch and store trades for one wallet. Returns (new, skipped)."""
    # Resolve to proxy address for Data API
    fetch_addr = addr_map.resolve_for_data_api(wallet) if addr_map else wallet
    trades = _fetch_trades(fetch_addr)
    new = 0
    skipped = 0

    for t in trades:
        tx_hash = t.get("transactionHash") or t.get("id", "")
        if not tx_hash:
            skipped += 1
            continue

        slug = t.get("slug") or t.get("market_slug", "")
        category = categorize_slug(slug)
        ts = _parse_ts(t.get("timestamp") or t.get("createdAt", ""))

        inserted = db.upsert_trade(
            wallet=wallet,
            proxy_wallet=t.get("proxyWallet"),
            asset=t.get("asset"),
            condition_id=t.get("conditionId"),
            side=t.get("side", "").upper(),
            size=_safe_float(t.get("size")),
            price=_safe_float(t.get("price")),
            timestamp=ts,
            title=t.get("title", ""),
            slug=slug,
            outcome=t.get("outcome", ""),
            event_slug=t.get("eventSlug", ""),
            transaction_hash=tx_hash,
            category=category,
            synced_at=_now_ts(),
        )
        if inserted:
            new += 1
        else:
            skipped += 1

    db.upsert_profile(wallet, last_synced_ts=_now_ts())
    return new, skipped


def _fetch_trades(wallet: str, limit: int = 500, max_pages: int = 20) -> list[dict[str, Any]]:
    """Fetch trades from Polymarket Data API with pagination.

    Paginates until fewer than ``limit`` results are returned or
    ``max_pages`` is reached. Each page sleeps 0.5s for rate limiting.
    """
    all_trades: list[dict[str, Any]] = []
    cursor: str | None = None

    for page in range(max_pages):
        try:
            params: dict[str, Any] = {"user": wallet, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(
                f"{_DATA_API}/trades",
                params=params,
                timeout=15,
            )
            if resp.status_code == 429:
                logger.debug("Rate limited for %s, sleeping 5s", wallet[:16])
                time.sleep(5)
                continue
            if resp.status_code != 200:
                break
            data = resp.json()
            if isinstance(data, list):
                batch = data
                cursor = None
            else:
                batch = data.get("data", data.get("trades", []))
                cursor = data.get("next_cursor", data.get("nextCursor"))

            if not batch:
                break
            all_trades.extend(batch)

            # If fewer than limit returned, we've exhausted the history
            if len(batch) < limit:
                break
            # If no cursor-based pagination, stop after first page
            if cursor is None:
                break
            time.sleep(0.5)
        except Exception as exc:
            logger.debug("Trade fetch page %d failed for %s: %s", page, wallet[:16], exc)
            break

    return all_trades


def _parse_ts(val: Any) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return 0


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _log_error(wallet: str, msg: str) -> None:
    """Append error details to the sync error log."""
    _ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with _ERROR_LOG.open("a") as f:
        ts = datetime.now(tz=timezone.utc).isoformat()
        f.write(f"{ts} wallet={wallet[:16]} error={msg}\n")