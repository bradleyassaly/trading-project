"""
Seed wallet_intelligence.db from Polymarket's leaderboard.

Scrapes wallet addresses from Polymarket leaderboard pages,
fetches trades and positions, and inserts into the intelligence DB.

Usage::

    from trading_platform.polymarket.seed_leaderboard import seed_from_leaderboard
    seed_from_leaderboard()
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

import requests

from trading_platform.polymarket.wallet_db import WalletDB, _now_ts
from trading_platform.polymarket.wallet_trade_sync import _sync_one_wallet
from trading_platform.polymarket.wallet_position_sync import _sync_one_wallet as _sync_positions
from trading_platform.polymarket.address_map import AddressMap

logger = logging.getLogger(__name__)

# Known top wallets (manually curated from PM leaderboard)
_KNOWN_TOP_WALLETS: dict[str, str] = {
    "0x8c80d213c0cbad777d06ee3f58f6ca4bc03102c3": "SecondWindCapital",
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": "Spirit of Ukraine",
    "0x96489abcb9f583d6835c8ef95ffc923d05a86825": "anoin123",
    "0xfd22b8843ae03a33a8a4c5e39ef1e5ff33ebad91": "AML",
    "0xdbade4c82fb72780a0db9a38f821d8671aba9c95": "SwissMiss",
    "0xcb3143ee858e14d0b3fe40ffeaea78416e646b02": "0xCB3143",
    "0x848921be10e51b1547d50548a54b38a1778ec184": "trevors4",
    "0xf2f6af4f27ec2dcf4072095ab804d2c0a4bc6082": "gopfan2",
}

_LEADERBOARD_URL = "https://polymarket.com/leaderboard"
_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def seed_from_leaderboard(
    *,
    db: WalletDB | None = None,
    scrape_html: bool = True,
    sleep_seconds: float = 0.5,
) -> dict[str, Any]:
    """Seed the intelligence DB with wallets from Polymarket's leaderboard."""
    db = db or WalletDB()
    addr_map = AddressMap()

    # Step 1: Collect wallet addresses
    wallets: dict[str, str] = dict(_KNOWN_TOP_WALLETS)  # addr -> name

    if scrape_html:
        scraped = _scrape_leaderboard_pages()
        for addr in scraped:
            if addr not in wallets:
                wallets[addr] = ""

    print(f"  Collected {len(wallets)} unique wallets ({len(_KNOWN_TOP_WALLETS)} known + {len(wallets) - len(_KNOWN_TOP_WALLETS)} scraped)")

    # Step 2: Filter to new wallets not already in DB
    existing = set(db.get_all_wallets())
    new_wallets = {a: n for a, n in wallets.items() if a.lower() not in {e.lower() for e in existing}}
    print(f"  New wallets (not in DB): {len(new_wallets)}")

    if not new_wallets:
        print("  All wallets already in DB, syncing trades/positions only")
        new_wallets = wallets  # resync all for trade updates

    # Step 3: Seed profiles and sync data
    synced = 0
    new_trades = 0
    new_positions = 0
    errors = 0

    for i, (addr, name) in enumerate(new_wallets.items()):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Seeding {i+1}/{len(new_wallets)}: {name or addr[:16]}...")

        # Ensure profile exists
        db.upsert_profile(
            addr,
            notes=name if name else None,
            last_synced_ts=0,  # force sync
        )

        # Sync trades
        try:
            nt, _ = _sync_one_wallet(db, addr, addr_map)
            new_trades += nt
        except Exception as exc:
            logger.debug("Trade sync failed for %s: %s", addr[:16], exc)
            errors += 1

        # Sync positions
        try:
            np = _sync_positions(db, addr)
            new_positions += np
        except Exception as exc:
            logger.debug("Position sync failed for %s: %s", addr[:16], exc)

        synced += 1
        time.sleep(sleep_seconds)

    return {
        "wallets_collected": len(wallets),
        "wallets_synced": synced,
        "new_trades": new_trades,
        "new_positions": new_positions,
        "errors": errors,
    }


def _scrape_leaderboard_pages() -> set[str]:
    """Scrape Polymarket leaderboard HTML for wallet addresses."""
    addresses: set[str] = set()

    categories = ["all", "politics", "crypto", "sports"]
    windows = ["weekly", "monthly", "all"]

    for cat in categories:
        for window in windows:
            url = f"{_LEADERBOARD_URL}/{cat}/{window}/profit"
            try:
                resp = requests.get(url, timeout=15, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; TradingPlatform/1.0)",
                })
                if resp.status_code != 200:
                    logger.debug("Leaderboard %s/%s: HTTP %d", cat, window, resp.status_code)
                    continue

                # Extract wallet addresses from profile links
                found = set(_ADDR_RE.findall(resp.text))
                addresses.update(a.lower() for a in found)
                logger.debug("Leaderboard %s/%s: found %d addresses", cat, window, len(found))
            except Exception as exc:
                logger.debug("Leaderboard scrape failed %s/%s: %s", cat, window, exc)

            time.sleep(0.5)

    return addresses
