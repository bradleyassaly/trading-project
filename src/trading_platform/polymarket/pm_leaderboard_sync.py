"""Sync Polymarket's authoritative PnL leaderboard into wallet_profiles.

Our internal net_pnl_usdc is computed only over resolved trades we have
in wallet_trades — undercounts wallets with many open positions or
sells-before-resolution. Polymarket's public leaderboard computes the
real number. This module pulls it and stores alongside our computed
values so the GUI/analytics can surface the authoritative figure.

Adds two columns to wallet_profiles (idempotent ALTER):
  - pm_pnl_usdc      — authoritative PnL from lb-api.polymarket.com/profit
  - pm_volume_usdc   — lifetime volume from lb-api...volume
  - pm_synced_at     — epoch seconds of last refresh
  - pseudonym, name  — from the leaderboard profile
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)

PROFIT_URL = "https://lb-api.polymarket.com/profit"
VOLUME_URL = "https://lb-api.polymarket.com/volume"


def _ensure_columns() -> None:
    with db() as c:
        for col, typ in (
            ("pm_pnl_usdc", "DOUBLE PRECISION"),
            ("pm_volume_usdc", "DOUBLE PRECISION"),
            ("pm_synced_at", "BIGINT"),
        ):
            try:
                c.execute(f'ALTER TABLE wallet_profiles ADD COLUMN {col} {typ}')
            except Exception:
                pass  # already exists


def fetch_leaderboard(url: str, window: str = "all", limit: int = 500) -> list[dict]:
    """Pull a leaderboard window, paginating with offset. The PM endpoint
    returns at most 50 rows per call; we step offset to reach `limit`.
    """
    out: list[dict] = []
    page_size = 50  # endpoint hard cap
    offset = 0
    while len(out) < limit:
        try:
            r = requests.get(
                url,
                params={"window": window, "limit": page_size, "offset": offset},
                timeout=20,
            )
            r.raise_for_status()
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            out.extend(batch)
            if len(batch) < page_size:
                break  # reached the end
            offset += page_size
            time.sleep(0.1)  # courtesy rate limit
        except Exception as exc:
            logger.warning("leaderboard fetch failed %s %s offset=%s: %s",
                           url, window, offset, exc)
            break
    return out[:limit]


def sync(limit: int = 500) -> dict[str, Any]:
    """Pull top-N by profit and volume across multiple time windows.

    PM's lb-api caps each request at 50 rows and doesn't respect
    offset pagination, so we diversify by window (`all`, `1d`) — each
    returns a different top-50 slate. Combined profit + volume across
    both windows typically yields ~150–200 unique wallets.
    """
    _ensure_columns()
    t0 = time.time()
    now = int(time.time())

    profits: list[dict] = []
    volumes: list[dict] = []
    for window in ("all", "1d"):
        profits.extend(fetch_leaderboard(PROFIT_URL, window=window, limit=50))
        volumes.extend(fetch_leaderboard(VOLUME_URL, window=window, limit=50))

    # Build merged view keyed by wallet
    merged: dict[str, dict] = {}
    for row in profits:
        w = (row.get("proxyWallet") or "").lower()
        if not w:
            continue
        merged[w] = {
            "wallet": w,
            "pm_pnl_usdc": float(row.get("amount") or 0),
            "pseudonym": row.get("pseudonym") or row.get("name"),
            "name": row.get("name"),
        }
    for row in volumes:
        w = (row.get("proxyWallet") or "").lower()
        if not w:
            continue
        entry = merged.setdefault(w, {"wallet": w})
        entry["pm_volume_usdc"] = float(row.get("amount") or 0)
        if not entry.get("pseudonym"):
            entry["pseudonym"] = row.get("pseudonym") or row.get("name")
        if not entry.get("name"):
            entry["name"] = row.get("name")

    # Write. Seed wallet_profiles row if missing (so all top wallets
    # become known to the rest of the pipeline).
    written = 0
    new_profiles = 0
    conn = get_connection()
    try:
        for w, data in merged.items():
            exists = conn.execute(
                "SELECT 1 FROM wallet_profiles WHERE wallet = ?", (w,)
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO wallet_profiles (wallet, wallet_type, notes) "
                    "VALUES (?, ?, ?)",
                    (w, "pm_leaderboard_discovered",
                     f"auto-seeded from PM leaderboard {time.strftime('%Y-%m-%d')}"),
                )
                new_profiles += 1
            conn.execute(
                "UPDATE wallet_profiles SET pm_pnl_usdc = ?, pm_volume_usdc = ?, "
                "pm_synced_at = ?, pseudonym = COALESCE(pseudonym, ?) "
                "WHERE wallet = ?",
                (
                    data.get("pm_pnl_usdc"),
                    data.get("pm_volume_usdc"),
                    now,
                    data.get("pseudonym"),
                    w,
                ),
            )
            written += 1
        conn.commit()
    finally:
        conn.close()

    # Promote PM-authoritative whales to tier1h regardless of our
    # internal stats. Our computed net_pnl_usdc undercounts wallets that
    # sell before resolution; PM's leaderboard is the ground truth. Any
    # wallet with pm_pnl_usdc ≥ $100K earns tier1h treatment in signals.
    promoted = _promote_pm_whales(min_pnl=100_000)

    return {
        "profits_fetched": len(profits),
        "volumes_fetched": len(volumes),
        "unique_wallets": len(merged),
        "new_profiles_seeded": new_profiles,
        "upserts": written,
        "tier1h_promoted": promoted,
        "elapsed_seconds": round(time.time() - t0, 1),
    }


def _promote_pm_whales(min_pnl: float = 100_000) -> int:
    """Upsert wallets with high PM-authoritative PnL into leaderboard as tier1h."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT wallet, pm_pnl_usdc, pseudonym FROM wallet_profiles "
            "WHERE pm_pnl_usdc IS NOT NULL AND pm_pnl_usdc >= ?",
            (min_pnl,),
        ).fetchall()
        upserted = 0
        now = int(time.time())
        for wallet, pnl, pseudo in rows:
            try:
                conn.execute(
                    """INSERT INTO leaderboard
                       (wallet, tier, leaderboard_source, net_pnl_usdc, pseudonym, updated_at)
                       VALUES (?, 'tier1h', 'pm_authoritative', ?, ?, ?)
                       ON CONFLICT (wallet) DO UPDATE SET
                         tier = 'tier1h',
                         leaderboard_source = 'pm_authoritative',
                         net_pnl_usdc = EXCLUDED.net_pnl_usdc,
                         pseudonym = COALESCE(EXCLUDED.pseudonym, leaderboard.pseudonym),
                         updated_at = EXCLUDED.updated_at""",
                    (wallet, pnl, pseudo, now),
                )
                upserted += 1
            except Exception as exc:
                logger.debug("promote failed for %s: %s", wallet[:12], exc)
        conn.commit()
        return upserted
    finally:
        conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = sync()
    print(f"pm leaderboard sync: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
