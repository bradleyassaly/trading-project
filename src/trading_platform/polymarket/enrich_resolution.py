"""
Enrich wallet_trades with resolution outcomes from gamma_resolution.csv.

Matches trades by asset (token_id) to find which markets have resolved,
then sets market_resolved=1, market_outcome, and computes PnL.

Usage::

    from trading_platform.polymarket.enrich_resolution import enrich_trades
    enrich_trades()
"""
from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def enrich_trades(*, db: WalletDB | None = None) -> dict[str, Any]:
    """Match wallet_trades against gamma resolution data and set outcomes."""
    db = db or WalletDB()
    gamma_path = PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"

    if not gamma_path.exists():
        logger.warning("gamma_resolution.csv not found")
        return {"enriched": 0, "total_unresolved": 0}

    t0 = time.time()

    # Build resolution lookup: token_id -> resolution_price
    # resolution_price = 100.0 means this token won (YES outcome)
    # resolution_price = 0.0 means this token lost (NO outcome won)
    rp_map: dict[str, float] = {}
    q_map: dict[str, str] = {}
    with gamma_path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            tid = row.get("ticker", "").strip()
            rp_str = row.get("resolution_price", "")
            if not tid or not rp_str:
                continue
            try:
                rp = float(rp_str)
            except ValueError:
                continue
            if rp in (0.0, 100.0):
                rp_map[tid] = rp
            q = row.get("question", "")
            if q:
                q_map[tid] = q

    logger.info("Loaded %d resolved tokens from gamma", len(rp_map))

    # Get all unresolved trades from DB
    conn = db._conn
    with db._lock:
        rows = conn.execute(
            "SELECT id, asset, side, size, price, outcome FROM wallet_trades WHERE market_resolved = 0 AND asset IS NOT NULL"
        ).fetchall()

    logger.info("Unresolved trades to check: %d", len(rows))

    enriched = 0
    batch: list[tuple] = []

    for trade_id, asset, side, size, price, outcome_name in rows:
        if not asset:
            continue

        rp = rp_map.get(asset)
        if rp is None:
            continue

        # This token resolved. rp=100 means this specific token paid out.
        # The "outcome" field from the data API tells us what this token
        # represents — "Yes", "No", "Up", "Down", team names, etc. For
        # binary markets (Yes/No), we can derive the market-level outcome:
        #   YES token won (rp=100) → market outcome is YES
        #   YES token lost (rp=0) → market outcome is NO
        #   NO token won (rp=100) → market outcome is NO
        #   NO token lost (rp=0)  → market outcome is YES
        # The previous code used `"YES" if token_won else "NO"` which is
        # wrong for NO-token trades (inverts the market outcome).
        token_won = rp >= 99.0
        on_str = (outcome_name or "").strip().lower()
        if on_str in ("yes", ""):
            market_outcome = "YES" if token_won else "NO"
        elif on_str == "no":
            market_outcome = "NO" if token_won else "YES"
        else:
            # Non-binary market (sports team, Up/Down, Over/Under, etc.)
            # We don't know what "YES" means here — label by the token name
            market_outcome = outcome_name if token_won else f"NOT_{outcome_name}"

        # PnL calculation:
        # BUY side: trader paid (size * price) for the token
        #   If token won: payout = size, profit = size * (1 - price)
        #   If token lost: payout = 0, loss = -size * price
        # SELL side: trader received (size * price) for selling the token
        #   If token won: they missed the payout, loss = -size * (1 - price)
        #   If token lost: they avoided the loss, profit = size * price
        size = size or 0
        price = price or 0

        side_upper = (side or "").upper()
        if side_upper == "BUY":
            if token_won:
                pnl = size * (1.0 - price)
            else:
                pnl = -(size * price)
        elif side_upper == "SELL":
            if token_won:
                pnl = -(size * (1.0 - price))
            else:
                pnl = size * price
        else:
            pnl = 0

        batch.append((1, market_outcome, round(pnl, 4), trade_id))
        enriched += 1

    # Batch update
    if batch:
        with db._lock:
            conn.executemany(
                "UPDATE wallet_trades SET market_resolved = ?, market_outcome = ?, pnl = ? WHERE id = ?",
                batch,
            )
            conn.commit()

    # ── Pass 2: Enrich from Data API positions (curPrice=0/1 = resolved) ────
    pos_enriched = _enrich_from_positions(db)

    elapsed = time.time() - t0
    total_enriched = enriched + pos_enriched
    logger.info("Enriched %d trades total (gamma: %d, positions: %d) in %.1fs",
                 total_enriched, enriched, pos_enriched, elapsed)

    return {
        "enriched": total_enriched,
        "gamma_enriched": enriched,
        "positions_enriched": pos_enriched,
        "total_unresolved": len(rows),
        "resolved_tokens_available": len(rp_map),
        "elapsed": round(elapsed, 1),
    }


def _enrich_from_positions(db: WalletDB) -> int:
    """Enrich trades using resolved positions from the Data API.

    For each wallet with unresolved trades, fetch positions and look for
    resolved markets (curPrice == 0 or 1).
    """
    import requests
    from trading_platform.polymarket.address_map import AddressMap

    addr_map = AddressMap()
    conn = db._conn

    # Get wallets that still have unresolved trades
    with db._lock:
        wallet_rows = conn.execute(
            "SELECT DISTINCT wallet FROM wallet_trades WHERE market_resolved = 0"
        ).fetchall()
    wallets = [r[0] for r in wallet_rows]
    logger.info("Pass 2: checking positions for %d wallets with unresolved trades", len(wallets))

    # Build a map of asset -> resolved outcome from positions
    resolved_assets: dict[str, str] = {}  # asset -> "YES" or "NO"
    fetched = 0

    for i, wallet in enumerate(wallets):
        if (i + 1) % 50 == 0:
            logger.info("  Fetching positions %d/%d...", i + 1, len(wallets))

        proxy = addr_map.resolve_for_data_api(wallet)
        try:
            resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": proxy, "limit": 500},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            positions = data if isinstance(data, list) else data.get("data", data.get("positions", []))
        except Exception:
            continue

        fetched += 1
        for pos in positions:
            asset = pos.get("asset", "")
            if not asset:
                continue
            cur_price = pos.get("curPrice")
            redeemable = pos.get("redeemable", False)

            if cur_price is None:
                continue

            try:
                cp = float(cur_price)
            except (TypeError, ValueError):
                continue

            # curPrice == 1.0 means this token's outcome won
            # curPrice == 0.0 means this token's outcome lost
            # redeemable also indicates resolution
            if cp >= 0.99 or cp <= 0.01 or redeemable:
                token_won = cp >= 0.99
                resolved_assets[asset] = "YES" if token_won else "NO"

        time.sleep(0.3)

    logger.info("Pass 2: found %d resolved assets from %d wallets fetched", len(resolved_assets), fetched)

    if not resolved_assets:
        return 0

    # Now match against unresolved trades
    with db._lock:
        unresolved = conn.execute(
            "SELECT id, asset, side, size, price FROM wallet_trades WHERE market_resolved = 0 AND asset IS NOT NULL"
        ).fetchall()

    batch: list[tuple] = []
    for trade_id, asset, side, size, price in unresolved:
        if asset not in resolved_assets:
            continue

        market_outcome = resolved_assets[asset]
        token_won = market_outcome == "YES"
        size = size or 0
        price = price or 0
        side_upper = (side or "").upper()

        if side_upper == "BUY":
            pnl = size * (1.0 - price) if token_won else -(size * price)
        elif side_upper == "SELL":
            pnl = -(size * (1.0 - price)) if token_won else size * price
        else:
            pnl = 0

        batch.append((1, market_outcome, round(pnl, 4), trade_id))

    if batch:
        with db._lock:
            conn.executemany(
                "UPDATE wallet_trades SET market_resolved = ?, market_outcome = ?, pnl = ? WHERE id = ?",
                batch,
            )
            conn.commit()

    return len(batch)
