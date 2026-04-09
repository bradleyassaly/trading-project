"""
Polymarket positions fetcher.

Fetches current open positions from Polymarket's data-api positions
endpoint and stores them in wallet_positions for analytics use.

Usage::

    from trading_platform.polymarket.position_fetcher import PositionFetcher
    fetcher = PositionFetcher()
    n = fetcher.fetch_and_store(wallet_address, db_path)
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

POSITIONS_URL = "https://data-api.polymarket.com/positions"


class PositionFetcher:
    """Fetch current open positions from Polymarket data-api."""

    def fetch_wallet_positions(self, wallet: str, size_threshold: float = 0.01) -> list[dict[str, Any]]:
        """Fetch current open positions for one wallet."""
        params = {
            "user": wallet,
            "limit": 500,
            "sizeThreshold": str(size_threshold),
        }
        try:
            r = requests.get(
                POSITIONS_URL,
                params=params,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else data.get("data", [])
        except Exception as exc:
            logger.warning("Position fetch failed for %s: %s", wallet[:16], exc)
            return []

    def fetch_and_store(self, wallet: str, db_path: str) -> int:
        """Fetch and upsert positions for one wallet. Returns count stored.

        After upserting the live set, deletes any rows for this wallet
        whose ``updated_at`` is older than the start of this refresh —
        those represent positions Polymarket no longer returns (because
        the market resolved and the user was paid out, or they sold to
        zero). Without this prune, ``wallet_positions`` accumulates
        zombie rows for resolved markets — see data_validation.md DV-7.
        """
        positions = self.fetch_wallet_positions(wallet)
        # Mark when this refresh started so the post-upsert prune can
        # identify rows the API didn't return this time around.
        refresh_started = int(time.time())
        if not positions:
            # Empty response can mean "wallet has no open positions" —
            # also a valid signal to clear stale rows.
            try:
                conn = sqlite3.connect(db_path)
                conn.execute(
                    "DELETE FROM wallet_positions WHERE wallet = ?",
                    (wallet,),
                )
                conn.commit()
                conn.close()
            except Exception as exc:
                logger.debug("empty-response prune failed for %s: %s", wallet[:14], exc)
            return 0

        conn = sqlite3.connect(db_path)
        now = int(time.time())
        stored = 0

        for p in positions:
            try:
                outcome = (p.get("outcome") or "").upper()
                side = "YES" if outcome in ("YES", "1") else "NO"
                asset = p.get("asset") or ""

                conn.execute(
                    """INSERT INTO wallet_positions
                       (wallet, condition_id, asset, title, slug, outcome, side,
                        size, avg_price, initial_value, current_value, cur_price,
                        cash_pnl, percent_pnl, realized_pnl, total_bought,
                        redeemable, end_date, last_updated, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(wallet, asset) DO UPDATE SET
                         condition_id=excluded.condition_id,
                         title=excluded.title, slug=excluded.slug,
                         outcome=excluded.outcome, side=excluded.side,
                         size=excluded.size, avg_price=excluded.avg_price,
                         initial_value=excluded.initial_value,
                         current_value=excluded.current_value,
                         cur_price=excluded.cur_price,
                         cash_pnl=excluded.cash_pnl,
                         percent_pnl=excluded.percent_pnl,
                         realized_pnl=excluded.realized_pnl,
                         total_bought=excluded.total_bought,
                         redeemable=excluded.redeemable,
                         end_date=excluded.end_date,
                         last_updated=excluded.last_updated,
                         updated_at=excluded.updated_at""",
                    (
                        wallet,
                        p.get("conditionId", ""),
                        asset,
                        p.get("title", ""),
                        p.get("slug", ""),
                        outcome,
                        side,
                        p.get("size"),
                        p.get("avgPrice"),
                        p.get("initialValue"),
                        p.get("currentValue"),
                        p.get("curPrice"),
                        p.get("cashPnl"),
                        p.get("percentPnl"),
                        p.get("realizedPnl"),
                        p.get("totalBought"),
                        1 if p.get("redeemable") else 0,
                        p.get("endDate", ""),
                        now,
                        now,
                    ),
                )
                stored += 1
            except Exception as exc:
                logger.debug("Position upsert failed: %s", exc)

        # Prune: any row for this wallet not touched in the current
        # refresh is a position the API no longer returns. Delete it.
        try:
            cur = conn.execute(
                """DELETE FROM wallet_positions
                   WHERE wallet = ? AND updated_at < ?""",
                (wallet, refresh_started),
            )
            pruned = cur.rowcount or 0
            if pruned:
                logger.info(
                    "[POSITIONS] pruned %d stale rows for %s",
                    pruned, wallet[:14],
                )
        except Exception as exc:
            logger.debug("prune failed for %s: %s", wallet[:14], exc)

        conn.commit()
        conn.close()
        return stored

    def fetch_batch(self, wallets: list[str], db_path: str, sleep_between: float = 0.3) -> dict[str, int]:
        """Fetch positions for multiple wallets with rate limiting.

        Skips wallets refreshed within the last 4 hours.
        """
        results: dict[str, int] = {}
        cutoff = int(time.time()) - 4 * 3600

        # Filter out recently-refreshed wallets
        conn = sqlite3.connect(db_path)
        try:
            recent = conn.execute(
                f"""SELECT DISTINCT wallet FROM wallet_positions
                    WHERE wallet IN ({",".join("?" * len(wallets))})
                    AND COALESCE(updated_at, 0) > ?""",
                (*wallets, cutoff),
            ).fetchall() if wallets else []
            recent_set = {r[0] for r in recent}
        except Exception:
            recent_set = set()
        finally:
            conn.close()

        to_fetch = [w for w in wallets if w not in recent_set]
        skipped = len(wallets) - len(to_fetch)
        if skipped:
            print(f"  Skipping {skipped} wallets refreshed within last 4h")

        for i, wallet in enumerate(to_fetch):
            n = self.fetch_and_store(wallet, db_path)
            results[wallet] = n
            if (i + 1) % 10 == 0 or i == len(to_fetch) - 1:
                print(f"  [{i+1}/{len(to_fetch)}] {wallet[:16]}... {n} positions")
            if i < len(to_fetch) - 1:
                time.sleep(sleep_between)
        return results
