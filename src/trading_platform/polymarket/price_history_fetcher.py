"""
Polymarket market price history fetcher.

Fetches OHLCV-style price history from CLOB API and stores in
market_price_history for backtesting and visualization.

Usage::

    from trading_platform.polymarket.price_history_fetcher import MarketPriceHistoryFetcher
    fetcher = MarketPriceHistoryFetcher()
    n = fetcher.fetch_and_store(condition_id, db_path)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_URL = "https://clob.polymarket.com/prices-history"


class MarketPriceHistoryFetcher:
    """Fetch and store CLOB price history per market."""

    def get_token_id(self, condition_id: str) -> str | None:
        """Get YES token_id from Gamma API for a condition_id.

        Note: Gamma API param is `condition_ids` (plural). Using `conditionId`
        is silently ignored and returns the default top 20 markets.
        """
        try:
            r = requests.get(
                GAMMA_URL,
                params={"condition_ids": condition_id},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            data = r.json()
            m = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
            if not m:
                return None
            # Verify we got the right market back (defensive against API quirks)
            ret_cid = (m.get("conditionId") or "").lower()
            if ret_cid and ret_cid != condition_id.lower():
                logger.debug("gamma returned wrong cid: asked %s got %s", condition_id[:16], ret_cid[:16])
                return None
            tokens_raw = m.get("clobTokenIds", "[]")
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            return tokens[0] if tokens else None
        except Exception as exc:
            logger.debug("get_token_id failed for %s: %s", condition_id[:16], exc)
            return None

    def fetch_price_history(self, token_id: str, fidelity: int = 60) -> list[dict[str, Any]]:
        """Fetch full price history from CLOB API.

        Tries `fidelity=60` (minute granularity) first; if empty, falls back
        to `fidelity=1440` (daily) which is the only resolution the CLOB API
        retains for older/long-lived resolved markets.
        """
        for fid in (fidelity, 1440):
            try:
                r = requests.get(
                    CLOB_URL,
                    params={"market": token_id, "interval": "max", "fidelity": fid},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=15,
                )
                hist = r.json().get("history", []) or []
                if hist:
                    return hist
            except Exception as exc:
                logger.debug("fetch_price_history failed for %s fid=%d: %s", token_id[:16], fid, exc)
                return []
            if fid == 1440:
                break
        return []

    def fetch_and_store(
        self,
        condition_id: str,
        db_path: str,
        force_refresh: bool = False,
    ) -> int:
        """Fetch and store price history for one market.

        Returns count stored. Returns 0 with reason captured on
        ``self.last_skip_reason`` ('no_token', 'no_hist', or '' on success).
        """
        self.last_skip_reason = ""
        conn = sqlite3.connect(db_path)
        try:
            if not force_refresh:
                existing = conn.execute(
                    "SELECT COUNT(*) FROM market_price_history WHERE condition_id = ?",
                    (condition_id,),
                ).fetchone()[0]
                if existing > 10:
                    self.last_skip_reason = "cached"
                    return existing

            token_id = self.get_token_id(condition_id)
            if not token_id:
                self.last_skip_reason = "no_token"
                return 0

            qrow = conn.execute(
                "SELECT DISTINCT title FROM wallet_trades WHERE condition_id=? AND title != '' LIMIT 1",
                (condition_id,),
            ).fetchone()
            question = qrow[0] if qrow else ""

            crow = conn.execute(
                "SELECT DISTINCT category FROM wallet_trades WHERE condition_id=? AND category != '' LIMIT 1",
                (condition_id,),
            ).fetchone()
            category = crow[0] if crow else ""

            history = self.fetch_price_history(token_id)
            if not history:
                self.last_skip_reason = "no_hist"
                return 0

            conn.execute(
                "DELETE FROM market_price_history WHERE condition_id = ?",
                (condition_id,),
            )
            conn.executemany(
                """INSERT OR IGNORE INTO market_price_history
                   (condition_id, token_id, question, category, t, p)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (condition_id, token_id, question, category, pt["t"], pt["p"])
                    for pt in history
                    if "t" in pt and "p" in pt
                ],
            )
            conn.commit()
            return len(history)
        finally:
            conn.close()

    def batch_fetch(
        self,
        condition_ids: list[str],
        db_path: str,
        sleep_between: float = 0.4,
    ) -> dict[str, int]:
        """Fetch price history for multiple markets. Returns {cid: points_stored}."""
        results: dict[str, int] = {}
        reasons: dict[str, int] = {"stored": 0, "cached": 0, "no_token": 0, "no_hist": 0, "error": 0}
        for i, cid in enumerate(condition_ids):
            try:
                n = self.fetch_and_store(cid, db_path)
                results[cid] = n
                reason = getattr(self, "last_skip_reason", "")
                if n > 0 and reason == "":
                    reasons["stored"] += 1
                elif reason in reasons:
                    reasons[reason] += 1
            except Exception as exc:
                results[cid] = 0
                reasons["error"] += 1
                logger.debug("batch fetch failed for %s: %s", cid[:16], exc)
            if (i + 1) % 20 == 0 or i == len(condition_ids) - 1:
                print(
                    f"  [PRICE_HISTORY] {i+1}/{len(condition_ids)} "
                    f"(stored:{reasons['stored']} cached:{reasons['cached']} "
                    f"no_token:{reasons['no_token']} no_hist:{reasons['no_hist']} "
                    f"err:{reasons['error']})"
                )
            if i < len(condition_ids) - 1:
                time.sleep(sleep_between)
        return results
