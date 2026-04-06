"""
Consolidated price lookup for Polymarket markets.

Queries prices.db first, falls back to CLOB API for bid/ask,
and Gamma API for price/question on untracked markets.

Usage::

    from trading_platform.polymarket.price_lookup import PriceLookup
    lookup = PriceLookup()
    info = lookup.get_price("token_id_here")
    if info:
        print(info["price"], info["spread"], info["tradeable"])
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Sports keywords for content-based filtering
import re

# Keywords that can appear as substrings safely
_SPORTS_SUBSTRING_KEYWORDS = [
    "win on 202", "vs.", "fc ", " fc ", "city fc",
    "predators", "blackhawks", "kraken",
    "lakers", "celtics", "warriors", "knicks",
    "melbourne victory", "wellington ph",
    "hiroshima", "frankfurt", "torino fc",
    "saudi club", "al ahli", "al shabab", "al qadisiyah",
    "o/u ", "over/under", "moneyline", "spread:",
    "timberwolves", "suns", "cavaliers", "nuggets", "thunder",
    "celtics", "mavericks", "pacers", "rockets", "bucks",
    "76ers", "pistons", "grizzlies", "spurs", "wizards",
    "hornets", "pelicans", "raptors", "magic", "hawks",
    "nets", "clippers", "blazers", "kings",
    "eagles", "chiefs", "ravens", "49ers", "bills",
    "cowboys", "dolphins", "steelers", "packers", "lions",
    "bears", "bengals", "texans", "vikings", "saints",
    "broncos", "colts", "chargers", "commanders", "panthers",
    "titans", "jaguars", "cardinals", "seahawks", "falcons",
    "raiders", "patriots", "rams", "giants",
    "premier league", "la liga", "serie a", "bundesliga",
    "ligue 1", "champions league", "europa league",
    "soccer", "championship",
]

# Keywords that need word-boundary matching (avoid false positives like "inflation" -> "nfl")
_SPORTS_WORD_KEYWORDS = [
    "nfl", "nba", "nhl", "mlb", "mls", "ufc",
    "match", "game", "fight", "bout", "race",
    "jets", "heat", "united",
]

_SPORTS_WORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in _SPORTS_WORD_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


def is_sports_market(question: str) -> bool:
    """Check if a market question is about sports."""
    q = question.lower()
    if any(kw in q for kw in _SPORTS_SUBSTRING_KEYWORDS):
        return True
    if _SPORTS_WORD_RE.search(question):
        return True
    return False


class PriceLookup:
    """Unified price/spread/question lookup for Polymarket tokens."""

    def __init__(
        self,
        prices_db_path: str | Path = "data/polymarket/live/prices.db",
        *,
        cache_seconds: float = 60.0,
    ) -> None:
        self._db_path = Path(prices_db_path)
        self._cache_seconds = cache_seconds
        self._db_lookup: dict[str, dict[str, Any]] | None = None
        self._db_loaded_at: float = 0
        self._clob_cache: dict[str, dict[str, Any] | None] = {}
        self._clob_cache_ts: dict[str, float] = {}
        self._gamma_cache: dict[str, dict[str, Any] | None] = {}

    def get_price(self, token_id: str) -> dict[str, Any] | None:
        """Look up price, bid, ask, spread, question, tradeable for a token.

        Returns dict with keys: price, best_bid, best_ask, spread, question,
        end_date_iso, tradeable, source. Returns None if no data found.
        """
        # Step 1: prices.db
        db = self._get_db_lookup()
        mkt = db.get(token_id, {})
        price = mkt.get("price")
        best_bid = mkt.get("best_bid")
        best_ask = mkt.get("best_ask")
        question = mkt.get("question", "")
        end_date = mkt.get("end_date_iso")
        source = "prices_db" if price is not None else None

        # Step 2: Gamma API fallback for price/question
        if price is None:
            gm = self._get_gamma(token_id)
            if gm:
                price = gm.get("price")
                question = gm.get("question", question)
                end_date = gm.get("end_date", end_date)
                source = "gamma"

        if price is None:
            return None

        # Step 3: CLOB API fallback for bid/ask
        spread = None
        if best_bid is not None and best_ask is not None and best_bid > 0.01 and best_ask < 0.99:
            spread = best_ask - best_bid
        else:
            book = self._get_clob_book(token_id)
            if book and book.get("best_bid") is not None and book.get("best_ask") is not None:
                best_bid = book["best_bid"]
                best_ask = book["best_ask"]
                spread = best_ask - best_bid

        tradeable = (
            spread is not None
            and spread < 0.15
            and best_bid is not None and best_bid > 0.01
            and best_ask is not None and best_ask < 0.99
        )

        return {
            "price": price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "question": question,
            "end_date_iso": end_date,
            "tradeable": tradeable,
            "source": source,
        }

    def _get_db_lookup(self) -> dict[str, dict[str, Any]]:
        """Load/refresh the prices.db cache."""
        now = time.time()
        if self._db_lookup is not None and (now - self._db_loaded_at) < self._cache_seconds:
            return self._db_lookup

        self._db_lookup = {}
        if not self._db_path.exists():
            return self._db_lookup

        try:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            markets = {}
            for mid, q, tid, end_dt in conn.execute(
                "SELECT market_id, question, yes_token_id, end_date_iso "
                "FROM markets WHERE yes_token_id IS NOT NULL"
            ).fetchall():
                markets[tid] = {"market_id": mid, "question": q, "end_date_iso": end_dt}

            price_rows = conn.execute("""
                SELECT market_id, price FROM ticks
                WHERE msg_type IN ('last_trade_price', 'book')
                AND id IN (SELECT MAX(id) FROM ticks
                           WHERE msg_type IN ('last_trade_price', 'book')
                           GROUP BY market_id)
            """).fetchall()
            prices = {r[0]: r[1] for r in price_rows}

            book_rows = conn.execute("""
                SELECT market_id, best_bid, best_ask FROM ticks
                WHERE msg_type = 'book'
                AND id IN (SELECT MAX(id) FROM ticks WHERE msg_type = 'book' GROUP BY market_id)
            """).fetchall()
            books = {r[0]: (r[1], r[2]) for r in book_rows}
            conn.close()

            for tid, meta in markets.items():
                mid = meta["market_id"]
                p = prices.get(mid)
                ba = books.get(mid, (None, None))
                self._db_lookup[tid] = {
                    "price": float(p) if p is not None else None,
                    "best_bid": float(ba[0]) if ba[0] is not None else None,
                    "best_ask": float(ba[1]) if ba[1] is not None else None,
                    "question": meta["question"],
                    "end_date_iso": meta.get("end_date_iso"),
                }
            self._db_loaded_at = now
        except Exception as exc:
            logger.warning("Failed to load prices.db: %s", exc)

        return self._db_lookup

    def _get_gamma(self, token_id: str) -> dict[str, Any] | None:
        if token_id in self._gamma_cache:
            return self._gamma_cache[token_id]
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": token_id},
                timeout=5,
            )
            if resp.status_code != 200 or not resp.json():
                self._gamma_cache[token_id] = None
                return None
            mkt = resp.json()[0] if isinstance(resp.json(), list) else resp.json()
            price = None
            outcome_prices = mkt.get("outcomePrices")
            if outcome_prices:
                import json as _json
                prices = outcome_prices if isinstance(outcome_prices, list) else _json.loads(outcome_prices)
                price = float(prices[0])
            result = {
                "price": price,
                "question": mkt.get("question", ""),
                "end_date": mkt.get("endDate") or mkt.get("end_date_iso") or "",
            }
            self._gamma_cache[token_id] = result
            return result
        except Exception:
            self._gamma_cache[token_id] = None
            return None

    def _get_clob_book(self, token_id: str) -> dict[str, Any] | None:
        now = time.time()
        if token_id in self._clob_cache:
            if (now - self._clob_cache_ts.get(token_id, 0)) < self._cache_seconds:
                return self._clob_cache[token_id]
        try:
            resp = requests.get(
                "https://clob.polymarket.com/book",
                params={"token_id": token_id},
                timeout=5,
            )
            if resp.status_code != 200:
                self._clob_cache[token_id] = None
                self._clob_cache_ts[token_id] = now
                return None
            data = resp.json()
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            best_bid = max((float(b.get("price", 0)) for b in bids), default=None) if bids else None
            best_ask = min((float(a.get("price", 1)) for a in asks), default=None) if asks else None
            result = {"best_bid": best_bid, "best_ask": best_ask}
            self._clob_cache[token_id] = result
            self._clob_cache_ts[token_id] = now
            return result
        except Exception:
            self._clob_cache[token_id] = None
            self._clob_cache_ts[token_id] = now
            return None
