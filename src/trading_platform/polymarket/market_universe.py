"""
Market universe manager for Polymarket whale monitoring.

Fetches top markets per category from the Gamma API, categorizes them,
and persists the universe to JSON for use by WhaleTripwire and the
live collector.

Usage::

    from trading_platform.polymarket.market_universe import MarketUniverse
    universe = MarketUniverse()
    universe.refresh(max_per_category=25)
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
_UNIVERSE_PATH = Path("data/polymarket/market_universe.json")

ALL_CATEGORIES = [
    "politics", "economics", "crypto", "finance",
    "sports", "culture", "tech", "mentions", "weather",
]

_CATEGORY_TAGS: dict[str, list[str]] = {
    "politics": ["politics", "elections", "government", "trump", "us-politics"],
    "economics": ["economics", "fed", "macro", "finance", "inflation"],
    "crypto": ["crypto", "bitcoin", "ethereum", "defi", "web3"],
    "finance": ["stocks", "equities", "markets", "business"],
    "sports": ["sports", "nba", "nfl", "mlb", "soccer", "ufc", "tennis"],
    "culture": ["culture", "entertainment", "pop-culture", "music", "awards"],
    "tech": ["tech", "technology", "ai", "science"],
    "mentions": ["twitter", "social-media", "mentions"],
    "weather": ["weather", "climate", "natural-disaster"],
}

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "politics": [
        "election", "president", "congress", "senate", "trump", "harris",
        "republican", "democrat", "vote", "governor", "parliament", "minister",
        "political", "party", "ballot", "campaign", "legislation", "policy",
    ],
    "economics": [
        "fed", "cpi", "gdp", "jobs", "inflation", "rate", "fomc",
        "unemployment", "pce", "recession", "treasury", "nonfarm", "payroll",
        "interest rate", "central bank", "monetary",
    ],
    "crypto": [
        "bitcoin", "ethereum", "crypto", "btc", "eth", "solana", "price",
        "defi", "nft", "blockchain", "token", "coin", "doge", "xrp", "bnb",
    ],
    "finance": [
        "stock", "market", "sp500", "s&p", "nasdaq", "dow", "shares",
        "earnings", "ipo", "merger", "acquisition", "bankruptcy", "index",
    ],
    "sports": [
        "nba", "nfl", "mlb", "nhl", "ufc", "tennis", "soccer", "golf",
        "championship", "league", "playoff", "super bowl", "world cup",
        "match", "game", "season", "win", "score",
    ],
    "tech": [
        "ai", "artificial intelligence", "openai", "google", "apple",
        "microsoft", "meta", "amazon", "tesla", "technology", "launch",
        "release", "product", "model", "gpt",
    ],
    "culture": [
        "oscar", "grammy", "emmy", "award", "movie", "album", "song",
        "celebrity", "tv show", "netflix", "viral", "music", "film",
    ],
    "mentions": [
        "tweet", "post", "followers", "views", "likes", "social media",
        "mention", "trending",
    ],
    "weather": [
        "hurricane", "temperature", "rainfall", "snow", "storm",
        "climate", "degrees", "weather", "forecast",
    ],
}


class MarketUniverse:
    """Manages the set of markets monitored for whale activity."""

    def __init__(self, path: str | Path = _UNIVERSE_PATH) -> None:
        self._path = Path(path)
        self._by_category: dict[str, list[dict[str, Any]]] = {}
        self._by_condition: dict[str, dict[str, Any]] = {}

    # ── Public API ───────────────────────────────────────────────────────────

    def refresh(self, max_per_category: int = 25) -> dict[str, list[dict]]:
        """Fetch top markets per category from Polymarket Gamma API."""
        all_markets = self._fetch_all_markets()
        categorized: dict[str, list[dict]] = {c: [] for c in ALL_CATEGORIES}
        categorized["other"] = []

        for m in all_markets:
            entry = self._extract_entry(m)
            if not entry:
                continue
            cat = self.categorize_question(entry["question"], tags=entry.get("tags"))
            bucket = categorized.get(cat, categorized["other"])
            if len(bucket) < max_per_category:
                entry["category"] = cat
                bucket.append(entry)

        # Remove empty categories
        categorized = {k: v for k, v in categorized.items() if v}

        total = sum(len(v) for v in categorized.values())
        self._by_category = categorized
        self._rebuild_index()

        # Persist
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "refreshed_at": int(time.time()),
            "total": total,
            "by_category": categorized,
        }
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        # Print summary
        print()
        print(f"{'Category':<16} | {'Markets':>7} | {'Total Volume':>14}")
        print(f"{'-'*16}-+-{'-'*7}-+-{'-'*14}")
        grand_vol = 0
        for cat in ALL_CATEGORIES + ["other"]:
            mkts = categorized.get(cat, [])
            if not mkts:
                continue
            vol = sum(m.get("volume", 0) for m in mkts)
            grand_vol += vol
            print(f"{cat:<16} | {len(mkts):>7} | ${vol:>12,.0f}")
        print(f"{'-'*16}-+-{'-'*7}-+-{'-'*14}")
        print(f"{'TOTAL':<16} | {total:>7} | ${grand_vol:>12,.0f}")
        print()

        return categorized

    def get_all_condition_ids(self) -> set[str]:
        return set(self._by_condition.keys())

    def get_question(self, condition_id: str) -> str:
        entry = self._by_condition.get(condition_id)
        return entry["question"] if entry else ""

    def get_category(self, condition_id: str) -> str:
        entry = self._by_condition.get(condition_id)
        return entry.get("category", "other") if entry else "other"

    def get_token_to_condition_map(self) -> dict[str, str]:
        """Return mapping from token_id → condition_id."""
        m: dict[str, str] = {}
        for cid, entry in self._by_condition.items():
            for tid in entry.get("token_ids", []):
                m[tid] = cid
        return m

    def load_cached(self, max_age_hours: float = 6.0) -> bool:
        """Load from JSON if fresh enough. Return True if loaded."""
        if not self._path.exists():
            return False
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            age_hours = (time.time() - data.get("refreshed_at", 0)) / 3600
            if age_hours > max_age_hours:
                return False
            self._by_category = data.get("by_category", {})
            self._rebuild_index()
            return True
        except Exception:
            return False

    def categorize_question(self, question: str, tags: list[str] | None = None) -> str:
        """Try tags first, then keyword matching. First match wins."""
        # Tags-based classification (higher priority)
        if tags:
            tags_lower = [t.lower() for t in tags if isinstance(t, str)]
            for cat in ALL_CATEGORIES:
                cat_tags = _CATEGORY_TAGS.get(cat, [])
                if any(t in tags_lower for t in cat_tags):
                    return cat

        # Keyword fallback
        q = question.lower()
        for cat in ALL_CATEGORIES:
            keywords = _CATEGORY_KEYWORDS.get(cat, [])
            if any(kw in q for kw in keywords):
                return cat
        return "other"

    # ── Internal ─────────────────────────────────────────────────────────────

    def _rebuild_index(self) -> None:
        self._by_condition = {}
        for cat, markets in self._by_category.items():
            for m in markets:
                cid = m.get("condition_id")
                if cid:
                    m["category"] = cat
                    self._by_condition[cid] = m

    def _fetch_all_markets(self) -> list[dict]:
        """Paginate through Gamma API to get active markets sorted by volume."""
        all_results: list[dict] = []
        offset = 0
        limit = 100
        max_pages = 10

        for _ in range(max_pages):
            try:
                resp = requests.get(
                    _GAMMA_URL,
                    params={
                        "active": "true",
                        "closed": "false",
                        "order": "volume",
                        "ascending": "false",
                        "limit": limit,
                        "offset": offset,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                page = resp.json()
                if not page:
                    break
                all_results.extend(page)
                if len(page) < limit:
                    break
                offset += limit
                time.sleep(0.2)
            except Exception as exc:
                logger.warning("Gamma API fetch failed at offset %d: %s", offset, exc)
                break

        return all_results

    def _extract_entry(self, raw: dict) -> dict[str, Any] | None:
        """Extract market entry from Gamma API response."""
        condition_id = raw.get("conditionId") or raw.get("condition_id")
        question = raw.get("question", "")
        if not condition_id or not question:
            return None

        # Extract token IDs from clobTokenIds or tokens
        token_ids = []
        clob_str = raw.get("clobTokenIds")
        if clob_str:
            try:
                token_ids = json.loads(clob_str) if isinstance(clob_str, str) else clob_str
            except (json.JSONDecodeError, TypeError):
                pass
        if not token_ids:
            tokens = raw.get("tokens", [])
            if isinstance(tokens, list):
                token_ids = [t.get("token_id", "") for t in tokens if isinstance(t, dict)]

        volume = 0
        try:
            volume = float(raw.get("volume", 0) or 0)
        except (TypeError, ValueError):
            pass

        end_date = raw.get("endDate") or raw.get("end_date_iso", "")

        # Extract tags
        tags_raw = raw.get("tags")
        tags: list[str] = []
        if isinstance(tags_raw, list):
            tags = [t for t in tags_raw if isinstance(t, str)]
        elif isinstance(tags_raw, str):
            try:
                parsed = json.loads(tags_raw)
                if isinstance(parsed, list):
                    tags = [t for t in parsed if isinstance(t, str)]
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "condition_id": condition_id,
            "token_ids": [t for t in token_ids if t],
            "question": question,
            "volume": volume,
            "end_date_iso": end_date,
            "tags": tags,
        }
