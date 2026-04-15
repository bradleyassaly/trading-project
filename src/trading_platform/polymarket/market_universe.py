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
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_UNIVERSE_PATH = _PROJECT_ROOT / "data" / "polymarket" / "market_universe.json"

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
        self._by_token: dict[str, dict[str, Any]] = {}

    # ── Public API ───────────────────────────────────────────────────────────

    def refresh(self, max_per_category: int = 50) -> dict[str, list[dict]]:
        """Fetch top markets per category from Polymarket Gamma API.

        Also adds wallet-derived markets so we always subscribe to markets
        where our watched wallets have been active recently.
        """
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

        # Wallet-derived markets bucket — populated below
        wallet_bucket: list[dict] = []
        try:
            from trading_platform.polymarket.db_connection import get_connection
            db_path = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
            if db_path.exists():
                conn = get_connection(db_path)
                watched = [r[0] for r in conn.execute(
                    "SELECT wallet FROM leaderboard WHERE tier IN ('tier1','tier1h','tier2')"
                ).fetchall()]
                if watched:
                    placeholders = ",".join("?" * len(watched))
                    cutoff = int(time.time()) - 14 * 86400
                    rows = conn.execute(
                        f"""SELECT DISTINCT condition_id, asset FROM wallet_trades
                            WHERE wallet IN ({placeholders}) AND timestamp > ?""",
                        (*watched, cutoff),
                    ).fetchall()
                    seen_cids = {
                        m.get("condition_id") for cat in categorized.values() for m in cat
                    }
                    derived_cids = {r[0] for r in rows if r[0] and r[0] not in seen_cids}
                    print(f"[Universe] Found {len(derived_cids)} wallet-active markets not in volume universe")
                    # Fetch metadata for each
                    for cid in list(derived_cids)[:500]:  # cap at 500 per refresh
                        try:
                            resp = requests.get(
                                _GAMMA_URL,
                                params={"condition_ids": cid, "limit": 1},
                                timeout=10,
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                if data:
                                    raw = data[0] if isinstance(data, list) else data
                                    entry = self._extract_entry(raw)
                                    if entry:
                                        cat = self.categorize_question(
                                            entry["question"], tags=entry.get("tags"))
                                        entry["category"] = cat
                                        wallet_bucket.append(entry)
                        except Exception:
                            pass
                        time.sleep(0.05)
                conn.close()
        except Exception as exc:
            logger.warning("Wallet-derived market lookup failed: %s", exc)

        if wallet_bucket:
            categorized["wallet_derived"] = wallet_bucket
            print(f"[Universe] Added {len(wallet_bucket)} wallet-derived markets to subscription")

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

    def refresh_full_coverage(self, min_volume: int = 10_000) -> dict[str, Any]:
        """Fetch ALL active markets above a volume threshold.

        Comprehensive sweep used by the weekly discovery script.
        """
        t0 = time.time()
        all_markets: list[dict] = []
        offset = 0
        page_size = 100
        max_pages = 30

        for _ in range(max_pages):
            try:
                resp = requests.get(
                    _GAMMA_URL,
                    params={
                        "active": "true",
                        "closed": "false",
                        "order": "volume",
                        "ascending": "false",
                        "limit": page_size,
                        "offset": offset,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                page = resp.json()
                if not page:
                    break

                # Stop early when volume drops below threshold
                last_vol = float(page[-1].get("volume", 0) or 0)
                all_markets.extend(page)
                if last_vol < min_volume:
                    break
                if len(page) < page_size:
                    break
                offset += page_size
                time.sleep(0.2)
            except Exception as exc:
                logger.warning("Full coverage fetch failed at offset %d: %s", offset, exc)
                break

        # Categorize without per-category cap
        categorized: dict[str, list[dict]] = {c: [] for c in ALL_CATEGORIES}
        categorized["other"] = []
        for m in all_markets:
            entry = self._extract_entry(m)
            if not entry or entry.get("volume", 0) < min_volume:
                continue
            cat = self.categorize_question(entry["question"], tags=entry.get("tags"))
            entry["category"] = cat
            categorized.setdefault(cat, []).append(entry)

        # Add wallet-derived markets
        wallet_cids = self.get_wallet_derived_markets(days_back=14)
        existing_cids = {m["condition_id"] for ms in categorized.values() for m in ms}
        new_wallet_cids = wallet_cids - existing_cids
        if new_wallet_cids:
            print(f"[Universe FULL] Adding {len(new_wallet_cids)} wallet-derived markets")
            wallet_bucket = categorized.setdefault("wallet_derived", [])
            for cid in list(new_wallet_cids)[:1000]:
                wallet_bucket.append({
                    "condition_id": cid,
                    "token_ids": [],
                    "question": "",
                    "volume": 0,
                    "end_date_iso": "",
                    "category": "wallet_derived",
                    "tags": [],
                })

        categorized = {k: v for k, v in categorized.items() if v}
        total = sum(len(v) for v in categorized.values())
        self._by_category = categorized
        self._rebuild_index()

        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "refreshed_at": int(time.time()),
            "total": total,
            "by_category": categorized,
            "mode": "full_coverage",
            "min_volume": min_volume,
        }
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        elapsed = round(time.time() - t0, 1)
        print(f"[Universe FULL] {total} markets across {len(categorized)} categories in {elapsed}s")
        return {"total_markets": total, "by_category": {k: len(v) for k, v in categorized.items()}, "elapsed_seconds": elapsed}

    def get_all_condition_ids(self) -> set[str]:
        return set(self._by_condition.keys())

    def get_all_token_ids(self) -> set[str]:
        return set(self._by_token.keys())

    def _lookup(self, identifier: str) -> dict[str, Any] | None:
        """Look up market entry by either condition_id or token_id."""
        if not identifier:
            return None
        return self._by_condition.get(identifier) or self._by_token.get(identifier)

    def get_question(self, identifier: str) -> str:
        entry = self._lookup(identifier)
        return entry["question"] if entry else ""

    def get_category(self, identifier: str) -> str:
        entry = self._lookup(identifier)
        return entry.get("category", "other") if entry else "other"

    def get_token_to_condition_map(self) -> dict[str, str]:
        """Return mapping from token_id → condition_id."""
        m: dict[str, str] = {}
        for cid, entry in self._by_condition.items():
            for tid in entry.get("token_ids", []):
                m[tid] = cid
        return m

    def discover_from_wallet_activity(self, watched_wallets: list[str], days_back: int = 14) -> set[str]:
        """Alias for get_wallet_derived_markets() (kept for compatibility)."""
        return self.get_wallet_derived_markets(watched_wallets, days_back=days_back)

    def get_wallet_derived_markets(
        self,
        watched_wallets: list[str] | None = None,
        days_back: int = 14,
    ) -> set[str]:
        """Return condition_ids where watched wallets traded recently.

        If watched_wallets is None, auto-loads tier1+1h+2 from leaderboard.
        These markets MUST be in our subscription regardless of other filters.
        """
        try:
            import sqlite3 as _sq
            db_path = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
            if not db_path.exists():
                return set()
            conn = _sq.connect(str(db_path))
            if watched_wallets is None:
                watched_wallets = [r[0] for r in conn.execute(
                    "SELECT wallet FROM leaderboard WHERE tier IN ('tier1','tier1h','tier2')"
                ).fetchall()]
            if not watched_wallets:
                conn.close()
                return set()
            placeholders = ",".join("?" * len(watched_wallets))
            cutoff = int(time.time()) - days_back * 86400
            rows = conn.execute(
                f"""SELECT DISTINCT condition_id FROM wallet_trades
                    WHERE wallet IN ({placeholders})
                    AND timestamp > ?
                    AND condition_id IS NOT NULL AND condition_id != ''""",
                (*watched_wallets, cutoff),
            ).fetchall()
            conn.close()
            return {r[0] for r in rows if r[0]}
        except Exception as exc:
            logger.warning("get_wallet_derived_markets failed: %s", exc)
            return set()

    def add_wallet_derived_markets(self, condition_ids: set[str]) -> int:
        """Fetch metadata for wallet-derived markets and add them to the universe.

        Markets that don't appear in the Gamma volume-sorted list but are
        being actively traded by our whales. Returns count added.
        """
        if not condition_ids:
            return 0

        # Skip ones we already have
        existing = self.get_all_condition_ids()
        new_cids = [c for c in condition_ids if c not in existing]
        if not new_cids:
            return 0

        added = 0
        wallet_bucket = self._by_category.setdefault("wallet_derived", [])

        for cid in new_cids:
            try:
                resp = requests.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": cid, "limit": 1},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                if not data:
                    continue
                raw = data[0] if isinstance(data, list) else data
                entry = self._extract_entry(raw)
                if not entry:
                    continue
                cat = self.categorize_question(entry["question"], tags=entry.get("tags"))
                entry["category"] = cat
                wallet_bucket.append(entry)
                added += 1
            except Exception as exc:
                logger.debug("Could not fetch wallet-derived market %s: %s", cid[:16], exc)
            time.sleep(0.1)

        if added:
            self._rebuild_index()
            # Persist
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "refreshed_at": int(time.time()),
                "total": sum(len(v) for v in self._by_category.values()),
                "by_category": self._by_category,
            }
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        return added

    # ── Duration helpers ──────────────────────────────────────────────────
    #
    # Validated via scripts/wallet_deep_dive.py: short-duration markets
    # provide a faster feedback loop (resolution within 1-30 days) which
    # is what unblocks live-trading validation. Not a hard filter on the
    # universe — callers opt in so long-dated conviction plays still
    # appear in the main subscription.

    @staticmethod
    def _days_until_resolution(entry: dict) -> float | None:
        """Parse end_date_iso → days-from-now. None if missing/unparseable."""
        raw = entry.get("end_date_iso") or ""
        if not raw:
            return None
        try:
            from datetime import datetime, timezone
            raw_clean = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
            dt = datetime.fromisoformat(raw_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(tz=timezone.utc)
            return (dt - now).total_seconds() / 86400.0
        except Exception:
            return None

    def short_duration_markets(
        self,
        *,
        max_days: float = 30.0,
        min_days: float = 0.0,
        categories: list[str] | None = None,
    ) -> list[dict]:
        """Return markets resolving within the [min_days, max_days] window.

        Useful for signal engine runs that want to bias paper-trading
        toward faster-feedback markets — accelerates the validation loop
        against the live-trading gate of 20+ resolved outcomes.
        """
        out: list[dict] = []
        buckets = self._by_category.items()
        for cat, items in buckets:
            if categories and cat not in categories:
                continue
            for m in items:
                d = self._days_until_resolution(m)
                if d is None:
                    continue
                if min_days <= d <= max_days:
                    out.append(m)
        # Sort by soonest-resolving first
        out.sort(key=lambda m: self._days_until_resolution(m) or float("inf"))
        return out

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
        self._by_token = {}
        for cat, markets in self._by_category.items():
            for m in markets:
                cid = m.get("condition_id")
                if cid:
                    m["category"] = cat
                    self._by_condition[cid] = m
                    # Also index by token_ids — WebSocket events use token_id
                    for tid in m.get("token_ids", []):
                        if tid:
                            self._by_token[tid] = m

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
