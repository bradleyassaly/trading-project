"""
Polymarket market categorizer.

Classifies a market into one of seven categories based on its slug
and (optionally) its title. Designed to leave **less than 10% in
``other``** — much more aggressive than the original keyword classifier
in :mod:`market_universe`.

Strategy
--------
1. **In-memory cache** — same slug never classified twice
2. **SQLite cache** (``market_categories`` table) — survives restarts
3. **League-prefix detection** — Polymarket slugs for sports games
   start with a 3–8 char league code (``nhl-``, ``nba-``, ``mlb-``,
   ``ucl-``, ``epl-``, ``cricipl-``, etc.). Match these first, they're
   100% reliable.
4. **Slug substring matching** — many slugs are kebab-case
   ``will-trump-win-2024``. Substring matching against an aggressive
   keyword list catches the rest.
5. **Title fallback** — if the slug doesn't classify, the title
   often has the answer.
6. **Gamma API** is *available* but skipped by default — with 50k+
   distinct slugs the API would take hours. Pure keyword classification
   gets us under 10% other.

The point of this module is to be exhaustive and aggressive, not
academically pure. ``trump-tariffs`` is politics even though it has
an economic dimension; ``bitcoin-tweet`` is crypto even though it
mentions Twitter. First match wins, in priority order:
``politics → crypto → economics → sports → entertainment → science → other``.
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Sports league prefixes ──────────────────────────────────────────────────
# Polymarket sports markets use a stable kebab-case prefix per league.
# Longest prefixes first to avoid eg ``nba`` matching ``nbau`` etc.
SPORTS_LEAGUE_PREFIXES = (
    # Cricket
    "cricipl-", "cricbpl-", "cricoxi-", "cricwc-", "cricinternational-",
    "cricbtl-", "cricfanco-", "cric-",
    # Soccer — top European leagues
    "ucl-", "uel-", "uecl-", "uef-", "epl-", "elc-", "fa-", "efa-", "efl-",
    "sea-", "lal-", "fra-", "fl1-", "fr1-", "fr2-",
    "esp-", "ger-", "bun-", "ita-", "por-", "ned-", "tur-", "scotpl-", "saudi-",
    "bel-", "swe-", "nor-", "den-", "swiss-", "aut-",
    "mls-", "usl-", "argpl-", "arg-", "brapl-", "bra-", "sud-", "lib-",
    "concacaf-", "copa-",
    # US sports
    "nba-", "wnba-", "cbb-", "ncaab-", "ncaaf-", "ncaaw-", "ncaa-",
    "nhl-", "ahl-", "khl-",
    "nfl-", "cfl-",
    "mlb-", "milb-", "kbo-", "npb-",
    # Combat / individual
    "ufc-", "ufcoct-", "boxing-", "tennisatp-", "tenniswta-", "wta-", "atp-",
    "golfpga-", "pga-", "lpga-", "f1-", "formula-",
    "nascar-", "indycar-", "olympics-", "esports-", "lol-", "csgo-",
    "valorant-", "dota-",
)

# ── Aggressive keyword sets (priority order) ────────────────────────────────
#
# Each list is checked in order; the FIRST list that matches wins. This
# means political markets that mention bitcoin still classify as politics,
# which is the desired behavior.

GEOPOLITICS_KEYWORDS = frozenset({
    # Foreign leaders / actors
    "zelenskyy", "zelensky", "putin", "xi-jinping", "xi-", "netanyahu",
    "khamenei", "ayatollah", "supreme-leader", "ayatollah-out",
    "kim-jong", "kim-jong-un", "mohammed-bin-salman", "mbs-",
    "erdogan", "orban", "tisza", "respect-and-freedom",
    "macron", "merkel", "scholz", "starmer", "sunak", "johnson-boris",
    "milei", "lula", "modi", "maduro", "assad",
    # Countries / regions with heavy Polymarket geopolitical volume
    "iran", "iranian", "tehran", "fordow", "natanz", "iraq", "iraqi",
    "israel", "israeli", "idf", "idf-", "gaza", "west-bank",
    "palestine", "palestinian", "lebanon", "lebanese",
    "ukraine", "ukrainian", "kyiv", "kiev", "donbas", "crimea",
    "russia", "russian", "kremlin", "wagner", "moscow",
    "china-taiwan", "taiwan", "cross-strait",
    "north-korea", "dprk", "pyongyang",
    "venezuela", "syria", "yemen", "houthi", "houthis",
    "hamas", "hezbollah", "irgc", "taliban",
    "afghanistan", "pakistan", "india-pakistan", "myanmar", "sudan",
    "ethiopia", "eritrea", "saudi-arabia", "saudi",
    "hungary", "orban-out",
    # Institutions and actions
    "nato", "un-", "united-nations", "unsc", "ic-c", "icc-",
    "iaea", "g7", "g20", "brics", "eu-sanctions",
    "sanctions", "sanction", "embargo", "blockade",
    "ceasefire", "peace-deal", "peace-talks", "peace-treaty",
    "war", "wars", "invasion", "invasions", "territorial",
    "sovereignty", "regime", "regime-fall", "regime-change", "coup",
    "airstrike", "drone-strike", "missile-strike", "missile",
    "nuclear", "nuclear-facility", "nuclear-deal", "nuclear-weapon",
    "us-forces", "us-strikes", "us-x-iran", "us-x-russia",
    "us-troops", "troop-withdrawal", "withdrawal",
    "iran-deal", "jcpoa", "putin-fall", "putin-out",
    "summit", "bilateral", "diplomatic", "embassy", "consulate",
    "arms-deal", "defense-pact", "alliance", "treaty",
    "refugee", "refugees", "humanitarian",
    "military-action", "military-operation", "strike-on",
    "hormuz", "kharg", "strait-of-hormuz", "red-sea",
    "ukraine-war", "russia-ukraine", "israel-hamas", "israel-iran",
    "iran-x-israel", "iran-israel",
})

POLITICS_KEYWORDS = frozenset({
    # US politicians
    "trump", "biden", "harris", "pence", "obama", "clinton", "kamala",
    "vance", "desantis", "ramaswamy", "newsom", "musk-political",
    "vivek", "pelosi", "schumer", "mcconnell", "gaetz", "jordan",
    "powell", "yellen", "comer", "barr", "garland", "wray", "bondi",
    "hegseth", "rubio", "kennedy-rfk", "rfk", "rfk-jr", "tulsi",
    # Offices and institutions
    "election", "elections", "election-day", "president", "presidential",
    "vice-president", "potus", "congress", "senate", "senator",
    "house-of-representatives", "house", "speaker", "governor",
    "republican", "democrat", "democratic", "gop", "political", "politics",
    "primary-election", "primary", "vote", "voter", "voting", "ballot",
    "campaign", "polls", "polling", "election-results", "swing-state",
    "electoral-college", "electoral",
    "cabinet", "confirmation", "nominee", "nomination", "appointee",
    "impeach", "impeachment", "pardon", "executive-order", "eo-",
    "scotus", "supreme-court", "judge", "justice", "doj", "fbi", "cia",
    "nsa", "pentagon", "white-house", "whitehouse", "state-of-the-union",
    "us-politics", "us-election", "presidential-election",
    # Domestic policy / border / law
    "border", "immigration", "deportation", "tariff", "tariffs",
    "trade-war", "habeas-corpus", "martial-law",
    "constitution", "constitutional", "amendment", "ratify", "law",
    "legislation", "policy", "bill-",
    # Cabinet / agency picks
    "cabinet-pick", "secretary-of-state", "treasury-secretary",
    "attorney-general", "fed-chair", "confirmation-hearing",
    # Foreign elections (domestic to the target country, still "politics")
    "peruvian-presidential", "peruvian-election",
    "prime-minister", "next-prime-minister",
})

CRYPTO_KEYWORDS = frozenset({
    "bitcoin", "btc", "ethereum", "ether", "eth", "solana", "sol",
    "crypto", "cryptocurrency", "altcoin", "altcoins", "memecoin",
    "memecoins", "shitcoin", "defi", "nft", "nfts", "blockchain",
    "token", "tokens", "tokenized", "stablecoin", "usdc", "usdt",
    "tether", "circle", "binance", "coinbase", "kraken", "okx",
    "ftx", "ftt", "celsius", "luna", "terra", "ust",
    "doge", "dogecoin", "shiba", "shib", "pepe", "wif", "bonk",
    "xrp", "ripple", "ada", "cardano", "polkadot", "dot",
    "matic", "polygon", "avax", "avalanche", "atom", "cosmos",
    "near", "fantom", "ftm", "aptos", "sui", "arbitrum", "arb",
    "optimism", "op-", "base-", "bnb", "trx", "tron",
    "halving", "halvening", "mining", "miner", "miners", "hashrate",
    "hashprice", "etf-bitcoin", "btc-etf", "spot-etf", "ethereum-etf",
    "sec-crypto", "gary-gensler", "uniswap", "uni-", "aave", "compound",
    "makerdao", "mkr", "lido", "ldo", "curve", "crv",
    "web3", "smart-contract", "metamask", "wallet-",
    "vitalik", "buterin", "satoshi", "saylor", "michael-saylor",
    "cz-", "binance-cz", "sbf", "do-kwon",
})

ECONOMICS_KEYWORDS = frozenset({
    "fed", "federal-reserve", "fomc", "interest-rate", "rate-cut",
    "rate-hike", "rates-cut", "rates-hike", "rate-decision",
    "basis-points", "bps", "fed-chair", "powell", "yellen",
    "cpi", "inflation", "deflation", "stagflation", "core-cpi",
    "ppi", "pce", "core-pce", "gdp", "gdp-growth", "recession",
    "soft-landing", "hard-landing", "jobs", "jobs-report", "payroll",
    "non-farm-payroll", "nonfarm", "unemployment", "unemployment-rate",
    "treasury", "treasury-yield", "yield", "10-year", "yield-curve",
    "inversion", "stock-market", "stocks", "s&p", "sp500", "sandp",
    "nasdaq", "dow", "djia", "russell-2000", "vix", "volatility",
    "earnings", "ipo", "merger", "acquisition", "bankruptcy",
    "oil", "wti", "brent", "crude", "crude-oil", "cl-", "natural-gas",
    "natgas", "gold", "silver", "commodity", "commodities", "copper",
    "housing", "mortgage", "30-year-mortgage", "home-prices",
    "case-shiller", "consumer-confidence", "retail-sales",
    "ecb", "bank-of-england", "boe", "boj", "bank-of-japan",
    "central-bank", "monetary-policy", "qt-", "quantitative-easing",
    "qe-", "balance-sheet", "deficit", "debt-ceiling", "shutdown",
    "trade-deficit", "tariff-impact",
})

ENTERTAINMENT_KEYWORDS = frozenset({
    "oscar", "oscars", "academy-award", "academy-awards", "best-picture",
    "best-actor", "best-actress", "best-director", "grammy", "grammys",
    "emmy", "emmys", "golden-globe", "globes", "tony-award",
    "movie", "film", "box-office", "opening-weekend", "billion",
    "marvel", "mcu", "dc-", "disney", "pixar", "warner",
    "tv-show", "series", "season-finale", "renewal", "cancellation",
    "streaming", "netflix", "hbo", "hulu", "amazon-prime", "apple-tv",
    "spotify", "album", "billboard", "hot-100", "billboard-200",
    "celebrity", "kardashian", "kim-k", "kanye", "ye-", "drake",
    "kendrick", "taylor-swift", "swiftie", "beyonce", "rihanna",
    "ariana", "weeknd", "bieber", "selena", "jay-z", "eminem",
    "elon-tweet", "musk-tweet", "musk-poll", "elon",
    "viral", "tiktok", "instagram", "twitter-blue", "x-platform",
    "tweet", "post-views", "follower", "followers", "subscribers",
    "youtube", "youtuber", "mr-beast", "logan-paul", "ksi-",
    "rogan", "joe-rogan", "podcast", "interview",
    "wedding", "divorce", "engagement", "baby-",
    "game-of-the-year", "goty", "video-game", "fortnite", "roblox",
    "gta", "call-of-duty", "cod-",
    "eurovision", "song-contest",
})

SCIENCE_KEYWORDS = frozenset({
    "space", "nasa", "spacex", "starship", "starlink", "rocket-launch",
    "launch", "moon-mission", "mars", "satellite", "astronaut",
    "iss-", "international-space-station", "blue-origin", "boeing-starliner",
    "asteroid", "comet", "meteor", "eclipse", "supermoon",
    "climate", "climate-change", "global-warming", "carbon", "co2",
    "sea-level", "ice-cap", "antarctic", "arctic",
    "hurricane", "typhoon", "tornado", "earthquake", "tsunami",
    "volcano", "eruption", "wildfire", "flood", "drought",
    "temperature", "heatwave", "cold-snap", "snowstorm", "blizzard",
    "weather-", "high-temperature", "low-temperature", "rainfall",
    "pandemic", "covid", "covid-19", "vaccine", "vaccination",
    "fda", "drug-approval", "fda-approval", "clinical-trial",
    "phase-3", "who-", "cdc-", "outbreak", "epidemic",
    "ai-", "artificial-intelligence", "openai", "gpt", "chatgpt",
    "claude-", "anthropic", "gemini-", "grok-", "llama",
    "ai-model", "ai-release", "agi", "asi", "robot", "robotics",
    "humanoid", "self-driving", "fsd", "tesla-fsd", "waymo",
    "cybertruck", "rivian", "lucid", "electric-vehicle", "ev-",
    "fusion", "nuclear-fusion", "ignition", "iter-",
    "quantum", "quantum-computing",
    "google", "apple", "microsoft", "meta", "amazon", "tesla",
    "alphabet", "nvidia", "amd", "intel", "tsmc", "asml",
    "iphone", "android", "samsung", "macbook", "vision-pro",
    "tech", "technology",
})

# Sports keywords (used in addition to league prefixes)
SPORTS_KEYWORDS = frozenset({
    "nba", "wnba", "nfl", "mlb", "nhl", "ufc", "boxing", "f1",
    "formula-1", "premier-league", "epl", "champions-league", "ucl",
    "world-cup", "fifa", "uefa", "concacaf", "copa-america",
    "super-bowl", "superbowl", "playoff", "playoffs", "championship",
    "championships", "title", "mvp", "rookie-of-the-year",
    "draft", "trade-deadline", "free-agency", "transfer-window",
    "olympics", "olympic", "summer-games", "winter-games",
    "soccer", "football", "basketball", "baseball", "hockey",
    "tennis", "atp", "wta", "wimbledon", "us-open", "french-open",
    "australian-open", "grand-slam", "grand-slams",
    "golf", "pga", "lpga", "masters", "us-open-golf",
    "ufc-", "fight-night", "knockout", "submission", "ko-",
    "lakers", "warriors", "celtics", "heat", "knicks", "bulls",
    "yankees", "dodgers", "red-sox", "cubs", "mets", "giants",
    "patriots", "cowboys", "chiefs", "eagles", "ravens", "bills",
    "rangers", "bruins", "blackhawks", "penguins",
    "messi", "ronaldo", "lebron", "curry", "mahomes", "brady",
    "djokovic", "nadal", "federer", "alcaraz", "sinner",
    "tiger-woods", "scottie-scheffler", "rory-mcilroy",
    "indian-premier-league", "ipl", "cricket", "test-match",
    "race", "grand-prix", "qualifying", "pole-position",
})


# Priority order — first match wins. Geopolitics is checked BEFORE politics
# because markets like "Will Biden sanction Iran" are primarily geopolitical
# even though they mention a US politician.
_KEYWORD_LISTS = (
    ("geopolitics", GEOPOLITICS_KEYWORDS),
    ("politics", POLITICS_KEYWORDS),
    ("crypto", CRYPTO_KEYWORDS),
    ("economics", ECONOMICS_KEYWORDS),
    ("sports", SPORTS_KEYWORDS),
    ("entertainment", ENTERTAINMENT_KEYWORDS),
    ("science", SCIENCE_KEYWORDS),
)

# Output categories (also used by the dynamic tier engine)
CATEGORIES = ("politics", "geopolitics", "crypto", "sports", "economics", "entertainment", "science", "other")


def _normalize(text: str) -> str:
    """Lowercase + collapse whitespace + replace non-alphanumeric with dashes."""
    if not text:
        return ""
    s = text.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def classify_keywords(slug: str | None, title: str | None = None) -> tuple[str, str]:
    """Pure-function classifier. Returns ``(category, source)``.

    ``source`` is one of ``league_prefix``, ``slug_keyword``,
    ``title_keyword``, or ``other``.
    """
    norm_slug = _normalize(slug or "")

    # 1. League prefix (sports markets)
    for prefix in SPORTS_LEAGUE_PREFIXES:
        if norm_slug.startswith(prefix):
            return "sports", "league_prefix"

    # 2. Slug substring against the priority-ordered keyword lists.
    # Surround with dashes so we don't match "ai" inside "saidya".
    padded = f"-{norm_slug}-"
    for category, keywords in _KEYWORD_LISTS:
        for kw in keywords:
            needle = f"-{kw.strip('-')}-"
            if needle in padded:
                return category, "slug_keyword"

    # 3. Title fallback (often more verbose than the slug)
    if title:
        norm_title = _normalize(title)
        padded_t = f"-{norm_title}-"
        for category, keywords in _KEYWORD_LISTS:
            for kw in keywords:
                needle = f"-{kw.strip('-')}-"
                if needle in padded_t:
                    return category, "title_keyword"

    return "other", "fallback"


class MarketCategorizer:
    """Classifies Polymarket markets into categories with persistent caching."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._memo: dict[str, str] = {}
        if db_path is None:
            from trading_platform.polymarket.wallet_db import WalletDB
            db_path = str(WalletDB()._path)
        self._db_path = str(db_path)
        self._ensure_table()

    # ── Schema ─────────────────────────────────────────────────────────────

    def _ensure_table(self) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_categories (
                        market_slug TEXT PRIMARY KEY,
                        condition_id TEXT,
                        category TEXT NOT NULL,
                        source TEXT,
                        classified_at INTEGER NOT NULL
                    )
                """)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_mc_category ON market_categories(category)"
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("market_categories table ensure failed: %s", exc)

    def _load_db_cache(self) -> None:
        """Bulk-load all cached classifications into memory."""
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                rows = conn.execute(
                    "SELECT market_slug, category FROM market_categories"
                ).fetchall()
            finally:
                conn.close()
            for slug, cat in rows:
                if slug and cat:
                    self._memo[slug] = cat
        except Exception:
            pass

    # ── Public API ─────────────────────────────────────────────────────────

    def classify(
        self,
        market_slug: str | None,
        title: str | None = None,
        condition_id: str | None = None,
    ) -> str:
        """Classify a single market and persist to cache."""
        slug = (market_slug or "").strip()
        if slug and slug in self._memo:
            return self._memo[slug]

        category, source = classify_keywords(slug, title)
        if slug:
            self._memo[slug] = category
            self._persist_one(slug, condition_id, category, source)
        return category

    def _persist_one(self, slug: str, cid: str | None, category: str, source: str) -> None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.execute(
                    """INSERT INTO market_categories
                        (market_slug, condition_id, category, source, classified_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(market_slug) DO UPDATE SET
                           condition_id = excluded.condition_id,
                           category = excluded.category,
                           source = excluded.source,
                           classified_at = excluded.classified_at""",
                    (slug, cid, category, source, int(time.time())),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    # ── Bulk backfill ──────────────────────────────────────────────────────

    def backfill_wallet_trades(
        self,
        *,
        progress_every: int = 1000,
        log_fn=None,
        reclassify_other: bool = True,
    ) -> dict[str, Any]:
        """Classify every distinct null-category slug and UPDATE wallet_trades.

        Idempotent: running it twice produces the same result. Logs
        progress every ``progress_every`` slugs via ``log_fn`` (defaults
        to ``print``). Returns a summary dict with category counts and
        total rows updated.

        When ``reclassify_other=True`` (default), rows previously
        marked ``other`` or the legacy ``tweet_count`` bucket are also
        re-evaluated against the current keyword sets — useful after
        adding new keywords.
        """
        log = log_fn or (lambda m: print(m, flush=True))
        self._load_db_cache()

        conn = sqlite3.connect(self._db_path)
        try:
            if reclassify_other:
                where = (
                    "(category IS NULL OR category IN ('other','tweet_count')) "
                    "AND slug IS NOT NULL AND slug != ''"
                )
            else:
                where = (
                    "category IS NULL AND slug IS NOT NULL AND slug != ''"
                )
            slugs = conn.execute(
                f"""SELECT slug, MAX(title), MAX(condition_id), COUNT(*)
                    FROM wallet_trades WHERE {where}
                    GROUP BY slug"""
            ).fetchall()
        finally:
            conn.close()

        total_distinct = len(slugs)
        log(f"[CATEGORIZE] {total_distinct} distinct unclassified slugs")

        # Step 1: classify everything in memory
        slug_to_cat: dict[str, tuple[str, str | None, str]] = {}
        for i, (slug, title, cid, _n) in enumerate(slugs, 1):
            cached = self._memo.get(slug)
            if cached:
                slug_to_cat[slug] = (cached, cid, "cache")
                continue
            category, source = classify_keywords(slug, title)
            self._memo[slug] = category
            slug_to_cat[slug] = (category, cid, source)
            if i % progress_every == 0:
                pct = i / total_distinct * 100
                log(f"[CATEGORIZE] classified {i}/{total_distinct} slugs ({pct:.1f}%)")

        # Step 2: bulk persist to cache table
        log(f"[CATEGORIZE] persisting {len(slug_to_cat)} cache entries...")
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                now = int(time.time())
                conn.executemany(
                    """INSERT INTO market_categories
                        (market_slug, condition_id, category, source, classified_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(market_slug) DO UPDATE SET
                           category = excluded.category,
                           source = excluded.source,
                           classified_at = excluded.classified_at""",
                    [
                        (slug, cid, cat, source, now)
                        for slug, (cat, cid, source) in slug_to_cat.items()
                    ],
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            log(f"[CATEGORIZE] cache persist failed: {exc}")

        # Step 3: bulk UPDATE wallet_trades
        log(f"[CATEGORIZE] updating wallet_trades.category...")
        # Group slugs by target category for efficient batched UPDATEs
        by_cat: dict[str, list[str]] = {}
        for slug, (cat, _cid, _src) in slug_to_cat.items():
            by_cat.setdefault(cat, []).append(slug)

        rows_updated = 0
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                # When reclassifying, we may overwrite existing 'other' /
                # 'tweet_count' rows; otherwise only fill in NULLs.
                if reclassify_other:
                    where_filter = (
                        "(category IS NULL OR category IN ('other','tweet_count'))"
                    )
                else:
                    where_filter = "category IS NULL"
                for cat, slug_list in by_cat.items():
                    # Batch in chunks of 500 to keep the IN clause sane
                    for chunk_start in range(0, len(slug_list), 500):
                        chunk = slug_list[chunk_start:chunk_start + 500]
                        placeholders = ",".join("?" * len(chunk))
                        cur = conn.execute(
                            f"""UPDATE wallet_trades SET category = ?
                                WHERE {where_filter} AND slug IN ({placeholders})""",
                            (cat, *chunk),
                        )
                        rows_updated += cur.rowcount
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            log(f"[CATEGORIZE] wallet_trades UPDATE failed: {exc}")

        # Step 4: summary
        summary: dict[str, Any] = {
            "distinct_slugs_classified": total_distinct,
            "rows_updated": rows_updated,
            "by_category": {},
        }
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                rows = conn.execute(
                    """SELECT category, COUNT(*) FROM wallet_trades
                       GROUP BY category ORDER BY COUNT(*) DESC"""
                ).fetchall()
                total = sum(r[1] for r in rows)
                for cat, n in rows:
                    pct = round(n / total * 100, 1) if total else 0.0
                    summary["by_category"][cat or "(null)"] = {"count": n, "pct": pct}
                summary["total_trades"] = total
                still_null = conn.execute(
                    "SELECT COUNT(*) FROM wallet_trades WHERE category IS NULL"
                ).fetchone()[0]
                summary["still_null"] = still_null
            finally:
                conn.close()
        except Exception:
            pass

        log("[CATEGORIZE] done.")
        return summary
