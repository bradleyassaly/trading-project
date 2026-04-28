"""Sub-category classifier for prediction markets.

Polymarket markets aren't "sports" — they're "UFC Fight Night X vs Y"
or "Iran-US permanent peace deal by April 30." Smart money operates
at the event level. Treating a UFC specialist and an MLB specialist
as the same "sports" trader destroys signal.

This module assigns a `subcategory` to every market based on regex
patterns over the question + slug. The output is persisted to
`markets.subcategory` so downstream systems (alpha scoring,
specialization z-score, leaderboards, alerts) can operate at sport /
region granularity instead of just the 9 broad categories.

Sub-domain taxonomy:
    sports/ufc, sports/tennis, sports/nba, sports/nfl, sports/mlb,
    sports/nhl, sports/soccer, sports/golf, sports/f1, sports/nascar,
    politics/iran, politics/russia, politics/china, politics/israel,
    politics/election, politics/trump, politics/biden,
    crypto/bitcoin, crypto/ethereum, crypto/altcoin,
    geopolitics/middle_east, geopolitics/asia, geopolitics/europe,
    entertainment/eurovision, entertainment/musk_tweets, entertainment/movies
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# Order matters: more specific patterns checked first within each parent
PATTERNS: list[tuple[str, str, str]] = [
    # (subcategory, pattern, applies_to_parent_or_None)
    # ── Sports ──
    ("sports/ufc",     r"\b(ufc|mma|bellator|pfl)\b",                           "sports"),
    ("sports/tennis",  r"\b(tennis|atp|wta|wimbledon|us open|french open|australian open|roland garros|nadal|djokovic|alcaraz|sabalenka|swiatek)\b", "sports"),
    ("sports/nba",     r"\b(nba|warriors|lakers|celtics|knicks|nets|heat|bulls|spurs|mavs|suns|nuggets|sixers|76ers|grizzlies)\b", "sports"),
    ("sports/nfl",     r"\b(nfl|super bowl|chiefs|cowboys|49ers|eagles|patriots|packers|ravens|bills|raiders|broncos)\b", "sports"),
    ("sports/mlb",     r"\b(mlb|yankees|dodgers|red sox|cubs|mets|braves|astros|cardinals|world series)\b", "sports"),
    ("sports/nhl",     r"\b(nhl|stanley cup|canucks|maple leafs|rangers|bruins|oilers|panthers)\b", "sports"),
    ("sports/soccer",  r"\b(world cup|fifa|champions league|premier league|la liga|serie a|bundesliga|messi|ronaldo|man city|liverpool fc|barcelona|real madrid|man united)\b", "sports"),
    ("sports/golf",    r"\b(pga|masters|us open golf|the open championship|ryder cup|liv golf|tiger woods|rory mcilroy)\b", "sports"),
    ("sports/f1",      r"\b(f1|formula 1|grand prix|verstappen|hamilton|leclerc|mclaren|ferrari)\b", "sports"),
    ("sports/nascar",  r"\b(nascar|daytona|talladega)\b", "sports"),

    # ── Politics by region/topic ──
    ("politics/iran",     r"\biran\b",                "politics|geopolitics"),
    ("politics/russia",   r"\b(russia|putin|ukraine)\b", "politics|geopolitics"),
    ("politics/china",    r"\b(china|xi jinping|taiwan|hong kong)\b", "politics|geopolitics"),
    ("politics/israel",   r"\b(israel|gaza|hamas|netanyahu)\b", "politics|geopolitics"),
    ("politics/election", r"\b(election|primary|caucus|senate|congress|house of representatives|governor)\b", "politics"),
    ("politics/trump",    r"\btrump\b",               "politics"),
    ("politics/biden",    r"\bbiden\b",               "politics"),
    ("politics/harris",   r"\bharris\b",              "politics"),

    # ── Crypto ──
    ("crypto/bitcoin",  r"\b(bitcoin|btc)\b",       "crypto"),
    ("crypto/ethereum", r"\b(ethereum|ether|eth)\b", "crypto"),
    ("crypto/solana",   r"\b(solana|sol)\b",        "crypto"),
    ("crypto/xrp",      r"\b(xrp|ripple)\b",         "crypto"),
    ("crypto/altcoin",  r"\b(doge|ada|cardano|avax|matic|link|chainlink|ltc|litecoin|altcoin)\b", "crypto"),

    # ── Geopolitics ──
    ("geopolitics/middle_east", r"\b(middle east|saudi|qatar|uae|yemen|syria|lebanon|hezbollah)\b", "geopolitics"),
    ("geopolitics/asia",        r"\b(north korea|south korea|japan|india|pakistan|asean)\b", "geopolitics"),
    ("geopolitics/europe",      r"\b(eu |european union|brexit|nato|france|germany|uk |united kingdom)\b", "geopolitics"),

    # ── Entertainment ──
    ("entertainment/eurovision", r"\beurovision\b",    "entertainment"),
    ("entertainment/musk",       r"\b(elon musk|musk tweets|musk posts)\b", "entertainment|tweet_count"),
    ("entertainment/awards",     r"\b(oscar|golden globe|grammy|emmy|tony)\b", "entertainment"),
    ("entertainment/movies",     r"\b(box office|movie|film)\b", "entertainment"),

    # ── Science ──
    ("science/weather",  r"\b(temperature|highest temperature|hottest|coldest|rain|snow)\b", "science"),
    ("science/space",    r"\b(spacex|nasa|launch|mars|moon|asteroid)\b", "science"),
]


def classify(question: str | None, slug: str | None,
             parent_category: str | None = None) -> str | None:
    """Return the most-specific subcategory tag, or None if no pattern matches.

    Patterns are scoped to a parent_category when applicable — e.g. the
    "ufc" pattern only matches when the parent is "sports". This avoids
    a `politics/iran` market accidentally matching a sports pattern.
    """
    text = " ".join(filter(None, [(question or "").lower(), (slug or "").lower()]))
    if not text:
        return None
    parent = (parent_category or "").lower()
    for sub, pat, scope in PATTERNS:
        # When parent is provided, restrict to matching scopes; when not,
        # allow any pattern (specific keywords like "ufc"/"iran"/"btc"
        # are unambiguous on their own).
        if parent and scope and parent not in scope.split("|"):
            continue
        if re.search(pat, text):
            return sub
    return None


_SCHEMA_PATCH = """
ALTER TABLE markets ADD COLUMN IF NOT EXISTS subcategory TEXT;
CREATE INDEX IF NOT EXISTS idx_markets_subcategory ON markets(subcategory);
"""


def run_classifier(db_path: str | None = None, force: bool = False) -> dict[str, Any]:
    """Classify every market and write to markets.subcategory.

    Idempotent. With force=False, only re-classifies markets where
    subcategory IS NULL. With force=True, re-classifies all rows
    (use after pattern updates).
    """
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        for stmt in _SCHEMA_PATCH.strip().split(";"):
            s = stmt.strip()
            if s:
                try:
                    conn.execute(s)
                except Exception:
                    pass
        where_clause = "" if force else " WHERE subcategory IS NULL"
        rows = conn.execute(
            f"SELECT condition_id, question, slug FROM markets{where_clause}"
        ).fetchall()
        n_total = len(rows)
        updates: list[tuple] = []
        by_sub: dict[str, int] = {}
        for cid, q, slug in rows:
            # No parent category in this schema — pass None and let
            # specific patterns (ufc/tennis/iran/btc) match unscoped.
            sub = classify(q, slug, None)
            if sub:
                updates.append((sub, cid))
                by_sub[sub] = by_sub.get(sub, 0) + 1
        # Batch write in 500-row chunks
        for i in range(0, len(updates), 500):
            chunk = updates[i:i + 500]
            conn.executemany(
                "UPDATE markets SET subcategory = ? WHERE condition_id = ?",
                chunk,
            )
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "markets_evaluated": n_total,
            "markets_classified": len(updates),
            "unclassified": n_total - len(updates),
            "by_subcategory": dict(sorted(by_sub.items(), key=lambda kv: -kv[1])[:20]),
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    force = "--force" in sys.argv
    print(run_classifier(force=force))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
