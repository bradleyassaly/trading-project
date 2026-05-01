"""Auto-map PM ↔ Kalshi market pairs by question/title similarity.

Naive but effective for high-overlap categories (politics, geopolitics,
sport-headline). Uses token Jaccard + length penalty + close-time
proximity. High-confidence pairs (score ≥ 0.7) auto-write to
cross_platform_market_map. Lower-confidence go to
cross_platform_market_map_candidates for human review.

Run via scheduler daily after both kalshi_ingest and the PM
markets_refresh have updated.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# Stop words — common in market titles that don't disambiguate
_STOPWORDS = {
    "will", "be", "the", "a", "an", "and", "or", "of", "to", "in",
    "on", "at", "for", "by", "with", "before", "after", "this", "that",
    "is", "are", "by", "vs", "vs.",
}


# High-confidence threshold to auto-promote
AUTO_PROMOTE_SCORE = 0.70

# Lower-confidence threshold to log as candidate
CANDIDATE_SCORE = 0.50

# Close-time match window — pairs whose close-times disagree by >X
# seconds are unlikely to be the same outcome
MAX_CLOSE_TIME_GAP_SECONDS = 7 * 86400


_SCHEMA_CANDIDATES = """
CREATE TABLE IF NOT EXISTS cross_platform_market_map_candidates (
    pm_condition_id TEXT NOT NULL,
    kalshi_ticker   TEXT NOT NULL,
    score           DOUBLE PRECISION,
    pm_question     TEXT,
    kalshi_title    TEXT,
    last_evaluated  BIGINT,
    PRIMARY KEY (pm_condition_id, kalshi_ticker)
);
"""


def _tokenize(s: str) -> set[str]:
    if not s:
        return set()
    s = s.lower()
    # Strip non-alphanumeric except spaces (preserves digits for $150K etc)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    toks = {t for t in s.split() if t and t not in _STOPWORDS and len(t) > 1}
    return toks


def _bigrams(toks: list[str]) -> set[str]:
    """Token bigrams capture 'New York' / 'Hong Kong' / etc."""
    if len(toks) < 2:
        return set()
    return {f"{toks[i]}_{toks[i+1]}" for i in range(len(toks) - 1)}


def _numeric_tokens(s: str) -> set[str]:
    """Extract numeric tokens (dates, dollar amounts, percentages).

    These are highly discriminating — '150k', '2026', '70%' rarely
    co-occur unless markets are about the same outcome.
    """
    if not s:
        return set()
    out = set()
    # Dollar amounts: $150k, $1m, $50,000
    for m in re.finditer(r"\$?(\d+(?:,\d{3})*(?:\.\d+)?)\s*([kmb]?)", s.lower()):
        num = m.group(1).replace(",", "")
        unit = m.group(2)
        out.add(f"num_{num}{unit}")
    # Year tokens (2025, 2026, 2027)
    for m in re.finditer(r"\b(202[3-9])\b", s):
        out.add(f"yr_{m.group(1)}")
    # Percentage thresholds
    for m in re.finditer(r"(\d+)\s*%", s):
        out.add(f"pct_{m.group(1)}")
    return out


# Generic political / geo / economic entities — bonus for shared mention
_ENTITY_PATTERNS = re.compile(
    r"\b(trump|biden|harris|putin|xi|netanyahu|powell|fed|fomc|"
    r"iran|china|russia|ukraine|israel|gaza|north korea|saudi|"
    r"taiwan|taiwan|venezuela|nato|congress|senate|"
    r"bitcoin|ethereum|btc|eth|sol|solana|"
    r"recession|inflation|cpi|gdp|yield|treasury|"
    r"openai|google|microsoft|apple|tesla|meta|amazon|nvidia|"
    r"musk|altman|gates|zuckerberg)\b",
    re.IGNORECASE,
)


def _entities(s: str) -> set[str]:
    if not s:
        return set()
    return {m.group(1).lower() for m in _ENTITY_PATTERNS.finditer(s)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _weighted_similarity(pm_q: str, kalshi_title: str) -> float:
    """Weighted combo: 0.4 unigram Jaccard + 0.3 bigram + 0.2 numeric +
    0.1 entity overlap. Heavily penalizes superficial-only matches.
    """
    pm_uni = _tokenize(pm_q)
    k_uni = _tokenize(kalshi_title)
    pm_bi = _bigrams(sorted(pm_uni))
    k_bi = _bigrams(sorted(k_uni))
    pm_num = _numeric_tokens(pm_q)
    k_num = _numeric_tokens(kalshi_title)
    pm_ent = _entities(pm_q)
    k_ent = _entities(kalshi_title)
    score = (
        0.40 * _jaccard(pm_uni, k_uni)
        + 0.30 * _jaccard(pm_bi, k_bi)
        + 0.20 * _jaccard(pm_num, k_num)
        + 0.10 * _jaccard(pm_ent, k_ent)
    )
    # Hard requirement: must share at least one entity OR one numeric
    # token. Pure-text overlap (e.g. "Trump-North-Korea" vs "Trump-North-
    # Carolina") otherwise scores too high.
    if not (pm_ent & k_ent) and not (pm_num & k_num):
        score *= 0.40  # heavy penalty
    return score


def _score(pm_q: str, kalshi_title: str,
           pm_close: int | None, kalshi_close: int | None) -> float:
    """2026-05-01: replaced single-Jaccard with weighted combo
    (unigram + bigram + numeric tokens + entity overlap). Hard
    requirement that shared entity OR shared numeric token must
    exist eliminates the 'Trump-North-Korea vs Trump-North-Carolina'
    false positives that plagued v1.
    """
    base = _weighted_similarity(pm_q, kalshi_title)
    if base < 0.20:
        return 0.0
    time_factor = 1.0
    if pm_close and kalshi_close:
        gap = abs(pm_close - kalshi_close)
        if gap > MAX_CLOSE_TIME_GAP_SECONDS:
            return 0.0
        time_factor = max(0.5, 1.0 - gap / MAX_CLOSE_TIME_GAP_SECONDS)
    len_factor = 1.0 - min(0.3, abs(len(pm_q) - len(kalshi_title)) / 200.0)
    return round(base * time_factor * len_factor, 4)


def _ensure_schemas(conn) -> None:
    try:
        conn.execute(_SCHEMA_CANDIDATES.strip())
        conn.execute(
            """CREATE TABLE IF NOT EXISTS cross_platform_market_map (
                pm_condition_id TEXT NOT NULL,
                kalshi_ticker   TEXT NOT NULL,
                quality         DOUBLE PRECISION DEFAULT 1.0,
                last_verified   BIGINT,
                PRIMARY KEY (pm_condition_id, kalshi_ticker)
            )"""
        )
        conn.commit()
    except Exception as exc:
        logger.debug("schema ensure failed: %s", exc)


def run_mapper(max_pm: int = 500, max_kalshi: int = 5000) -> dict[str, Any]:
    """Find candidate PM↔Kalshi pairs.

    Restrict to politics/geopolitics for the first pass — those are the
    categories where both platforms have most overlap. Sports markets
    have wildly different conventions; defer.
    """
    t0 = time.time()
    conn = get_connection()
    n_pairs_found = 0
    n_auto_promoted = 0
    n_candidates = 0
    try:
        _ensure_schemas(conn)
        # Pull PM open politics/geopolitics markets
        pm_rows = conn.execute(
            f"""SELECT condition_id, question, end_date_iso
                  FROM markets
                 WHERE end_date_iso > to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
                   AND yes_token_id IS NOT NULL
                   AND (LOWER(subcategory) LIKE 'politics%'
                        OR LOWER(subcategory) LIKE 'geopolitics%')
                 ORDER BY volume_24h DESC NULLS LAST
                 LIMIT {int(max_pm)}"""
        ).fetchall()
        # Pull Kalshi open markets. Exclude KXMVE* tickers — those are
        # sports multi-event parlays, structurally different from PM
        # single-question markets and can't arb cleanly.
        kalshi_rows = conn.execute(
            f"""SELECT ticker, title, close_time
                  FROM kalshi_markets
                 WHERE close_time IS NOT NULL
                   AND close_time > extract(epoch FROM NOW())::bigint
                   AND ticker NOT LIKE 'KXMVE%'
                 ORDER BY liquidity_usd DESC NULLS LAST
                 LIMIT {int(max_kalshi)}"""
        ).fetchall()

        if not pm_rows or not kalshi_rows:
            return {
                "elapsed_seconds": round(time.time() - t0, 1),
                "pm_markets": len(pm_rows), "kalshi_markets": len(kalshi_rows),
                "pairs_found": 0, "auto_promoted": 0, "candidates": 0,
                "note": "one or both market lists empty",
            }

        # Convert PM end_date_iso to ts
        from datetime import datetime, timezone
        pm_normalized = []
        for r in pm_rows:
            cid, q, end_iso = r
            try:
                clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
                end_dt = datetime.fromisoformat(clean)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                end_ts = int(end_dt.timestamp())
            except Exception:
                end_ts = None
            pm_normalized.append((cid, q or "", end_ts))

        now = int(time.time())
        for pm_cid, pm_q, pm_close in pm_normalized:
            best_score = 0.0
            best_ticker = None
            best_title = None
            for k_ticker, k_title, k_close in kalshi_rows:
                s = _score(pm_q, k_title or "", pm_close, k_close)
                if s > best_score:
                    best_score = s
                    best_ticker = k_ticker
                    best_title = k_title
            if best_score >= CANDIDATE_SCORE and best_ticker:
                n_pairs_found += 1
                try:
                    conn.execute(
                        """INSERT INTO cross_platform_market_map_candidates
                             (pm_condition_id, kalshi_ticker, score,
                              pm_question, kalshi_title, last_evaluated)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT (pm_condition_id, kalshi_ticker)
                             DO UPDATE SET
                               score = EXCLUDED.score,
                               pm_question = EXCLUDED.pm_question,
                               kalshi_title = EXCLUDED.kalshi_title,
                               last_evaluated = EXCLUDED.last_evaluated""",
                        (pm_cid, best_ticker, best_score,
                         pm_q[:240], (best_title or "")[:240], now),
                    )
                    n_candidates += 1
                except Exception as exc:
                    logger.debug("candidate upsert failed: %s", exc)
                if best_score >= AUTO_PROMOTE_SCORE:
                    try:
                        conn.execute(
                            """INSERT INTO cross_platform_market_map
                                 (pm_condition_id, kalshi_ticker,
                                  quality, last_verified)
                               VALUES (?,?,?,?)
                               ON CONFLICT (pm_condition_id, kalshi_ticker)
                                 DO UPDATE SET
                                   quality = EXCLUDED.quality,
                                   last_verified = EXCLUDED.last_verified""",
                            (pm_cid, best_ticker, best_score, now),
                        )
                        n_auto_promoted += 1
                    except Exception as exc:
                        logger.debug("auto promote failed: %s", exc)

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "pm_markets": len(pm_rows), "kalshi_markets": len(kalshi_rows),
            "pairs_found": n_pairs_found, "auto_promoted": n_auto_promoted,
            "candidates_logged": n_candidates,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_mapper())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
