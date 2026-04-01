"""
Metaculus divergence signal for Kalshi prediction markets.

Metaculus (metaculus.com) is a public forecasting platform where community
forecasters optimize for calibration accuracy rather than financial gain.
Their aggregate forecasts are consistently better-calibrated than market
prices on slow-moving, information-rich events — because market prices
reflect hedging motives, noise trading, and liquidity risk premia, while
Metaculus forecasters face no such distortions.

When the Metaculus community median diverges significantly from a Kalshi
market price, it may signal that one of them is mis-priced.

Architecture note
-----------------
Like the base rate signal, Metaculus features are market-level scalars.
``MetaculusFetcher`` fetches questions from the public Metaculus API (no
auth required) and matches them to Kalshi markets via fuzzy string
comparison.  Matched results are persisted to
``data/kalshi/metaculus/matches.json`` for offline use.

``KalshiMetaculusSignal.compute_for_market()`` returns a feature dict
suitable for passing to ``build_kalshi_features(extra_scalar_features=...)``.

The signal emits non-null values only when the match confidence exceeds
``METACULUS_MIN_CONFIDENCE`` (default 0.70).

Feature columns produced
------------------------
``metaculus_probability``   Metaculus community median forecast (0–100 scale).
``metaculus_divergence``    metaculus_probability − kalshi_yes_price.
                            Positive  →  Metaculus more bullish than market.
                            Negative  →  Metaculus more bearish than market.
``metaculus_confidence``    Fuzzy match quality (0.0–1.0).

Signal direction
----------------
When Metaculus is more bullish than the market (divergence > 0) it suggests
the market is underpriced → BUY YES → direction = +1.
"""
from __future__ import annotations

import difflib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.kalshi.signals import KalshiSignalFamily

logger = logging.getLogger(__name__)

METACULUS_API_BASE = "https://www.metaculus.com/api2"
METACULUS_MIN_CONFIDENCE = 0.70


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class MetaculusQuestion:
    question_id: int
    title: str
    community_median: float | None   # 0.0–1.0 probability
    url: str


@dataclass
class MetaculusMatch:
    kalshi_ticker: str
    kalshi_title: str
    metaculus_id: int
    metaculus_title: str
    metaculus_median: float | None   # 0.0–1.0 probability from Metaculus
    confidence: float                # fuzzy match score (0.0–1.0)


# ── Metaculus API fetcher ─────────────────────────────────────────────────────

class MetaculusFetcher:
    """
    Fetches open questions from the Metaculus public API and matches them
    to Kalshi markets by fuzzy title similarity.

    All network calls are wrapped in try/except.  If Metaculus is down or
    the API changes, the fetcher logs a warning and returns empty results.

    :param api_base: Metaculus API base URL.
    :param timeout:  Per-request timeout in seconds.
    """

    def __init__(
        self,
        api_base: str = METACULUS_API_BASE,
        timeout: int = 15,
    ) -> None:
        self.api_base = api_base
        self.timeout = timeout

    def fetch_questions(
        self,
        limit: int = 200,
        status: str = "open",
        question_type: str = "binary",
    ) -> list[MetaculusQuestion]:
        """
        Pull open binary questions from the Metaculus API.

        :param limit:         Maximum number of questions to fetch.
        :param status:        Question status filter (default ``"open"``).
        :param question_type: Question type filter (default ``"binary"``).
        :returns:             List of :class:`MetaculusQuestion`.  Empty on error.
        """
        try:
            import requests  # already a project dep
        except ImportError:
            logger.warning("requests not installed — cannot fetch Metaculus questions.")
            return []

        questions: list[MetaculusQuestion] = []
        url = f"{self.api_base}/questions/"
        params: dict[str, Any] = {
            "status": status,
            "type": question_type,
            "limit": min(limit, 100),  # Metaculus max page size is 100
            "format": "json",
        }

        fetched = 0
        while url and fetched < limit:
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("Metaculus API error: %s — skipping Metaculus signal.", exc)
                return questions

            results = data.get("results", [])
            for item in results:
                median = _extract_metaculus_median(item)
                q = MetaculusQuestion(
                    question_id=item.get("id", 0),
                    title=item.get("title", ""),
                    community_median=median,
                    url=item.get("url", ""),
                )
                questions.append(q)
                fetched += 1
                if fetched >= limit:
                    break

            # Pagination: Metaculus returns a "next" link
            next_url = data.get("next")
            if next_url and fetched < limit:
                url = next_url
                params = {}  # already encoded in next_url
            else:
                break

        logger.info("Fetched %d Metaculus questions.", len(questions))
        return questions

    def match_to_kalshi(
        self,
        kalshi_markets: list[dict[str, Any]],
        metaculus_questions: list[MetaculusQuestion],
        min_confidence: float = 0.35,
    ) -> list[MetaculusMatch]:
        """
        Match Metaculus questions to Kalshi markets via fuzzy title similarity.

        Uses Python's :mod:`difflib.SequenceMatcher` (stdlib — no extra deps).
        For each Kalshi market, find the best-matching Metaculus question.
        Only emit a match if the similarity exceeds ``min_confidence``.

        :param kalshi_markets:      List of raw Kalshi market dicts.
        :param metaculus_questions: List of :class:`MetaculusQuestion`.
        :param min_confidence:      Minimum similarity to record a match.
        :returns:                   List of :class:`MetaculusMatch`.
        """
        if not metaculus_questions:
            return []

        meta_titles = [_normalise_title(q.title) for q in metaculus_questions]
        matches: list[MetaculusMatch] = []

        for market in kalshi_markets:
            ticker = market.get("ticker", "")
            kalshi_title = market.get("title", "")
            norm_kalshi = _normalise_title(kalshi_title)

            best_idx = -1
            best_score = 0.0

            for i, meta_norm in enumerate(meta_titles):
                score = difflib.SequenceMatcher(None, norm_kalshi, meta_norm).ratio()
                if score > best_score:
                    best_score = score
                    best_idx = i

            if best_idx >= 0 and best_score >= min_confidence:
                q = metaculus_questions[best_idx]
                matches.append(MetaculusMatch(
                    kalshi_ticker=ticker,
                    kalshi_title=kalshi_title,
                    metaculus_id=q.question_id,
                    metaculus_title=q.title,
                    metaculus_median=q.community_median,
                    confidence=round(best_score, 4),
                ))

        logger.info("Matched %d Kalshi markets to Metaculus questions.", len(matches))
        return matches

    def save_matches(self, matches: list[MetaculusMatch], path: Path | str) -> None:
        """Persist matches to a JSON file for offline use."""
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        data = [
            {
                "kalshi_ticker": m.kalshi_ticker,
                "kalshi_title": m.kalshi_title,
                "metaculus_id": m.metaculus_id,
                "metaculus_title": m.metaculus_title,
                "metaculus_median": m.metaculus_median,
                "confidence": m.confidence,
            }
            for m in matches
        ]
        out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("Saved %d Metaculus matches to %s.", len(matches), out_path)

    def load_matches(self, path: Path | str) -> list[MetaculusMatch]:
        """Load previously saved matches from a JSON file."""
        p = Path(path)
        if not p.exists():
            return []
        raw = json.loads(p.read_text(encoding="utf-8"))
        return [
            MetaculusMatch(
                kalshi_ticker=item["kalshi_ticker"],
                kalshi_title=item.get("kalshi_title", ""),
                metaculus_id=item["metaculus_id"],
                metaculus_title=item.get("metaculus_title", ""),
                metaculus_median=item.get("metaculus_median"),
                confidence=item.get("confidence", 0.0),
            )
            for item in raw
        ]


# ── Signal class ──────────────────────────────────────────────────────────────

class KalshiMetaculusSignal:
    """
    Computes Metaculus divergence features for a Kalshi market.

    :param matches_path: Path to ``matches.json`` saved by :class:`MetaculusFetcher`.
    :param min_confidence: Minimum match confidence to emit a non-null signal.
    """

    def __init__(
        self,
        matches_path: Path | str | None = None,
        min_confidence: float = METACULUS_MIN_CONFIDENCE,
    ) -> None:
        self._min_confidence = min_confidence
        self._matches: dict[str, MetaculusMatch] = {}

        if matches_path is not None:
            fetcher = MetaculusFetcher()
            loaded = fetcher.load_matches(matches_path)
            self._matches = {m.kalshi_ticker: m for m in loaded}

    def compute_for_market(
        self,
        ticker: str,
        kalshi_yes_price: float,
    ) -> dict[str, float]:
        """
        Compute Metaculus divergence features for a single Kalshi market.

        :param ticker:           Kalshi market ticker.
        :param kalshi_yes_price: Last yes-price (0–100 scale).
        :returns: Dict with keys:
                  ``metaculus_probability``  — Metaculus median (0–100 scale),
                  ``metaculus_divergence``   — metaculus_probability − kalshi_yes_price,
                  ``metaculus_confidence``   — match quality (0–1).
                  All NaN if no match or confidence below threshold.
        """
        nan = float("nan")
        null_result: dict[str, float] = {
            "metaculus_probability": nan,
            "metaculus_divergence": nan,
            "metaculus_confidence": nan,
        }

        match = self._matches.get(ticker)
        if match is None or match.confidence < self._min_confidence:
            return null_result
        if match.metaculus_median is None:
            return null_result

        # Metaculus medians are in [0, 1]; convert to 0–100 scale
        meta_prob_100 = round(match.metaculus_median * 100.0, 2)
        divergence = round(meta_prob_100 - kalshi_yes_price, 4)

        return {
            "metaculus_probability": meta_prob_100,
            "metaculus_divergence": divergence,
            "metaculus_confidence": match.confidence,
        }


# ── KalshiSignalFamily-compatible object ──────────────────────────────────────
#
# Direction: positive divergence means Metaculus > Kalshi price → BUY YES (+1).

KALSHI_METACULUS_DIVERGENCE = KalshiSignalFamily(
    name="kalshi_metaculus_divergence",
    feature_col="metaculus_divergence",
    direction=1,
    description=(
        "Metaculus divergence: when the Metaculus community median exceeds the "
        "Kalshi yes-price, the market may be underpriced relative to calibrated "
        "forecasters → BUY YES. Requires metaculus_divergence column pre-populated "
        "during ingest from matched Metaculus questions."
    ),
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_title(title: str) -> str:
    """Lower-case, strip punctuation and extra whitespace for fuzzy matching."""
    t = title.lower()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extract_metaculus_median(item: dict[str, Any]) -> float | None:
    """
    Extract the community median probability from a Metaculus API question object.

    Metaculus stores forecasts in nested dicts that have changed across API
    versions; this function tries multiple known locations.
    """
    # v2 API: community_prediction.full.q2 (median)
    cp = item.get("community_prediction")
    if cp:
        full = cp.get("full") or {}
        q2 = full.get("q2")
        if q2 is not None:
            try:
                return float(q2)
            except (TypeError, ValueError):
                pass
    # Older fallback: resolution or probability fields
    for key in ("probability", "community_prediction_at_access_close"):
        val = item.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return None


def augment_dataframe_with_metaculus(
    df: pd.DataFrame,
    *,
    ticker: str,
    matches_path: Path | str | None = None,
    min_confidence: float = METACULUS_MIN_CONFIDENCE,
) -> pd.DataFrame:
    """
    Add Metaculus feature columns to an existing feature DataFrame.

    Convenience wrapper for augmenting pre-built feature DataFrames.

    :param df:             Feature DataFrame (must have a ``close`` column).
    :param ticker:         Kalshi market ticker.
    :param matches_path:   Path to matches JSON file.
    :param min_confidence: Minimum match confidence to emit features.
    :returns:              The same DataFrame with added scalar columns.
    """
    if df.empty or "close" not in df.columns:
        return df

    signal = KalshiMetaculusSignal(
        matches_path=matches_path,
        min_confidence=min_confidence,
    )
    last_close = float(pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1])
    features = signal.compute_for_market(ticker, last_close)

    for col, val in features.items():
        df[col] = val
    return df
