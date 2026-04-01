"""
Kalshi base rate signal.

Markets on recurring structured events (Fed decisions, CPI, NFP, weather,
legislative outcomes) have well-documented historical resolution frequencies
that are publicly available.  Traders systematically anchor to recent
narrative and discount these priors, creating a persistent edge for the
patient, calibrated participant.

Architecture note
-----------------
This module exposes two interfaces that work together:

1. ``KalshiBaseRateSignal.compute_for_market()`` — called during ingest/feature
   generation to produce a dict of scalar features for a given market.  These
   scalars are broadcast as constant columns in the feature parquet via
   ``build_kalshi_features(extra_scalar_features=...)``.

2. ``KALSHI_BASE_RATE`` — a ``KalshiSignalFamily``-compatible object that the
   backtester can use to score markets.  It reads the ``base_rate_edge`` column
   from the feature DataFrame, which must have been populated via step 1.
   The signal is *unusable* in the backtester unless the ingest pipeline ran
   first and populated that column.

Feature columns produced
------------------------
``base_rate_prior``       Historical resolution frequency (0–100 scale).
``base_rate_edge``        market_yes_price − base_rate_prior.
                          Positive  →  market overpriced vs history (sell YES).
                          Negative  →  market underpriced vs history (buy YES).
``base_rate_confidence``  Category match quality (0.0–1.0).  Low values mean
                          the keyword matching was uncertain.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.kalshi.signals import KalshiSignalFamily

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = Path("data/kalshi/base_rates/base_rate_db.json")

# Minimum keyword overlap score to treat a match as valid.
_CONFIDENCE_THRESHOLD = 0.15


# ── Database loading ──────────────────────────────────────────────────────────

def load_base_rate_db(path: Path | str | None = None) -> dict[str, Any]:
    """
    Load the base rate database JSON.

    :param path: Path to ``base_rate_db.json``.  Defaults to
                 ``data/kalshi/base_rates/base_rate_db.json``.
    :raises FileNotFoundError: If the file does not exist.
    """
    db_path = Path(path) if path else _DEFAULT_DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(
            f"Base rate database not found at {db_path}. "
            "Ensure the file exists at data/kalshi/base_rates/base_rate_db.json."
        )
    with open(db_path, encoding="utf-8") as f:
        db = json.load(f)
    return db.get("categories", db)


# ── Matching logic ────────────────────────────────────────────────────────────

def _tokenise(text: str) -> set[str]:
    """Lower-case word tokens, stripping punctuation."""
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _keyword_score(title_tokens: set[str], series_tokens: set[str], entry: dict[str, Any]) -> float:
    """
    Score a category entry against a market's title and series ticker tokens.

    Returns a confidence in [0, 1].  Higher means better match.

    Scoring:
    - Each keyword phrase from the entry's ``keywords`` list that overlaps with
      title tokens contributes positively.
    - Series patterns (upper-cased) that appear in the series ticker are a
      strong bonus.
    - Score is normalised by the number of keywords in the entry.
    """
    keywords: list[str] = entry.get("keywords", [])
    series_patterns: list[str] = entry.get("series_patterns", [])

    if not keywords and not series_patterns:
        return 0.0

    match_count = 0.0
    for kw in keywords:
        kw_tokens = _tokenise(kw)
        overlap = len(kw_tokens & title_tokens)
        if overlap > 0:
            match_count += overlap / len(kw_tokens)

    series_bonus = 0.0
    for pattern in series_patterns:
        if pattern.upper() in series_tokens:
            series_bonus += 1.0

    total_possible = max(len(keywords), 1) + len(series_patterns)
    raw = (match_count + series_bonus) / total_possible
    return min(raw, 1.0)


# ── Main signal class ─────────────────────────────────────────────────────────

@dataclass
class BaseRateMatch:
    category: str
    prior: float        # 0–100 scale
    confidence: float   # 0.0–1.0


class KalshiBaseRateSignal:
    """
    Matches a Kalshi market to a historical base rate category and computes
    the edge between market price and historical frequency.

    :param db_path: Path to ``base_rate_db.json``.  If None uses the default.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        try:
            self._db = load_base_rate_db(db_path)
        except FileNotFoundError:
            logger.warning("Base rate database not found — base rate features will be null.")
            self._db = {}

    def match(self, title: str, series_ticker: str) -> BaseRateMatch | None:
        """
        Find the best matching base rate category for a market.

        :param title:         Market title string.
        :param series_ticker: Market series ticker (e.g. "FED-RATE").
        :returns: :class:`BaseRateMatch` if a category exceeds the confidence
                  threshold, else None.
        """
        if not self._db:
            return None

        title_tokens = _tokenise(title)
        series_tokens = _tokenise(series_ticker)

        best_cat: str | None = None
        best_score = 0.0
        best_entry: dict[str, Any] | None = None

        for cat_name, entry in self._db.items():
            score = _keyword_score(title_tokens, series_tokens, entry)
            if score > best_score:
                best_score = score
                best_cat = cat_name
                best_entry = entry

        if best_score < _CONFIDENCE_THRESHOLD or best_cat is None or best_entry is None:
            return None

        prior = float(best_entry.get("prior", 50.0))
        return BaseRateMatch(category=best_cat, prior=prior, confidence=best_score)

    def compute_for_market(
        self,
        title: str,
        series_ticker: str,
        market_yes_price: float,
    ) -> dict[str, float]:
        """
        Compute base rate features for a single market.

        :param title:             Market title string.
        :param series_ticker:     Market series ticker.
        :param market_yes_price:  Current/last yes-price (0–100 scale).
        :returns: Dict with keys:
                  ``base_rate_prior``      — historical frequency (0–100),
                  ``base_rate_edge``       — market_yes_price − prior,
                  ``base_rate_confidence`` — match quality (0–1).
                  All values are NaN if no category match found.
        """
        nan = float("nan")
        match = self.match(title, series_ticker)
        if match is None:
            return {
                "base_rate_prior": nan,
                "base_rate_edge": nan,
                "base_rate_confidence": nan,
            }

        edge = market_yes_price - match.prior
        return {
            "base_rate_prior": match.prior,
            "base_rate_edge": round(edge, 4),
            "base_rate_confidence": round(match.confidence, 4),
        }


# ── KalshiSignalFamily-compatible object ──────────────────────────────────────
#
# The backtester reads feature columns via KalshiSignalFamily.score().
# The base_rate_edge column must already exist in the feature parquet
# (populated during ingest via compute_for_market + extra_scalar_features).
# Direction: positive edge means market is overpriced vs history → SELL YES
#            so direction = -1 (higher edge = worse bet on YES).

KALSHI_BASE_RATE = KalshiSignalFamily(
    name="kalshi_base_rate",
    feature_col="base_rate_edge",
    direction=-1,
    description=(
        "Base rate mean-reversion: compares market yes-price to the historical "
        "resolution frequency for this event category. "
        "Positive base_rate_edge → market overpriced vs history → SELL YES. "
        "Negative base_rate_edge → market underpriced vs history → BUY YES. "
        "Signal requires base_rate_edge column to be pre-populated during ingest."
    ),
)


def compute_base_rate_features_for_dataframe(
    df: pd.DataFrame,
    *,
    title: str,
    series_ticker: str,
    db_path: Path | str | None = None,
) -> pd.DataFrame:
    """
    Add base rate feature columns to an existing feature DataFrame in place.

    Convenience wrapper used when augmenting already-built feature DataFrames.

    :param df:            Feature DataFrame to augment (must have a ``close`` column).
    :param title:         Market title for category matching.
    :param series_ticker: Series ticker for category matching.
    :param db_path:       Optional override for base rate DB path.
    :returns:             The same DataFrame with added scalar columns.
    """
    if df.empty or "close" not in df.columns:
        return df

    signal = KalshiBaseRateSignal(db_path)
    last_close = float(pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1])
    features = signal.compute_for_market(title, series_ticker, last_close)

    for col, val in features.items():
        df[col] = val
    return df
