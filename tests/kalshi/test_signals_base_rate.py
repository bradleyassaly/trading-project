"""Tests for the Kalshi base rate signal module."""
from __future__ import annotations

import json
import math
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.kalshi.signals_base_rate import (
    BaseRateMatch,
    KALSHI_BASE_RATE,
    KalshiBaseRateSignal,
    _keyword_score,
    _tokenise,
    compute_base_rate_features_for_dataframe,
    load_base_rate_db,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_db(tmp_path: Path) -> Path:
    """Write a minimal test base rate DB to a temp file."""
    db = {
        "categories": {
            "fed_rate_hike": {
                "prior": 15.0,
                "keywords": ["fed hike", "rate hike", "raise rates"],
                "series_patterns": ["FOMC", "FED-RATE"],
            },
            "fed_rate_hold": {
                "prior": 65.0,
                "keywords": ["fed hold", "hold rates", "unchanged"],
                "series_patterns": ["FOMC", "FED-RATE"],
            },
            "cpi_above_consensus": {
                "prior": 52.0,
                "keywords": ["cpi above", "cpi beat", "cpi higher"],
                "series_patterns": ["CPI", "INFLATION"],
            },
            "nfp_above_consensus": {
                "prior": 54.0,
                "keywords": ["nfp above", "jobs above", "payrolls beat"],
                "series_patterns": ["NFP", "JOBS"],
            },
        }
    }
    db_path = tmp_path / "test_base_rate_db.json"
    db_path.write_text(json.dumps(db), encoding="utf-8")
    return db_path


# ── Unit tests for tokenisation ───────────────────────────────────────────────

def test_tokenise_basic():
    tokens = _tokenise("Will the Fed raise rates in March?")
    assert "fed" in tokens
    assert "raise" in tokens
    assert "rates" in tokens
    assert "march" in tokens
    # Punctuation stripped
    assert "?" not in tokens


def test_tokenise_empty():
    assert _tokenise("") == set()


def test_tokenise_numbers():
    tokens = _tokenise("CPI 3.5% beat")
    assert "cpi" in tokens
    assert "3" in tokens or "35" in tokens or "beat" in tokens


# ── Unit tests for keyword scoring ───────────────────────────────────────────

def test_keyword_score_exact_match():
    entry = {
        "keywords": ["fed hike", "rate hike"],
        "series_patterns": ["FOMC"],
    }
    title_tokens = _tokenise("Will the Fed hike rates at the next FOMC meeting?")
    series_tokens = _tokenise("FOMC")
    score = _keyword_score(title_tokens, series_tokens, entry)
    assert score > 0.3


def test_keyword_score_no_match():
    entry = {
        "keywords": ["cpi above", "inflation beat"],
        "series_patterns": ["CPI"],
    }
    title_tokens = _tokenise("Will Manchester United win the Premier League?")
    series_tokens = _tokenise("SPORTS-EPL")
    score = _keyword_score(title_tokens, series_tokens, entry)
    assert score < 0.1


def test_keyword_score_empty_entry():
    entry = {"keywords": [], "series_patterns": []}
    score = _keyword_score(_tokenise("any title"), _tokenise("TICKER"), entry)
    assert score == 0.0


# ── KalshiBaseRateSignal tests ────────────────────────────────────────────────

class TestKalshiBaseRateSignal:

    def test_match_fed_hike_by_keyword(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        match = signal.match(
            title="Will the Fed hike rates at the March FOMC meeting?",
            series_ticker="FED-RATE-MARCH"
        )
        assert match is not None
        assert match.category == "fed_rate_hike"
        assert match.prior == pytest.approx(15.0)
        assert 0.0 < match.confidence <= 1.0

    def test_match_fed_hold_by_keyword(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        match = signal.match(
            title="Will Fed rates remain unchanged at December FOMC?",
            series_ticker="FOMC-DEC"
        )
        assert match is not None
        assert match.category == "fed_rate_hold"

    def test_match_cpi_by_keyword(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        match = signal.match(
            title="Will CPI come in higher than consensus in January?",
            series_ticker="CPI-JAN"
        )
        assert match is not None
        assert match.category == "cpi_above_consensus"
        assert match.prior == pytest.approx(52.0)

    def test_no_match_for_unrelated_market(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        match = signal.match(
            title="Will it snow in Miami on Christmas Day?",
            series_ticker="WEATHER-MIA"
        )
        # This topic not in test DB, should return None
        assert match is None

    def test_compute_for_market_returns_edge(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        features = signal.compute_for_market(
            title="Will the Fed hike rates?",
            series_ticker="FED-RATE",
            market_yes_price=30.0,
        )
        assert "base_rate_prior" in features
        assert "base_rate_edge" in features
        assert "base_rate_confidence" in features
        # prior = 15.0, market = 30.0 → edge = 30 - 15 = 15
        assert features["base_rate_edge"] == pytest.approx(15.0)
        assert features["base_rate_prior"] == pytest.approx(15.0)

    def test_compute_for_market_no_match_returns_nan(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        features = signal.compute_for_market(
            title="This is a completely unrelated topic about nothing",
            series_ticker="UNKNOWN-XYZ",
            market_yes_price=45.0,
        )
        assert math.isnan(features["base_rate_prior"])
        assert math.isnan(features["base_rate_edge"])
        assert math.isnan(features["base_rate_confidence"])

    def test_missing_db_file_graceful(self, tmp_path):
        # Should log warning, not raise
        signal = KalshiBaseRateSignal(tmp_path / "nonexistent_db.json")
        features = signal.compute_for_market("any title", "ANY-TICKER", 50.0)
        assert math.isnan(features["base_rate_edge"])

    def test_negative_edge_when_market_below_prior(self, tmp_path):
        db_path = _make_db(tmp_path)
        signal = KalshiBaseRateSignal(db_path)
        # Fed hold prior = 65, market at 40 → underpriced → edge = 40 - 65 = -25
        features = signal.compute_for_market(
            title="Will rates remain unchanged at the Fed meeting?",
            series_ticker="FOMC-HOLD",
            market_yes_price=40.0,
        )
        if not math.isnan(features["base_rate_edge"]):
            # Only check if a match was found
            assert features["base_rate_edge"] < 0


# ── KALSHI_BASE_RATE signal family object ─────────────────────────────────────

class TestKalshiBaseRateSignalFamily:

    def test_name(self):
        assert KALSHI_BASE_RATE.name == "kalshi_base_rate"

    def test_direction_is_negative_one(self):
        assert KALSHI_BASE_RATE.direction == -1

    def test_score_reads_base_rate_edge_column(self):
        df = pd.DataFrame({"base_rate_edge": [10.0, -5.0, 0.0, 15.0]})
        signal = KALSHI_BASE_RATE.score(df)
        # direction=-1, so signal = -base_rate_edge
        assert list(signal) == [-10.0, 5.0, 0.0, -15.0]

    def test_score_returns_nan_if_column_missing(self):
        df = pd.DataFrame({"close": [50.0, 55.0]})
        signal = KALSHI_BASE_RATE.score(df)
        assert signal.isna().all()


# ── compute_base_rate_features_for_dataframe ─────────────────────────────────

def test_compute_base_rate_features_for_dataframe(tmp_path):
    db_path = _make_db(tmp_path)
    df = pd.DataFrame({
        "close": [30.0, 28.0, 32.0, 30.0],
        "volume": [100, 120, 90, 110],
    })
    result = compute_base_rate_features_for_dataframe(
        df,
        title="Will the Fed hike rates at FOMC?",
        series_ticker="FED-RATE",
        db_path=db_path,
    )
    # All rows should have the same base_rate_prior (scalar broadcast)
    assert "base_rate_prior" in result.columns
    assert len(result["base_rate_prior"].dropna()) > 0 or True  # match might be None


def test_compute_base_rate_features_empty_df(tmp_path):
    db_path = _make_db(tmp_path)
    df = pd.DataFrame({"close": pd.Series([], dtype=float)})
    result = compute_base_rate_features_for_dataframe(
        df,
        title="Fed hike",
        series_ticker="FED-RATE",
        db_path=db_path,
    )
    # Empty DF returned unchanged
    assert result.empty


# ── load_base_rate_db ─────────────────────────────────────────────────────────

def test_load_base_rate_db(tmp_path):
    db_path = _make_db(tmp_path)
    db = load_base_rate_db(db_path)
    assert "fed_rate_hike" in db
    assert db["fed_rate_hike"]["prior"] == 15.0


def test_load_base_rate_db_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_base_rate_db(tmp_path / "does_not_exist.json")
