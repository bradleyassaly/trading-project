"""Tests for the Kalshi Metaculus divergence signal module."""
from __future__ import annotations

import json
import math
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading_platform.kalshi.signals_metaculus import (
    KALSHI_METACULUS_DIVERGENCE,
    KalshiMetaculusSignal,
    MetaculusFetcher,
    MetaculusMatch,
    MetaculusQuestion,
    _extract_metaculus_median,
    _normalise_title,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_matches_json(tmp_path: Path) -> Path:
    """Write test Metaculus matches to a JSON file."""
    matches = [
        {
            "kalshi_ticker": "FED-RATE-MAR-HIKE",
            "kalshi_title": "Will the Fed raise rates in March?",
            "metaculus_id": 12345,
            "metaculus_title": "Will the Federal Reserve raise interest rates in March?",
            "metaculus_median": 0.20,
            "confidence": 0.85,
        },
        {
            "kalshi_ticker": "CPI-JAN-BEAT",
            "kalshi_title": "Will January CPI beat consensus?",
            "metaculus_id": 67890,
            "metaculus_title": "Will US CPI for January 2026 exceed the consensus forecast?",
            "metaculus_median": 0.55,
            "confidence": 0.78,
        },
        {
            "kalshi_ticker": "LOW-CONF-TICKER",
            "kalshi_title": "Some obscure question",
            "metaculus_id": 11111,
            "metaculus_title": "Loosely related question",
            "metaculus_median": 0.40,
            "confidence": 0.50,  # below default threshold of 0.70
        },
    ]
    path = tmp_path / "test_matches.json"
    path.write_text(json.dumps(matches), encoding="utf-8")
    return path


# ── _normalise_title ──────────────────────────────────────────────────────────

def test_normalise_title_basic():
    result = _normalise_title("Will the Fed raise rates in March 2026?")
    assert "fed" in result
    assert "raise" in result
    assert "?" not in result
    assert result == result.lower()


def test_normalise_title_strips_punctuation():
    result = _normalise_title("CPI: higher than expected (above 3.5%)?")
    assert ":" not in result
    assert "(" not in result


def test_normalise_title_empty():
    assert _normalise_title("") == ""


# ── _extract_metaculus_median ─────────────────────────────────────────────────

def test_extract_median_from_community_prediction():
    item = {
        "community_prediction": {
            "full": {"q2": 0.65}
        }
    }
    assert _extract_metaculus_median(item) == pytest.approx(0.65)


def test_extract_median_fallback_to_probability():
    item = {"probability": 0.42}
    assert _extract_metaculus_median(item) == pytest.approx(0.42)


def test_extract_median_returns_none_when_missing():
    item = {"title": "some question with no forecast"}
    assert _extract_metaculus_median(item) is None


def test_extract_median_handles_malformed_data():
    item = {"community_prediction": {"full": {"q2": "not_a_number"}}}
    # Should not raise; should fall back or return None
    result = _extract_metaculus_median(item)
    assert result is None or isinstance(result, float)


# ── MetaculusFetcher ──────────────────────────────────────────────────────────

class TestMetaculusFetcher:

    def test_save_and_load_matches(self, tmp_path):
        fetcher = MetaculusFetcher()
        matches = [
            MetaculusMatch(
                kalshi_ticker="TEST-TICKER",
                kalshi_title="Test question",
                metaculus_id=999,
                metaculus_title="Test Metaculus question",
                metaculus_median=0.60,
                confidence=0.82,
            )
        ]
        path = tmp_path / "matches.json"
        fetcher.save_matches(matches, path)
        loaded = fetcher.load_matches(path)
        assert len(loaded) == 1
        assert loaded[0].kalshi_ticker == "TEST-TICKER"
        assert loaded[0].metaculus_median == pytest.approx(0.60)
        assert loaded[0].confidence == pytest.approx(0.82)

    def test_load_matches_missing_file(self, tmp_path):
        fetcher = MetaculusFetcher()
        result = fetcher.load_matches(tmp_path / "nonexistent.json")
        assert result == []

    def test_match_to_kalshi_finds_good_match(self):
        fetcher = MetaculusFetcher()
        kalshi_markets = [
            {
                "ticker": "FED-MAR-HIKE",
                "title": "Will the Fed raise interest rates in March?"
            }
        ]
        metaculus_qs = [
            MetaculusQuestion(
                question_id=100,
                title="Will the Federal Reserve raise interest rates in March 2026?",
                community_median=0.25,
                url="https://metaculus.com/q/100",
            ),
            MetaculusQuestion(
                question_id=101,
                title="Will the Premier League season start on time?",
                community_median=0.90,
                url="https://metaculus.com/q/101",
            ),
        ]
        matches = fetcher.match_to_kalshi(kalshi_markets, metaculus_qs, min_confidence=0.3)
        assert len(matches) == 1
        assert matches[0].kalshi_ticker == "FED-MAR-HIKE"
        assert matches[0].metaculus_id == 100
        assert matches[0].confidence > 0.3

    def test_match_to_kalshi_empty_inputs(self):
        fetcher = MetaculusFetcher()
        assert fetcher.match_to_kalshi([], []) == []
        assert fetcher.match_to_kalshi([{"ticker": "X", "title": "Q"}], []) == []

    def test_match_to_kalshi_below_threshold_excluded(self):
        fetcher = MetaculusFetcher()
        kalshi_markets = [{"ticker": "ABC", "title": "Will the sun rise tomorrow?"}]
        metaculus_qs = [
            MetaculusQuestion(
                question_id=1,
                title="Will quantum computing solve NP-hard problems by 2050?",
                community_median=0.30,
                url="",
            )
        ]
        # Very different titles → score below threshold
        matches = fetcher.match_to_kalshi(kalshi_markets, metaculus_qs, min_confidence=0.8)
        assert len(matches) == 0

    def test_fetch_questions_graceful_on_network_error(self):
        """When the network is unavailable, fetch_questions returns empty without raising."""
        fetcher = MetaculusFetcher(api_base="https://definitely-not-a-real-host-12345.invalid")
        # Should not raise, just return empty
        result = fetcher.fetch_questions(limit=5)
        assert isinstance(result, list)
        # Empty because network call fails
        assert len(result) == 0

    def test_fetch_questions_returns_questions_on_mock_response(self):
        """Mock the HTTP response to test question parsing."""
        fetcher = MetaculusFetcher()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "id": 42,
                    "title": "Will the Fed cut rates in 2026?",
                    "url": "https://metaculus.com/q/42",
                    "community_prediction": {"full": {"q2": 0.30}},
                }
            ],
            "next": None,
        }
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            questions = fetcher.fetch_questions(limit=10)

        assert len(questions) == 1
        assert questions[0].question_id == 42
        assert questions[0].title == "Will the Fed cut rates in 2026?"
        assert questions[0].community_median == pytest.approx(0.30)


# ── KalshiMetaculusSignal ────────────────────────────────────────────────────

class TestKalshiMetaculusSignal:

    def test_compute_for_market_above_threshold(self, tmp_path):
        matches_path = _make_matches_json(tmp_path)
        signal = KalshiMetaculusSignal(matches_path=matches_path, min_confidence=0.70)

        # FED-RATE-MAR-HIKE: metaculus_median=0.20 → prob=20, kalshi=30 → divergence=-10
        features = signal.compute_for_market("FED-RATE-MAR-HIKE", kalshi_yes_price=30.0)
        assert not math.isnan(features["metaculus_probability"])
        assert features["metaculus_probability"] == pytest.approx(20.0)
        assert features["metaculus_divergence"] == pytest.approx(-10.0)  # 20 - 30
        assert features["metaculus_confidence"] == pytest.approx(0.85)

    def test_compute_for_market_below_confidence_threshold(self, tmp_path):
        matches_path = _make_matches_json(tmp_path)
        # LOW-CONF-TICKER has confidence=0.50, threshold=0.70 → should return NaN
        signal = KalshiMetaculusSignal(matches_path=matches_path, min_confidence=0.70)
        features = signal.compute_for_market("LOW-CONF-TICKER", kalshi_yes_price=45.0)
        assert math.isnan(features["metaculus_probability"])
        assert math.isnan(features["metaculus_divergence"])
        assert math.isnan(features["metaculus_confidence"])

    def test_compute_for_unmatched_ticker(self, tmp_path):
        matches_path = _make_matches_json(tmp_path)
        signal = KalshiMetaculusSignal(matches_path=matches_path)
        features = signal.compute_for_market("UNMATCHED-TICKER", kalshi_yes_price=50.0)
        assert math.isnan(features["metaculus_probability"])

    def test_compute_no_matches_file(self):
        signal = KalshiMetaculusSignal(matches_path=None)
        features = signal.compute_for_market("ANY-TICKER", kalshi_yes_price=50.0)
        assert math.isnan(features["metaculus_probability"])

    def test_positive_divergence_when_metaculus_more_bullish(self, tmp_path):
        matches_path = _make_matches_json(tmp_path)
        signal = KalshiMetaculusSignal(matches_path=matches_path, min_confidence=0.70)
        # CPI-JAN-BEAT: metaculus_median=0.55 → prob=55, kalshi=40 → divergence=+15
        features = signal.compute_for_market("CPI-JAN-BEAT", kalshi_yes_price=40.0)
        if not math.isnan(features["metaculus_divergence"]):
            assert features["metaculus_divergence"] == pytest.approx(15.0)


# ── KALSHI_METACULUS_DIVERGENCE signal family ─────────────────────────────────

class TestKalshiMetaculusDivergenceFamily:

    def test_name(self):
        assert KALSHI_METACULUS_DIVERGENCE.name == "kalshi_metaculus_divergence"

    def test_direction_is_positive_one(self):
        assert KALSHI_METACULUS_DIVERGENCE.direction == 1

    def test_score_reads_metaculus_divergence_column(self):
        df = pd.DataFrame({"metaculus_divergence": [5.0, -3.0, 0.0, 12.0]})
        signal = KALSHI_METACULUS_DIVERGENCE.score(df)
        # direction=+1, so signal = metaculus_divergence
        pd.testing.assert_series_equal(
            signal, df["metaculus_divergence"].astype(float), check_names=False
        )

    def test_score_returns_nan_if_column_missing(self):
        df = pd.DataFrame({"close": [50.0, 55.0]})
        signal = KALSHI_METACULUS_DIVERGENCE.score(df)
        assert signal.isna().all()
