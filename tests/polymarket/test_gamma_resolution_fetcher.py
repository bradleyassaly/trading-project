"""Tests for GammaResolutionFetcher."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trading_platform.polymarket.gamma_resolution_fetcher import GammaResolutionFetcher


def _gamma_market(
    market_id: str = "m1",
    outcomes: list[str] | None = None,
    outcome_prices: list[str] | None = None,
    clob_token_ids: list[str] | None = None,
) -> dict:
    return {
        "id": market_id,
        "question": f"Will {market_id} happen?",
        "outcomes": json.dumps(outcomes or ["Yes", "No"]),
        "outcomePrices": json.dumps(outcome_prices or ["1", "0"]),
        "clobTokenIds": json.dumps(clob_token_ids or ["tok1", "tok2"]),
        "endDateIso": "2026-04-01",
        "volume": 5000,
    }


class TestGammaResolutionFetcher:
    def test_fetch_by_token_ids(self) -> None:
        fetcher = GammaResolutionFetcher()
        mock = MagicMock()
        resp = MagicMock()
        resp.json.return_value = [_gamma_market(outcome_prices=["1", "0"], clob_token_ids=["tok1"])]
        resp.raise_for_status = MagicMock()
        mock.get.return_value = resp
        mock.headers = MagicMock()
        fetcher._session = mock
        fetcher._sleep = 0

        results = fetcher.fetch_resolved(token_ids=["tok1"])
        assert len(results) >= 1

    def test_yes_resolution(self) -> None:
        rp = GammaResolutionFetcher._extract_resolution(
            _gamma_market(outcomes=["Yes", "No"], outcome_prices=["1", "0"])
        )
        assert rp == 100.0

    def test_no_resolution(self) -> None:
        rp = GammaResolutionFetcher._extract_resolution(
            _gamma_market(outcomes=["Yes", "No"], outcome_prices=["0", "1"])
        )
        assert rp == 0.0

    def test_unresolved(self) -> None:
        rp = GammaResolutionFetcher._extract_resolution(
            _gamma_market(outcomes=["Yes", "No"], outcome_prices=["0.5", "0.5"])
        )
        assert rp is None

    def test_fetch_all_resolved_writes_csv(self, tmp_path: Path) -> None:
        fetcher = GammaResolutionFetcher()
        mock = MagicMock()
        resp = MagicMock()
        resp.json.return_value = [
            _gamma_market("m1", outcome_prices=["1", "0"], clob_token_ids=["t1"]),
            _gamma_market("m2", outcome_prices=["0", "1"], clob_token_ids=["t2"]),
        ]
        resp.raise_for_status = MagicMock()
        mock.get.return_value = resp
        mock.headers = MagicMock()
        fetcher._session = mock
        fetcher._sleep = 0

        output = tmp_path / "res.csv"
        count = fetcher.fetch_all_resolved(output, since_days=30)
        assert count == 2
        assert output.exists()

    def test_canonical_normalization(self) -> None:
        """Token IDs should be normalized to canonical decimal form."""
        from trading_platform.polymarket.resolution_resolver import normalize_token_id
        assert normalize_token_id("0xff") == "255"
