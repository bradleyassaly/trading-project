"""Tests for ResolutionResolver and normalize_token_id."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from trading_platform.polymarket.resolution_resolver import (
    ResolutionResolver,
    normalize_token_id,
)


class TestNormalizeTokenId:
    def test_decimal_passthrough(self) -> None:
        assert normalize_token_id("1059799830661903") == "1059799830661903"

    def test_0x_hex_string(self) -> None:
        assert normalize_token_id("0xff") == "255"
        assert normalize_token_id("0x3c3db7") == "3947959"

    def test_hex_without_prefix(self) -> None:
        assert normalize_token_id("ff") == "255"
        assert normalize_token_id("3c3db7") == "3947959"

    def test_large_hex(self) -> None:
        # A real Polymarket condition_id
        result = normalize_token_id("0x4bfb")
        assert result == "19451"

    def test_whitespace_stripped(self) -> None:
        assert normalize_token_id("  123  ") == "123"
        assert normalize_token_id(" 0xff ") == "255"

    def test_empty_string(self) -> None:
        assert normalize_token_id("") == ""

    def test_non_hex_passthrough(self) -> None:
        # Contains non-hex chars — returned as-is
        assert normalize_token_id("hello-world") == "hello-world"


def _write_resolution_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = ["ticker", "resolution_price", "resolves_yes", "question"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class TestResolutionResolver:
    def test_resolve_decimal_ticker(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "12345", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q?"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.resolve("12345") == 100.0

    def test_resolve_hex_ticker_matches_decimal(self, tmp_path: Path) -> None:
        """Resolution CSV has decimal, query with hex → should match."""
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "255", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q?"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.resolve("0xff") == 0.0
        assert resolver.resolve("ff") == 0.0
        assert resolver.resolve("255") == 0.0

    def test_resolve_returns_none_for_unknown(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "12345", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q?"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.resolve("99999") is None

    def test_is_winner_buy_yes(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q?"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.is_winner("100", "BUY") == True  # YES won, bought YES
        assert resolver.is_winner("100", "SELL") == False  # YES won, sold YES

    def test_is_winner_buy_no(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "200", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q?"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.is_winner("200", "BUY") == False  # NO won, bought YES
        assert resolver.is_winner("200", "SELL") == True  # NO won, sold YES

    def test_is_winner_unknown_token(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [])
        resolver = ResolutionResolver(csv_path)
        assert resolver.is_winner("unknown", "BUY") is None

    def test_count_property(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "1", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q1"},
            {"ticker": "2", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q2"},
        ])
        resolver = ResolutionResolver(csv_path)
        assert resolver.count == 2

    def test_overlap_with(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "resolution.csv"
        _write_resolution_csv(csv_path, [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q1"},
            {"ticker": "200", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q2"},
        ])
        resolver = ResolutionResolver(csv_path)
        overlap, unmatched_res, unmatched_tok = resolver.overlap_with({"100", "300"})
        assert overlap == 1
        assert "200" in unmatched_res
        assert "300" in unmatched_tok

    def test_missing_csv(self, tmp_path: Path) -> None:
        resolver = ResolutionResolver(tmp_path / "missing.csv")
        assert resolver.count == 0
        assert resolver.resolve("anything") is None
