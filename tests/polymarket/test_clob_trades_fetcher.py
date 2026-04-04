"""Tests for CLOB trades fetcher and Goldsky orderbook fetcher."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trading_platform.polymarket.clob_trades_fetcher import ClobTradesFetcher
from trading_platform.polymarket.graph_orderbook import GraphOrderbookFetcher


class TestClobTradesFetcher:
    def test_fetch_market_trades(self, tmp_path: Path) -> None:
        trades_response = [
            {"match_time": "2026-04-01T12:00:00Z", "maker_address": "0xabc",
             "side": "BUY", "price": "0.65", "size": "100"},
            {"match_time": "2026-04-01T13:00:00Z", "maker_address": "0xdef",
             "side": "SELL", "price": "0.60", "size": "50"},
        ]

        fetcher = ClobTradesFetcher()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = trades_response
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp
        mock_session.headers = MagicMock()
        fetcher._session = mock_session
        fetcher._sleep = 0

        count = fetcher.fetch_market_trades("tok-1", tmp_path / "trades")

        assert count == 2
        csv_files = list((tmp_path / "trades").glob("*.csv"))
        assert len(csv_files) == 1

        with csv_files[0].open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert rows[0]["side"] == "BUY"

    def test_empty_response(self, tmp_path: Path) -> None:
        fetcher = ClobTradesFetcher()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = []
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp
        mock_session.headers = MagicMock()
        fetcher._session = mock_session
        fetcher._sleep = 0

        count = fetcher.fetch_market_trades("tok-1", tmp_path / "trades")
        assert count == 0


class TestGraphOrderbookFetcher:
    def test_fetch_orderbooks(self) -> None:
        fetcher = GraphOrderbookFetcher()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": {
                "tokenOrderbooks": [
                    {"tokenId": "tok-1", "bestBid": "0.60", "bestAsk": "0.62",
                     "totalBidDepth": "1000", "totalAskDepth": "800",
                     "lastTradePrice": "0.61"},
                ]
            }
        }
        resp.raise_for_status = MagicMock()
        mock_session.post.return_value = resp
        mock_session.headers = MagicMock()
        fetcher._session = mock_session

        df = fetcher.fetch_orderbooks(["tok-1"])

        assert len(df) == 1
        row = df.iloc[0]
        assert row["best_bid"] == 0.60
        assert row["best_ask"] == 0.62
        assert row["spread"] == pytest.approx(0.02)
        assert row["orderbook_imbalance"] > 0  # bid > ask depth

    def test_empty_response(self) -> None:
        fetcher = GraphOrderbookFetcher()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {"data": {"tokenOrderbooks": []}}
        resp.raise_for_status = MagicMock()
        mock_session.post.return_value = resp
        mock_session.headers = MagicMock()
        fetcher._session = mock_session

        df = fetcher.fetch_orderbooks(["tok-1"])
        assert len(df) == 0

    def test_get_schema(self) -> None:
        fetcher = GraphOrderbookFetcher()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": {"__schema": {"queryType": {"fields": [
                {"name": "markets", "description": ""},
                {"name": "tokenOrderbooks", "description": ""},
            ]}}}
        }
        resp.raise_for_status = MagicMock()
        mock_session.post.return_value = resp
        mock_session.headers = MagicMock()
        fetcher._session = mock_session

        fields = fetcher.get_schema()
        assert "markets" in fields
        assert "tokenOrderbooks" in fields

    def test_empty_token_list(self) -> None:
        fetcher = GraphOrderbookFetcher()
        df = fetcher.fetch_orderbooks([])
        assert df.empty
