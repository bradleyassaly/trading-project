"""Tests for Polymarket Data API fetcher."""
from __future__ import annotations

import csv
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trading_platform.polymarket.data_api_fetcher import (
    PolymarketDataApiFetcher,
    _trade_to_row,
)


def _mock_trade(ts: int = 1712000000, wallet: str = "0xabc", price: float = 0.65) -> dict:
    return {
        "proxyWallet": wallet,
        "side": "BUY",
        "asset": "tok-1",
        "conditionId": "0xcond1",
        "size": 100.0,
        "price": price,
        "timestamp": ts,
        "title": "Will X happen?",
        "outcome": "Yes",
        "outcomeIndex": 0,
    }


class TestTradeToRow:
    def test_converts_data_api_format(self) -> None:
        row = _trade_to_row(_mock_trade())
        assert row is not None
        assert row["wallet"] == "0xabc"
        assert row["token_id"] == "tok-1"
        assert row["condition_id"] == "0xcond1"
        assert row["side"] == "BUY"
        assert float(row["price"]) == 0.65
        assert float(row["total_usdc"]) == pytest.approx(65.0)

    def test_handles_missing_fields(self) -> None:
        row = _trade_to_row({"price": "0.5", "size": "10"})
        assert row is not None
        assert row["wallet"] == ""


class TestDataApiFetcher:
    def _make_fetcher(self, trades: list[dict]) -> PolymarketDataApiFetcher:
        fetcher = PolymarketDataApiFetcher()
        mock_session = MagicMock()
        # Return trades on first call, empty on subsequent calls (stops pagination)
        first_resp = MagicMock()
        first_resp.json.return_value = trades
        first_resp.raise_for_status = MagicMock()
        empty_resp = MagicMock()
        empty_resp.json.return_value = []
        empty_resp.raise_for_status = MagicMock()
        mock_session.get.side_effect = [first_resp, empty_resp, empty_resp]
        mock_session.headers = MagicMock()
        fetcher._session = mock_session
        fetcher._sleep = 0
        return fetcher

    def test_fetch_recent_trades(self, tmp_path: Path) -> None:
        now = int(time.time())
        trades = [_mock_trade(ts=now - i * 60) for i in range(5)]
        fetcher = self._make_fetcher(trades)

        count = fetcher.fetch_recent_trades(tmp_path / "out", hours_back=1)
        assert count == 5

        csv_files = list((tmp_path / "out").glob("recent_*.csv"))
        assert len(csv_files) == 1
        with csv_files[0].open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 5

    def test_fetch_market_trades(self, tmp_path: Path) -> None:
        trades = [_mock_trade() for _ in range(3)]
        fetcher = self._make_fetcher(trades)

        count = fetcher.fetch_market_trades("0xcond1", tmp_path / "out")
        assert count == 3

    def test_fetch_wallet_history(self, tmp_path: Path) -> None:
        trades = [_mock_trade(wallet="0xsmartmoney") for _ in range(4)]
        fetcher = self._make_fetcher(trades)

        count = fetcher.fetch_wallet_history("0xsmartmoney", tmp_path / "out")
        assert count == 4

    def test_empty_response(self, tmp_path: Path) -> None:
        fetcher = self._make_fetcher([])
        count = fetcher.fetch_recent_trades(tmp_path / "out", hours_back=1)
        assert count == 0


class TestWalletProfilerFormatDetection:
    def test_data_api_format_accepted(self, tmp_path: Path) -> None:
        """WalletProfiler should accept Data API CSV format (proxyWallet, asset)."""
        from trading_platform.polymarket.wallet_profiler import WalletProfiler

        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        # Write data API format CSV
        with trades_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["proxyWallet", "asset", "side", "total_usdc", "timestamp"])
            writer.writeheader()
            for i in range(10):
                writer.writerow({
                    "proxyWallet": "wallet_A",
                    "asset": "mkt1xxxxxxxxxxxx",
                    "side": "BUY",
                    "total_usdc": "100",
                    "timestamp": f"2026-03-{20+i}T12:00:00+00:00",
                })

        with res_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["ticker", "resolution_price", "resolves_yes", "close_time"])
            writer.writeheader()
            writer.writerow({
                "ticker": "mkt1xxxxxxxxxxxx",
                "resolution_price": "100.0",
                "resolves_yes": "True",
                "close_time": "2026-04-03T00:00:00+00:00",
            })

        result = WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5,
        )
        assert result.wallets_with_resolved_trades == 1
