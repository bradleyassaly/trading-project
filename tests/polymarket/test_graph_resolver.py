"""Tests for Graph resolver and data-API-based wallet profiling."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trading_platform.polymarket.graph_resolver import GraphResolver
from trading_platform.polymarket.wallet_profiler import WalletProfiler


class TestGraphResolver:
    def test_get_resolutions(self) -> None:
        resolver = GraphResolver()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": {
                "conditions": [
                    {"id": "0xabc", "payouts": ["1", "0"]},  # YES won
                    {"id": "0xdef", "payouts": ["0", "1"]},  # NO won
                    {"id": "0xghi", "payouts": None},         # unresolved
                ]
            }
        }
        resp.raise_for_status = MagicMock()
        mock_session.post.return_value = resp
        mock_session.headers = MagicMock()
        resolver._session = mock_session
        resolver._sleep = 0

        results = resolver.get_resolutions(["0xabc", "0xdef", "0xghi"])
        assert results["0xabc"] == True   # YES won
        assert results["0xdef"] == False  # NO won
        assert "0xghi" not in results     # unresolved

    def test_empty_input(self) -> None:
        resolver = GraphResolver()
        assert resolver.get_resolutions([]) == {}

    def test_get_token_conditions(self) -> None:
        resolver = GraphResolver()
        mock_session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "data": {
                "tokenIdConditions": [
                    {"id": "tok1", "condition": {"id": "0xabc"}},
                    {"id": "tok2", "condition": {"id": "0xdef"}},
                ]
            }
        }
        resp.raise_for_status = MagicMock()
        mock_session.post.return_value = resp
        mock_session.headers = MagicMock()
        resolver._session = mock_session
        resolver._sleep = 0

        results = resolver.get_token_conditions(["tok1", "tok2"])
        assert results["tok1"] == "0xabc"
        assert results["tok2"] == "0xdef"


def _write_trade_csv(path: Path, trades: list[dict]) -> None:
    fieldnames = ["timestamp", "wallet", "token_id", "condition_id",
                   "side", "price", "total_usdc", "outcome", "title"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)


class TestWalletProfilerDataApi:
    def test_build_profiles_from_data_api(self, tmp_path: Path) -> None:
        trades_dir = tmp_path / "trades"
        trades_dir.mkdir()

        # Wallet A: buys YES on market that resolved YES → wins
        # Wallet B: buys YES on market that resolved NO → loses
        trades = [
            {"timestamp": f"17120000{i}0", "wallet": "walletA", "token_id": "t1",
             "condition_id": "0xwin", "side": "BUY", "price": "0.65",
             "total_usdc": "100", "outcome": "Yes", "title": "Test"}
            for i in range(10)
        ] + [
            {"timestamp": f"17120000{i}0", "wallet": "walletB", "token_id": "t2",
             "condition_id": "0xwin", "side": "BUY", "price": "0.65",
             "total_usdc": "100", "outcome": "No", "title": "Test"}
            for i in range(10)
        ]
        _write_trade_csv(trades_dir / "market1.csv", trades)

        # Mock GraphResolver
        from unittest.mock import patch
        mock_resolutions = {"0xwin": True}  # YES won

        profiler = WalletProfiler()
        # Patch GraphResolver at its source module so the lazy import picks it up
        with patch("trading_platform.polymarket.graph_resolver.GraphResolver") as MockCls:
            MockCls.return_value.get_resolutions.return_value = mock_resolutions
            result = profiler.build_profiles_from_data_api(trades_dir, tmp_path / "profiles.parquet")

        assert result.wallets_with_resolved_trades == 2
        df = pd.read_parquet(tmp_path / "profiles.parquet")

        wallet_a = df[df["wallet"] == "walletA"].iloc[0]
        assert wallet_a["win_rate"] == 1.0  # YES bought, YES won
        assert wallet_a["smart_money"] == True

        wallet_b = df[df["wallet"] == "walletB"].iloc[0]
        assert wallet_b["win_rate"] == 0.0  # NO bought, YES won → loss

    def test_empty_trades_dir(self, tmp_path: Path) -> None:
        trades_dir = tmp_path / "empty"
        trades_dir.mkdir()
        profiler = WalletProfiler()
        result = profiler.build_profiles_from_data_api(trades_dir, tmp_path / "profiles.parquet")
        assert result.wallets_analyzed == 0
