"""Tests for OrderFlowCollector."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trading_platform.polymarket.orderflow_collector import OrderFlowCollector


class TestOrderFlowCollector:
    def test_run_once_calls_fetch_fills(self, tmp_path: Path) -> None:
        goldsky = MagicMock()
        goldsky.fetch_fills.return_value = pd.DataFrame({
            "id": ["f1"], "timestamp": [pd.Timestamp.now("UTC")],
            "maker_wallet": ["w1"], "taker_wallet": ["w2"],
            "maker_amount": [100.0], "taker_amount": [65.0],
            "maker_asset_id": ["a"], "taker_asset_id": ["b"], "fee": [0.5],
        })
        collector = OrderFlowCollector(
            goldsky_client=goldsky, fills_dir=str(tmp_path),
            polymarket_token_ids=["tok1"],
        )
        summary = collector.run_once()
        assert summary["polymarket_markets_updated"] == 1
        assert summary["new_fills_total"] > 0

    def test_run_once_skips_on_error(self, tmp_path: Path) -> None:
        goldsky = MagicMock()
        goldsky.fetch_fills.side_effect = RuntimeError("API error")
        collector = OrderFlowCollector(
            goldsky_client=goldsky, fills_dir=str(tmp_path),
            polymarket_token_ids=["tok1"],
        )
        summary = collector.run_once()
        assert summary["polymarket_markets_updated"] == 0
        assert len(summary["errors"]) == 1

    def test_deduplication(self, tmp_path: Path) -> None:
        fill = pd.DataFrame({
            "id": ["f1"], "timestamp": [pd.Timestamp.now("UTC")],
            "maker_wallet": ["w1"], "taker_wallet": ["w2"],
            "maker_amount": [100.0], "taker_amount": [65.0],
            "maker_asset_id": ["a"], "taker_asset_id": ["b"], "fee": [0.5],
        })
        goldsky = MagicMock()
        goldsky.fetch_fills.return_value = fill
        collector = OrderFlowCollector(
            goldsky_client=goldsky, fills_dir=str(tmp_path),
            polymarket_token_ids=["tok1"],
        )
        collector.run_once()
        collector.run_once()  # second run with same fill
        df = pd.read_parquet(tmp_path / "tok1.parquet")
        assert len(df) == 1  # deduplicated

    def test_empty_token_ids(self, tmp_path: Path) -> None:
        collector = OrderFlowCollector(fills_dir=str(tmp_path))
        summary = collector.run_once()
        assert summary["polymarket_markets_updated"] == 0
        assert summary["new_fills_total"] == 0
        assert summary["errors"] == []

    def test_summary_counts(self, tmp_path: Path) -> None:
        goldsky = MagicMock()
        goldsky.fetch_fills.return_value = pd.DataFrame()  # empty
        kalshi = MagicMock()
        kalshi.snapshot_all.return_value = {"T1": pd.DataFrame({"a": [1]})}
        collector = OrderFlowCollector(
            goldsky_client=goldsky, kalshi_collector=kalshi,
            fills_dir=str(tmp_path),
            polymarket_token_ids=["tok1"],
            kalshi_tickers=["T1"],
        )
        summary = collector.run_once()
        assert summary["kalshi_snapshots_taken"] == 1
