"""Tests for KalshiOrderBookCollector."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trading_platform.kalshi.orderbook_collector import KalshiOrderBookCollector


def _mock_client(yes_levels=None, no_levels=None):
    client = MagicMock()
    client._get.return_value = {
        "orderbook": {
            "yes": yes_levels if yes_levels is not None else [[55, 100], [50, 200]],
            "no": no_levels if no_levels is not None else [[40, 150]],
        }
    }
    return client


class TestOrderBookCollector:
    def test_snapshot_correct_columns(self, tmp_path: Path) -> None:
        collector = KalshiOrderBookCollector(_mock_client(), snapshot_dir=str(tmp_path))
        df = collector.snapshot("KXCPI-T0.3")
        assert not df.empty
        assert set(df.columns) >= {"ticker", "side", "price", "size", "ts"}
        assert df[df["side"] == "YES"].iloc[0]["price"] == 0.55  # 55 cents -> 0.55

    def test_snapshot_empty_orderbook(self, tmp_path: Path) -> None:
        client = MagicMock()
        client._get.return_value = {"orderbook": {"yes": [], "no": []}}
        collector = KalshiOrderBookCollector(client, snapshot_dir=str(tmp_path))
        df = collector.snapshot("EMPTY")
        assert df.empty

    def test_compute_features_needs_2_snapshots(self, tmp_path: Path) -> None:
        collector = KalshiOrderBookCollector(_mock_client(), snapshot_dir=str(tmp_path))
        # Only 1 snapshot
        collector.snapshot("T1")
        features = collector.compute_book_features("T1")
        assert features == {}

    def test_depth_imbalance_all_yes(self, tmp_path: Path) -> None:
        client = _mock_client(yes_levels=[[55, 1000]], no_levels=[])
        collector = KalshiOrderBookCollector(client, snapshot_dir=str(tmp_path))
        collector.snapshot("T1")
        import time; time.sleep(0.01)
        collector.snapshot("T1")
        features = collector.compute_book_features("T1")
        assert features["depth_imbalance"] == pytest.approx(1.0)

    def test_depth_imbalance_all_no(self, tmp_path: Path) -> None:
        client = _mock_client(yes_levels=[], no_levels=[[40, 1000]])
        collector = KalshiOrderBookCollector(client, snapshot_dir=str(tmp_path))
        collector.snapshot("T1")
        import time; time.sleep(0.01)
        collector.snapshot("T1")
        features = collector.compute_book_features("T1")
        assert features["depth_imbalance"] == pytest.approx(-1.0)

    def test_spread_calculation(self, tmp_path: Path) -> None:
        client = _mock_client(yes_levels=[[55, 100]], no_levels=[[40, 100]])
        collector = KalshiOrderBookCollector(client, snapshot_dir=str(tmp_path))
        collector.snapshot("T1")
        import time; time.sleep(0.01)
        collector.snapshot("T1")
        features = collector.compute_book_features("T1")
        # spread = 1.0 - 0.55 - 0.40 = 0.05
        assert features["spread"] == pytest.approx(0.05)

    def test_snapshot_all_continues_on_error(self, tmp_path: Path) -> None:
        client = MagicMock()
        call_count = [0]
        def _get_side_effect(path, params):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("API error")
            return {"orderbook": {"yes": [[55, 100]], "no": [[40, 100]]}}
        client._get.side_effect = _get_side_effect
        collector = KalshiOrderBookCollector(client, snapshot_dir=str(tmp_path))
        results = collector.snapshot_all(["BAD", "GOOD"])
        assert results["BAD"].empty
        assert not results["GOOD"].empty
