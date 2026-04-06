"""Tests for order flow signal computations."""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from trading_platform.research.signals.order_flow import (
    OrderFlowFeatures,
    depth_imbalance_signal,
)


def _fills(rows: list[dict]) -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc)
    for r in rows:
        if "timestamp" not in r:
            r["timestamp"] = now
    return pd.DataFrame(rows)


class TestTakerImbalance:
    def test_all_yes_flow(self) -> None:
        fills = _fills([
            {"taker_amount": 100.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"},
            {"taker_amount": 200.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"},
        ])
        result = OrderFlowFeatures.taker_imbalance(fills)
        assert result == pytest.approx(1.0)

    def test_all_no_flow(self) -> None:
        fills = _fills([
            {"taker_amount": 100.0, "side": "SELL", "maker_asset_id": "b", "taker_asset_id": "a"},
            {"taker_amount": 200.0, "side": "SELL", "maker_asset_id": "b", "taker_asset_id": "a"},
        ])
        result = OrderFlowFeatures.taker_imbalance(fills)
        assert result == pytest.approx(-1.0)

    def test_low_volume_nan(self) -> None:
        fills = _fills([{"taker_amount": 5.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"}])
        assert math.isnan(OrderFlowFeatures.taker_imbalance(fills))

    def test_empty_nan(self) -> None:
        assert math.isnan(OrderFlowFeatures.taker_imbalance(pd.DataFrame()))


class TestLargeOrder:
    def test_no_large_fills(self) -> None:
        fills = _fills([{"taker_amount": 100.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"}])
        assert OrderFlowFeatures.large_order(fills, threshold_usdc=500.0) == 0.0

    def test_ignores_small_fills(self) -> None:
        fills = _fills([
            {"taker_amount": 100.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"},
            {"taker_amount": 1000.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"},
        ])
        result = OrderFlowFeatures.large_order(fills, threshold_usdc=500.0)
        assert result == pytest.approx(1.0)  # only the 1000 fill counts, all YES

    def test_empty_nan(self) -> None:
        assert math.isnan(OrderFlowFeatures.large_order(pd.DataFrame()))


class TestUnexplainedMove:
    def test_price_up_beyond_flow(self) -> None:
        """Price rises more than flow explains → positive residual (bullish leak)."""
        # All YES flow, but price jumps even more than expected
        fills = _fills([{"taker_amount": 50.0, "side": "BUY", "maker_asset_id": "a", "taker_asset_id": "b"}] * 3)
        prices = pd.Series([0.40, 0.50, 0.60])  # up 0.20 in window of 2
        result = OrderFlowFeatures.unexplained_move(fills, prices, move_threshold=0.03, window_hours=2)
        assert result > 0  # price moved more than flow explains

    def test_price_down_beyond_flow(self) -> None:
        """Price falls more than flow explains → negative residual (bearish leak)."""
        fills = _fills([{"taker_amount": 50.0, "side": "SELL", "maker_asset_id": "b", "taker_asset_id": "a"}] * 3)
        prices = pd.Series([0.60, 0.50, 0.40])  # down 0.20 in window of 2
        result = OrderFlowFeatures.unexplained_move(fills, prices, move_threshold=0.03, window_hours=2)
        assert result < 0  # price fell more than flow explains

    def test_empty_nan(self) -> None:
        assert math.isnan(OrderFlowFeatures.unexplained_move(pd.DataFrame(), pd.Series(dtype=float)))


class TestDepthImbalance:
    def test_empty_dict_nan(self) -> None:
        assert math.isnan(depth_imbalance_signal({}))

    def test_returns_value(self) -> None:
        assert depth_imbalance_signal({"depth_imbalance": 0.75}) == 0.75
