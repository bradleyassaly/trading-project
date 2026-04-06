"""
Order flow signal computations.

Computes taker_imbalance, large_order, and unexplained_move from
fill-level data, plus depth_imbalance from orderbook snapshots.

All methods return ``float`` or ``float('nan')`` when data is
insufficient. Callers must handle NaN gracefully.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pandas as pd


class OrderFlowFeatures:
    """Static methods for order flow signal computation."""

    @staticmethod
    def taker_imbalance(fills_df: pd.DataFrame, window_hours: int = 4) -> float:
        """Compute net taker direction from recent fills.

        Returns value in [-1, 1]. +1 = all YES taker flow, -1 = all NO.
        Returns NaN if insufficient data.
        """
        if fills_df.empty:
            return float("nan")

        # Filter to window
        if "timestamp" in fills_df.columns:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)
            try:
                recent = fills_df[fills_df["timestamp"] >= cutoff]
            except TypeError:
                recent = fills_df
        else:
            recent = fills_df

        if recent.empty:
            return float("nan")

        amt_col = "taker_amount" if "taker_amount" in recent.columns else "amount"
        if amt_col not in recent.columns:
            return float("nan")

        # Determine YES vs NO taker flow from maker_asset_id
        # Convention: if maker sold token_id X (maker_asset_id = X), taker bought X
        # We treat the primary asset as YES
        yes_vol = 0.0
        no_vol = 0.0
        for _, row in recent.iterrows():
            amt = float(row.get(amt_col, 0))
            maker_asset = str(row.get("maker_asset_id", ""))
            taker_asset = str(row.get("taker_asset_id", ""))
            side = str(row.get("side", "")).upper()

            if side == "BUY" or maker_asset < taker_asset:
                yes_vol += amt
            else:
                no_vol += amt

        total = yes_vol + no_vol
        if total < 10.0:
            return float("nan")

        return (yes_vol - no_vol) / total

    @staticmethod
    def large_order(fills_df: pd.DataFrame, threshold_usdc: float = 500.0) -> float:
        """Detect large order direction from recent fills.

        Returns 0.0 if no large fills. NaN if fills_df is empty.
        """
        if fills_df.empty:
            return float("nan")

        amt_col = "taker_amount" if "taker_amount" in fills_df.columns else "amount"
        if amt_col not in fills_df.columns:
            return float("nan")

        # Filter to last 24h
        if "timestamp" in fills_df.columns:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
            try:
                recent = fills_df[fills_df["timestamp"] >= cutoff]
            except TypeError:
                recent = fills_df
        else:
            recent = fills_df

        large = recent[pd.to_numeric(recent[amt_col], errors="coerce") >= threshold_usdc]
        if large.empty:
            return 0.0

        yes_vol = 0.0
        no_vol = 0.0
        for _, row in large.iterrows():
            amt = float(row.get(amt_col, 0))
            side = str(row.get("side", "")).upper()
            maker_asset = str(row.get("maker_asset_id", ""))
            taker_asset = str(row.get("taker_asset_id", ""))

            if side == "BUY" or maker_asset < taker_asset:
                yes_vol += amt
            else:
                no_vol += amt

        total = yes_vol + no_vol
        if total < 1.0:
            return 0.0
        return (yes_vol - no_vol) / total

    @staticmethod
    def unexplained_move(
        fills_df: pd.DataFrame,
        price_series: pd.Series,
        move_threshold: float = 0.03,
        window_hours: int = 2,
    ) -> float:
        """Detect price moves not explained by order flow.

        Returns clipped value in [-3, 3]. NaN if insufficient data.
        """
        if fills_df.empty or price_series.empty:
            return float("nan")

        # Actual price move
        try:
            recent_prices = price_series.iloc[-max(1, window_hours):]
            actual_move = float(recent_prices.iloc[-1] - recent_prices.iloc[0])
        except (IndexError, TypeError):
            return float("nan")

        # Flow direction
        flow = OrderFlowFeatures.taker_imbalance(fills_df, window_hours)
        if math.isnan(flow):
            return float("nan")

        expected_move = flow * 0.08
        residual = actual_move - expected_move
        return max(-3.0, min(3.0, residual / move_threshold))


def depth_imbalance_signal(book_features: dict) -> float:
    """Extract depth imbalance from orderbook features dict."""
    val = book_features.get("depth_imbalance")
    if val is None:
        return float("nan")
    return float(val)
