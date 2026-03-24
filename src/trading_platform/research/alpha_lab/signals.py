from __future__ import annotations

import numpy as np
import pandas as pd


SUPPORTED_SIGNAL_FAMILIES = (
    "momentum",
    "short_term_reversal",
    "vol_adjusted_momentum",
    "volatility_adjusted_momentum",
    "equity_context_momentum",
    "short_horizon_mean_reversion",
    "momentum_acceleration",
    "cross_sectional_relative_strength",
    "volume_shock_momentum",
)


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window).mean()
    rolling_std = series.rolling(window).std()
    return (series - rolling_mean) / rolling_std.replace(0.0, np.nan)


def build_signal(
    df: pd.DataFrame,
    *,
    signal_family: str,
    lookback: int,
    close_column: str = "close",
) -> pd.Series:
    close = df[close_column]

    if signal_family == "momentum":
        return close.pct_change(lookback)

    if signal_family == "short_term_reversal":
        return -close.pct_change(lookback)

    if signal_family in {"vol_adjusted_momentum", "volatility_adjusted_momentum"}:
        returns = close.pct_change()
        vol = returns.rolling(lookback).std()
        raw_momentum = close.pct_change(lookback)
        return raw_momentum / vol.replace(0.0, np.nan)

    if signal_family == "short_horizon_mean_reversion":
        returns = close.pct_change()
        return -_rolling_zscore(returns, lookback)

    if signal_family == "momentum_acceleration":
        fast_lookback = max(1, lookback // 2)
        return close.pct_change(fast_lookback) - close.pct_change(lookback)

    if signal_family == "cross_sectional_relative_strength":
        relative_column = f"relative_return_{lookback}"
        if relative_column in df.columns:
            return pd.to_numeric(df[relative_column], errors="coerce")
        market_column = f"market_return_{lookback}"
        raw_momentum = close.pct_change(lookback)
        if market_column in df.columns:
            market_return = pd.to_numeric(df[market_column], errors="coerce")
            return raw_momentum - market_return
        return raw_momentum

    if signal_family == "volume_shock_momentum":
        raw_momentum = close.pct_change(lookback)
        if "volume_ratio_20" in df.columns:
            volume_ratio = pd.to_numeric(df["volume_ratio_20"], errors="coerce")
        elif "volume" in df.columns:
            volume = pd.to_numeric(df["volume"], errors="coerce")
            volume_ratio = volume / volume.rolling(max(lookback, 5)).mean()
        else:
            volume_ratio = pd.Series(1.0, index=df.index, dtype=float)
        return raw_momentum * volume_ratio.clip(lower=0.5, upper=2.0)

    if signal_family == "equity_context_momentum":
        raw_signal = pd.to_numeric(
            df.get(f"relative_return_{lookback}", close.pct_change(lookback)),
            errors="coerce",
        )
        if f"breadth_impulse_{lookback}" in df.columns:
            breadth = pd.to_numeric(df[f"breadth_impulse_{lookback}"], errors="coerce").fillna(0.0)
            raw_signal = raw_signal * (1.0 + breadth)
        for column in ("realized_vol_20", f"realized_vol_{lookback}"):
            if column in df.columns:
                realized_vol = pd.to_numeric(df[column], errors="coerce")
                volatility_scale = realized_vol.where(realized_vol > 1e-6, 1.0).fillna(1.0)
                raw_signal = raw_signal / volatility_scale
                break
        if "volume_ratio_20" in df.columns:
            volume_ratio = pd.to_numeric(df["volume_ratio_20"], errors="coerce").clip(lower=0.5, upper=1.5)
            raw_signal = raw_signal * volume_ratio.fillna(1.0)
        return raw_signal

    raise ValueError(f"Unsupported signal family: {signal_family}")
