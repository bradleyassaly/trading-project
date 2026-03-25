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
    "cross_sectional_momentum",
    "cross_sectional_relative_strength",
    "breakout_continuation",
    "benchmark_relative_rotation",
    "regime_conditioned_momentum",
    "volume_shock_momentum",
)


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window).mean()
    rolling_std = series.rolling(window).std()
    return (series - rolling_mean) / rolling_std.replace(0.0, np.nan)


def _relative_strength_signal(
    df: pd.DataFrame,
    *,
    lookback: int,
    close: pd.Series,
) -> pd.Series:
    relative_column = f"relative_return_{lookback}"
    if relative_column in df.columns:
        return pd.to_numeric(df[relative_column], errors="coerce")
    market_column = f"market_return_{lookback}"
    raw_momentum = close.pct_change(lookback)
    if market_column in df.columns:
        market_return = pd.to_numeric(df[market_column], errors="coerce")
        return raw_momentum - market_return
    return raw_momentum


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

    if signal_family in {"cross_sectional_relative_strength", "cross_sectional_momentum"}:
        return _relative_strength_signal(df, lookback=lookback, close=close)

    if signal_family == "breakout_continuation":
        rolling_high = close.shift(1).rolling(lookback).max()
        rolling_low = close.shift(1).rolling(lookback).min()
        breakout_distance = (close - rolling_high) / rolling_high.replace(0.0, np.nan)
        continuation_momentum = close.pct_change(lookback)
        trading_range = (rolling_high - rolling_low) / rolling_low.replace(0.0, np.nan)
        signal = continuation_momentum + breakout_distance / trading_range.replace(0.0, np.nan)
        if f"market_return_{lookback}" in df.columns:
            signal = signal + 0.25 * pd.to_numeric(df[f"market_return_{lookback}"], errors="coerce")
        return signal

    if signal_family == "benchmark_relative_rotation":
        relative_signal = _relative_strength_signal(df, lookback=lookback, close=close)
        breadth = pd.to_numeric(
            df.get(f"breadth_impulse_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        realized_vol = pd.to_numeric(
            df.get("realized_vol_20", df.get(f"realized_vol_{lookback}", pd.Series(1.0, index=df.index))),
            errors="coerce",
        ).where(lambda values: values.abs() > 1e-6, 1.0).fillna(1.0)
        return relative_signal * (1.0 + breadth.clip(lower=-0.5, upper=0.5)) / realized_vol

    if signal_family == "regime_conditioned_momentum":
        raw_momentum = close.pct_change(lookback)
        market_return = pd.to_numeric(
            df.get(f"market_return_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        breadth = pd.to_numeric(
            df.get(f"breadth_impulse_{lookback}", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)
        risk_multiplier = np.where(market_return >= 0.0, 1.0, 0.25)
        return raw_momentum * risk_multiplier * (1.0 + breadth.clip(lower=-0.5, upper=0.5))

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
