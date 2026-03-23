from __future__ import annotations

import numpy as np
import pandas as pd


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

    if signal_family == "vol_adjusted_momentum":
        returns = close.pct_change()
        vol = returns.rolling(lookback).std()
        raw_momentum = close.pct_change(lookback)
        return raw_momentum / vol.replace(0.0, np.nan)

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
