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

    raise ValueError(f"Unsupported signal family: {signal_family}")