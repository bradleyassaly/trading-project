from __future__ import annotations

import math

import pandas as pd


def rolling_volatility(
    asset_returns: pd.DataFrame,
    *,
    window: int = 20,
    periods_per_year: int = 252,
) -> pd.DataFrame:
    if window <= 1:
        raise ValueError(f"window must be > 1, got {window}")

    returns = asset_returns.fillna(0.0).astype(float)
    vol = returns.rolling(window).std(ddof=0) * math.sqrt(periods_per_year)
    return vol


def safe_inverse_volatility(
    volatility: pd.DataFrame,
    *,
    floor: float = 1e-8,
) -> pd.DataFrame:
    return 1.0 / volatility.clip(lower=floor)