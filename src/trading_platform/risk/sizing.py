from __future__ import annotations

import pandas as pd

from trading_platform.risk.volatility import rolling_volatility, safe_inverse_volatility


def normalize_weights(weights: pd.DataFrame) -> pd.DataFrame:
    row_sums = weights.sum(axis=1)
    normalized = weights.div(row_sums.replace(0, pd.NA), axis=0)
    return normalized.fillna(0.0)


def equal_weight_target_weights(selection: pd.DataFrame) -> pd.DataFrame:
    selection = selection.fillna(0.0).astype(float)
    return normalize_weights(selection)


def inverse_vol_target_weights(
    selection: pd.DataFrame,
    asset_returns: pd.DataFrame,
    *,
    vol_window: int = 20,
    periods_per_year: int = 252,
) -> pd.DataFrame:
    selection = selection.fillna(0.0).astype(float)
    asset_returns = asset_returns.fillna(0.0).astype(float)

    vol = rolling_volatility(
        asset_returns,
        window=vol_window,
        periods_per_year=periods_per_year,
    )
    inv_vol = safe_inverse_volatility(vol)

    raw_weights = selection * inv_vol
    return normalize_weights(raw_weights)