from __future__ import annotations

import pandas as pd


def add_forward_return_labels(
    df: pd.DataFrame,
    *,
    close_column: str = "close",
    horizons: list[int] | tuple[int, ...] = (1, 5, 20),
) -> pd.DataFrame:
    """
    Add forward return label columns like fwd_return_1d, fwd_return_5d, etc.

    Expected input:
    - df sorted ascending by timestamp within a symbol
    - close_column exists
    """
    result = df.copy()

    for horizon in horizons:
        result[f"fwd_return_{horizon}d"] = (
            result[close_column].shift(-horizon) / result[close_column] - 1.0
        )

    return result