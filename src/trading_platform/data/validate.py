from __future__ import annotations

import pandas as pd

from trading_platform.schemas.bars import NUMERIC_BAR_COLUMNS, REQUIRED_BAR_COLUMNS


def validate_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate canonical OHLCV bar data.

    Returns the dataframe unchanged if valid.
    Raises ValueError on failure.
    """
    missing = [col for col in REQUIRED_BAR_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required bar columns: {missing}")

    if df.empty:
        raise ValueError("Bar dataframe is empty")

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        raise ValueError("Column 'timestamp' must be datetime-like")

    for col in NUMERIC_BAR_COLUMNS:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Column '{col}' must be numeric")

    if df["volume"].lt(0).any():
        raise ValueError("Column 'volume' contains negative values")

    if (df["high"] < df["low"]).any():
        raise ValueError("Found rows where high < low")

    for col in ("open", "high", "low", "close"):
        if ((df[col] < df["low"]) | (df[col] > df["high"])).any():
            raise ValueError(
                f"Found rows where '{col}' is outside the [low, high] range"
            )

    if df[["timestamp", "symbol"]].duplicated().any():
        dupes = df.loc[df[["timestamp", "symbol"]].duplicated(), ["timestamp", "symbol"]]
        raise ValueError(
            "Duplicate (timestamp, symbol) rows found. "
            f"Examples: {dupes.head(5).to_dict(orient='records')}"
        )

    if not df["timestamp"].is_monotonic_increasing:
        raise ValueError("Timestamps must be sorted in ascending order")

    if df[["open", "high", "low", "close", "volume"]].isnull().any().any():
        raise ValueError("Found nulls in required OHLCV columns")

    if df["symbol"].isnull().any():
        raise ValueError("Found nulls in 'symbol' column")

    return df