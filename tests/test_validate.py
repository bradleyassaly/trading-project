from __future__ import annotations

import pandas as pd
import pytest

from trading_platform.data.validate import validate_bars


def make_valid_bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 1100],
        }
    )


def test_validate_bars_accepts_valid_input() -> None:
    df = make_valid_bars()
    out = validate_bars(df)
    assert out.equals(df)


def test_validate_bars_rejects_duplicate_timestamp_symbol() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 1100],
        }
    )

    with pytest.raises(ValueError, match="Duplicate"):
        validate_bars(df)


def test_validate_bars_rejects_negative_volume() -> None:
    df = make_valid_bars()
    df.loc[0, "volume"] = -1

    with pytest.raises(ValueError, match="negative"):
        validate_bars(df)


def test_validate_bars_rejects_ohlc_outside_range() -> None:
    df = make_valid_bars()
    df.loc[0, "close"] = 1000.0

    with pytest.raises(ValueError, match="outside the \\[low, high\\] range"):
        validate_bars(df)


def test_validate_bars_rejects_unsorted_timestamps() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-02", "2024-01-01"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [101.0, 100.0],
            "high": [103.0, 102.0],
            "low": [100.0, 99.0],
            "close": [102.0, 101.0],
            "volume": [1100, 1000],
        }
    )

    with pytest.raises(ValueError, match="sorted"):
        validate_bars(df)