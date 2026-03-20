from __future__ import annotations

import pandas as pd

from trading_platform.data.canonical import normalize_research_frame


def test_normalize_research_frame_standardizes_optional_columns() -> None:
    frame = normalize_research_frame(
        pd.DataFrame(
            {
                "Date": pd.date_range("2025-01-01", periods=2, freq="D"),
                "Adj Close": [10.0, 11.0],
                "Open": [9.5, 10.5],
                "High": [10.5, 11.5],
                "Low": [9.0, 10.0],
                "Volume": [100, 200],
                "Dollar Volume": [1000, 2200],
            }
        ),
        symbol="AAPL",
    )

    assert "timestamp" in frame.columns
    assert "close" in frame.columns
    assert "open" in frame.columns
    assert "high" in frame.columns
    assert "low" in frame.columns
    assert "volume" in frame.columns
    assert "dollar_volume" in frame.columns
    assert frame["symbol"].tolist() == ["AAPL", "AAPL"]
