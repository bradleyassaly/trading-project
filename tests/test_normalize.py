from __future__ import annotations

import pandas as pd

from trading_platform.data.normalize import normalize_yahoo_bars


def test_normalize_yahoo_bars_standard_columns() -> None:
    raw = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "High": [102.0, 103.0],
            "Low": [99.0, 100.0],
            "Close": [101.0, 102.0],
            "Volume": [1000, 1100],
        },
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    out = normalize_yahoo_bars(raw, symbol="AAPL")

    assert list(out.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "timeframe",
        "provider",
        "asset_class",
        "schema_version",
    ]
    assert out["symbol"].tolist() == ["AAPL", "AAPL"]
    assert out["open"].tolist() == [100.0, 101.0]
    assert out["timeframe"].tolist() == ["1d", "1d"]
    assert out["provider"].tolist() == ["yahoo", "yahoo"]
    assert out["asset_class"].tolist() == ["equity", "equity"]
    assert pd.api.types.is_datetime64_any_dtype(out["timestamp"])


def test_normalize_yahoo_bars_raises_when_required_columns_missing() -> None:
    raw = pd.DataFrame(
        {
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            # missing Close
            "Volume": [1000],
        },
        index=pd.to_datetime(["2024-01-01"]),
    )

    try:
        normalize_yahoo_bars(raw, symbol="AAPL")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Missing required columns" in str(exc)
