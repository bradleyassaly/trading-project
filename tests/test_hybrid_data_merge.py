from __future__ import annotations

import pandas as pd

from trading_platform.ingestion.alpaca_data import merge_historical_with_latest


def test_merge_historical_with_latest_overrides_overlaps_and_sorts() -> None:
    historical = pd.DataFrame(
        [
            {"timestamp": "2026-03-20", "symbol": "AAPL", "close": 100.0},
            {"timestamp": "2026-03-21", "symbol": "AAPL", "close": 101.0},
            {"timestamp": "2026-03-20", "symbol": "MSFT", "close": 200.0},
            {"timestamp": "2026-03-21", "symbol": "MSFT", "close": 201.0},
        ]
    )
    latest = pd.DataFrame(
        [
            {"date": "2026-03-21", "symbol": "AAPL", "close": 111.0, "source": "alpaca"},
            {"date": "2026-03-22", "symbol": "AAPL", "close": 112.0, "source": "alpaca"},
            {"date": "2026-03-22", "symbol": "MSFT", "close": 212.0, "source": "alpaca"},
        ]
    )

    merged = merge_historical_with_latest(historical, latest)

    assert list(merged[["symbol", "date"]].itertuples(index=False, name=None)) == [
        ("AAPL", pd.Timestamp("2026-03-20")),
        ("AAPL", pd.Timestamp("2026-03-21")),
        ("AAPL", pd.Timestamp("2026-03-22")),
        ("MSFT", pd.Timestamp("2026-03-20")),
        ("MSFT", pd.Timestamp("2026-03-21")),
        ("MSFT", pd.Timestamp("2026-03-22")),
    ]
    assert len(merged.index) == 6
    assert float(merged.loc[(merged["symbol"] == "AAPL") & (merged["date"] == pd.Timestamp("2026-03-21")), "close"].iloc[0]) == 111.0
    assert merged.duplicated(subset=["symbol", "date"]).sum() == 0
