from __future__ import annotations

import pandas as pd

from trading_platform.ingestion.alignment import (
    TimeAlignmentConfig,
    align_daily_to_intraday_without_lookahead,
    align_timeframe_frames,
)


def test_align_timeframe_frames_event_mode_uses_latest_past_value() -> None:
    left = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01 09:31:00", "2025-01-01 09:33:00"]),
            "symbol": ["AAPL", "AAPL"],
        }
    )
    right = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01 09:30:00", "2025-01-01 09:32:00"]),
            "symbol": ["AAPL", "AAPL"],
            "close": [100.0, 101.0],
        }
    )

    aligned = align_timeframe_frames(left, right, config=TimeAlignmentConfig(right_prefix="bar_"))

    assert aligned["bar_close"].tolist() == [100.0, 101.0]


def test_align_daily_to_intraday_without_lookahead_delays_daily_bar_until_next_period() -> None:
    intraday = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-02 10:00:00",
                    "2025-01-03 10:00:00",
                ]
            ),
            "symbol": ["AAPL", "AAPL"],
        }
    )
    daily = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "symbol": ["AAPL", "AAPL"],
            "close": [99.0, 100.0],
        }
    )

    aligned = align_daily_to_intraday_without_lookahead(intraday, daily)

    assert aligned["daily_close"].tolist() == [99.0, 100.0]


def test_align_timeframe_frames_handles_multiple_symbols_independently() -> None:
    left = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01 10:00:00", "2025-01-01 10:00:00"]),
            "symbol": ["AAPL", "MSFT"],
        }
    )
    right = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01 09:30:00", "2025-01-01 09:45:00"]),
            "symbol": ["AAPL", "MSFT"],
            "signal": [1.0, -1.0],
        }
    )

    aligned = align_timeframe_frames(left, right, config=TimeAlignmentConfig(right_prefix="signal_"))

    assert aligned["signal_signal"].tolist() == [1.0, -1.0]


def test_time_alignment_config_rejects_forward_direction() -> None:
    try:
        TimeAlignmentConfig(direction="forward")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "prevent forward-looking leakage" in str(exc)
