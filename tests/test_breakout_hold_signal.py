from __future__ import annotations

import pandas as pd

from trading_platform.signals.breakout_hold import generate_signal_frame


def test_breakout_hold_enters_on_breakout_and_holds_until_exit() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=8, freq="D"),
            "Close": [10.0, 10.5, 10.4, 10.6, 11.2, 11.4, 10.8, 10.1],
        }
    )

    signal_frame = generate_signal_frame(
        df,
        entry_lookback=3,
        exit_lookback=2,
    )

    assert signal_frame["position"].iloc[4] == 1.0
    assert signal_frame["position"].iloc[5] == 1.0
    assert signal_frame["position"].iloc[-1] == 0.0


def test_breakout_hold_respects_positive_momentum_filter() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=7, freq="D"),
            "Close": [12.0, 11.0, 10.0, 9.5, 9.6, 9.7, 10.1],
        }
    )

    signal_frame = generate_signal_frame(
        df,
        entry_lookback=3,
        exit_lookback=2,
        momentum_lookback=5,
    )

    assert signal_frame["position"].max() == 0.0


def test_breakout_hold_without_filter_can_enter_same_series() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=7, freq="D"),
            "Close": [12.0, 11.0, 10.0, 9.5, 9.6, 9.7, 10.1],
        }
    )

    signal_frame = generate_signal_frame(
        df,
        entry_lookback=3,
        exit_lookback=2,
        momentum_lookback=None,
    )

    assert signal_frame["position"].max() == 1.0
