from __future__ import annotations

import warnings

import pandas as pd

from trading_platform.backtests.engine import run_backtest_on_df


def test_run_backtest_on_df_finalizes_end_of_test_positions(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeBacktest:
        def __init__(self, df, strategy_cls, **kwargs):
            captured["kwargs"] = kwargs

        def run(self, **kwargs):
            if not captured["kwargs"].get("finalize_trades"):
                warnings.warn("Some trades remain open at the end of backtest", UserWarning)
            return {"Return [%]": 4.0, "Sharpe Ratio": 1.0, "Max. Drawdown [%]": -2.0, "_trades": pd.DataFrame(), "_equity_curve": pd.DataFrame(index=range(5))}

    monkeypatch.setattr("trading_platform.backtests.engine.Backtest", FakeBacktest)

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="D"),
            "Open": [100, 101, 102, 103, 104],
            "High": [101, 102, 103, 104, 105],
            "Low": [99, 100, 101, 102, 103],
            "Close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "Volume": [1000, 1000, 1000, 1000, 1000],
        }
    )

    with warnings.catch_warnings(record=True) as recorded:
        stats = run_backtest_on_df(
            df=df,
            symbol="AAPL",
            strategy="momentum_hold",
            lookback=1,
        )

    assert stats["Return [%]"] == 4.0
    assert captured["kwargs"]["finalize_trades"] is True
    assert not recorded
    assert stats["trade_count"] == 0
    assert stats["percent_time_in_market"] == 0.0
    assert stats["ended_in_cash"] is True
