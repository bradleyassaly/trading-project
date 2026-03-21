from __future__ import annotations

import pandas as pd

from trading_platform.execution.models import ExecutionConfig
from trading_platform.portfolio.backtest import backtest_portfolio_from_weights


def test_backtest_portfolio_from_weights_returns_expected_columns() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT"],
            "weight": [0.5, 0.5],
        }
    )
    forward_returns_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT"],
            "forward_return": [0.10, 0.00],
        }
    )

    result = backtest_portfolio_from_weights(
        weights_df=weights_df,
        forward_returns_df=forward_returns_df,
        transaction_cost_bps=0.0,
    )

    assert list(result.columns) == [
        "timestamp",
        "gross_return",
        "turnover",
        "cost",
        "net_return",
        "transaction_cost_bps",
    ]
    assert len(result) == 1
    assert result.loc[0, "gross_return"] == 0.05
    assert result.loc[0, "net_return"] == 0.05


def test_backtest_portfolio_from_weights_applies_transaction_costs() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"],
            "symbol": ["AAPL", "MSFT", "AAPL", "NVDA"],
            "weight": [0.5, 0.5, 0.5, 0.5],
        }
    )
    forward_returns_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"],
            "symbol": ["AAPL", "MSFT", "AAPL", "NVDA"],
            "forward_return": [0.02, 0.00, 0.01, 0.03],
        }
    )

    result = backtest_portfolio_from_weights(
        weights_df=weights_df,
        forward_returns_df=forward_returns_df,
        transaction_cost_bps=10.0,
    )

    assert len(result) == 2
    assert "cost" in result.columns
    assert (result["cost"] >= 0.0).all()
    assert (result["net_return"] <= result["gross_return"] + 1e-12).all()


def test_backtest_portfolio_from_weights_computes_turnover_between_periods() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "weight": [0.5, 0.5, 0.8, 0.2],
        }
    )
    forward_returns_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "forward_return": [0.0, 0.0, 0.0, 0.0],
        }
    )

    result = backtest_portfolio_from_weights(
        weights_df=weights_df,
        forward_returns_df=forward_returns_df,
        transaction_cost_bps=0.0,
    )

    assert result.loc[0, "turnover"] == 1.0
    assert round(float(result.loc[1, "turnover"]), 10) == 0.6


def test_backtest_portfolio_from_weights_supports_execution_config() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT"],
            "weight": [0.5, 0.5],
        }
    )
    forward_returns_df = pd.DataFrame(
        {
            "timestamp": ["2024-01-31", "2024-01-31"],
            "symbol": ["AAPL", "MSFT"],
            "forward_return": [0.01, 0.01],
        }
    )

    result = backtest_portfolio_from_weights(
        weights_df=weights_df,
        forward_returns_df=forward_returns_df,
        execution_config=ExecutionConfig(commission_bps=5.0, slippage_model_type="fixed_bps", fixed_slippage_bps=5.0),
    )

    assert result.loc[0, "transaction_cost_bps"] == 10.0
    assert result.loc[0, "cost"] > 0.0
