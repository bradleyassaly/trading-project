from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.config.models import (
    BacktestConfig,
    PortfolioConstructionConfig,
    UniverseConfig,
    UniverseWalkForwardConfig,
    WalkForwardConfig,
)
from trading_platform.services.universe_walk_forward_service import (
    run_universe_walk_forward_research,
)


class DummyWalkForwardResult:
    def __init__(self, results_df: pd.DataFrame) -> None:
        self.results_df = results_df


def test_run_universe_walk_forward_research_combines_symbols(
    monkeypatch,
    tmp_path: Path,
) -> None:
    called_symbols: list[str] = []

    def fake_run_walk_forward_evaluation(*, feature_path, backtest_config, walk_forward_config):
        called_symbols.append(backtest_config.symbol)
        results_df = pd.DataFrame(
            {
                "fold": [1, 2],
                "test_start": pd.to_datetime(["2024-01-31", "2024-02-29"]),
                "test_return_pct": [5.0, 2.0] if backtest_config.symbol == "AAPL" else [3.0, 4.0],
                "test_sharpe_ratio": [1.5, 1.0] if backtest_config.symbol == "AAPL" else [0.9, 1.3],
                "test_max_drawdown_pct": [-2.0, -1.0],
                "selected_fast": [10, 20],
                "selected_slow": [100, 150],
            }
        )
        return DummyWalkForwardResult(results_df)

    monkeypatch.setattr(
        "trading_platform.services.universe_walk_forward_service.run_walk_forward_evaluation",
        fake_run_walk_forward_evaluation,
    )

    config = UniverseWalkForwardConfig(
        feature_path=tmp_path / "features.parquet",
        universe=UniverseConfig(symbols=["AAPL", "MSFT"]),
        walk_forward=WalkForwardConfig(
            symbol="PLACEHOLDER",
            strategy="sma_cross",
            walk_forward_mode="optimize",
            train_window_bars=252,
            test_window_bars=21,
            step_bars=21,
            min_required_bars=252,
            rank_metric="sharpe_ratio",
            fast_values=[10, 20],
            slow_values=[100, 150],
        ),
        backtest=BacktestConfig(
            symbol="PLACEHOLDER",
            strategy="sma_cross",
            fast=10,
            slow=100,
            lookback=20,
            cash=100000.0,
            commission=0.001,
        ),
        portfolio=PortfolioConstructionConfig(
            method="top_n",
            top_n=1,
            max_weight=1.0,
            transaction_cost_bps=0.0,
        ),
    )

    result = run_universe_walk_forward_research(config)

    assert called_symbols == ["AAPL", "MSFT"]
    assert len(result.fold_results_df) == 4
    assert set(result.fold_results_df["symbol"]) == {"AAPL", "MSFT"}
    assert len(result.oos_scores_df) == 4
    assert result.summary["n_symbols"] == 2
    assert result.portfolio_result.summary["top_n"] == 1


def test_run_universe_walk_forward_research_builds_portfolio_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    def fake_run_walk_forward_evaluation(*, feature_path, backtest_config, walk_forward_config):
        results_df = pd.DataFrame(
            {
                "fold": [1],
                "test_start": pd.to_datetime(["2024-01-31"]),
                "test_return_pct": [4.0 if backtest_config.symbol == "AAPL" else 2.0],
                "test_sharpe_ratio": [1.1 if backtest_config.symbol == "AAPL" else 0.8],
                "test_max_drawdown_pct": [-1.0],
            }
        )
        return DummyWalkForwardResult(results_df)

    monkeypatch.setattr(
        "trading_platform.services.universe_walk_forward_service.run_walk_forward_evaluation",
        fake_run_walk_forward_evaluation,
    )

    config = UniverseWalkForwardConfig(
        feature_path=tmp_path / "features.parquet",
        universe=UniverseConfig(symbols=["AAPL", "MSFT"]),
        walk_forward=WalkForwardConfig(
            symbol="PLACEHOLDER",
            strategy="sma_cross",
            walk_forward_mode="optimize",
            train_window_bars=252,
            test_window_bars=21,
            step_bars=21,
            min_required_bars=252,
            rank_metric="sharpe_ratio",
            fast_values=[10, 20],
            slow_values=[100, 150],
        ),
        backtest=BacktestConfig(
            symbol="PLACEHOLDER",
            strategy="sma_cross",
            fast=10,
            slow=100,
            lookback=20,
            cash=100000.0,
            commission=0.001,
        ),
        portfolio=PortfolioConstructionConfig(
            method="top_n",
            top_n=1,
            max_weight=1.0,
            transaction_cost_bps=0.0,
        ),
    )

    result = run_universe_walk_forward_research(config)

    assert not result.portfolio_result.weights_df.empty
    assert not result.portfolio_result.portfolio_returns_df.empty
    assert result.portfolio_result.summary["n_weight_rows"] >= 1