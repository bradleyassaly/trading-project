from __future__ import annotations

from trading_platform.backtests.engine import run_backtest
from trading_platform.config.models import BacktestConfig
from trading_platform.experiments.tracker import log_experiment


def run_backtest_workflow(
    config: BacktestConfig,
) -> dict[str, object]:
    """
    Run a backtest for a symbol and log the experiment.
    """
    stats = run_backtest(
        symbol=config.symbol,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        cash=config.cash,
        commission=config.commission,
    )
    experiment_id = log_experiment(stats)

    return {
        "stats": stats,
        "experiment_id": experiment_id,
    }