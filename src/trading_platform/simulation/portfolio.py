from __future__ import annotations

import pandas as pd

from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_weights
from trading_platform.simulation.contracts import PortfolioSimulationResult
from trading_platform.simulation.costs import linear_cost_from_turnover, turnover_from_weights
from trading_platform.simulation.metrics import summarize_equity_curve


def equal_weight_active_positions(positions: pd.DataFrame) -> pd.DataFrame:
    active_counts = positions.sum(axis=1)
    weights = positions.div(active_counts.replace(0, pd.NA), axis=0)
    return weights.fillna(0.0)


def equal_weight_buy_and_hold_returns(asset_returns: pd.DataFrame) -> pd.Series:
    return asset_returns.mean(axis=1).fillna(0.0)


def simulate_equal_weight_portfolio(
    asset_returns: pd.DataFrame,
    positions: pd.DataFrame,
    *,
    cost_per_turnover: float = 0.0,
    initial_equity: float = 1.0,
    execution_policy: ExecutionPolicy | None = None,
) -> PortfolioSimulationResult:
    if asset_returns.empty:
        raise ValueError("asset_returns is empty")
    if positions.empty:
        raise ValueError("positions is empty")

    policy = execution_policy or ExecutionPolicy()

    asset_returns = asset_returns.sort_index()
    positions = positions.sort_index()

    common_index = asset_returns.index.intersection(positions.index)
    common_cols = [c for c in asset_returns.columns if c in positions.columns]

    if len(common_index) == 0:
        raise ValueError("No overlapping dates between asset_returns and positions")
    if len(common_cols) == 0:
        raise ValueError("No overlapping symbols between asset_returns and positions")

    asset_returns = asset_returns.loc[common_index, common_cols].fillna(0.0)
    positions = positions.loc[common_index, common_cols].fillna(0.0)

    raw_weights = equal_weight_active_positions(positions)
    scheduled_weights, effective_weights = build_executed_weights(
        raw_weights,
        policy=policy,
    )

    turnover = turnover_from_weights(scheduled_weights)
    transaction_cost = linear_cost_from_turnover(turnover, cost_per_unit=cost_per_turnover)

    portfolio_return_gross = (effective_weights * asset_returns).sum(axis=1)
    portfolio_return_net = portfolio_return_gross - transaction_cost
    portfolio_equity = initial_equity * (1.0 + portfolio_return_net).cumprod()

    benchmark_return = equal_weight_buy_and_hold_returns(asset_returns)
    benchmark_equity = initial_equity * (1.0 + benchmark_return).cumprod()

    timeseries = pd.DataFrame(
        {
            "portfolio_return": portfolio_return_net,
            "portfolio_return_gross": portfolio_return_gross,
            "portfolio_return_net": portfolio_return_net,
            "portfolio_equity": portfolio_equity,
            "benchmark_return": benchmark_return,
            "benchmark_equity": benchmark_equity,
            "turnover": turnover,
            "transaction_cost": transaction_cost,
            "active_positions": positions.sum(axis=1),
        },
        index=common_index,
    )

    summary = {}
    summary.update(
        summarize_equity_curve(
            returns=timeseries["portfolio_return_net"],
            equity=timeseries["portfolio_equity"],
            prefix="portfolio_",
        )
    )
    summary.update(
        summarize_equity_curve(
            returns=timeseries["benchmark_return"],
            equity=timeseries["benchmark_equity"],
            prefix="benchmark_",
        )
    )
    summary["excess_total_return"] = (
        summary["portfolio_total_return"] - summary["benchmark_total_return"]
    )

    return PortfolioSimulationResult(
        timeseries=timeseries,
        weights=scheduled_weights,
        positions=positions,
        summary=summary,
    )


def simulate_target_weight_portfolio(
    asset_returns: pd.DataFrame,
    target_weights: pd.DataFrame,
    *,
    cost_per_turnover: float = 0.0,
    initial_equity: float = 1.0,
    execution_policy: ExecutionPolicy | None = None,
) -> PortfolioSimulationResult:
    if asset_returns.empty:
        raise ValueError("asset_returns is empty")
    if target_weights.empty:
        raise ValueError("target_weights is empty")

    policy = execution_policy or ExecutionPolicy()

    asset_returns = asset_returns.sort_index()
    target_weights = target_weights.sort_index()

    common_index = asset_returns.index.intersection(target_weights.index)
    common_cols = [c for c in asset_returns.columns if c in target_weights.columns]

    if len(common_index) == 0:
        raise ValueError("No overlapping dates between asset_returns and target_weights")
    if len(common_cols) == 0:
        raise ValueError("No overlapping symbols between asset_returns and target_weights")

    asset_returns = asset_returns.loc[common_index, common_cols].fillna(0.0)
    target_weights = target_weights.loc[common_index, common_cols].fillna(0.0)

    scheduled_weights, effective_weights = build_executed_weights(
        target_weights,
        policy=policy,
    )

    turnover = turnover_from_weights(scheduled_weights)
    transaction_cost = linear_cost_from_turnover(turnover, cost_per_unit=cost_per_turnover)

    portfolio_return_gross = (effective_weights * asset_returns).sum(axis=1)
    portfolio_return_net = portfolio_return_gross - transaction_cost
    portfolio_equity = initial_equity * (1.0 + portfolio_return_net).cumprod()

    benchmark_return = equal_weight_buy_and_hold_returns(asset_returns)
    benchmark_equity = initial_equity * (1.0 + benchmark_return).cumprod()

    timeseries = pd.DataFrame(
        {
            "portfolio_return": portfolio_return_net,
            "portfolio_return_gross": portfolio_return_gross,
            "portfolio_return_net": portfolio_return_net,
            "portfolio_equity": portfolio_equity,
            "benchmark_return": benchmark_return,
            "benchmark_equity": benchmark_equity,
            "turnover": turnover,
            "transaction_cost": transaction_cost,
            "active_positions": (target_weights > 0).sum(axis=1),
        },
        index=common_index,
    )

    summary = {}
    summary.update(
        summarize_equity_curve(
            returns=timeseries["portfolio_return_net"],
            equity=timeseries["portfolio_equity"],
            prefix="portfolio_",
        )
    )
    summary.update(
        summarize_equity_curve(
            returns=timeseries["benchmark_return"],
            equity=timeseries["benchmark_equity"],
            prefix="benchmark_",
        )
    )
    summary["excess_total_return"] = (
        summary["portfolio_total_return"] - summary["benchmark_total_return"]
    )

    return PortfolioSimulationResult(
        timeseries=timeseries,
        weights=scheduled_weights,
        positions=(target_weights > 0).astype(float),
        summary=summary,
    )