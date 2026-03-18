from __future__ import annotations

import pandas as pd

from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.transforms import build_executed_positions
from trading_platform.simulation.contracts import SingleAssetSimulationResult
from trading_platform.simulation.costs import linear_cost_from_turnover, turnover_from_positions
from trading_platform.simulation.metrics import summarize_equity_curve


def simulate_single_asset(
    signal_frame: pd.DataFrame,
    *,
    position_col: str = "position",
    return_col: str = "asset_return",
    cost_per_turnover: float = 0.0,
    initial_equity: float = 1.0,
    execution_policy: ExecutionPolicy | None = None,
) -> SingleAssetSimulationResult:
    required = {position_col, return_col}
    missing = required - set(signal_frame.columns)
    if missing:
        raise ValueError(f"signal_frame missing required columns: {sorted(missing)}")

    policy = execution_policy or ExecutionPolicy()

    df = signal_frame.copy().sort_index()
    raw_position = df[position_col].fillna(0.0).astype(float)
    asset_return = df[return_col].fillna(0.0).astype(float)

    scheduled_position, effective_position = build_executed_positions(
        raw_position,
        policy=policy,
    )

    turnover = turnover_from_positions(scheduled_position)
    transaction_cost = linear_cost_from_turnover(turnover, cost_per_unit=cost_per_turnover)

    strategy_return_gross = effective_position * asset_return
    strategy_return_net = strategy_return_gross - transaction_cost
    equity = initial_equity * (1.0 + strategy_return_net).cumprod()

    timeseries = pd.DataFrame(
        {
            "asset_return": asset_return,
            "raw_position": raw_position,
            "scheduled_position": scheduled_position,
            "position": scheduled_position,
            "effective_position": effective_position,
            "turnover": turnover,
            "transaction_cost": transaction_cost,
            "strategy_return": strategy_return_net,
            "strategy_return_gross": strategy_return_gross,
            "strategy_return_net": strategy_return_net,
            "equity": equity,
        },
        index=df.index,
    )

    summary = summarize_equity_curve(
        returns=timeseries["strategy_return_net"],
        equity=timeseries["equity"],
    )

    return SingleAssetSimulationResult(timeseries=timeseries, summary=summary)