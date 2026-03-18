from __future__ import annotations

import pandas as pd

from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.execution.rebalance import build_rebalance_mask


def apply_rebalance_schedule_to_positions(
    positions: pd.Series,
    *,
    rebalance_frequency: str = "daily",
) -> pd.Series:
    raw = positions.fillna(0.0).astype(float).copy()
    mask = build_rebalance_mask(raw.index, rebalance_frequency)
    scheduled = raw.where(mask)
    return scheduled.ffill().fillna(0.0).astype(float)


def apply_rebalance_schedule_to_weights(
    weights: pd.DataFrame,
    *,
    rebalance_frequency: str = "daily",
) -> pd.DataFrame:
    raw = weights.fillna(0.0).astype(float).copy()
    mask = build_rebalance_mask(raw.index, rebalance_frequency)
    scheduled = raw.where(mask, other=pd.NA)
    return scheduled.ffill().fillna(0.0).astype(float)


def apply_execution_timing_to_positions(
    positions: pd.Series,
    *,
    timing: str = "next_bar",
) -> pd.Series:
    if timing == "next_bar":
        return positions.shift(1).fillna(0.0).astype(float)
    raise ValueError(f"Unsupported execution timing: {timing}")


def apply_execution_timing_to_weights(
    weights: pd.DataFrame,
    *,
    timing: str = "next_bar",
) -> pd.DataFrame:
    if timing == "next_bar":
        return weights.shift(1).fillna(0.0).astype(float)
    raise ValueError(f"Unsupported execution timing: {timing}")


def build_executed_positions(
    raw_positions: pd.Series,
    *,
    policy: ExecutionPolicy,
) -> tuple[pd.Series, pd.Series]:
    scheduled_positions = apply_rebalance_schedule_to_positions(
        raw_positions,
        rebalance_frequency=policy.rebalance_frequency,
    )
    effective_positions = apply_execution_timing_to_positions(
        scheduled_positions,
        timing=policy.timing,
    )
    return scheduled_positions, effective_positions


def build_executed_weights(
    raw_weights: pd.DataFrame,
    *,
    policy: ExecutionPolicy,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scheduled_weights = apply_rebalance_schedule_to_weights(
        raw_weights,
        rebalance_frequency=policy.rebalance_frequency,
    )
    effective_weights = apply_execution_timing_to_weights(
        scheduled_weights,
        timing=policy.timing,
    )
    return scheduled_weights, effective_weights