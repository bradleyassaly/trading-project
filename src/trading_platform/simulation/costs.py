from __future__ import annotations

import pandas as pd


def turnover_from_positions(positions: pd.Series) -> pd.Series:
    """
    For long-flat positions, turnover is absolute change in position.
    Example:
      0 -> 1 = 1 turn
      1 -> 0 = 1 turn
      1 -> 1 = 0
    """
    return positions.fillna(0.0).diff().abs().fillna(0.0)


def turnover_from_weights(weights: pd.DataFrame) -> pd.Series:
    """
    Portfolio turnover as sum of absolute day-over-day weight changes.
    """
    return weights.fillna(0.0).diff().abs().sum(axis=1).fillna(0.0)


def linear_cost_from_turnover(
    turnover: pd.Series,
    *,
    cost_per_unit: float = 0.0,
) -> pd.Series:
    return turnover.fillna(0.0) * cost_per_unit