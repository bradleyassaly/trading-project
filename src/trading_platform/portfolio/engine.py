from __future__ import annotations

import pandas as pd

from trading_platform.simulation.portfolio import simulate_equal_weight_portfolio


def run_equal_weight_portfolio_backtest(
    asset_returns: pd.DataFrame,
    positions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    result = simulate_equal_weight_portfolio(
        asset_returns=asset_returns,
        positions=positions,
    )
    return result.timeseries, result.weights