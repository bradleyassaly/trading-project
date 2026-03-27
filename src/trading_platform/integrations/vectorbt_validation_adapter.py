from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from trading_platform.integrations.optional_dependencies import require_dependency


@dataclass(frozen=True)
class VectorbtValidationResult:
    returns: pd.Series
    equity: pd.Series
    trades: pd.DataFrame
    metrics: dict[str, Any]


def run_vectorbt_target_weight_scenario(
    *,
    close_prices: pd.DataFrame,
    target_weights: pd.DataFrame,
    fees: float = 0.0,
    package_override=None,
) -> VectorbtValidationResult:
    vectorbt = require_dependency(
        "vectorbt",
        purpose="running vectorbt benchmark validation",
        package_override=package_override,
    )
    portfolio = vectorbt.Portfolio.from_orders(
        close=close_prices.sort_index(),
        size=target_weights.sort_index(),
        size_type="targetpercent",
        fees=float(fees),
        cash_sharing=True,
        init_cash=1.0,
    )
    returns = pd.Series(portfolio.returns(), name="vectorbt_return")
    equity = pd.Series(portfolio.value(), name="vectorbt_equity")
    trades = portfolio.trades.records_readable if hasattr(portfolio.trades, "records_readable") else pd.DataFrame()
    stats = portfolio.stats() if hasattr(portfolio, "stats") else {}
    metrics = {}
    if isinstance(stats, pd.Series):
        metrics = {str(key): value for key, value in stats.to_dict().items()}
    elif isinstance(stats, dict):
        metrics = {str(key): value for key, value in stats.items()}
    return VectorbtValidationResult(
        returns=returns,
        equity=equity,
        trades=trades if isinstance(trades, pd.DataFrame) else pd.DataFrame(trades),
        metrics=metrics,
    )
