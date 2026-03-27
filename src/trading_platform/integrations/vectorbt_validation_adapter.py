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
    turnover: float
    trade_count: int


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
    aligned_close = close_prices.sort_index()
    aligned_weights = target_weights.sort_index().reindex_like(aligned_close).fillna(0.0)
    changed_mask = aligned_weights.ne(aligned_weights.shift(1)).any(axis=1)
    if len(changed_mask):
        changed_mask.iloc[0] = True
    order_weights = aligned_weights.where(changed_mask, other=pd.NA)
    portfolio = vectorbt.Portfolio.from_orders(
        close=aligned_close,
        size=order_weights,
        size_type="targetpercent",
        fees=float(fees),
        cash_sharing=True,
        init_cash=1.0,
    )
    returns = pd.Series(portfolio.returns(), name="vectorbt_return")
    equity = pd.Series(portfolio.value(), name="vectorbt_equity")
    trades = portfolio.trades.records_readable if hasattr(portfolio.trades, "records_readable") else pd.DataFrame()
    try:
        stats = portfolio.stats(settings={"freq": "1D"}) if hasattr(portfolio, "stats") else {}
    except Exception:
        stats = {}
    metrics = {}
    if isinstance(stats, pd.Series):
        metrics = {str(key): value for key, value in stats.to_dict().items()}
    elif isinstance(stats, dict):
        metrics = {str(key): value for key, value in stats.items()}
    turnover = float(aligned_weights.diff().abs().sum(axis=1).fillna(0.0).sum())
    trade_count = int(len(trades.index)) if isinstance(trades, pd.DataFrame) else 0
    return VectorbtValidationResult(
        returns=returns,
        equity=equity,
        trades=trades if isinstance(trades, pd.DataFrame) else pd.DataFrame(trades),
        metrics=metrics,
        turnover=turnover,
        trade_count=trade_count,
    )
