from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from trading_platform.signals.loaders import load_feature_frame
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.simulation.contracts import SingleAssetSimulationResult
from trading_platform.simulation.single_asset import simulate_single_asset
from trading_platform.execution.policies import ExecutionPolicy


@dataclass
class ResearchRunResult:
    symbol: str
    strategy: str
    signal_frame: pd.DataFrame
    simulation: SingleAssetSimulationResult

execution_policy: ExecutionPolicy | None = None,

def run_vectorized_research(
    *,
    symbol: str,
    strategy: str,
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
    cost_per_turnover: float = 0.0,
    initial_equity: float = 1.0,
    execution_policy: ExecutionPolicy | None = None,

) -> ResearchRunResult:
    if strategy not in SIGNAL_REGISTRY:
        raise ValueError(f"Unsupported strategy: {strategy}")

    feature_df = load_feature_frame(symbol)
    signal_fn = SIGNAL_REGISTRY[strategy]

    signal_frame = signal_fn(
        feature_df,
        fast=fast,
        slow=slow,
        lookback=lookback,
    )

    simulation = simulate_single_asset(
        signal_frame,
        cost_per_turnover=cost_per_turnover,
        initial_equity=initial_equity,
        execution_policy=execution_policy,
    )

    return ResearchRunResult(
        symbol=symbol,
        strategy=strategy,
        signal_frame=signal_frame,
        simulation=simulation,
    )

def to_legacy_stats(
    result: ResearchRunResult,
    *,
    symbol: str | None = None,
    strategy: str | None = None,
    fast: int | None = None,
    slow: int | None = None,
    lookback: int | None = None,
    cash: float | None = None,
    commission: float | None = None,
) -> dict[str, object]:
    summary = result.simulation.summary

    stats: dict[str, object] = {
        "Symbol": symbol or result.symbol,
        "Strategy": strategy or result.strategy,
        "fast": fast,
        "slow": slow,
        "lookback": lookback,
        "cash": cash,
        "commission": commission,
        "Return [%]": (
            summary["total_return"] * 100.0
            if "total_return" in summary and pd.notna(summary["total_return"])
            else float("nan")
        ),
        "Sharpe Ratio": summary.get("sharpe", float("nan")),
        "Max. Drawdown [%]": (
            summary["max_drawdown"] * 100.0
            if "max_drawdown" in summary and pd.notna(summary["max_drawdown"])
            else float("nan")
        ),
        "Annual Return [%]": (
            summary["annual_return"] * 100.0
            if "annual_return" in summary and pd.notna(summary["annual_return"])
            else float("nan")
        ),
        "Annual Volatility [%]": (
            summary["annual_vol"] * 100.0
            if "annual_vol" in summary and pd.notna(summary["annual_vol"])
            else float("nan")
        ),
    }

    return stats

def run_vectorized_research_on_df(
    *,
    df: pd.DataFrame,
    symbol: str,
    strategy: str,
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
    cost_per_turnover: float = 0.0,
    initial_equity: float = 1.0,
    execution_policy: ExecutionPolicy | None = None,
) -> ResearchRunResult:
    if strategy not in SIGNAL_REGISTRY:
        raise ValueError(f"Unsupported strategy: {strategy}")

    signal_fn = SIGNAL_REGISTRY[strategy]

    signal_frame = signal_fn(
        df,
        fast=fast,
        slow=slow,
        lookback=lookback,
    )

    simulation = simulate_single_asset(
        signal_frame,
        cost_per_turnover=cost_per_turnover,
        initial_equity=initial_equity,
        execution_policy=execution_policy,
    )

    return ResearchRunResult(
        symbol=symbol,
        strategy=strategy,
        signal_frame=signal_frame,
        simulation=simulation,
    )