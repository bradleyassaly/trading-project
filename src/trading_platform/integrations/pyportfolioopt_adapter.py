from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd

from trading_platform.integrations.optional_dependencies import require_dependency


@dataclass(frozen=True)
class OptimizerAdapterResult:
    weights: pd.DataFrame
    diagnostics: dict[str, Any]


def _fallback_weights(symbols: list[str], optimizer_name: str, reason: str) -> OptimizerAdapterResult:
    count = len(symbols)
    weight = 1.0 / count if count else 0.0
    weights = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "target_weight": weight,
                "optimizer_name": optimizer_name,
                "expected_return": pd.NA,
                "risk": pd.NA,
                "metadata_json": json.dumps({"fallback_reason": reason}),
            }
            for symbol in symbols
        ]
    )
    return OptimizerAdapterResult(
        weights=weights,
        diagnostics={"status": "fallback", "reason": reason, "optimizer_name": optimizer_name},
    )


def run_pyportfolioopt_optimizer(
    *,
    returns_frame: pd.DataFrame,
    optimizer_name: str,
    risk_free_rate: float = 0.0,
    package_override=None,
) -> OptimizerAdapterResult:
    if returns_frame.empty or len(returns_frame.columns) == 0:
        raise ValueError("returns_frame must contain at least one asset column")
    symbols = list(returns_frame.columns)
    if len(symbols) < 2 and optimizer_name in {"min_vol", "max_sharpe", "hrp"}:
        return _fallback_weights(symbols, optimizer_name, "insufficient_universe_size")

    try:
        pypfopt = require_dependency(
            "pypfopt",
            purpose="running PyPortfolioOpt optimizer adapters",
            package_override=package_override,
        )
    except ImportError:
        return _fallback_weights(symbols, optimizer_name, "optional_dependency_unavailable")

    clean_returns = returns_frame.copy().dropna(how="all").fillna(0.0)
    try:
        expected_returns = pypfopt.expected_returns.mean_historical_return(clean_returns, returns_data=True)
        cov_matrix = pypfopt.risk_models.sample_cov(clean_returns, returns_data=True)
        if optimizer_name == "hrp":
            optimizer = pypfopt.hierarchical_portfolio.HRPOpt(clean_returns)
            raw_weights = optimizer.optimize()
        else:
            optimizer = pypfopt.efficient_frontier.EfficientFrontier(expected_returns, cov_matrix)
            if optimizer_name == "min_vol":
                raw_weights = optimizer.min_volatility()
            elif optimizer_name == "max_sharpe":
                raw_weights = optimizer.max_sharpe(risk_free_rate=float(risk_free_rate))
            else:
                raise ValueError(f"Unsupported optimizer_name: {optimizer_name}")
        cleaned = optimizer.clean_weights() if hasattr(optimizer, "clean_weights") else raw_weights
        performance = (
            optimizer.portfolio_performance(verbose=False, risk_free_rate=float(risk_free_rate))
            if hasattr(optimizer, "portfolio_performance")
            else (pd.NA, pd.NA, pd.NA)
        )
    except Exception as exc:
        return _fallback_weights(symbols, optimizer_name, f"optimizer_failed:{exc}")

    weights = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "target_weight": float(cleaned.get(symbol, 0.0) or 0.0),
                "optimizer_name": optimizer_name,
                "expected_return": float(performance[0]) if performance[0] is not pd.NA else pd.NA,
                "risk": float(performance[1]) if performance[1] is not pd.NA else pd.NA,
                "metadata_json": json.dumps({"sharpe": performance[2]}),
            }
            for symbol in symbols
        ]
    )
    return OptimizerAdapterResult(
        weights=weights,
        diagnostics={"status": "optimized", "optimizer_name": optimizer_name},
    )
