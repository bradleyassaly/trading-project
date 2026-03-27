from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.integrations.pyportfolioopt_adapter import run_pyportfolioopt_optimizer


@dataclass(frozen=True)
class PortfolioOptimizerPolicyConfig:
    optimizer_name: str = "equal_weight"
    risk_free_rate: float = 0.0
    fallback_optimizer_name: str = "equal_weight"
    min_history_rows: int = 20

    def __post_init__(self) -> None:
        if self.optimizer_name not in {"equal_weight", "metric_weighted", "min_vol", "max_sharpe", "hrp"}:
            raise ValueError("optimizer_name must be one of: equal_weight, metric_weighted, min_vol, max_sharpe, hrp")
        if self.fallback_optimizer_name not in {"equal_weight", "metric_weighted"}:
            raise ValueError("fallback_optimizer_name must be one of: equal_weight, metric_weighted")
        if self.min_history_rows <= 1:
            raise ValueError("min_history_rows must be > 1")


def _baseline_weights(returns_frame: pd.DataFrame, policy: PortfolioOptimizerPolicyConfig) -> pd.DataFrame:
    symbols = list(returns_frame.columns)
    if not symbols:
        raise ValueError("returns_frame must contain at least one asset column")
    if policy.fallback_optimizer_name == "metric_weighted":
        scores = returns_frame.mean().clip(lower=0.0)
        total = float(scores.sum())
        if total <= 0:
            scores[:] = 1.0
            total = float(scores.sum())
        weights = scores / total
    else:
        weights = pd.Series(1.0 / len(symbols), index=symbols)
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "target_weight": float(weights.get(symbol, 0.0)),
                "optimizer_name": policy.fallback_optimizer_name,
                "expected_return": pd.NA,
                "risk": pd.NA,
                "metadata_json": json.dumps({"fallback": True}),
            }
            for symbol in symbols
        ]
    )


def run_optimizer_experiment(
    *,
    returns_frame: pd.DataFrame,
    policy: PortfolioOptimizerPolicyConfig,
    package_override=None,
) -> dict[str, Any]:
    clean_returns = returns_frame.copy().dropna(how="all")
    if len(clean_returns) < policy.min_history_rows or len(clean_returns.columns) == 0:
        baseline = _baseline_weights(clean_returns if not clean_returns.empty else returns_frame, policy)
        return {
            "weights": baseline,
            "diagnostics": {"status": "fallback", "reason": "insufficient_history"},
        }
    if policy.optimizer_name in {"equal_weight", "metric_weighted"}:
        baseline = _baseline_weights(clean_returns, policy)
        baseline["optimizer_name"] = policy.optimizer_name
        return {"weights": baseline, "diagnostics": {"status": "baseline", "optimizer_name": policy.optimizer_name}}
    result = run_pyportfolioopt_optimizer(
        returns_frame=clean_returns,
        optimizer_name=policy.optimizer_name,
        risk_free_rate=policy.risk_free_rate,
        package_override=package_override,
    )
    baseline = _baseline_weights(clean_returns, policy)
    comparison = (
        baseline[["symbol", "target_weight"]]
        .rename(columns={"target_weight": "baseline_weight"})
        .merge(
            result.weights[["symbol", "target_weight"]].rename(columns={"target_weight": "optimized_weight"}),
            on="symbol",
            how="outer",
        )
    )
    return {
        "weights": result.weights,
        "comparison": comparison.fillna(0.0),
        "diagnostics": result.diagnostics,
    }


def write_optimizer_artifacts(
    *,
    result: dict[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    weights_path = output_path / "optimizer_weights.csv"
    diagnostics_path = output_path / "optimizer_diagnostics.json"
    comparison_path = output_path / "optimizer_weight_comparison.csv"
    pd.DataFrame(result["weights"]).to_csv(weights_path, index=False)
    pd.DataFrame(result.get("comparison", pd.DataFrame())).to_csv(comparison_path, index=False)
    diagnostics_path.write_text(json.dumps(result.get("diagnostics", {}), indent=2), encoding="utf-8")
    return {
        "optimizer_weights_path": weights_path,
        "optimizer_diagnostics_path": diagnostics_path,
        "optimizer_weight_comparison_path": comparison_path,
    }
