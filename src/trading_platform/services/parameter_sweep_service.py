from __future__ import annotations

from dataclasses import asdict
from itertools import product
from typing import Any

import pandas as pd

from trading_platform.config.models import ParameterSweepConfig, ResearchWorkflowConfig
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.research_service import run_research_workflow


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_sma_cross_configs(
    config: ParameterSweepConfig,
) -> list[ResearchWorkflowConfig]:
    configs: list[ResearchWorkflowConfig] = []

    for fast, slow in product(config.fast_values, config.slow_values):
        if fast >= slow:
            continue

        configs.append(
            ResearchWorkflowConfig(
                symbol=config.symbol,
                start=config.start,
                end=config.end,
                interval=config.interval,
                feature_groups=config.feature_groups,
                strategy=config.strategy,
                fast=fast,
                slow=slow,
                cash=config.cash,
                commission=config.commission,
            )
        )

    return configs


def _build_momentum_configs(
    config: ParameterSweepConfig,
) -> list[ResearchWorkflowConfig]:
    return [
        ResearchWorkflowConfig(
            symbol=config.symbol,
            start=config.start,
            end=config.end,
            interval=config.interval,
            feature_groups=config.feature_groups,
            strategy=config.strategy,
            lookback=lookback,
            cash=config.cash,
            commission=config.commission,
        )
        for lookback in config.lookback_values
    ]


def build_sweep_workflow_configs(
    config: ParameterSweepConfig,
) -> list[ResearchWorkflowConfig]:
    if config.strategy == "sma_cross":
        return _build_sma_cross_configs(config)

    if config.strategy == "momentum":
        return _build_momentum_configs(config)

    raise ValueError(f"Unsupported sweep strategy: {config.strategy}")


def run_parameter_sweep(
    config: ParameterSweepConfig,
    provider: BarDataProvider | None = None,
    continue_on_error: bool = True,
) -> dict[str, Any]:
    workflow_configs = build_sweep_workflow_configs(config)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for workflow_config in workflow_configs:
        try:
            output = run_research_workflow(
                config=workflow_config,
                provider=provider,
            )
            stats = output["stats"]

            results.append(
                {
                    "symbol": workflow_config.symbol,
                    "strategy": workflow_config.strategy,
                    "fast": workflow_config.fast,
                    "slow": workflow_config.slow,
                    "lookback": workflow_config.lookback,
                    "experiment_id": output["experiment_id"],
                    "normalized_path": str(output["normalized_path"]),
                    "features_path": str(output["features_path"]),
                    "return_pct": _safe_float(stats.get("Return [%]")),
                    "sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                    "max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
                    "stats": stats,
                }
            )
        except Exception as exc:
            error_record = {
                "symbol": workflow_config.symbol,
                "strategy": workflow_config.strategy,
                "fast": workflow_config.fast,
                "slow": workflow_config.slow,
                "lookback": workflow_config.lookback,
                "error": f"{type(exc).__name__}: {exc}",
            }
            errors.append(error_record)
            if not continue_on_error:
                raise

    leaderboard = pd.DataFrame(results)
    if not leaderboard.empty:
        sort_col = {
            "Return [%]": "return_pct",
            "Sharpe Ratio": "sharpe_ratio",
            "Max. Drawdown [%]": "max_drawdown_pct",
        }.get(config.rank_metric, "return_pct")

        ascending = sort_col == "max_drawdown_pct"
        leaderboard = leaderboard.sort_values(
            by=sort_col,
            ascending=ascending,
            na_position="last",
        ).reset_index(drop=True)

    return {
        "config": asdict(config),
        "results": results,
        "errors": errors,
        "leaderboard": leaderboard,
    }