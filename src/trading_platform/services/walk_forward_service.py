from __future__ import annotations

from dataclasses import asdict
from statistics import mean, median
from typing import Any

import pandas as pd

from trading_platform.config.models import ResearchWorkflowConfig, WalkForwardConfig
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.research_service import run_research_workflow


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_walk_forward_windows(
    timestamps: list[pd.Timestamp],
    train_window_bars: int,
    test_window_bars: int,
    step_bars: int,
) -> list[dict[str, pd.Timestamp]]:
    windows: list[dict[str, pd.Timestamp]] = []

    n = len(timestamps)
    train_end_idx = train_window_bars - 1
    test_end_idx = train_end_idx + test_window_bars

    while test_end_idx < n:
        train_start = timestamps[train_end_idx - train_window_bars + 1]
        train_end = timestamps[train_end_idx]
        test_start = timestamps[train_end_idx + 1]
        test_end = timestamps[test_end_idx]

        windows.append(
            {
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            }
        )

        train_end_idx += step_bars
        test_end_idx = train_end_idx + test_window_bars

    return windows


def run_walk_forward_evaluation(
    config: WalkForwardConfig,
    provider: BarDataProvider | None = None,
) -> dict[str, Any]:
    prep_config = ResearchWorkflowConfig(
        symbol=config.symbol,
        start=config.start,
        end=config.end,
        interval=config.interval,
        feature_groups=config.feature_groups,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        cash=config.cash,
        commission=config.commission,
    )

    prep_outputs = run_research_workflow(config=prep_config, provider=provider)

    feature_path = prep_outputs["features_path"]
    df = pd.read_parquet(feature_path)

    if "timestamp" not in df.columns:
        raise ValueError("Feature data must include a timestamp column for walk-forward evaluation")

    timestamps = sorted(pd.to_datetime(df["timestamp"]).tolist())

    if len(timestamps) < config.min_required_bars:
        raise ValueError(
            f"Not enough bars for walk-forward evaluation: have {len(timestamps)}, "
            f"need at least {config.min_required_bars}"
        )

    windows = build_walk_forward_windows(
        timestamps=timestamps,
        train_window_bars=config.train_window_bars,
        test_window_bars=config.test_window_bars,
        step_bars=config.step_bars,
    )

    results: list[dict[str, Any]] = []

    for i, window in enumerate(windows, start=1):
        workflow_config = ResearchWorkflowConfig(
            symbol=config.symbol,
            start=str(pd.Timestamp(window["test_start"]).date()),
            end=str(pd.Timestamp(window["test_end"]).date()),
            interval=config.interval,
            feature_groups=config.feature_groups,
            strategy=config.strategy,
            fast=config.fast,
            slow=config.slow,
            lookback=config.lookback,
            cash=config.cash,
            commission=config.commission,
        )

        output = run_research_workflow(config=workflow_config, provider=provider)
        stats = output["stats"]

        results.append(
            {
                "window_index": i,
                "train_start": str(pd.Timestamp(window["train_start"]).date()),
                "train_end": str(pd.Timestamp(window["train_end"]).date()),
                "test_start": str(pd.Timestamp(window["test_start"]).date()),
                "test_end": str(pd.Timestamp(window["test_end"]).date()),
                "experiment_id": output["experiment_id"],
                "return_pct": _safe_float(stats.get("Return [%]")),
                "sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                "max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
            }
        )

    results_df = pd.DataFrame(results)

    summary = {
        "window_count": len(results),
        "mean_return_pct": mean(results_df["return_pct"]) if not results_df.empty else None,
        "median_return_pct": median(results_df["return_pct"]) if not results_df.empty else None,
        "mean_sharpe_ratio": mean(results_df["sharpe_ratio"]) if not results_df.empty else None,
        "worst_drawdown_pct": min(results_df["max_drawdown_pct"]) if not results_df.empty else None,
    }

    return {
        "config": asdict(config),
        "feature_path": str(feature_path),
        "prep_experiment_id": prep_outputs["experiment_id"],
        "windows": results,
        "results_df": results_df,
        "summary": summary,
    }