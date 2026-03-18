from __future__ import annotations

from dataclasses import asdict
from statistics import mean, median
from typing import Any

import pandas as pd

from trading_platform.config.models import (
    ParameterSweepConfig,
    ResearchWorkflowConfig,
    WalkForwardConfig,
)
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.parameter_sweep_service import run_parameter_sweep
from trading_platform.services.research_service import run_research_workflow
from trading_platform.config.models import FeatureConfig, IngestConfig
from trading_platform.services.pipeline_service import run_research_prep_pipeline

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


def _prepare_feature_data(
    config: WalkForwardConfig,
    provider: BarDataProvider | None = None,
) -> tuple[str, list[pd.Timestamp]]:
    ingest_config = IngestConfig(
        symbol=config.symbol,
        start=config.start,
        end=config.end,
        interval=config.interval,
    )
    feature_config = FeatureConfig(
        symbol=config.symbol,
        feature_groups=config.feature_groups,
    )

    prep_outputs = run_research_prep_pipeline(
        ingest_config=ingest_config,
        feature_config=feature_config,
        provider=provider,
    )

    feature_path = str(prep_outputs["features_path"])

    df = pd.read_parquet(feature_path)
    if "timestamp" not in df.columns:
        raise ValueError("Feature data must include a timestamp column for walk-forward evaluation")

    timestamps = sorted(pd.to_datetime(df["timestamp"]).tolist())

    if len(timestamps) < config.min_required_bars:
        raise ValueError(
            f"Not enough bars for walk-forward evaluation: have {len(timestamps)}, "
            f"need at least {config.min_required_bars}"
        )

    return feature_path, timestamps


def _build_fixed_test_config(
    config: WalkForwardConfig,
    test_start: str,
    test_end: str,
) -> ResearchWorkflowConfig:
    return ResearchWorkflowConfig(
        symbol=config.symbol,
        start=test_start,
        end=test_end,
        interval=config.interval,
        feature_groups=config.feature_groups,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        cash=config.cash,
        commission=config.commission,
    )


def _build_train_sweep_config(
    config: WalkForwardConfig,
    train_start: str,
    train_end: str,
) -> ParameterSweepConfig:
    return ParameterSweepConfig(
        symbol=config.symbol,
        start=train_start,
        end=train_end,
        interval=config.interval,
        feature_groups=config.feature_groups,
        strategy=config.strategy,
        fast_values=config.fast_values,
        slow_values=config.slow_values,
        lookback_values=config.lookback_values,
        cash=config.cash,
        commission=config.commission,
        rank_metric=config.rank_metric,
    )


def _build_optimized_test_config(
    config: WalkForwardConfig,
    selected_row: pd.Series,
    test_start: str,
    test_end: str,
) -> ResearchWorkflowConfig:
    fast = None
    slow = None
    lookback = None

    if "fast" in selected_row and pd.notna(selected_row["fast"]):
        fast = int(selected_row["fast"])
    if "slow" in selected_row and pd.notna(selected_row["slow"]):
        slow = int(selected_row["slow"])
    if "lookback" in selected_row and pd.notna(selected_row["lookback"]):
        lookback = int(selected_row["lookback"])

    return ResearchWorkflowConfig(
        symbol=config.symbol,
        start=test_start,
        end=test_end,
        interval=config.interval,
        feature_groups=config.feature_groups,
        strategy=config.strategy,
        fast=fast,
        slow=slow,
        lookback=lookback,
        cash=config.cash,
        commission=config.commission,
    )


def _summarize_results(results_df: pd.DataFrame) -> dict[str, object]:
    if results_df.empty:
        return {
            "window_count": 0,
            "mean_return_pct": None,
            "median_return_pct": None,
            "mean_sharpe_ratio": None,
            "worst_drawdown_pct": None,
        }

    returns = [x for x in results_df["test_return_pct"].tolist() if pd.notna(x)]
    sharpes = [x for x in results_df["test_sharpe_ratio"].tolist() if pd.notna(x)]
    drawdowns = [x for x in results_df["test_max_drawdown_pct"].tolist() if pd.notna(x)]

    return {
        "window_count": int(len(results_df)),
        "mean_return_pct": mean(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "mean_sharpe_ratio": mean(sharpes) if sharpes else None,
        "worst_drawdown_pct": min(drawdowns) if drawdowns else None,
    }


def _run_fixed_walk_forward(
    config: WalkForwardConfig,
    windows: list[dict[str, pd.Timestamp]],
    provider: BarDataProvider | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for i, window in enumerate(windows, start=1):
        test_start = str(pd.Timestamp(window["test_start"]).date())
        test_end = str(pd.Timestamp(window["test_end"]).date())

        test_config = _build_fixed_test_config(
            config=config,
            test_start=test_start,
            test_end=test_end,
        )

        output = run_research_workflow(config=test_config, provider=provider)
        stats = output["stats"]

        results.append(
            {
                "window_index": i,
                "mode": "fixed",
                "train_start": str(pd.Timestamp(window["train_start"]).date()),
                "train_end": str(pd.Timestamp(window["train_end"]).date()),
                "test_start": test_start,
                "test_end": test_end,
                "selected_fast": config.fast,
                "selected_slow": config.slow,
                "selected_lookback": config.lookback,
                "train_rank_metric_value": None,
                "test_experiment_id": output["experiment_id"],
                "test_return_pct": _safe_float(stats.get("Return [%]")),
                "test_sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                "test_max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
            }
        )

    return results


def _run_optimized_walk_forward(
    config: WalkForwardConfig,
    windows: list[dict[str, pd.Timestamp]],
    provider: BarDataProvider | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    rank_metric_to_col = {
        "Return [%]": "return_pct",
        "Sharpe Ratio": "sharpe_ratio",
        "Max. Drawdown [%]": "max_drawdown_pct",
    }

    rank_col = rank_metric_to_col.get(config.rank_metric, "return_pct")

    for i, window in enumerate(windows, start=1):
        train_start = str(pd.Timestamp(window["train_start"]).date())
        train_end = str(pd.Timestamp(window["train_end"]).date())
        test_start = str(pd.Timestamp(window["test_start"]).date())
        test_end = str(pd.Timestamp(window["test_end"]).date())

        sweep_config = _build_train_sweep_config(
            config=config,
            train_start=train_start,
            train_end=train_end,
        )

        sweep_outputs = run_parameter_sweep(
            config=sweep_config,
            provider=provider,
            continue_on_error=True,
        )

        leaderboard = sweep_outputs["leaderboard"]
        if leaderboard.empty:
            results.append(
                {
                    "window_index": i,
                    "mode": "optimize",
                    "train_start": train_start,
                    "train_end": train_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "selected_fast": None,
                    "selected_slow": None,
                    "selected_lookback": None,
                    "train_rank_metric_value": None,
                    "test_experiment_id": None,
                    "test_return_pct": None,
                    "test_sharpe_ratio": None,
                    "test_max_drawdown_pct": None,
                }
            )
            continue

        best = leaderboard.iloc[0]

        test_config = _build_optimized_test_config(
            config=config,
            selected_row=best,
            test_start=test_start,
            test_end=test_end,
        )

        output = run_research_workflow(config=test_config, provider=provider)
        stats = output["stats"]

        results.append(
            {
                "window_index": i,
                "mode": "optimize",
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "selected_fast": int(best["fast"]) if "fast" in best and pd.notna(best["fast"]) else None,
                "selected_slow": int(best["slow"]) if "slow" in best and pd.notna(best["slow"]) else None,
                "selected_lookback": int(best["lookback"]) if "lookback" in best and pd.notna(best["lookback"]) else None,
                "train_rank_metric_value": _safe_float(best.get(rank_col)),
                "test_experiment_id": output["experiment_id"],
                "test_return_pct": _safe_float(stats.get("Return [%]")),
                "test_sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                "test_max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
            }
        )

    return results


def run_walk_forward_evaluation(
    config: WalkForwardConfig,
    provider: BarDataProvider | None = None,
) -> dict[str, Any]:
    feature_path, timestamps = _prepare_feature_data(
        config=config,
        provider=provider,
    )

    windows = build_walk_forward_windows(
        timestamps=timestamps,
        train_window_bars=config.train_window_bars,
        test_window_bars=config.test_window_bars,
        step_bars=config.step_bars,
    )

    if config.walk_forward_mode == "fixed":
        results = _run_fixed_walk_forward(
            config=config,
            windows=windows,
            provider=provider,
        )
    elif config.walk_forward_mode == "optimize":
        results = _run_optimized_walk_forward(
            config=config,
            windows=windows,
            provider=provider,
        )
    else:
        raise ValueError(f"Unsupported walk_forward_mode: {config.walk_forward_mode}")

    results_df = pd.DataFrame(results)
    summary = _summarize_results(results_df)

    return {
        "config": asdict(config),
        "feature_path": feature_path,
        "prep_experiment_id": None,
        "windows": results,
        "results_df": results_df,
        "summary": summary,
    }