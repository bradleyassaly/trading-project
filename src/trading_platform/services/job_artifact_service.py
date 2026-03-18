from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.universe_summary_service import (
    build_universe_aggregate_summary,
    build_universe_leaderboard,
)
from trading_platform.settings import JOB_ARTIFACTS_DIR


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_job_summary(
    *,
    config: ResearchWorkflowConfig,
    symbols: list[str],
    outputs: dict[str, Any],
    leaderboard_csv_path: str | None = None,
) -> dict[str, Any]:
    results = outputs.get("results", {})
    errors = outputs.get("errors", {})

    successful_symbols = sorted(results.keys())
    failed_symbols = sorted(errors.keys())

    run_results: dict[str, Any] = {}
    for symbol, result in results.items():
        stats = result.get("stats", {})
        run_results[symbol] = {
            "normalized_path": str(result.get("normalized_path", "")),
            "features_path": str(result.get("features_path", "")),
            "experiment_id": result.get("experiment_id"),
            "metrics": {
                "return_pct": _safe_float(stats.get("Return [%]")),
                "sharpe_ratio": _safe_float(stats.get("Sharpe Ratio")),
                "max_drawdown_pct": _safe_float(stats.get("Max. Drawdown [%]")),
            },
        }

    leaderboard = build_universe_leaderboard(outputs)
    aggregate_summary = build_universe_aggregate_summary(
        leaderboard=leaderboard,
        error_count=len(errors),
    )

    return {
        "run_timestamp_utc": datetime.now(UTC).isoformat(),
        "config": {
            "symbol": config.symbol,
            "start": config.start,
            "end": config.end,
            "interval": config.interval,
            "feature_groups": config.feature_groups,
            "strategy": config.strategy,
            "fast": config.fast,
            "slow": config.slow,
            "lookback": config.lookback,
            "cash": config.cash,
            "commission": config.commission,
        },
        "symbols_requested": symbols,
        "successful_symbols": successful_symbols,
        "failed_symbols": failed_symbols,
        "results": run_results,
        "errors": errors,
        "summary": {
            "requested_count": len(symbols),
            "success_count": len(successful_symbols),
            "failure_count": len(failed_symbols),
        },
        "aggregate_summary": aggregate_summary,
        "leaderboard_csv_path": leaderboard_csv_path,
    }


def make_job_artifact_stem() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"job_{timestamp}"


def save_job_summary(summary: dict[str, Any], stem: str | None = None) -> Path:
    stem = stem or make_job_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return path


def save_leaderboard_csv(
    leaderboard: pd.DataFrame,
    stem: str | None = None,
) -> Path:
    stem = stem or make_job_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.leaderboard.csv"
    leaderboard.to_csv(path, index=False)
    return path