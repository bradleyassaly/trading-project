from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.replay_assembly import ReplayAssemblyRequest
from trading_platform.research.replay_consumer import (
    ReplayConsumerRequest,
    ReplayConsumerResult,
    load_replay_consumer_input,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_corr(left: pd.Series, right: pd.Series, *, method: str) -> float | None:
    aligned = pd.concat([pd.to_numeric(left, errors="coerce"), pd.to_numeric(right, errors="coerce")], axis=1).dropna()
    if len(aligned.index) < 2:
        return None
    value = aligned.iloc[:, 0].corr(aligned.iloc[:, 1], method=method)
    if pd.isna(value):
        return None
    return float(value)


def _directional_accuracy(feature: pd.Series, target: pd.Series) -> float | None:
    aligned = pd.concat([pd.to_numeric(feature, errors="coerce"), pd.to_numeric(target, errors="coerce")], axis=1).dropna()
    if aligned.empty:
        return None
    return float((aligned.iloc[:, 0].gt(0) == aligned.iloc[:, 1].gt(0)).mean())


def _bucket_summary(feature: pd.Series, target: pd.Series) -> dict[str, Any]:
    aligned = pd.concat([pd.to_numeric(feature, errors="coerce"), pd.to_numeric(target, errors="coerce")], axis=1).dropna()
    if len(aligned.index) < 4:
        return {"bucket_count": 0, "top_bottom_spread": None}
    working = aligned.copy()
    working.columns = ["feature", "target"]
    working["bucket"] = pd.qcut(working["feature"].rank(method="first"), q=min(4, len(working.index)), labels=False, duplicates="drop")
    grouped = working.groupby("bucket", dropna=False)["target"].mean().reset_index()
    if grouped.empty:
        return {"bucket_count": 0, "top_bottom_spread": None}
    return {
        "bucket_count": int(len(grouped.index)),
        "top_bottom_spread": float(grouped["target"].iloc[-1] - grouped["target"].iloc[0]) if len(grouped.index) >= 2 else 0.0,
    }


@dataclass(frozen=True)
class ReplayEvaluationRequest:
    consumer_request: ReplayConsumerRequest
    feature_columns: list[str] = field(default_factory=list)
    target_columns: list[str] = field(default_factory=list)
    evaluation_name: str = "shared_replay_evaluation"


@dataclass(frozen=True)
class ReplayEvaluationMetric:
    feature_column: str
    target_column: str
    row_count: int
    pearson_correlation: float | None
    spearman_correlation: float | None
    directional_accuracy: float | None
    bucket_count: int
    top_bottom_spread: float | None


@dataclass(frozen=True)
class ReplayEvaluationResult:
    request: ReplayEvaluationRequest
    consumer_summary: dict[str, Any]
    metrics: list[ReplayEvaluationMetric]
    warnings: list[str]

    def to_summary(self) -> dict[str, Any]:
        return {
            "generated_at": _now_utc(),
            "evaluation_name": self.request.evaluation_name,
            "request": {
                "evaluation_name": self.request.evaluation_name,
                "feature_columns": list(self.request.feature_columns),
                "target_columns": list(self.request.target_columns),
                "consumer_request": {
                    "registry_path": str(self.request.consumer_request.assembly_request.registry_path)
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "dataset_keys": list(self.request.consumer_request.assembly_request.dataset_keys)
                    if self.request.consumer_request.assembly_request is not None
                    else [],
                    "providers": list(self.request.consumer_request.assembly_request.providers)
                    if self.request.consumer_request.assembly_request is not None
                    else [],
                    "dataset_names": list(self.request.consumer_request.assembly_request.dataset_names)
                    if self.request.consumer_request.assembly_request is not None
                    else [],
                    "symbols": list(self.request.consumer_request.assembly_request.symbols)
                    if self.request.consumer_request.assembly_request is not None
                    else [],
                    "intervals": list(self.request.consumer_request.assembly_request.intervals)
                    if self.request.consumer_request.assembly_request is not None
                    else [],
                    "start": self.request.consumer_request.assembly_request.start
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "end": self.request.consumer_request.assembly_request.end
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "alignment_mode": self.request.consumer_request.assembly_request.alignment_mode
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "anchor_dataset_key": self.request.consumer_request.assembly_request.anchor_dataset_key
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "tolerance": self.request.consumer_request.assembly_request.tolerance
                    if self.request.consumer_request.assembly_request is not None
                    else None,
                    "limit": self.request.consumer_request.limit,
                },
            },
            "consumer_summary": self.consumer_summary,
            "metrics": [asdict(metric) for metric in self.metrics],
            "warnings": list(self.warnings),
        }


def evaluate_replay_consumer(result: ReplayConsumerResult, *, request: ReplayEvaluationRequest) -> ReplayEvaluationResult:
    feature_columns = list(request.feature_columns or result.feature_columns)
    target_columns = list(request.target_columns or result.target_columns)
    warnings = list(result.warnings)
    metrics: list[ReplayEvaluationMetric] = []
    if not feature_columns:
        warnings.append("no_feature_columns_selected")
    if not target_columns:
        warnings.append("no_target_columns_selected")

    for feature_column in feature_columns:
        if feature_column not in result.frame.columns:
            continue
        for target_column in target_columns:
            if target_column not in result.frame.columns:
                continue
            aligned = pd.concat(
                [
                    pd.to_numeric(result.frame[feature_column], errors="coerce"),
                    pd.to_numeric(result.frame[target_column], errors="coerce"),
                ],
                axis=1,
            ).dropna()
            bucket = _bucket_summary(result.frame[feature_column], result.frame[target_column])
            metrics.append(
                ReplayEvaluationMetric(
                    feature_column=feature_column,
                    target_column=target_column,
                    row_count=int(len(aligned.index)),
                    pearson_correlation=_safe_corr(result.frame[feature_column], result.frame[target_column], method="pearson"),
                    spearman_correlation=_safe_corr(result.frame[feature_column], result.frame[target_column], method="spearman"),
                    directional_accuracy=_directional_accuracy(result.frame[feature_column], result.frame[target_column]),
                    bucket_count=int(bucket["bucket_count"]),
                    top_bottom_spread=bucket["top_bottom_spread"],
                )
            )
    if not metrics:
        warnings.append("no_evaluable_feature_target_pairs")
    return ReplayEvaluationResult(
        request=request,
        consumer_summary=result.to_summary(),
        metrics=metrics,
        warnings=warnings,
    )


def run_replay_evaluation(request: ReplayEvaluationRequest) -> ReplayEvaluationResult:
    consumer_result = load_replay_consumer_input(request.consumer_request)
    return evaluate_replay_consumer(consumer_result, request=request)


def write_replay_evaluation_artifacts(
    *,
    result: ReplayEvaluationResult,
    output_dir: str | Path,
) -> dict[str, str]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / "latest_replay_evaluation_summary.json"
    metrics_path = root / "latest_replay_evaluation_metrics.csv"
    summary_path.write_text(json.dumps(result.to_summary(), indent=2), encoding="utf-8")
    pd.DataFrame(
        [asdict(metric) for metric in result.metrics],
        columns=[
            "feature_column",
            "target_column",
            "row_count",
            "pearson_correlation",
            "spearman_correlation",
            "directional_accuracy",
            "bucket_count",
            "top_bottom_spread",
        ],
    ).to_csv(metrics_path, index=False)
    return {"summary_path": str(summary_path), "metrics_path": str(metrics_path)}


def build_replay_evaluation_request(
    *,
    registry_path: str | Path,
    dataset_keys: list[str] | None = None,
    providers: list[str] | None = None,
    dataset_names: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int | None = None,
    feature_columns: list[str] | None = None,
    target_columns: list[str] | None = None,
    evaluation_name: str = "shared_replay_evaluation",
) -> ReplayEvaluationRequest:
    return ReplayEvaluationRequest(
        consumer_request=ReplayConsumerRequest(
            assembly_request=ReplayAssemblyRequest(
                registry_path=registry_path,
                dataset_keys=list(dataset_keys or []),
                providers=list(providers or []),
                dataset_names=list(dataset_names or []),
                symbols=list(symbols or []),
                intervals=list(intervals or []),
                start=start,
                end=end,
                alignment_mode=alignment_mode,
                anchor_dataset_key=anchor_dataset_key,
                tolerance=tolerance,
            ),
            limit=limit,
        ),
        feature_columns=list(feature_columns or []),
        target_columns=list(target_columns or []),
        evaluation_name=evaluation_name,
    )
