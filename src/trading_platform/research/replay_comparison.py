from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.replay_evaluation import (
    ReplayEvaluationRequest,
    ReplayEvaluationResult,
    build_replay_evaluation_request,
    run_replay_evaluation,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _composite_score(
    *,
    row_count: int,
    pearson_correlation: float | None,
    spearman_correlation: float | None,
    directional_accuracy: float | None,
    top_bottom_spread: float | None,
    min_row_count: int,
) -> float:
    correlation_component = max(
        abs(float(value))
        for value in (spearman_correlation, pearson_correlation, 0.0)
        if value is not None
    )
    directional_component = 0.0
    if directional_accuracy is not None:
        directional_component = max(float(directional_accuracy) - 0.5, 0.0) * 2.0
    spread_component = min(abs(float(top_bottom_spread or 0.0)), 1.0)
    coverage_component = min(float(row_count) / max(float(min_row_count), 1.0), 1.0)
    return round(
        0.4 * correlation_component
        + 0.25 * directional_component
        + 0.2 * spread_component
        + 0.15 * coverage_component,
        6,
    )


def _load_summary(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _providers_from_summary(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("consumer_summary", {}).get("metadata", {}) or {}
    request = summary.get("request", {}).get("consumer_request", {}) or {}
    providers = list(metadata.get("providers") or request.get("providers") or [])
    if providers:
        return sorted({str(provider) for provider in providers})
    datasets = metadata.get("datasets") or []
    return sorted({str(item.get("provider")) for item in datasets if item.get("provider")})


def _dataset_keys_from_summary(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("consumer_summary", {}).get("metadata", {}) or {}
    datasets = metadata.get("datasets") or []
    keys = [str(item.get("dataset_key")) for item in datasets if item.get("dataset_key")]
    if keys:
        return keys
    request = summary.get("request", {}).get("consumer_request", {}) or {}
    return list(request.get("dataset_keys") or [])


@dataclass(frozen=True)
class ReplayComparisonRequest:
    registry_path: str | Path | None = None
    evaluation_summary_paths: list[str] = field(default_factory=list)
    dataset_keys: list[str] = field(default_factory=list)
    providers: list[str] = field(default_factory=list)
    dataset_names: list[str] = field(default_factory=list)
    symbols: list[str] = field(default_factory=list)
    intervals: list[str] = field(default_factory=list)
    start: str | None = None
    end: str | None = None
    alignment_modes: list[str] = field(default_factory=lambda: ["outer_union"])
    anchor_dataset_key: str | None = None
    tolerance: str | None = None
    limit: int | None = None
    feature_columns: list[str] = field(default_factory=list)
    target_columns: list[str] = field(default_factory=list)
    comparison_mode: str = "provider"
    min_row_count: int = 25
    max_candidates: int = 10
    comparison_name: str = "shared_replay_comparison"


@dataclass(frozen=True)
class ReplayComparisonSlice:
    evaluation_name: str
    summary_path: str | None
    providers: list[str]
    dataset_keys: list[str]
    alignment_mode: str
    feature_column: str
    target_column: str
    row_count: int
    pearson_correlation: float | None
    spearman_correlation: float | None
    directional_accuracy: float | None
    top_bottom_spread: float | None
    composite_score: float
    eligible: bool
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReplayComparisonResult:
    request: ReplayComparisonRequest
    rankings: list[ReplayComparisonSlice]
    candidate_slices: list[ReplayComparisonSlice]
    excluded_slices: list[ReplayComparisonSlice]
    warnings: list[str] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        return {
            "generated_at": _now_utc(),
            "comparison_name": self.request.comparison_name,
            "request": {
                "comparison_name": self.request.comparison_name,
                "comparison_mode": self.request.comparison_mode,
                "registry_path": str(self.request.registry_path) if self.request.registry_path is not None else None,
                "evaluation_summary_paths": list(self.request.evaluation_summary_paths),
                "dataset_keys": list(self.request.dataset_keys),
                "providers": list(self.request.providers),
                "dataset_names": list(self.request.dataset_names),
                "symbols": list(self.request.symbols),
                "intervals": list(self.request.intervals),
                "start": self.request.start,
                "end": self.request.end,
                "alignment_modes": list(self.request.alignment_modes),
                "anchor_dataset_key": self.request.anchor_dataset_key,
                "tolerance": self.request.tolerance,
                "limit": self.request.limit,
                "feature_columns": list(self.request.feature_columns),
                "target_columns": list(self.request.target_columns),
                "min_row_count": self.request.min_row_count,
                "max_candidates": self.request.max_candidates,
            },
            "ranking_count": len(self.rankings),
            "candidate_count": len(self.candidate_slices),
            "excluded_count": len(self.excluded_slices),
            "rankings": [asdict(item) for item in self.rankings],
            "candidate_slices": [asdict(item) for item in self.candidate_slices],
            "excluded_slices": [asdict(item) for item in self.excluded_slices],
            "warnings": list(self.warnings),
        }


def _slice_from_summary_metric(
    *,
    summary: dict[str, Any],
    metric: dict[str, Any],
    request: ReplayComparisonRequest,
    summary_path: str | None = None,
) -> ReplayComparisonSlice:
    row_count = int(metric.get("row_count") or 0)
    pearson = metric.get("pearson_correlation")
    spearman = metric.get("spearman_correlation")
    directional = metric.get("directional_accuracy")
    spread = metric.get("top_bottom_spread")
    composite = _composite_score(
        row_count=row_count,
        pearson_correlation=pearson,
        spearman_correlation=spearman,
        directional_accuracy=directional,
        top_bottom_spread=spread,
        min_row_count=request.min_row_count,
    )
    eligible = row_count >= request.min_row_count
    warnings = list(summary.get("warnings") or [])
    if not eligible:
        warnings = [*warnings, "below_min_row_count"]
    return ReplayComparisonSlice(
        evaluation_name=str(summary.get("evaluation_name") or request.comparison_name),
        summary_path=summary_path,
        providers=_providers_from_summary(summary),
        dataset_keys=_dataset_keys_from_summary(summary),
        alignment_mode=str(
            summary.get("request", {}).get("consumer_request", {}).get("alignment_mode")
            or summary.get("consumer_summary", {}).get("metadata", {}).get("alignment_mode")
            or "unknown"
        ),
        feature_column=str(metric.get("feature_column") or ""),
        target_column=str(metric.get("target_column") or ""),
        row_count=row_count,
        pearson_correlation=float(pearson) if pearson is not None else None,
        spearman_correlation=float(spearman) if spearman is not None else None,
        directional_accuracy=float(directional) if directional is not None else None,
        top_bottom_spread=float(spread) if spread is not None else None,
        composite_score=composite,
        eligible=eligible,
        warnings=warnings,
    )


def _build_generated_evaluation_requests(request: ReplayComparisonRequest) -> list[ReplayEvaluationRequest]:
    if request.registry_path is None:
        raise ValueError("registry_path is required when evaluation_summary_paths are not provided")

    evaluation_requests: list[ReplayEvaluationRequest] = []
    if request.comparison_mode == "provider" and request.providers:
        for provider in request.providers:
            for alignment_mode in request.alignment_modes:
                evaluation_requests.append(
                    build_replay_evaluation_request(
                        registry_path=request.registry_path,
                        providers=[provider],
                        dataset_names=request.dataset_names,
                        symbols=request.symbols,
                        intervals=request.intervals,
                        start=request.start,
                        end=request.end,
                        alignment_mode=alignment_mode,
                        anchor_dataset_key=request.anchor_dataset_key,
                        tolerance=request.tolerance,
                        limit=request.limit,
                        feature_columns=request.feature_columns,
                        target_columns=request.target_columns,
                        evaluation_name=f"{provider}_{alignment_mode}",
                    )
                )
    elif request.comparison_mode == "dataset" and request.dataset_keys:
        for dataset_key in request.dataset_keys:
            for alignment_mode in request.alignment_modes:
                evaluation_requests.append(
                    build_replay_evaluation_request(
                        registry_path=request.registry_path,
                        dataset_keys=[dataset_key],
                        symbols=request.symbols,
                        intervals=request.intervals,
                        start=request.start,
                        end=request.end,
                        alignment_mode=alignment_mode,
                        anchor_dataset_key=request.anchor_dataset_key,
                        tolerance=request.tolerance,
                        limit=request.limit,
                        feature_columns=request.feature_columns,
                        target_columns=request.target_columns,
                        evaluation_name=f"{dataset_key}_{alignment_mode}",
                    )
                )
    else:
        for alignment_mode in request.alignment_modes:
            evaluation_requests.append(
                build_replay_evaluation_request(
                    registry_path=request.registry_path,
                    dataset_keys=request.dataset_keys,
                    providers=request.providers,
                    dataset_names=request.dataset_names,
                    symbols=request.symbols,
                    intervals=request.intervals,
                    start=request.start,
                    end=request.end,
                    alignment_mode=alignment_mode,
                    anchor_dataset_key=request.anchor_dataset_key,
                    tolerance=request.tolerance,
                    limit=request.limit,
                    feature_columns=request.feature_columns,
                    target_columns=request.target_columns,
                    evaluation_name=f"{request.comparison_name}_{alignment_mode}",
                )
            )
    return evaluation_requests


def _load_source_summaries(request: ReplayComparisonRequest) -> tuple[list[tuple[str | None, dict[str, Any]]], list[str]]:
    warnings: list[str] = []
    if request.evaluation_summary_paths:
        summaries: list[tuple[str | None, dict[str, Any]]] = []
        for path in request.evaluation_summary_paths:
            try:
                summaries.append((str(path), _load_summary(path)))
            except FileNotFoundError:
                warnings.append(f"missing_evaluation_summary:{path}")
        return summaries, warnings
    evaluation_requests = _build_generated_evaluation_requests(request)
    summaries = []
    for item in evaluation_requests:
        try:
            summaries.append((None, run_replay_evaluation(item).to_summary()))
        except (KeyError, ValueError) as exc:
            warnings.append(f"skipped_evaluation:{item.evaluation_name}:{exc}")
    return summaries, warnings


def run_replay_comparison(request: ReplayComparisonRequest) -> ReplayComparisonResult:
    source_summaries, warnings = _load_source_summaries(request)
    rankings: list[ReplayComparisonSlice] = []
    for summary_path, summary in source_summaries:
        metrics = list(summary.get("metrics") or [])
        if not metrics:
            warnings.append(f"no_metrics:{summary.get('evaluation_name', 'unknown')}")
        for metric in metrics:
            rankings.append(
                _slice_from_summary_metric(
                    summary=summary,
                    metric=metric,
                    request=request,
                    summary_path=summary_path,
                )
            )

    rankings = sorted(
        rankings,
        key=lambda item: (
            -item.composite_score,
            -item.row_count,
            item.providers,
            item.alignment_mode,
            item.feature_column,
            item.target_column,
        ),
    )
    candidate_slices = [item for item in rankings if item.eligible][: max(int(request.max_candidates), 0)]
    excluded_slices = [item for item in rankings if not item.eligible]
    if not candidate_slices and rankings:
        warnings.append("no_candidate_slices_met_thresholds")
    return ReplayComparisonResult(
        request=request,
        rankings=rankings,
        candidate_slices=candidate_slices,
        excluded_slices=excluded_slices,
        warnings=warnings,
    )


def write_replay_comparison_artifacts(
    *,
    result: ReplayComparisonResult,
    output_dir: str | Path,
) -> dict[str, str]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / "latest_replay_comparison_summary.json"
    rankings_path = root / "latest_replay_comparison_rankings.csv"
    candidates_path = root / "latest_replay_candidate_slices.json"
    summary_path.write_text(json.dumps(result.to_summary(), indent=2), encoding="utf-8")
    pd.DataFrame(
        [asdict(item) for item in result.rankings],
        columns=[
            "evaluation_name",
            "summary_path",
            "providers",
            "dataset_keys",
            "alignment_mode",
            "feature_column",
            "target_column",
            "row_count",
            "pearson_correlation",
            "spearman_correlation",
            "directional_accuracy",
            "top_bottom_spread",
            "composite_score",
            "eligible",
            "warnings",
        ],
    ).to_csv(rankings_path, index=False)
    candidates_path.write_text(
        json.dumps([asdict(item) for item in result.candidate_slices], indent=2),
        encoding="utf-8",
    )
    return {
        "summary_path": str(summary_path),
        "rankings_path": str(rankings_path),
        "candidates_path": str(candidates_path),
    }
