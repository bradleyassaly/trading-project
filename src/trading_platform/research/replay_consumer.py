from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.replay_assembly import (
    ReplayAssemblyRequest,
    ReplayAssemblyResult,
    assemble_replay_dataset,
)


def _classify_columns(columns: list[str]) -> tuple[list[str], list[str], list[str]]:
    identity_columns = [column for column in ("event_time", "symbol", "interval") if column in columns]
    target_columns = [column for column in columns if "__target_" in column or column.startswith("target_")]
    feature_columns = [column for column in columns if column not in identity_columns and column not in target_columns]
    return identity_columns, feature_columns, target_columns


def _coverage_summary(frame: pd.DataFrame, columns: list[str]) -> dict[str, float]:
    if frame.empty:
        return {column: 0.0 for column in columns}
    row_count = max(len(frame.index), 1)
    return {
        column: round(float(frame[column].notna().sum()) / row_count, 6)
        for column in columns
        if column in frame.columns
    }


@dataclass(frozen=True)
class ReplayConsumerRequest:
    assembly_request: ReplayAssemblyRequest | None = None
    assembly_dataset_path: str | Path | None = None
    assembly_summary_path: str | Path | None = None
    columns: list[str] = field(default_factory=list)
    limit: int | None = None


@dataclass(frozen=True)
class ReplayConsumerResult:
    frame: pd.DataFrame
    metadata: dict[str, Any]
    feature_columns: list[str]
    target_columns: list[str]
    identity_columns: list[str]
    warnings: list[str]

    def to_summary(self) -> dict[str, Any]:
        return {
            "metadata": dict(self.metadata),
            "feature_columns": list(self.feature_columns),
            "target_columns": list(self.target_columns),
            "identity_columns": list(self.identity_columns),
            "row_count": int(len(self.frame.index)),
            "warnings": list(self.warnings),
        }


def _load_materialized_assembly(
    *,
    dataset_path: str | Path,
    summary_path: str | Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = pd.read_parquet(dataset_path)
    summary: dict[str, Any] = {}
    if summary_path is not None and Path(summary_path).exists():
        summary = dict(json.loads(Path(summary_path).read_text(encoding="utf-8")) or {})
    return frame, summary


def _consume_assembly_result(
    result: ReplayAssemblyResult,
    *,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> ReplayConsumerResult:
    frame = result.frame.copy()
    warnings: list[str] = []
    if columns:
        keep = [column for column in columns if column in frame.columns]
        frame = frame.loc[:, keep]
    if limit is not None:
        frame = frame.head(max(int(limit), 0))
    identity_columns, feature_columns, target_columns = _classify_columns(list(frame.columns))
    if len(result.components) > 1:
        sparse_targets = _coverage_summary(frame, target_columns)
        sparse_features = _coverage_summary(frame, feature_columns)
        if any(value < 0.5 for value in sparse_features.values()):
            warnings.append("sparse_feature_alignment")
        if target_columns and any(value < 0.5 for value in sparse_targets.values()):
            warnings.append("sparse_target_coverage")
    metadata = {
        **dict(result.metadata),
        "providers": sorted({component.provider for component in result.components}),
        "datasets": [asdict(component) for component in result.components],
        "alignment_mode": result.request.alignment_mode,
        "time_bounds": {"start": result.request.start, "end": result.request.end},
        "registry_path": str(result.request.registry_path),
        "feature_coverage": _coverage_summary(frame, feature_columns),
        "target_coverage": _coverage_summary(frame, target_columns),
    }
    return ReplayConsumerResult(
        frame=frame,
        metadata=metadata,
        feature_columns=feature_columns,
        target_columns=target_columns,
        identity_columns=identity_columns,
        warnings=warnings,
    )


def load_replay_consumer_input(request: ReplayConsumerRequest) -> ReplayConsumerResult:
    if request.assembly_request is not None:
        return _consume_assembly_result(
            assemble_replay_dataset(request.assembly_request),
            columns=list(request.columns),
            limit=request.limit,
        )
    if request.assembly_dataset_path is None:
        raise ValueError("Replay consumer requires either an assembly_request or an assembly_dataset_path")

    frame, summary = _load_materialized_assembly(
        dataset_path=request.assembly_dataset_path,
        summary_path=request.assembly_summary_path,
    )
    if request.columns:
        keep = [column for column in request.columns if column in frame.columns]
        frame = frame.loc[:, keep]
    if request.limit is not None:
        frame = frame.head(max(int(request.limit), 0))
    identity_columns, feature_columns, target_columns = _classify_columns(list(frame.columns))
    metadata = {
        "assembly_source": "materialized_artifact",
        "assembly_dataset_path": str(request.assembly_dataset_path),
        "assembly_summary_path": str(request.assembly_summary_path) if request.assembly_summary_path else None,
        "providers": list(summary.get("metadata", {}).get("providers", [])),
        "resolved_dataset_keys": list(summary.get("metadata", {}).get("resolved_dataset_keys", [])),
        "alignment_mode": summary.get("request", {}).get("alignment_mode"),
        "time_bounds": {
            "start": summary.get("request", {}).get("start"),
            "end": summary.get("request", {}).get("end"),
        },
        "feature_coverage": _coverage_summary(frame, feature_columns),
        "target_coverage": _coverage_summary(frame, target_columns),
        "assembly_summary": summary,
    }
    return ReplayConsumerResult(
        frame=frame,
        metadata=metadata,
        feature_columns=feature_columns,
        target_columns=target_columns,
        identity_columns=identity_columns,
        warnings=[],
    )


def write_replay_consumer_summary(
    *,
    result: ReplayConsumerResult,
    output_path: str | Path,
) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_summary(), indent=2), encoding="utf-8")
    return str(path)
