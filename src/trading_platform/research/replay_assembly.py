from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.dataset_reader import (
    ResearchDatasetDescriptor,
    ResearchDatasetLoadResult,
    ResearchDatasetReadRequest,
    list_research_datasets,
    load_research_dataset,
)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def _identity_columns(frame: pd.DataFrame) -> list[str]:
    return [column for column in ("event_time", "symbol", "interval") if column in frame.columns]


def _non_identity_columns(frame: pd.DataFrame) -> list[str]:
    return [column for column in frame.columns if column not in _identity_columns(frame)]


def _namespace_prefix(descriptor: ResearchDatasetDescriptor) -> str:
    return f"{_slug(descriptor.provider)}__{_slug(descriptor.dataset_name)}__"


def _normalize_component(result: ResearchDatasetLoadResult) -> tuple[pd.DataFrame, dict[str, str]]:
    frame = result.frame.copy()
    descriptor = result.descriptor
    rename_map: dict[str, str] = {}
    time_column = descriptor.time_column
    if time_column and time_column in frame.columns and time_column != "event_time":
        frame = frame.rename(columns={time_column: "event_time"})
        rename_map[time_column] = "event_time"
    if "event_time" in frame.columns:
        frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    identity = _identity_columns(frame)
    prefix = _namespace_prefix(descriptor)
    for column in list(frame.columns):
        if column in identity:
            continue
        namespaced = f"{prefix}{column}"
        frame = frame.rename(columns={column: namespaced})
        rename_map[column] = namespaced
    sort_columns = _identity_columns(frame)
    if sort_columns:
        frame = frame.sort_values(sort_columns).reset_index(drop=True)
    return frame, rename_map


def _common_join_columns(left: pd.DataFrame, right: pd.DataFrame) -> list[str]:
    join_columns = ["event_time"]
    for candidate in ("symbol", "interval"):
        if candidate in left.columns and candidate in right.columns:
            join_columns.append(candidate)
    return [column for column in join_columns if column in left.columns and column in right.columns]


def _merge_outer(left: pd.DataFrame, right: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    join_columns = _common_join_columns(left, right)
    if not join_columns:
        raise ValueError("Replay assembly requires at least one shared identity column")
    merged = left.merge(right, how="outer", on=join_columns, sort=True)
    return merged, join_columns


def _merge_anchor(
    anchor: pd.DataFrame,
    component: pd.DataFrame,
    *,
    tolerance: str | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    join_columns = _common_join_columns(anchor, component)
    if not join_columns:
        raise ValueError("Replay assembly anchor mode requires at least one shared identity column")
    by_columns = [column for column in join_columns if column != "event_time"]
    if "event_time" not in join_columns:
        return anchor.merge(component, how="left", on=join_columns, sort=False), join_columns

    anchor_sorted = anchor.sort_values(join_columns).reset_index(drop=True)
    component_sorted = component.sort_values(join_columns).reset_index(drop=True)
    merged = pd.merge_asof(
        anchor_sorted,
        component_sorted,
        on="event_time",
        by=by_columns or None,
        direction="backward",
        tolerance=pd.Timedelta(tolerance) if tolerance else None,
    )
    return merged, join_columns


@dataclass(frozen=True)
class ReplayComponentSummary:
    dataset_key: str
    provider: str
    asset_class: str
    dataset_name: str
    row_count: int
    time_column: str | None
    identity_columns: list[str] = field(default_factory=list)
    namespaced_columns: list[str] = field(default_factory=list)
    applied_filters: dict[str, Any] = field(default_factory=dict)
    manifest_references: dict[str, Any] = field(default_factory=dict)
    health_references: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplayAssemblyRequest:
    registry_path: str | Path
    dataset_keys: list[str] = field(default_factory=list)
    providers: list[str] = field(default_factory=list)
    asset_class: str | None = None
    dataset_names: list[str] = field(default_factory=list)
    symbols: list[str] = field(default_factory=list)
    intervals: list[str] = field(default_factory=list)
    start: str | None = None
    end: str | None = None
    alignment_mode: str = "outer_union"
    anchor_dataset_key: str | None = None
    tolerance: str | None = None


@dataclass(frozen=True)
class ReplayAssemblyResult:
    request: ReplayAssemblyRequest
    frame: pd.DataFrame
    components: list[ReplayComponentSummary]
    join_plan: list[dict[str, Any]]
    metadata: dict[str, Any]

    def to_summary(self) -> dict[str, Any]:
        return {
            "request": {
                "registry_path": str(self.request.registry_path),
                "dataset_keys": list(self.request.dataset_keys),
                "providers": list(self.request.providers),
                "asset_class": self.request.asset_class,
                "dataset_names": list(self.request.dataset_names),
                "symbols": list(self.request.symbols),
                "intervals": list(self.request.intervals),
                "start": self.request.start,
                "end": self.request.end,
                "alignment_mode": self.request.alignment_mode,
                "anchor_dataset_key": self.request.anchor_dataset_key,
                "tolerance": self.request.tolerance,
            },
            "row_count": int(len(self.frame.index)),
            "columns": list(self.frame.columns),
            "components": [asdict(component) for component in self.components],
            "join_plan": list(self.join_plan),
            "metadata": dict(self.metadata),
        }


def resolve_replay_component_descriptors(request: ReplayAssemblyRequest) -> list[ResearchDatasetDescriptor]:
    descriptors: list[ResearchDatasetDescriptor] = []
    seen: set[str] = set()
    if request.dataset_keys:
        for dataset_key in request.dataset_keys:
            result = load_research_dataset(
                ResearchDatasetReadRequest(
                    registry_path=request.registry_path,
                    dataset_key=dataset_key,
                )
            )
            if result.descriptor.dataset_key not in seen:
                descriptors.append(result.descriptor)
                seen.add(result.descriptor.dataset_key)
        return descriptors

    provider_filter = {provider for provider in request.providers if provider}
    name_filter = {name for name in request.dataset_names if name}
    entries = list_research_datasets(
        registry_path=request.registry_path,
        asset_class=request.asset_class,
    )
    for descriptor in entries:
        if provider_filter and descriptor.provider not in provider_filter:
            continue
        if name_filter and descriptor.dataset_name not in name_filter:
            continue
        if descriptor.dataset_key not in seen:
            descriptors.append(descriptor)
            seen.add(descriptor.dataset_key)
    if not descriptors:
        raise KeyError("No replay assembly datasets matched the requested filters")
    return descriptors


def assemble_replay_dataset(request: ReplayAssemblyRequest) -> ReplayAssemblyResult:
    descriptors = resolve_replay_component_descriptors(request)
    components: list[ReplayComponentSummary] = []
    join_plan: list[dict[str, Any]] = []
    loaded_components: list[tuple[ResearchDatasetDescriptor, pd.DataFrame, dict[str, str], dict[str, Any]]] = []

    for descriptor in descriptors:
        load_result = load_research_dataset(
            ResearchDatasetReadRequest(
                registry_path=request.registry_path,
                dataset_key=descriptor.dataset_key,
                symbols=list(request.symbols),
                intervals=list(request.intervals),
                start=request.start,
                end=request.end,
            )
        )
        normalized_frame, rename_map = _normalize_component(load_result)
        loaded_components.append((descriptor, normalized_frame, rename_map, load_result.filters_applied))
        components.append(
            ReplayComponentSummary(
                dataset_key=descriptor.dataset_key,
                provider=descriptor.provider,
                asset_class=descriptor.asset_class,
                dataset_name=descriptor.dataset_name,
                row_count=int(len(load_result.frame.index)),
                time_column=descriptor.time_column,
                identity_columns=_identity_columns(normalized_frame),
                namespaced_columns=_non_identity_columns(normalized_frame),
                applied_filters=dict(load_result.filters_applied),
                manifest_references=dict(descriptor.manifest_references),
                health_references=dict(descriptor.health_references),
            )
        )

    if not loaded_components:
        return ReplayAssemblyResult(
            request=request,
            frame=pd.DataFrame(),
            components=components,
            join_plan=[],
            metadata={"resolved_dataset_keys": [], "alignment_mode": request.alignment_mode},
        )

    if request.alignment_mode not in {"outer_union", "anchor"}:
        raise ValueError(f"Unsupported replay assembly alignment mode: {request.alignment_mode}")

    if request.alignment_mode == "anchor":
        anchor_key = request.anchor_dataset_key or loaded_components[0][0].dataset_key
        anchor_tuple = next((item for item in loaded_components if item[0].dataset_key == anchor_key), None)
        if anchor_tuple is None:
            raise KeyError(f"Unknown anchor dataset key: {anchor_key}")
        merged = anchor_tuple[1]
        ordered_components = [anchor_tuple] + [item for item in loaded_components if item[0].dataset_key != anchor_key]
        for descriptor, frame, _, _ in ordered_components[1:]:
            merged, join_columns = _merge_anchor(merged, frame, tolerance=request.tolerance)
            join_plan.append(
                {
                    "mode": "anchor",
                    "anchor_dataset_key": anchor_key,
                    "merged_dataset_key": descriptor.dataset_key,
                    "join_columns": join_columns,
                    "tolerance": request.tolerance,
                }
            )
    else:
        merged = loaded_components[0][1]
        for descriptor, frame, _, _ in loaded_components[1:]:
            merged, join_columns = _merge_outer(merged, frame)
            join_plan.append(
                {
                    "mode": "outer_union",
                    "merged_dataset_key": descriptor.dataset_key,
                    "join_columns": join_columns,
                }
            )

    sort_columns = _identity_columns(merged)
    if sort_columns and not merged.empty:
        merged = merged.sort_values(sort_columns).reset_index(drop=True)

    metadata = {
        "resolved_dataset_keys": [descriptor.dataset_key for descriptor, _, _, _ in loaded_components],
        "providers": sorted({descriptor.provider for descriptor, _, _, _ in loaded_components}),
        "alignment_mode": request.alignment_mode,
        "anchor_dataset_key": request.anchor_dataset_key or (loaded_components[0][0].dataset_key if request.alignment_mode == "anchor" else None),
        "row_identity_columns": _identity_columns(merged),
        "namespacing_rule": "{provider}__{dataset_name}__{column}",
        "timestamp_alignment": (
            "outer merge on shared identity columns including event_time"
            if request.alignment_mode == "outer_union"
            else "left merge onto the anchor dataset using backward event_time alignment and optional tolerance"
        ),
    }
    return ReplayAssemblyResult(
        request=request,
        frame=merged,
        components=components,
        join_plan=join_plan,
        metadata=metadata,
    )


def write_replay_assembly_artifacts(
    *,
    result: ReplayAssemblyResult,
    output_path: str | Path,
    summary_path: str | Path | None = None,
) -> dict[str, str]:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.frame.to_parquet(output, index=False)
    summary = Path(summary_path) if summary_path is not None else output.with_suffix(".summary.json")
    summary.parent.mkdir(parents=True, exist_ok=True)
    summary.write_text(json.dumps(result.to_summary(), indent=2), encoding="utf-8")
    return {"output_path": str(output), "summary_path": str(summary)}
