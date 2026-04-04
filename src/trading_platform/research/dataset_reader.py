from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.research.dataset_registry import (
    ResearchDatasetRegistryEntry,
    get_dataset_registry_entry,
    list_dataset_registry_entries,
)


def _clean_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return [str(value).strip() for value in (values or []) if str(value).strip()]


def _read_entry_frame(entry: ResearchDatasetRegistryEntry) -> pd.DataFrame:
    path = Path(entry.dataset_path)
    if entry.storage_type == "parquet_directory":
        if not path.exists() or not path.is_dir():
            return pd.DataFrame()
        frames = [pd.read_parquet(parquet_path) for parquet_path in sorted(path.glob("*.parquet"))]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _schema_columns(entry: ResearchDatasetRegistryEntry) -> list[str]:
    path = Path(entry.dataset_path)
    try:
        if entry.storage_type == "parquet_directory":
            parquet_paths = sorted(path.glob("*.parquet")) if path.exists() and path.is_dir() else []
            if not parquet_paths:
                return []
            return list(pd.read_parquet(parquet_paths[0]).columns)
        if not path.exists():
            return []
        return list(pd.read_parquet(path).columns)
    except Exception:
        return []


def _infer_time_column(entry: ResearchDatasetRegistryEntry, columns: list[str]) -> str | None:
    metadata = dict(entry.metadata or {})
    time_semantics = metadata.get("time_semantics")
    if isinstance(time_semantics, dict):
        for key in time_semantics:
            if key in columns:
                return str(key)
    for candidate in ("feature_time", "timestamp", "event_time", "materialized_at"):
        if candidate in columns:
            return candidate
    return None


def _infer_primary_keys(entry: ResearchDatasetRegistryEntry, columns: list[str]) -> list[str]:
    metadata = dict(entry.metadata or {})
    keys = metadata.get("keys")
    if isinstance(keys, list) and keys:
        return [str(key) for key in keys]
    return [column for column in ("symbol", "interval", "timestamp") if column in columns]


@dataclass(frozen=True)
class ResearchDatasetDescriptor:
    dataset_key: str
    provider: str
    asset_class: str
    dataset_name: str
    dataset_path: str
    storage_type: str
    schema_version: str
    available_symbols: list[str] = field(default_factory=list)
    available_intervals: list[str] = field(default_factory=list)
    target_horizons: list[int] = field(default_factory=list)
    summary_path: str | None = None
    latest_materialized_at: str | None = None
    latest_event_time: str | None = None
    time_column: str | None = None
    primary_keys: list[str] = field(default_factory=list)
    schema_columns: list[str] = field(default_factory=list)
    manifest_references: dict[str, Any] = field(default_factory=dict)
    health_references: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_entry(cls, entry: ResearchDatasetRegistryEntry) -> "ResearchDatasetDescriptor":
        columns = _schema_columns(entry)
        return cls(
            dataset_key=entry.dataset_key,
            provider=entry.provider,
            asset_class=entry.asset_class,
            dataset_name=entry.dataset_name,
            dataset_path=entry.dataset_path,
            storage_type=entry.storage_type,
            schema_version=entry.schema_version,
            available_symbols=list(entry.symbols),
            available_intervals=list(entry.intervals),
            target_horizons=list(entry.target_horizons),
            summary_path=entry.summary_path,
            latest_materialized_at=entry.latest_materialized_at,
            latest_event_time=entry.latest_event_time,
            time_column=_infer_time_column(entry, columns),
            primary_keys=_infer_primary_keys(entry, columns),
            schema_columns=columns,
            manifest_references=dict(entry.manifest_references),
            health_references=dict(entry.health_references),
            metadata=dict(entry.metadata),
        )


@dataclass(frozen=True)
class ResearchDatasetReadRequest:
    registry_path: str | Path
    dataset_key: str | None = None
    provider: str | None = None
    asset_class: str | None = None
    dataset_name: str | None = None
    symbols: list[str] = field(default_factory=list)
    intervals: list[str] = field(default_factory=list)
    start: str | None = None
    end: str | None = None
    columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ResearchDatasetLoadResult:
    descriptor: ResearchDatasetDescriptor
    frame: pd.DataFrame
    filters_applied: dict[str, Any]


def list_research_datasets(
    *,
    registry_path: str | Path,
    provider: str | None = None,
    asset_class: str | None = None,
    dataset_name: str | None = None,
) -> list[ResearchDatasetDescriptor]:
    entries = list_dataset_registry_entries(
        registry_path=registry_path,
        provider=provider,
        asset_class=asset_class,
        dataset_name=dataset_name,
    )
    return [ResearchDatasetDescriptor.from_entry(entry) for entry in entries]


def resolve_research_dataset(
    *,
    registry_path: str | Path,
    dataset_key: str | None = None,
    provider: str | None = None,
    asset_class: str | None = None,
    dataset_name: str | None = None,
) -> ResearchDatasetDescriptor:
    if dataset_key:
        return ResearchDatasetDescriptor.from_entry(
            get_dataset_registry_entry(registry_path=registry_path, dataset_key=dataset_key)
        )
    entries = list_dataset_registry_entries(
        registry_path=registry_path,
        provider=provider,
        asset_class=asset_class,
        dataset_name=dataset_name,
    )
    if not entries:
        raise KeyError("No research dataset matched the requested filters")
    if len(entries) > 1:
        keys = ", ".join(sorted(entry.dataset_key for entry in entries))
        raise ValueError(f"Ambiguous research dataset selection: {keys}")
    return ResearchDatasetDescriptor.from_entry(entries[0])


def load_research_dataset(request: ResearchDatasetReadRequest) -> ResearchDatasetLoadResult:
    descriptor = resolve_research_dataset(
        registry_path=request.registry_path,
        dataset_key=request.dataset_key,
        provider=request.provider,
        asset_class=request.asset_class,
        dataset_name=request.dataset_name,
    )
    entry = get_dataset_registry_entry(registry_path=request.registry_path, dataset_key=descriptor.dataset_key)
    frame = _read_entry_frame(entry)
    symbols = {value.upper() for value in _clean_list(request.symbols)}
    intervals = set(_clean_list(request.intervals))
    if not frame.empty and symbols and "symbol" in frame.columns:
        frame = frame.loc[frame["symbol"].astype(str).str.upper().isin(symbols)]
    if not frame.empty and intervals and "interval" in frame.columns:
        frame = frame.loc[frame["interval"].astype(str).isin(intervals)]

    time_column = descriptor.time_column
    if not frame.empty and request.start is not None and time_column and time_column in frame.columns:
        frame = frame.loc[pd.to_datetime(frame[time_column], utc=True) >= pd.to_datetime(request.start, utc=True)]
    if not frame.empty and request.end is not None and time_column and time_column in frame.columns:
        frame = frame.loc[pd.to_datetime(frame[time_column], utc=True) <= pd.to_datetime(request.end, utc=True)]
    if not frame.empty and request.columns:
        keep = [column for column in request.columns if column in frame.columns]
        frame = frame.loc[:, keep]

    sort_columns = [column for column in descriptor.primary_keys if column in frame.columns]
    if not sort_columns and time_column and time_column in frame.columns:
        sort_columns = [time_column]
    if sort_columns and not frame.empty:
        frame = frame.sort_values(sort_columns).reset_index(drop=True)

    return ResearchDatasetLoadResult(
        descriptor=descriptor,
        frame=frame,
        filters_applied={
            "provider": request.provider,
            "asset_class": request.asset_class,
            "dataset_name": request.dataset_name,
            "dataset_key": descriptor.dataset_key,
            "symbols": sorted(symbols),
            "intervals": sorted(intervals),
            "start": request.start,
            "end": request.end,
            "columns": list(request.columns),
        },
    )
