from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _clean_str_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return sorted({str(value).strip() for value in (values or []) if str(value).strip()})


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_mapping(values: dict[str, Any] | None) -> dict[str, Any]:
    if not values:
        return {}
    return {str(key): values[key] for key in sorted(values)}


def _read_registry(path: str | Path) -> dict[str, Any]:
    registry_path = Path(path)
    if not registry_path.exists():
        return {"generated_at": None, "entry_count": 0, "entries": []}
    payload = json.loads(registry_path.read_text(encoding="utf-8")) or {}
    return {
        "generated_at": payload.get("generated_at"),
        "entry_count": int(payload.get("entry_count", 0) or 0),
        "entries": list(payload.get("entries") or []),
    }


@dataclass(frozen=True)
class ResearchDatasetRegistryEntry:
    dataset_key: str
    provider: str
    asset_class: str
    dataset_name: str
    dataset_path: str
    storage_type: str = "parquet_file"
    symbols: list[str] = field(default_factory=list)
    intervals: list[str] = field(default_factory=list)
    target_horizons: list[int] = field(default_factory=list)
    schema_version: str = "research_dataset_registry_v1"
    latest_materialized_at: str | None = None
    latest_event_time: str | None = None
    summary_path: str | None = None
    manifest_references: dict[str, Any] = field(default_factory=dict)
    health_references: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "storage_type", _clean_optional_text(self.storage_type) or "parquet_file")
        object.__setattr__(self, "symbols", _clean_str_list(self.symbols))
        object.__setattr__(self, "intervals", _clean_str_list(self.intervals))
        object.__setattr__(self, "target_horizons", sorted({int(value) for value in self.target_horizons}))
        object.__setattr__(self, "latest_materialized_at", _clean_optional_text(self.latest_materialized_at))
        object.__setattr__(self, "latest_event_time", _clean_optional_text(self.latest_event_time))
        object.__setattr__(self, "summary_path", _clean_optional_text(self.summary_path))
        object.__setattr__(self, "manifest_references", _clean_mapping(self.manifest_references))
        object.__setattr__(self, "health_references", _clean_mapping(self.health_references))
        object.__setattr__(self, "metadata", _clean_mapping(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ResearchDatasetRegistryEntry":
        data = dict(payload or {})
        return cls(
            dataset_key=str(data["dataset_key"]),
            provider=str(data["provider"]),
            asset_class=str(data["asset_class"]),
            dataset_name=str(data["dataset_name"]),
            dataset_path=str(data["dataset_path"]),
            storage_type=str(data.get("storage_type", "parquet_file")),
            symbols=list(data.get("symbols") or []),
            intervals=list(data.get("intervals") or []),
            target_horizons=[int(value) for value in (data.get("target_horizons") or [])],
            schema_version=str(data.get("schema_version", "research_dataset_registry_v1")),
            latest_materialized_at=data.get("latest_materialized_at"),
            latest_event_time=data.get("latest_event_time"),
            summary_path=data.get("summary_path"),
            manifest_references=dict(data.get("manifest_references") or {}),
            health_references=dict(data.get("health_references") or {}),
            metadata=dict(data.get("metadata") or {}),
        )


def write_dataset_registry(
    *,
    registry_path: str | Path,
    entries: list[ResearchDatasetRegistryEntry],
) -> str:
    path = Path(registry_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_entries = sorted(entries, key=lambda entry: (entry.provider, entry.asset_class, entry.dataset_name, entry.dataset_key))
    payload = {
        "generated_at": _now_utc(),
        "entry_count": len(sorted_entries),
        "entries": [entry.to_dict() for entry in sorted_entries],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def upsert_dataset_registry_entry(
    *,
    registry_path: str | Path,
    entry: ResearchDatasetRegistryEntry,
) -> str:
    existing_payload = _read_registry(registry_path)
    entries = {
        stored.dataset_key: stored
        for stored in (
            ResearchDatasetRegistryEntry.from_dict(item)
            for item in existing_payload.get("entries", [])
        )
    }
    entries[entry.dataset_key] = entry
    return write_dataset_registry(registry_path=registry_path, entries=list(entries.values()))


def list_dataset_registry_entries(
    *,
    registry_path: str | Path,
    provider: str | None = None,
    asset_class: str | None = None,
    dataset_name: str | None = None,
) -> list[ResearchDatasetRegistryEntry]:
    payload = _read_registry(registry_path)
    entries = [ResearchDatasetRegistryEntry.from_dict(item) for item in payload.get("entries", [])]
    if provider is not None:
        entries = [entry for entry in entries if entry.provider == provider]
    if asset_class is not None:
        entries = [entry for entry in entries if entry.asset_class == asset_class]
    if dataset_name is not None:
        entries = [entry for entry in entries if entry.dataset_name == dataset_name]
    return entries


def get_dataset_registry_entry(
    *,
    registry_path: str | Path,
    dataset_key: str,
) -> ResearchDatasetRegistryEntry:
    for entry in list_dataset_registry_entries(registry_path=registry_path):
        if entry.dataset_key == dataset_key:
            return entry
    raise KeyError(f"Unknown research dataset key: {dataset_key}")


def _load_entry_frame(
    entry: ResearchDatasetRegistryEntry,
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame:
    path = Path(entry.dataset_path)
    if entry.storage_type == "parquet_directory":
        if not path.exists() or not path.is_dir():
            return pd.DataFrame()
        symbol_filter = {str(value).upper() for value in (symbols or [])}
        parquet_paths = sorted(path.glob("*.parquet"))
        if symbol_filter:
            candidate_paths = []
            for parquet_path in parquet_paths:
                if parquet_path.stem.upper() in symbol_filter:
                    candidate_paths.append(parquet_path)
            if candidate_paths:
                parquet_paths = candidate_paths
        frames = [pd.read_parquet(parquet_path) for parquet_path in parquet_paths]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_registered_dataset_frame(
    *,
    registry_path: str | Path,
    dataset_key: str,
    symbols: list[str] | tuple[str, ...] | None = None,
    intervals: list[str] | tuple[str, ...] | None = None,
    start: str | None = None,
    end: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    entry = get_dataset_registry_entry(registry_path=registry_path, dataset_key=dataset_key)
    frame = _load_entry_frame(entry, symbols=symbols)
    if frame.empty:
        return frame
    if symbols and "symbol" in frame.columns:
        symbol_filter = {str(value).upper() for value in symbols}
        frame = frame.loc[frame["symbol"].astype(str).str.upper().isin(symbol_filter)]
    if intervals and "interval" in frame.columns:
        interval_filter = {str(value) for value in intervals}
        frame = frame.loc[frame["interval"].astype(str).isin(interval_filter)]
    if start is not None and "timestamp" in frame.columns:
        frame = frame.loc[pd.to_datetime(frame["timestamp"], utc=True) >= pd.to_datetime(start, utc=True)]
    if end is not None and "timestamp" in frame.columns:
        frame = frame.loc[pd.to_datetime(frame["timestamp"], utc=True) <= pd.to_datetime(end, utc=True)]
    if columns:
        keep = [column for column in columns if column in frame.columns]
        frame = frame.loc[:, keep]
    sort_columns = [column for column in ("symbol", "interval", "timestamp") if column in frame.columns]
    if sort_columns:
        frame = frame.sort_values(sort_columns).reset_index(drop=True)
    return frame
