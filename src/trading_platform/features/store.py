from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.settings import FEATURE_STORE_DIR


FEATURE_STORE_SCHEMA_VERSION = "feature_store_v1"


def _normalize_feature_groups(feature_groups: list[str] | None) -> list[str]:
    values = [str(value).strip() for value in feature_groups or [] if str(value).strip()]
    return sorted(dict.fromkeys(values))


def _feature_set_id(feature_groups: list[str] | None) -> str:
    normalized = _normalize_feature_groups(feature_groups)
    if not normalized:
        return "default"
    return "__".join(normalized)


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


@dataclass(frozen=True)
class FeatureStoreArtifact:
    symbol: str
    timeframe: str
    feature_set_id: str
    feature_groups: list[str] = field(default_factory=list)
    schema_version: str = FEATURE_STORE_SCHEMA_VERSION
    row_count: int = 0
    data_path: str | None = None
    manifest_path: str | None = None
    start_timestamp: str | None = None
    end_timestamp: str | None = None
    feature_columns: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "feature_groups", _normalize_feature_groups(self.feature_groups))
        object.__setattr__(self, "feature_columns", sorted(str(column) for column in self.feature_columns))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "feature_set_id": self.feature_set_id,
            "feature_groups": list(self.feature_groups),
            "schema_version": self.schema_version,
            "row_count": int(self.row_count),
            "data_path": self.data_path,
            "manifest_path": self.manifest_path,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "feature_columns": list(self.feature_columns),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FeatureStoreArtifact":
        data = dict(payload or {})
        return cls(
            symbol=str(data["symbol"]),
            timeframe=str(data["timeframe"]),
            feature_set_id=str(data["feature_set_id"]),
            feature_groups=list(data.get("feature_groups") or []),
            schema_version=str(data.get("schema_version", FEATURE_STORE_SCHEMA_VERSION)),
            row_count=int(data.get("row_count", 0) or 0),
            data_path=str(data["data_path"]) if data.get("data_path") is not None else None,
            manifest_path=str(data["manifest_path"]) if data.get("manifest_path") is not None else None,
            start_timestamp=str(data["start_timestamp"]) if data.get("start_timestamp") is not None else None,
            end_timestamp=str(data["end_timestamp"]) if data.get("end_timestamp") is not None else None,
            feature_columns=list(data.get("feature_columns") or []),
            metadata=dict(data.get("metadata") or {}),
        )


class LocalFeatureStore:
    def __init__(self, root_dir: str | Path = FEATURE_STORE_DIR) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def build_paths(
        self,
        *,
        symbol: str,
        timeframe: str = "1d",
        feature_groups: list[str] | None = None,
    ) -> dict[str, Path]:
        feature_set_id = _feature_set_id(feature_groups)
        root = self.root_dir / str(timeframe) / str(symbol).upper()
        root.mkdir(parents=True, exist_ok=True)
        return {
            "data_path": root / f"{feature_set_id}.parquet",
            "manifest_path": root / f"{feature_set_id}.manifest.json",
        }

    def write_from_parquet(
        self,
        *,
        source_path: str | Path,
        symbol: str,
        timeframe: str = "1d",
        feature_groups: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> FeatureStoreArtifact:
        source = Path(source_path)
        paths = self.build_paths(symbol=symbol, timeframe=timeframe, feature_groups=feature_groups)
        shutil.copy2(source, paths["data_path"])
        frame = pd.read_parquet(paths["data_path"])
        timestamps = pd.to_datetime(frame["timestamp"], errors="coerce") if "timestamp" in frame.columns else pd.Series(dtype="datetime64[ns]")
        base_columns = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
        artifact = FeatureStoreArtifact(
            symbol=str(symbol).upper(),
            timeframe=str(timeframe),
            feature_set_id=_feature_set_id(feature_groups),
            feature_groups=feature_groups or [],
            row_count=int(len(frame.index)),
            data_path=str(paths["data_path"]),
            manifest_path=str(paths["manifest_path"]),
            start_timestamp=timestamps.min().isoformat() if not timestamps.empty and pd.notna(timestamps.min()) else None,
            end_timestamp=timestamps.max().isoformat() if not timestamps.empty and pd.notna(timestamps.max()) else None,
            feature_columns=[column for column in frame.columns if column not in base_columns],
            metadata=metadata,
        )
        paths["manifest_path"].write_text(json.dumps(artifact.to_dict(), indent=2), encoding="utf-8")
        return artifact

    def read_artifact(
        self,
        *,
        symbol: str,
        timeframe: str = "1d",
        feature_groups: list[str] | None = None,
    ) -> FeatureStoreArtifact:
        manifest_path = self.build_paths(symbol=symbol, timeframe=timeframe, feature_groups=feature_groups)["manifest_path"]
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return FeatureStoreArtifact.from_dict(payload)

    def read_frame(
        self,
        *,
        symbol: str,
        timeframe: str = "1d",
        feature_groups: list[str] | None = None,
    ) -> pd.DataFrame:
        artifact = self.read_artifact(symbol=symbol, timeframe=timeframe, feature_groups=feature_groups)
        if artifact.data_path is None:
            raise FileNotFoundError("Feature-store artifact is missing data_path")
        return pd.read_parquet(artifact.data_path)
