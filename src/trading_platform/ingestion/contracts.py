from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


MARKET_DATA_SCHEMA_VERSION = "market_data_v1"
CANONICAL_MARKET_DATA_COLUMNS = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "timeframe",
    "provider",
    "asset_class",
    "schema_version",
]


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


@dataclass(frozen=True)
class MarketDataArtifactManifest:
    symbol: str
    timeframe: str
    provider: str
    asset_class: str
    row_count: int
    schema_version: str = MARKET_DATA_SCHEMA_VERSION
    start_timestamp: str | None = None
    end_timestamp: str | None = None
    raw_path: str | None = None
    normalized_path: str | None = None
    manifest_path: str | None = None
    validation_report_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "provider": self.provider,
            "asset_class": self.asset_class,
            "row_count": int(self.row_count),
            "schema_version": self.schema_version,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "raw_path": self.raw_path,
            "normalized_path": self.normalized_path,
            "manifest_path": self.manifest_path,
            "validation_report_path": self.validation_report_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketDataArtifactManifest":
        data = dict(payload or {})
        return cls(
            symbol=str(data["symbol"]),
            timeframe=str(data["timeframe"]),
            provider=str(data["provider"]),
            asset_class=str(data["asset_class"]),
            row_count=int(data["row_count"]),
            schema_version=str(data.get("schema_version", MARKET_DATA_SCHEMA_VERSION)),
            start_timestamp=str(data["start_timestamp"]) if data.get("start_timestamp") is not None else None,
            end_timestamp=str(data["end_timestamp"]) if data.get("end_timestamp") is not None else None,
            raw_path=str(data["raw_path"]) if data.get("raw_path") is not None else None,
            normalized_path=str(data["normalized_path"]) if data.get("normalized_path") is not None else None,
            manifest_path=str(data["manifest_path"]) if data.get("manifest_path") is not None else None,
            validation_report_path=(
                str(data["validation_report_path"]) if data.get("validation_report_path") is not None else None
            ),
            metadata=dict(data.get("metadata") or {}),
        )
