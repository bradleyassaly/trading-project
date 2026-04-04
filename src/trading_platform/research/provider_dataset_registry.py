from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from trading_platform.research.dataset_registry import (
    ResearchDatasetRegistryEntry,
    list_dataset_registry_entries,
    upsert_dataset_registry_entry,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _read_yaml(path: str | Path) -> dict[str, Any]:
    return dict(yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {})


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    candidate = Path(path)
    if not candidate.exists():
        return {}
    try:
        return dict(json.loads(candidate.read_text(encoding="utf-8")) or {})
    except json.JSONDecodeError:
        return {}


def _resolve_path(project_root: Path, path: str | None, default: str) -> str:
    value = Path(path or default)
    if value.is_absolute():
        return str(value)
    return str(project_root / value)


def _feature_symbols(features_dir: Path) -> list[str]:
    if not features_dir.exists():
        return []
    return sorted({path.stem.upper() for path in features_dir.glob("*.parquet")})


def _latest_mtime_iso(paths: list[Path]) -> str | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    latest = max(existing, key=lambda path: path.stat().st_mtime)
    return datetime.fromtimestamp(latest.stat().st_mtime, tz=UTC).isoformat()


def _latest_matching_file(root: Path, pattern: str) -> str | None:
    if not root.exists():
        return None
    matches = sorted(root.rglob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    return str(matches[0]) if matches else None


@dataclass(frozen=True)
class DatasetRegistryPublishResult:
    registry_path: str
    published_count: int
    dataset_keys: list[str]
    summary_path: str


def _registry_section(raw_config: dict[str, Any], *, default_key: str, default_name: str, default_asset_class: str) -> dict[str, Any]:
    section = dict(raw_config.get("research_registry", {}) or {})
    return {
        "enabled": bool(section.get("enabled", True)),
        "path": section.get("path", "data/research/dataset_registry.json"),
        "dataset_key": str(section.get("dataset_key", default_key)),
        "dataset_name": str(section.get("dataset_name", default_name)),
        "asset_class": str(section.get("asset_class", default_asset_class)),
        "schema_version": str(section.get("schema_version", "research_dataset_registry_v1")),
    }


def _build_kalshi_entry(config_path: str | Path, *, project_root: Path, registry_path: str | None = None) -> ResearchDatasetRegistryEntry | None:
    raw_config = _read_yaml(config_path)
    history_cfg = dict(raw_config.get("historical_ingest", {}) or {})
    validation_cfg = dict(raw_config.get("data_validation", {}) or {})
    registry_cfg = _registry_section(
        raw_config,
        default_key="kalshi.prediction_market_features",
        default_name="prediction_market_features",
        default_asset_class="prediction_market",
    )
    if not registry_cfg["enabled"]:
        return None

    features_dir = Path(_resolve_path(project_root, history_cfg.get("features_dir"), "data/kalshi/features/real"))
    manifest_path = Path(_resolve_path(project_root, history_cfg.get("manifest_path"), "data/kalshi/raw/ingest_manifest.json"))
    summary_path = Path(_resolve_path(project_root, history_cfg.get("summary_path"), "data/kalshi/raw/ingest_summary.json"))
    validation_output_dir = Path(_resolve_path(project_root, validation_cfg.get("output_dir"), "data/kalshi/validation"))
    validation_summary_path = validation_output_dir / "kalshi_data_validation_summary.json"
    status_root = Path(_resolve_path(project_root, history_cfg.get("status_artifacts_root"), "artifacts/kalshi_ingest"))
    latest_status_path = _latest_matching_file(status_root, "ingest_status.json")
    latest_run_summary_path = _latest_matching_file(status_root, "ingest_run_summary.json")

    ingest_summary = _read_json(summary_path)
    symbols = _feature_symbols(features_dir)
    latest_materialized_at = _latest_mtime_iso(
        [features_dir / f"{symbol}.parquet" for symbol in symbols]
        + [manifest_path, summary_path, validation_summary_path]
    )
    return ResearchDatasetRegistryEntry(
        dataset_key=registry_cfg["dataset_key"],
        provider="kalshi",
        asset_class=registry_cfg["asset_class"],
        dataset_name=registry_cfg["dataset_name"],
        dataset_path=str(features_dir),
        storage_type="parquet_directory",
        symbols=symbols,
        intervals=[str(history_cfg.get("feature_period", "1h"))],
        schema_version=registry_cfg["schema_version"],
        latest_materialized_at=latest_materialized_at,
        latest_event_time=ingest_summary.get("date_range_end"),
        summary_path=str(summary_path),
        manifest_references={
            "config_path": str(Path(config_path)),
            "ingest_manifest_path": str(manifest_path),
            "ingest_summary_path": str(summary_path),
            "latest_run_summary_path": latest_run_summary_path,
        },
        health_references={
            "validation_summary_path": str(validation_summary_path),
            "latest_status_artifact_path": latest_status_path,
            "latest_run_summary_path": latest_run_summary_path,
        },
        metadata={
            "dataset_type": "feature_directory",
            "purpose": "prediction_market_research_features",
            "feature_period": str(history_cfg.get("feature_period", "1h")),
            "registry_path": registry_path or _resolve_path(project_root, registry_cfg["path"], registry_cfg["path"]),
            "universe_size": len(symbols),
        },
    )


def _build_polymarket_entry(config_path: str | Path, *, project_root: Path, registry_path: str | None = None) -> ResearchDatasetRegistryEntry | None:
    raw_config = _read_yaml(config_path)
    registry_cfg = _registry_section(
        raw_config,
        default_key="polymarket.prediction_market_features",
        default_name="prediction_market_features",
        default_asset_class="prediction_market",
    )
    if not registry_cfg["enabled"]:
        return None

    features_dir = Path(_resolve_path(project_root, raw_config.get("features_dir"), "data/polymarket/features"))
    manifest_path = Path(_resolve_path(project_root, raw_config.get("manifest_path"), "data/polymarket/raw/ingest_manifest.json"))
    resolution_csv_path = Path(_resolve_path(project_root, raw_config.get("resolution_csv_path"), "data/polymarket/resolution.csv"))
    manifest = _read_json(manifest_path)
    symbols = _feature_symbols(features_dir)
    latest_materialized_at = _latest_mtime_iso(
        [features_dir / f"{symbol}.parquet" for symbol in symbols] + [manifest_path, resolution_csv_path]
    )
    return ResearchDatasetRegistryEntry(
        dataset_key=registry_cfg["dataset_key"],
        provider="polymarket",
        asset_class=registry_cfg["asset_class"],
        dataset_name=registry_cfg["dataset_name"],
        dataset_path=str(features_dir),
        storage_type="parquet_directory",
        symbols=symbols,
        intervals=[str(raw_config.get("feature_period", "1h"))],
        schema_version=registry_cfg["schema_version"],
        latest_materialized_at=latest_materialized_at,
        latest_event_time=manifest.get("date_range_end"),
        summary_path=str(manifest_path),
        manifest_references={
            "config_path": str(Path(config_path)),
            "ingest_manifest_path": str(manifest_path),
            "resolution_csv_path": str(resolution_csv_path),
        },
        health_references={},
        metadata={
            "dataset_type": "feature_directory",
            "purpose": "prediction_market_research_features",
            "feature_period": str(raw_config.get("feature_period", "1h")),
            "registry_path": registry_path or _resolve_path(project_root, registry_cfg["path"], registry_cfg["path"]),
            "tag_slugs": list(raw_config.get("tag_slugs") or []),
            "universe_size": len(symbols),
        },
    )


def publish_shared_dataset_registry(
    *,
    project_root: str | Path,
    registry_path: str | Path,
    kalshi_config_path: str | Path | None = None,
    polymarket_config_path: str | Path | None = None,
    include_providers: list[str] | tuple[str, ...] | None = None,
    summary_path: str | Path | None = None,
) -> DatasetRegistryPublishResult:
    root = Path(project_root)
    include = {provider.lower() for provider in (include_providers or ("kalshi", "polymarket"))}
    published: list[ResearchDatasetRegistryEntry] = []

    if "kalshi" in include and kalshi_config_path:
        entry = _build_kalshi_entry(kalshi_config_path, project_root=root, registry_path=str(registry_path))
        if entry is not None:
            upsert_dataset_registry_entry(registry_path=registry_path, entry=entry)
            published.append(entry)
    if "polymarket" in include and polymarket_config_path:
        entry = _build_polymarket_entry(polymarket_config_path, project_root=root, registry_path=str(registry_path))
        if entry is not None:
            upsert_dataset_registry_entry(registry_path=registry_path, entry=entry)
            published.append(entry)

    entries = list_dataset_registry_entries(registry_path=registry_path)
    provider_counts: dict[str, int] = {}
    for entry in entries:
        provider_counts[entry.provider] = provider_counts.get(entry.provider, 0) + 1

    resolved_summary_path = Path(summary_path) if summary_path is not None else Path(registry_path).with_name("latest_registry_summary.json")
    resolved_summary_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_summary_path.write_text(
        json.dumps(
            {
                "generated_at": _now_utc(),
                "registry_path": str(registry_path),
                "published_count": len(published),
                "published_dataset_keys": [entry.dataset_key for entry in published],
                "entry_count": len(entries),
                "provider_counts": provider_counts,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return DatasetRegistryPublishResult(
        registry_path=str(registry_path),
        published_count=len(published),
        dataset_keys=[entry.dataset_key for entry in published],
        summary_path=str(resolved_summary_path),
    )
