from __future__ import annotations

from pathlib import Path
from typing import Iterable

import json
import pandas as pd

from trading_platform.binance.models import BinanceResearchDatasetConfig, BinanceResearchDatasetResult
from trading_platform.features.store import LocalFeatureStore
from trading_platform.research.dataset_registry import (
    ResearchDatasetRegistryEntry,
    get_dataset_registry_entry,
    load_registered_dataset_frame,
    upsert_dataset_registry_entry,
)


BINANCE_FEATURE_GROUPS = ["crypto", "binance", "market_features"]


def _feature_store_manifest_paths(
    *,
    feature_store_root: str | Path,
    symbols: Iterable[str],
    intervals: Iterable[str],
) -> list[str]:
    store = LocalFeatureStore(feature_store_root)
    paths: list[str] = []
    for symbol in sorted({str(value).upper() for value in symbols}):
        for interval in sorted({str(value) for value in intervals}):
            manifest_path = store.build_paths(
                symbol=symbol,
                timeframe=interval,
                feature_groups=BINANCE_FEATURE_GROUPS,
            )["manifest_path"]
            if manifest_path.exists():
                paths.append(str(manifest_path))
    return paths


def load_binance_feature_frame(
    *,
    feature_store_root: str | Path,
    symbols: Iterable[str],
    intervals: Iterable[str],
    start: str | None = None,
    end: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    store = LocalFeatureStore(feature_store_root)
    frames: list[pd.DataFrame] = []
    for symbol in sorted({str(value).upper() for value in symbols}):
        for interval in sorted({str(value) for value in intervals}):
            try:
                frame = store.read_frame(
                    symbol=symbol,
                    timeframe=interval,
                    feature_groups=BINANCE_FEATURE_GROUPS,
                ).copy()
            except FileNotFoundError:
                continue
            if frame.empty:
                continue
            frame["symbol"] = frame["symbol"].astype(str).str.upper()
            frame["interval"] = str(interval)
            if start is not None:
                frame = frame.loc[pd.to_datetime(frame["feature_time"], utc=True) >= pd.to_datetime(start, utc=True)]
            if end is not None:
                frame = frame.loc[pd.to_datetime(frame["feature_time"], utc=True) <= pd.to_datetime(end, utc=True)]
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True).sort_values(["symbol", "interval", "feature_time"]).reset_index(drop=True)
    if columns:
        keep = [column for column in columns if column in combined.columns]
        return combined.loc[:, keep]
    return combined


def assemble_binance_research_dataset(
    *,
    feature_store_root: str | Path,
    symbols: Iterable[str],
    intervals: Iterable[str],
    start: str | None = None,
    end: str | None = None,
    target_horizon_bars: int | None = None,
) -> pd.DataFrame:
    frame = load_binance_feature_frame(
        feature_store_root=feature_store_root,
        symbols=symbols,
        intervals=intervals,
        start=start,
        end=end,
    )
    if frame.empty:
        return frame
    dataset = frame.copy()
    dataset["feature_time"] = pd.to_datetime(dataset["feature_time"], utc=True)
    dataset["timestamp"] = pd.to_datetime(dataset["timestamp"], utc=True)
    dataset = dataset.sort_values(["symbol", "interval", "feature_time"]).reset_index(drop=True)
    if target_horizon_bars is not None and "close" in dataset.columns:
        dataset[f"target_return_{target_horizon_bars}"] = (
            dataset.groupby(["symbol", "interval"])["close"].shift(-target_horizon_bars).div(dataset["close"]) - 1.0
        )
    return dataset


def load_binance_research_frame(
    *,
    dataset_path: str | Path,
    symbols: Iterable[str] | None = None,
    intervals: Iterable[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    frame = pd.read_parquet(dataset_path)
    if frame.empty:
        return frame
    if symbols is not None and "symbol" in frame.columns:
        symbol_filter = {str(value).upper() for value in symbols}
        frame = frame.loc[frame["symbol"].astype(str).str.upper().isin(symbol_filter)]
    if intervals is not None and "interval" in frame.columns:
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


def resolve_binance_research_registry_entry(
    *,
    registry_path: str | Path,
    dataset_key: str = "binance.crypto_market_features",
) -> ResearchDatasetRegistryEntry:
    return get_dataset_registry_entry(registry_path=registry_path, dataset_key=dataset_key)


def load_binance_research_frame_from_registry(
    *,
    registry_path: str | Path,
    dataset_key: str = "binance.crypto_market_features",
    symbols: Iterable[str] | None = None,
    intervals: Iterable[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    return load_registered_dataset_frame(
        registry_path=registry_path,
        dataset_key=dataset_key,
        symbols=list(symbols or []),
        intervals=list(intervals or []),
        start=start,
        end=end,
        columns=columns,
    )


def materialize_binance_research_dataset(config: BinanceResearchDatasetConfig) -> BinanceResearchDatasetResult:
    dataset = load_binance_feature_frame(
        feature_store_root=config.feature_store_root,
        symbols=config.symbols,
        intervals=config.intervals,
        start=config.start,
        end=config.end,
    )
    if not dataset.empty:
        dataset["feature_time"] = pd.to_datetime(dataset["feature_time"], utc=True)
        dataset["timestamp"] = pd.to_datetime(dataset["timestamp"], utc=True)
        dataset = dataset.sort_values(["symbol", "interval", "feature_time"]).reset_index(drop=True)
        for horizon in config.target_horizons:
            if "close" in dataset.columns:
                dataset[f"target_return_{horizon}"] = (
                    dataset.groupby(["symbol", "interval"])["close"].shift(-horizon).div(dataset["close"]) - 1.0
                )
    output_root = Path(config.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    dataset_path = output_root / "binance_research_dataset.parquet"
    dataset.to_parquet(dataset_path, index=False)
    base_columns = {
        "timestamp",
        "feature_time",
        "symbol",
        "interval",
        "provider",
        "source",
        "asset_class",
        "schema_version",
        "feature_set",
        "dedupe_key",
    }
    target_columns = [column for column in dataset.columns if column.startswith("target_return_")]
    feature_columns = [column for column in dataset.columns if column not in base_columns and column not in target_columns]
    latest_feature_time = (
        pd.to_datetime(dataset["feature_time"], utc=True).max().isoformat()
        if not dataset.empty and "feature_time" in dataset.columns
        else None
    )
    materialized_at = pd.Timestamp.utcnow().isoformat()
    feature_store_manifest_paths = _feature_store_manifest_paths(
        feature_store_root=config.feature_store_root,
        symbols=config.symbols,
        intervals=config.intervals,
    )
    summary = {
        "dataset_path": str(dataset_path),
        "row_count": int(len(dataset.index)),
        "symbols": sorted({str(value).upper() for value in config.symbols}),
        "intervals": sorted({str(value) for value in config.intervals}),
        "keys": ["symbol", "interval", "timestamp"],
        "time_semantics": {
            "timestamp": "bar open timestamp",
            "feature_time": "bar close timestamp used as the effective feature timestamp",
        },
        "feature_columns": feature_columns,
        "target_columns": target_columns,
        "target_horizons": list(config.target_horizons),
        "feature_store_root": str(config.feature_store_root),
        "feature_store_manifest_paths": feature_store_manifest_paths,
        "start": config.start,
        "end": config.end,
        "latest_feature_time": latest_feature_time,
        "materialized_at": materialized_at,
        "registry_path": config.registry.registry_path if config.registry.enabled else None,
        "dataset_key": config.registry.dataset_key if config.registry.enabled else None,
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    registry_path: str | None = None
    dataset_key: str | None = None
    if config.registry.enabled:
        registry_path = upsert_dataset_registry_entry(
            registry_path=config.registry.registry_path,
            entry=ResearchDatasetRegistryEntry(
                dataset_key=config.registry.dataset_key,
                provider="binance",
                asset_class=config.registry.asset_class,
                dataset_name=config.registry.dataset_name,
                dataset_path=str(dataset_path),
                symbols=summary["symbols"],
                intervals=summary["intervals"],
                target_horizons=list(config.target_horizons),
                schema_version=config.registry.schema_version,
                latest_materialized_at=materialized_at,
                latest_event_time=latest_feature_time,
                summary_path=str(summary_path),
                manifest_references={
                    "dataset_summary_path": str(summary_path),
                    "feature_store_manifest_paths": feature_store_manifest_paths,
                    "latest_sync_manifest_path": config.latest_sync_manifest_path,
                },
                health_references={
                    "status_summary_path": config.status_summary_path,
                    "alerts_summary_path": config.alerts_summary_path,
                    "health_summary_path": config.health_summary_path,
                },
                metadata={
                    "keys": ["symbol", "interval", "timestamp"],
                    "time_semantics": summary["time_semantics"],
                    "feature_columns": feature_columns,
                    "target_columns": target_columns,
                    "feature_store_root": str(config.feature_store_root),
                },
            ),
        )
        dataset_key = config.registry.dataset_key
    return BinanceResearchDatasetResult(
        dataset_path=str(dataset_path),
        summary_path=str(summary_path),
        row_count=int(len(dataset.index)),
        feature_columns=feature_columns,
        target_columns=target_columns,
        symbols=sorted({str(value).upper() for value in config.symbols}),
        intervals=sorted({str(value) for value in config.intervals}),
        registry_path=registry_path,
        dataset_key=dataset_key,
    )
