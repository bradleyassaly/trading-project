from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from trading_platform.binance.models import BinanceStatusConfig, BinanceStatusResult


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")) or {})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    converted = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(converted):
        return None
    return converted.isoformat()


def _age_seconds(now: datetime, latest_value: Any) -> float | None:
    converted = pd.to_datetime(latest_value, utc=True, errors="coerce")
    if pd.isna(converted):
        return None
    return round((now - converted.to_pydatetime()).total_seconds(), 6)


def _status_record(
    *,
    dataset_name: str,
    dataset_family: str,
    symbol: str,
    interval: str | None,
    latest_event_time: Any,
    latest_materialized_at: Any,
    threshold_seconds: int,
    row_count: int,
    source_reference: str,
    latest_sync_id: str | None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    age = _age_seconds(now, latest_event_time or latest_materialized_at)
    return {
        "dataset_name": dataset_name,
        "dataset_family": dataset_family,
        "symbol": symbol,
        "interval": interval,
        "latest_event_time": _to_iso(latest_event_time),
        "latest_materialized_at": _to_iso(latest_materialized_at),
        "freshness_age_seconds": age,
        "staleness_threshold_seconds": threshold_seconds,
        "stale": bool(age is not None and age > threshold_seconds),
        "source_reference": source_reference,
        "row_count": int(row_count),
        "last_successful_sync_id": latest_sync_id,
    }


def build_binance_sync_manifest(
    *,
    sync_id: str,
    manifest_root: str,
    latest_manifest_path: str,
    status: str,
    started_at: str,
    completed_at: str,
    provider: str,
    symbols: list[str],
    intervals: list[str],
    stream_families: list[str],
    step_statuses: dict[str, str],
    step_summaries: dict[str, dict[str, Any]],
    checkpoint_paths: dict[str, str],
    summary_paths: dict[str, str | None],
    freshness_summary_path: str | None,
) -> tuple[str, str]:
    warnings: list[str] = []
    failures: list[dict[str, Any]] = []
    output_artifacts: dict[str, list[str] | dict[str, Any]] = {}
    latest_event_time: str | None = None
    latest_feature_time: str | None = None
    raw_counts: dict[str, Any] = {}
    projection_counts: dict[str, Any] = {}
    feature_counts: dict[str, Any] = {}

    websocket_summary = _read_json(Path(summary_paths["websocket"])) if summary_paths.get("websocket") else {}
    projection_summary = _read_json(Path(summary_paths["projection"])) if summary_paths.get("projection") else {}
    feature_summary = _read_json(Path(summary_paths["features"])) if summary_paths.get("features") else {}
    freshness_summary = _read_json(Path(freshness_summary_path)) if freshness_summary_path else {}

    if websocket_summary:
        warnings.extend(list(websocket_summary.get("warnings") or []))
        failures.extend(list(websocket_summary.get("failures") or []))
        raw_counts = {
            "messages_processed": int(websocket_summary.get("messages_processed", 0) or 0),
            "messages_written": int(websocket_summary.get("messages_written", 0) or 0),
            "duplicates_dropped": int(websocket_summary.get("duplicates_dropped", 0) or 0),
            "reconnect_count": int(websocket_summary.get("reconnect_count", 0) or 0),
        }
        latest_event_time = websocket_summary.get("latest_event_time")
        output_artifacts["websocket"] = [str(websocket_summary.get("raw_incremental_root") or ""), str(websocket_summary.get("normalized_incremental_root") or "")]
    if projection_summary:
        projection_counts = dict(projection_summary.get("row_counts") or {})
        output_artifacts["projections"] = list((projection_summary.get("output_paths") or {}).values())
        latest_projection_times = [
            payload.get("latest_event_time")
            for payload in (projection_summary.get("latest_timestamps") or {}).values()
            if isinstance(payload, dict) and payload.get("latest_event_time")
        ]
        if latest_projection_times:
            latest_event_time = max([latest_event_time, *latest_projection_times] if latest_event_time else latest_projection_times)
    if feature_summary:
        feature_counts = {
            "rows_written": int(feature_summary.get("rows_written", 0) or 0),
            "artifacts_written": int(feature_summary.get("artifacts_written", 0) or 0),
        }
        latest_feature_time = feature_summary.get("latest_feature_time")
        output_artifacts["features"] = list(feature_summary.get("slice_paths") or [])
        output_artifacts["feature_store_manifests"] = list(feature_summary.get("feature_store_manifest_paths") or [])
    manifest = {
        "sync_id": sync_id,
        "provider": provider,
        "symbols": symbols,
        "intervals": intervals,
        "stream_families": stream_families,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "step_statuses": step_statuses,
        "step_durations": {
            name: payload.get("duration_seconds")
            for name, payload in step_summaries.items()
            if isinstance(payload, dict)
        },
        "checkpoint_paths": checkpoint_paths,
        "raw_ingest_counts": raw_counts,
        "projection_row_counts": projection_counts,
        "feature_counts": feature_counts,
        "warnings": warnings,
        "failures": failures,
        "output_artifacts": output_artifacts,
        "summary_paths": summary_paths,
        "freshness_summary_path": freshness_summary_path,
        "latest_event_time_observed": latest_event_time,
        "latest_feature_time_produced": latest_feature_time,
        "freshness_records": int(freshness_summary.get("dataset_count", 0) or 0) if freshness_summary else 0,
        "stale_dataset_count": int(freshness_summary.get("stale_dataset_count", 0) or 0) if freshness_summary else 0,
    }
    manifest_root_path = Path(manifest_root)
    manifest_root_path.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_root_path / f"{sync_id}.json"
    _write_json(manifest_path, manifest)
    latest_path = Path(latest_manifest_path)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(manifest_path, latest_path)
    return str(manifest_path), str(latest_path)


def build_binance_status(config: BinanceStatusConfig, *, latest_sync_id: str | None = None) -> BinanceStatusResult:
    projection_root = Path(config.projection_root)
    features_root = Path(config.features_root)
    latest_manifest_path = Path(config.latest_sync_manifest_path)
    if latest_sync_id is None:
        latest_manifest = _read_json(latest_manifest_path)
        latest_sync_id = str(latest_manifest.get("sync_id")) if latest_manifest.get("sync_id") else None

    records: list[dict[str, Any]] = []
    symbol_filter = {symbol.upper() for symbol in config.symbols} if config.symbols else None
    interval_filter = set(config.intervals) if config.intervals else None

    bars = _read_parquet_if_exists(projection_root / "crypto_ohlcv_bars.parquet")
    if not bars.empty:
        frame = bars.copy()
        if symbol_filter is not None:
            frame = frame.loc[frame["symbol"].str.upper().isin(symbol_filter)]
        if interval_filter is not None:
            frame = frame.loc[frame["interval"].isin(interval_filter)]
        for (symbol, interval), group in frame.groupby(["symbol", "interval"]):
            records.append(
                _status_record(
                    dataset_name="crypto_ohlcv_bars",
                    dataset_family="projection",
                    symbol=str(symbol).upper(),
                    interval=str(interval),
                    latest_event_time=group["event_time"].max() if "event_time" in group.columns else group["timestamp"].max(),
                    latest_materialized_at=group["ingested_at"].max() if "ingested_at" in group.columns else datetime.fromtimestamp((projection_root / "crypto_ohlcv_bars.parquet").stat().st_mtime, tz=UTC),
                    threshold_seconds=config.projection_staleness_threshold_sec,
                    row_count=len(group.index),
                    source_reference=str(projection_root / "crypto_ohlcv_bars.parquet"),
                    latest_sync_id=latest_sync_id,
                )
            )

    for dataset_name in ("crypto_agg_trades", "crypto_top_of_book"):
        frame = _read_parquet_if_exists(projection_root / f"{dataset_name}.parquet")
        if frame.empty:
            continue
        if symbol_filter is not None:
            frame = frame.loc[frame["symbol"].str.upper().isin(symbol_filter)]
        for symbol, group in frame.groupby(["symbol"]):
            records.append(
                _status_record(
                    dataset_name=dataset_name,
                    dataset_family="projection",
                    symbol=str(symbol).upper(),
                    interval=None,
                    latest_event_time=group["event_time"].max() if "event_time" in group.columns else group["timestamp"].max(),
                    latest_materialized_at=group["ingested_at"].max() if "ingested_at" in group.columns else datetime.fromtimestamp((projection_root / f"{dataset_name}.parquet").stat().st_mtime, tz=UTC),
                    threshold_seconds=config.projection_staleness_threshold_sec,
                    row_count=len(group.index),
                    source_reference=str(projection_root / f"{dataset_name}.parquet"),
                    latest_sync_id=latest_sync_id,
                )
            )

    feature_root = features_root / "crypto_market_features"
    if feature_root.exists():
        for symbol_dir in sorted(path for path in feature_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if symbol_filter is not None and symbol not in symbol_filter:
                continue
            for slice_path in sorted(symbol_dir.glob("*.parquet")):
                interval = slice_path.stem
                if interval_filter is not None and interval not in interval_filter:
                    continue
                frame = pd.read_parquet(slice_path)
                if frame.empty:
                    continue
                records.append(
                    _status_record(
                        dataset_name="crypto_market_features",
                        dataset_family="feature",
                        symbol=symbol,
                        interval=interval,
                        latest_event_time=frame["feature_time"].max() if "feature_time" in frame.columns else frame["timestamp"].max(),
                        latest_materialized_at=datetime.fromtimestamp(slice_path.stat().st_mtime, tz=UTC),
                        threshold_seconds=config.feature_staleness_threshold_sec,
                        row_count=len(frame.index),
                        source_reference=str(slice_path),
                        latest_sync_id=latest_sync_id,
                    )
                )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "latest_sync_manifest_path": str(latest_manifest_path) if latest_manifest_path.exists() else None,
        "latest_sync_id": latest_sync_id,
        "dataset_count": len(records),
        "stale_dataset_count": sum(1 for record in records if record["stale"]),
        "records": records,
    }
    summary_path = Path(config.summary_path)
    _write_json(summary_path, payload)
    return BinanceStatusResult(
        summary_path=str(summary_path),
        latest_sync_manifest_path=str(latest_manifest_path) if latest_manifest_path.exists() else None,
        dataset_count=len(records),
        stale_dataset_count=sum(1 for record in records if record["stale"]),
        records=records,
    )


def generate_sync_id(prefix: str = "binance-sync") -> str:
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
