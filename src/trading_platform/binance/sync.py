from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import (
    BinanceFeatureConfig,
    BinanceProjectionConfig,
    BinanceStatusConfig,
    BinanceSyncConfig,
    BinanceSyncResult,
    BinanceWebsocketIngestConfig,
)
from trading_platform.binance.projection import project_binance_market_data
from trading_platform.binance.status import build_binance_status, build_binance_sync_manifest, generate_sync_id
from trading_platform.binance.websocket import BinanceWebsocketIngestService


def _status_for_step(*, failures: list[Any] | None = None, skipped: bool = False) -> str:
    if skipped:
        return "skipped"
    if failures:
        return "completed_with_failures"
    return "completed"


def run_binance_incremental_sync(config: BinanceSyncConfig) -> BinanceSyncResult:
    sync_id = generate_sync_id()
    started_at = datetime.now(UTC)
    summary_path = Path(config.sync_summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    step_statuses: dict[str, str] = {}
    step_summaries: dict[str, dict[str, Any]] = {}
    websocket_summary_path: str | None = None
    projection_summary_path: str | None = None
    feature_summary_path: str | None = None
    freshness_summary_path: str | None = None
    overall_status = "completed"

    websocket_config = BinanceWebsocketIngestConfig(
        **{
            **config.websocket.__dict__,
            "symbols": config.symbols,
            "intervals": config.intervals,
            "stream_families": config.stream_families,
            "max_runtime_seconds": config.max_runtime_seconds
            if config.max_runtime_seconds is not None
            else config.websocket.max_runtime_seconds,
            "max_messages": config.max_messages if config.max_messages is not None else config.websocket.max_messages,
            "refresh_projection_after_ingest": False,
        }
    )
    websocket_started = monotonic()
    websocket_result = BinanceWebsocketIngestService(websocket_config).run()
    websocket_elapsed = monotonic() - websocket_started
    websocket_summary_path = websocket_result.summary_path
    step_statuses["websocket_ingest"] = _status_for_step(failures=websocket_result.failures)
    step_summaries["websocket_ingest"] = {
        "status": step_statuses["websocket_ingest"],
        "duration_seconds": round(websocket_elapsed, 6),
        "result": {
            "summary_path": websocket_result.summary_path,
            "checkpoint_path": websocket_result.checkpoint_path,
            "messages_processed": websocket_result.messages_processed,
            "messages_written": websocket_result.messages_written,
            "duplicates_dropped": websocket_result.duplicates_dropped,
            "reconnect_count": websocket_result.reconnect_count,
            "warnings": list(websocket_result.warnings),
            "failures": list(websocket_result.failures),
        },
    }
    if websocket_result.failures:
        overall_status = "completed_with_failures"

    if config.skip_projection:
        step_statuses["projection"] = "skipped"
        step_summaries["projection"] = {"status": "skipped"}
    else:
        projection_started = monotonic()
        projection_result = project_binance_market_data(
            BinanceProjectionConfig(
                historical_normalized_root=config.projection.historical_normalized_root,
                incremental_normalized_root=websocket_config.normalized_incremental_root,
                output_root=config.projection.output_root,
                summary_path=config.projection.summary_path,
                symbols=config.symbols,
                intervals=config.intervals,
            )
        )
        projection_elapsed = monotonic() - projection_started
        projection_summary_path = projection_result.summary_path
        step_statuses["projection"] = "completed"
        step_summaries["projection"] = {
            "status": "completed",
            "duration_seconds": round(projection_elapsed, 6),
            "result": {
                "summary_path": projection_result.summary_path,
                "row_counts": dict(projection_result.row_counts),
                "output_paths": dict(projection_result.output_paths),
                "mode": "full_rebuild",
            },
        }

    if config.skip_features:
        step_statuses["features"] = "skipped"
        step_summaries["features"] = {"status": "skipped"}
    else:
        if config.skip_projection:
            projection_summary_path = config.projection.summary_path
        features_started = monotonic()
        feature_result = build_binance_market_features(
            BinanceFeatureConfig(
                **{
                    **config.features.__dict__,
                    "projection_root": config.projection.output_root,
                    "symbols": config.symbols,
                    "intervals": config.intervals,
                }
            ),
            full_rebuild=config.full_feature_rebuild,
            run_context={"last_successful_sync_id": sync_id},
        )
        features_elapsed = monotonic() - features_started
        feature_summary_path = feature_result.summary_path
        step_statuses["features"] = "completed"
        step_summaries["features"] = {
            "status": "completed",
            "duration_seconds": round(features_elapsed, 6),
            "result": {
                "summary_path": feature_result.summary_path,
                "features_path": feature_result.features_path,
                "rows_written": feature_result.rows_written,
                "artifacts_written": feature_result.artifacts_written,
                "slice_paths": list(feature_result.slice_paths),
                "feature_store_manifest_paths": list(feature_result.feature_store_manifest_paths),
                "mode": "full_rebuild" if config.full_feature_rebuild else "incremental_refresh",
            },
        }

    status_config = BinanceStatusConfig(
        **{
            **config.status.__dict__,
            "projection_root": config.projection.output_root,
            "features_root": config.features.features_root,
            "feature_store_root": config.features.feature_store_root,
            "latest_sync_manifest_path": config.latest_sync_manifest_path,
            "symbols": config.symbols,
            "intervals": config.intervals,
        }
    )
    freshness_result = build_binance_status(status_config, latest_sync_id=sync_id)
    freshness_summary_path = freshness_result.summary_path

    ended_at = datetime.now(UTC)
    summary = {
        "sync_id": sync_id,
        "status": overall_status,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "symbols": list(config.symbols),
        "intervals": list(config.intervals),
        "stream_families": list(config.stream_families),
        "max_runtime_seconds": config.max_runtime_seconds,
        "max_messages": config.max_messages,
        "checkpoint_paths": {
            "websocket": websocket_config.checkpoint_path,
        },
        "step_statuses": dict(step_statuses),
        "steps": step_summaries,
        "summary_paths": {
            "websocket": websocket_summary_path,
            "projection": projection_summary_path,
            "features": feature_summary_path,
            "freshness": freshness_summary_path,
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    manifest_path, latest_manifest_path = build_binance_sync_manifest(
        sync_id=sync_id,
        manifest_root=config.sync_manifest_root,
        latest_manifest_path=config.latest_sync_manifest_path,
        status=overall_status,
        started_at=started_at.isoformat(),
        completed_at=ended_at.isoformat(),
        provider="binance",
        symbols=list(config.symbols),
        intervals=list(config.intervals),
        stream_families=list(config.stream_families),
        step_statuses=step_statuses,
        step_summaries=step_summaries,
        checkpoint_paths={"websocket": websocket_config.checkpoint_path},
        summary_paths={
            "websocket": websocket_summary_path,
            "projection": projection_summary_path,
            "features": feature_summary_path,
        },
        freshness_summary_path=freshness_summary_path,
    )
    return BinanceSyncResult(
        sync_id=sync_id,
        summary_path=str(summary_path),
        manifest_path=manifest_path,
        latest_manifest_path=latest_manifest_path,
        freshness_summary_path=freshness_summary_path,
        websocket_summary_path=websocket_summary_path,
        projection_summary_path=projection_summary_path,
        feature_summary_path=feature_summary_path,
        status=overall_status,
        step_statuses=step_statuses,
    )
