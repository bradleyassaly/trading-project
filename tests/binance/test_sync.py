from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trading_platform.binance.models import (
    BinanceFeatureConfig,
    BinanceProjectionConfig,
    BinanceStatusConfig,
    BinanceSyncConfig,
    BinanceWebsocketIngestConfig,
)
from trading_platform.binance.sync import run_binance_incremental_sync


def _sync_config(tmp_path: Path) -> BinanceSyncConfig:
    return BinanceSyncConfig(
        websocket=BinanceWebsocketIngestConfig(
            enabled=True,
            symbols=("BTCUSDT",),
            intervals=("1m",),
            stream_families=("kline", "agg_trade"),
            max_runtime_seconds=30,
            max_messages=25,
            raw_incremental_root=str(tmp_path / "raw"),
            normalized_incremental_root=str(tmp_path / "normalized_incremental"),
            checkpoint_path=str(tmp_path / "raw" / "checkpoint.json"),
            summary_path=str(tmp_path / "raw" / "summary.json"),
            projection_output_root=str(tmp_path / "projections"),
        ),
        projection=BinanceProjectionConfig(
            historical_normalized_root=str(tmp_path / "normalized"),
            incremental_normalized_root=str(tmp_path / "normalized_incremental"),
            output_root=str(tmp_path / "projections"),
            summary_path=str(tmp_path / "projections" / "projection_summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        ),
        features=BinanceFeatureConfig(
            projection_root=str(tmp_path / "projections"),
            features_root=str(tmp_path / "features"),
            feature_store_root=str(tmp_path / "feature_store"),
            summary_path=str(tmp_path / "features" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        ),
        status=BinanceStatusConfig(
            projection_root=str(tmp_path / "projections"),
            features_root=str(tmp_path / "features"),
            feature_store_root=str(tmp_path / "feature_store"),
            latest_sync_manifest_path=str(tmp_path / "sync" / "latest_sync_manifest.json"),
            summary_path=str(tmp_path / "status" / "status.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        ),
        symbols=("BTCUSDT",),
        intervals=("1m",),
        stream_families=("kline", "agg_trade"),
        skip_projection=False,
        skip_features=False,
        max_runtime_seconds=30,
        max_messages=25,
        full_feature_rebuild=False,
        sync_summary_path=str(tmp_path / "sync" / "summary.json"),
        sync_manifest_root=str(tmp_path / "sync" / "manifests"),
        latest_sync_manifest_path=str(tmp_path / "sync" / "latest_sync_manifest.json"),
    )


def test_sync_runner_orders_steps_and_writes_summary(tmp_path: Path) -> None:
    config = _sync_config(tmp_path)
    call_order: list[str] = []

    class FakeWebsocketService:
        def __init__(self, websocket_config):
            assert websocket_config.refresh_projection_after_ingest is False
            call_order.append("websocket_init")

        def run(self):
            call_order.append("websocket_run")
            return SimpleNamespace(
                summary_path=str(tmp_path / "raw" / "summary.json"),
                checkpoint_path=str(tmp_path / "raw" / "checkpoint.json"),
                messages_processed=25,
                messages_written=20,
                duplicates_dropped=5,
                reconnect_count=1,
                warnings=[],
                failures=[],
                projection_summary_path=None,
            )

    with patch("trading_platform.binance.sync.BinanceWebsocketIngestService", FakeWebsocketService):
        with patch(
            "trading_platform.binance.sync.project_binance_market_data",
            side_effect=lambda projection_config: call_order.append("project")
            or SimpleNamespace(
                summary_path=projection_config.summary_path,
                row_counts={"crypto_ohlcv_bars": 10, "crypto_agg_trades": 11, "crypto_top_of_book": 12},
                output_paths={"crypto_ohlcv_bars": str(tmp_path / "projections" / "crypto_ohlcv_bars.parquet")},
            ),
        ):
            with patch(
                "trading_platform.binance.sync.build_binance_market_features",
                side_effect=lambda feature_config, full_rebuild=False, run_context=None: call_order.append("features")
                or SimpleNamespace(
                    summary_path=feature_config.summary_path,
                    features_path=str(tmp_path / "features" / "crypto_market_features.parquet"),
                    rows_written=10,
                    artifacts_written=1,
                    slice_paths=[str(tmp_path / "features" / "crypto_market_features" / "BTCUSDT" / "1m.parquet")],
                    feature_store_manifest_paths=[str(tmp_path / "feature_store" / "1m" / "BTCUSDT" / "default.manifest.json")],
                    latest_feature_time="2024-01-01T00:02:59+00:00",
                    materialized_at="2024-01-01T00:03:00+00:00",
                ),
            ):
                with patch(
                    "trading_platform.binance.sync.build_binance_status",
                    side_effect=lambda status_config, latest_sync_id=None: call_order.append("status")
                    or SimpleNamespace(
                        summary_path=status_config.summary_path,
                        latest_sync_manifest_path=status_config.latest_sync_manifest_path,
                        dataset_count=4,
                        stale_dataset_count=0,
                        records=[],
                    ),
                ):
                    result = run_binance_incremental_sync(config)

    assert result.status == "completed"
    assert result.manifest_path.endswith(".json")
    assert call_order == ["websocket_init", "websocket_run", "project", "features", "status"]
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["step_statuses"]["websocket_ingest"] == "completed"
    assert summary["step_statuses"]["projection"] == "completed"
    assert summary["step_statuses"]["features"] == "completed"
    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert manifest["sync_id"] == result.sync_id
    assert manifest["freshness_summary_path"] == result.freshness_summary_path


def test_sync_runner_skips_optional_steps_and_records_failures(tmp_path: Path) -> None:
    config = BinanceSyncConfig(**{**_sync_config(tmp_path).__dict__, "skip_projection": True, "skip_features": True})

    class FakeWebsocketService:
        def __init__(self, _config):
            pass

        def run(self):
            return SimpleNamespace(
                summary_path=str(tmp_path / "raw" / "summary.json"),
                checkpoint_path=str(tmp_path / "raw" / "checkpoint.json"),
                messages_processed=5,
                messages_written=2,
                duplicates_dropped=0,
                reconnect_count=2,
                warnings=["warn"],
                failures=[{"error": "disconnect"}],
                projection_summary_path=None,
            )

    with patch("trading_platform.binance.sync.BinanceWebsocketIngestService", FakeWebsocketService):
        with patch(
            "trading_platform.binance.sync.build_binance_status",
            return_value=SimpleNamespace(
                summary_path=str(tmp_path / "status" / "status.json"),
                latest_sync_manifest_path=str(tmp_path / "sync" / "latest_sync_manifest.json"),
                dataset_count=0,
                stale_dataset_count=0,
                records=[],
            ),
        ):
            result = run_binance_incremental_sync(config)

    assert result.status == "completed_with_failures"
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["step_statuses"]["projection"] == "skipped"
    assert summary["step_statuses"]["features"] == "skipped"
    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert manifest["status"] == "completed_with_failures"


def test_sync_config_from_yaml_applies_sync_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    provider:
      symbols: [BTCUSDT, ETHUSDT]
      intervals: [1m, 5m]
    websocket:
      stream_families: [kline, agg_trade]
    sync:
      symbols: [BTCUSDT]
      intervals: [1m]
      stream_families: [agg_trade]
      max_runtime_seconds: 45
      max_messages: 50
      full_feature_rebuild: true
      summary_path: data/binance/sync/sync_summary.json
    outputs:
      normalized_root: data/binance/normalized
      normalized_incremental_root: data/binance/normalized/incremental
      websocket_checkpoint_path: data/binance/raw/websocket_checkpoint.json
      websocket_summary_path: data/binance/raw/websocket_summary.json
      projection_output_root: data/binance/projections
""",
        encoding="utf-8",
    )

    config = BinanceSyncConfig.from_yaml(config_path, project_root=tmp_path)

    assert config.symbols == ("BTCUSDT",)
    assert config.intervals == ("1m",)
    assert config.stream_families == ("agg_trade",)
    assert config.max_runtime_seconds == 45
    assert config.max_messages == 50
    assert config.full_feature_rebuild is True
