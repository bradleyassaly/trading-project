from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trading_platform.cli.commands import binance_crypto_historical_ingest as historical_cli
from trading_platform.cli.commands import binance_crypto_features as features_cli
from trading_platform.cli.commands import binance_crypto_alerts as alerts_cli
from trading_platform.cli.commands import binance_crypto_health_check as health_cli
from trading_platform.cli.commands import binance_crypto_normalize as normalize_cli
from trading_platform.cli.commands import binance_crypto_notify as notify_cli
from trading_platform.cli.commands import binance_crypto_project as project_cli
from trading_platform.cli.commands import binance_crypto_status as status_cli
from trading_platform.cli.commands import binance_crypto_sync as sync_cli
from trading_platform.cli.commands import binance_crypto_websocket_ingest as websocket_cli


def test_binance_historical_cli_builds_config_from_yaml(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    provider:
      symbols: [BTCUSDT]
      intervals: [1m]
      request_sleep_sec: 0.0
    historical_ingest:
      start: "2024-01-01T00:00:00Z"
      end: "2024-01-01T00:10:00Z"
    outputs:
      raw_root: data/binance/raw
      normalized_root: data/binance/normalized
      checkpoint_path: data/binance/raw/ingest_checkpoint.json
      summary_path: data/binance/raw/ingest_summary.json
      exchange_info_path: data/binance/raw/exchange_info.json
""",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                request_count=3,
                retry_count=0,
                pages_fetched=3,
                raw_artifacts_written=3,
                kline_rows_fetched=2,
                agg_trade_rows_fetched=1,
                book_ticker_snapshots_fetched=1,
                exchange_info_path=str(Path(config.exchange_info_path)),
                checkpoint_path=str(Path(config.checkpoint_path)),
                summary_path=str(Path(config.summary_path)),
                normalization_summary_path=str(Path(config.normalized_root) / "normalization_summary.json"),
            )

    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        start=None,
        end=None,
        kline_limit=None,
        agg_trade_limit=None,
        request_sleep_sec=None,
        max_retries=None,
        backoff_base_sec=None,
        backoff_max_sec=None,
        capture_book_ticker=None,
        skip_normalize=False,
        raw_root=None,
        normalized_root=None,
        checkpoint_path=None,
        summary_path=None,
        exchange_info_path=None,
    )
    with patch("trading_platform.cli.commands.binance_crypto_historical_ingest.BinanceHistoricalIngestPipeline", FakePipeline):
        with patch.object(historical_cli, "PROJECT_ROOT", tmp_path):
            historical_cli.cmd_binance_crypto_historical_ingest(args)

    config = captured["config"]
    assert config.symbols == ("BTCUSDT",)
    assert config.intervals == ("1m",)
    assert config.raw_root == str(tmp_path / "data/binance/raw")
    assert config.normalized_root == str(tmp_path / "data/binance/normalized")
    assert "Pages fetched            : 3" in capsys.readouterr().out


def test_binance_normalize_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text("crypto:\n  binance:\n    outputs:\n      raw_root: data/binance/raw\n      normalized_root: data/binance/normalized\n", encoding="utf-8")
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        raw_root=None,
        normalized_root=None,
        summary_path=None,
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_normalize.normalize_binance_artifacts",
        return_value=SimpleNamespace(
            kline_files_written=1,
            agg_trade_files_written=1,
            book_ticker_files_written=1,
            summary_path=str(tmp_path / "data/binance/normalized/normalization_summary.json"),
        ),
    ):
        with patch.object(normalize_cli, "PROJECT_ROOT", tmp_path):
            normalize_cli.cmd_binance_crypto_normalize(args)

    output = capsys.readouterr().out
    assert "Kline files written      : 1" in output


def test_binance_websocket_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    provider:
      symbols: [BTCUSDT]
      intervals: [1m]
    outputs:
      raw_incremental_root: data/binance/raw/websocket
      normalized_incremental_root: data/binance/normalized/incremental
      websocket_checkpoint_path: data/binance/raw/websocket_checkpoint.json
      websocket_summary_path: data/binance/raw/websocket_summary.json
      projection_output_root: data/binance/projections
""",
        encoding="utf-8",
    )
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        stream_families=None,
        combined_stream=None,
        max_runtime_seconds=10,
        max_messages=5,
        reconnect_backoff_base_sec=None,
        reconnect_backoff_max_sec=None,
        max_reconnect_attempts=None,
        receive_timeout_sec=None,
        raw_incremental_root=None,
        normalized_incremental_root=None,
        checkpoint_path=None,
        summary_path=None,
        projection_output_root=None,
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_websocket_ingest.BinanceWebsocketIngestService"
    ) as service_cls:
        service_cls.return_value.run.return_value = SimpleNamespace(
            messages_processed=5,
            messages_written=4,
            duplicates_dropped=1,
            reconnect_count=1,
            checkpoint_path=str(tmp_path / "data/binance/raw/websocket_checkpoint.json"),
            summary_path=str(tmp_path / "data/binance/raw/websocket_summary.json"),
            projection_summary_path=str(tmp_path / "data/binance/projections/projection_summary.json"),
            warnings=[],
            failures=[],
        )
        with patch.object(websocket_cli, "PROJECT_ROOT", tmp_path):
            websocket_cli.cmd_binance_crypto_websocket_ingest(args)

    output = capsys.readouterr().out
    assert "Messages processed       : 5" in output
    assert "Duplicates dropped       : 1" in output


def test_binance_project_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text("crypto:\n  binance:\n    outputs:\n      projection_output_root: data/binance/projections\n", encoding="utf-8")
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        historical_normalized_root=None,
        incremental_normalized_root=None,
        output_root=None,
        summary_path=None,
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_project.project_binance_market_data",
        return_value=SimpleNamespace(
            row_counts={
                "crypto_ohlcv_bars": 10,
                "crypto_agg_trades": 20,
                "crypto_top_of_book": 30,
            },
            summary_path=str(tmp_path / "data/binance/projections/projection_summary.json"),
        ),
    ):
        with patch.object(project_cli, "PROJECT_ROOT", tmp_path):
            project_cli.cmd_binance_crypto_project(args)

    output = capsys.readouterr().out
    assert "crypto_ohlcv_bars" in output
    assert "Summary                  :" in output


def test_binance_features_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    provider:
      symbols: [BTCUSDT]
      intervals: [1m]
    features:
      features_root: data/binance/features
      feature_store_root: data/feature_store
      summary_path: data/binance/features/feature_refresh_summary.json
    outputs:
      projection_output_root: data/binance/projections
""",
        encoding="utf-8",
    )
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        projection_root=None,
        features_root=None,
        feature_store_root=None,
        summary_path=None,
        incremental_refresh=None,
        full_rebuild=False,
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_features.build_binance_market_features",
        return_value=SimpleNamespace(
            rows_written=12,
            artifacts_written=1,
            features_path=str(tmp_path / "data/binance/features/crypto_market_features.parquet"),
            summary_path=str(tmp_path / "data/binance/features/feature_refresh_summary.json"),
            feature_store_manifest_paths=[str(tmp_path / "data/feature_store/1m/BTCUSDT/binance.manifest.json")],
        ),
    ):
        with patch.object(features_cli, "PROJECT_ROOT", tmp_path):
            features_cli.cmd_binance_crypto_features(args)

    output = capsys.readouterr().out
    assert "Rows written             : 12" in output
    assert "Feature-store manifests  : 1" in output


def test_binance_sync_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    provider:
      symbols: [BTCUSDT]
      intervals: [1m]
    websocket:
      stream_families: [kline, agg_trade]
    features:
      features_root: data/binance/features
      feature_store_root: data/feature_store
    sync:
      max_runtime_seconds: 30
      max_messages: 25
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
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        stream_families=None,
        max_runtime_seconds=None,
        max_messages=None,
        skip_projection=False,
        skip_features=False,
        full_feature_rebuild=False,
        incremental_refresh=None,
        raw_incremental_root=None,
        normalized_incremental_root=None,
        checkpoint_path=None,
        websocket_summary_path=None,
        historical_normalized_root=None,
        projection_output_root=None,
        projection_summary_path=None,
        features_root=None,
        feature_store_root=None,
        feature_summary_path=None,
        status_summary_path=None,
        sync_manifest_root=None,
        latest_sync_manifest_path=None,
        sync_summary_path=None,
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_sync.run_binance_incremental_sync",
        return_value=SimpleNamespace(
            sync_id="binance-sync-1",
            manifest_path=str(tmp_path / "data/binance/sync/manifests/binance-sync-1.json"),
            latest_manifest_path=str(tmp_path / "data/binance/sync/latest_sync_manifest.json"),
            freshness_summary_path=str(tmp_path / "data/binance/status/binance_status.json"),
            status="completed",
            websocket_summary_path=str(tmp_path / "data/binance/raw/websocket_summary.json"),
            projection_summary_path=str(tmp_path / "data/binance/projections/projection_summary.json"),
            feature_summary_path=str(tmp_path / "data/binance/features/feature_refresh_summary.json"),
            summary_path=str(tmp_path / "data/binance/sync/sync_summary.json"),
        ),
    ):
        with patch.object(sync_cli, "PROJECT_ROOT", tmp_path):
            sync_cli.cmd_binance_crypto_sync(args)

    output = capsys.readouterr().out
    assert "Status                   : completed" in output
    assert "Sync manifest            :" in output
    assert "Sync summary             :" in output


def test_binance_status_cli_runs_in_json_mode(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text(
        """
crypto:
  binance:
    status:
      summary_path: data/binance/status/binance_status.json
    outputs:
      projection_output_root: data/binance/projections
""",
        encoding="utf-8",
    )
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        projection_root=None,
        features_root=None,
        feature_store_root=None,
        latest_sync_manifest_path=None,
        summary_path=None,
        format="json",
    )
    summary_path = tmp_path / "data/binance/status/binance_status.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        '{"dataset_count": 1, "stale_dataset_count": 0, "records": [{"dataset_name": "crypto_ohlcv_bars", "symbol": "BTCUSDT", "interval": "1m", "stale": false}]}',
        encoding="utf-8",
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_status.build_binance_status",
        return_value=SimpleNamespace(
            summary_path=str(summary_path),
            latest_sync_manifest_path=None,
            dataset_count=1,
            stale_dataset_count=0,
            records=[],
        ),
    ):
        with patch.object(status_cli, "PROJECT_ROOT", tmp_path):
            status_cli.cmd_binance_crypto_status(args)

    output = capsys.readouterr().out
    assert '"dataset_count": 1' in output


def test_binance_alerts_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text("crypto:\n  binance:\n    alerts:\n      summary_path: data/binance/monitoring/alerts/alerts_summary.json\n", encoding="utf-8")
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        latest_sync_manifest_path=None,
        status_summary_path=None,
        output_root=None,
        summary_path=None,
        format="text",
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_alerts.evaluate_binance_alerts",
        return_value=SimpleNamespace(
            summary_path=str(tmp_path / "data/binance/monitoring/alerts/alerts_summary.json"),
            alerts_json_path=str(tmp_path / "data/binance/monitoring/alerts/alerts.json"),
            alerts_csv_path=str(tmp_path / "data/binance/monitoring/alerts/alerts.csv"),
            status="warning",
            alert_counts={"info": 0, "warning": 1, "critical": 0},
            alert_count=1,
        ),
    ):
        with patch.object(alerts_cli, "PROJECT_ROOT", tmp_path):
            alerts_cli.cmd_binance_crypto_alerts(args)
    output = capsys.readouterr().out
    assert "alert count   : 1" in output


def test_binance_health_check_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text("crypto:\n  binance:\n    health:\n      summary_path: data/binance/monitoring/health/health_check.json\n", encoding="utf-8")
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        latest_sync_manifest_path=None,
        status_summary_path=None,
        output_root=None,
        summary_path=None,
        format="text",
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_health_check.evaluate_binance_health_check",
        return_value=SimpleNamespace(
            summary_path=str(tmp_path / "data/binance/monitoring/health/health_check.json"),
            status="healthy",
            alert_counts={"info": 0, "warning": 0, "critical": 0},
            check_count=5,
        ),
    ):
        with patch.object(health_cli, "PROJECT_ROOT", tmp_path):
            health_cli.cmd_binance_crypto_health_check(args)
    output = capsys.readouterr().out
    assert "status        : healthy" in output


def test_binance_notify_cli_runs(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "binance.yaml"
    config_path.write_text("crypto:\n  binance:\n    notify:\n      summary_path: data/binance/monitoring/notifications/notify_summary.json\n", encoding="utf-8")
    args = Namespace(
        config=str(config_path),
        symbols=None,
        intervals=None,
        latest_sync_manifest_path=None,
        status_summary_path=None,
        alerts_output_root=None,
        alerts_summary_path=None,
        health_output_root=None,
        health_summary_path=None,
        output_root=None,
        summary_path=None,
        state_path=None,
        notification_config_path=None,
        enabled=None,
        dry_run=False,
        subject_prefix=None,
        format="text",
    )
    with patch(
        "trading_platform.cli.commands.binance_crypto_notify.run_binance_monitor_notifications",
        return_value=SimpleNamespace(
            summary_path=str(tmp_path / "data/binance/monitoring/notifications/notify_summary.json"),
            state_path=str(tmp_path / "data/binance/monitoring/notifications/notify_state.json"),
            status="warning",
            transition="healthy->warning",
            should_notify=True,
            notified=False,
            suppressed=False,
            alert_count=2,
        ),
    ):
        with patch.object(notify_cli, "PROJECT_ROOT", tmp_path):
            notify_cli.cmd_binance_crypto_notify(args)
    output = capsys.readouterr().out
    assert "transition    : healthy->warning" in output
    assert "should notify : True" in output
