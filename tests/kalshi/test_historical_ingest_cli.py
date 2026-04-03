from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.cli.commands import kalshi_historical_ingest as cli_module


DUMMY_PEM = "-----BEGIN RSA PRIVATE KEY-----\ndummy\n-----END RSA PRIVATE KEY-----"


def test_cli_builds_historical_ingest_config_from_yaml_and_project_root(tmp_path, capsys):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "ingestion": {"backfill_days": 123},
            }
        ),
        encoding="utf-8",
    )

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["client"] = client
            captured["config"] = config

        def run(self):
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(captured["config"].manifest_path),
                summary_path=Path(captured["config"].summary_path),
                status_artifact_path=Path(captured["config"].status_artifacts_root) / "run-1" / "ingest_status.json",
                run_summary_artifact_path=Path(captured["config"].status_artifacts_root) / "run-1" / "ingest_run_summary.json",
            )

    fake_kalshi_config = KalshiConfig(api_key_id="public-only", private_key_pem=DUMMY_PEM, demo=True)

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", side_effect=ValueError("missing")), \
         patch(
             "trading_platform.cli.commands.kalshi_historical_ingest._build_public_only_config",
            return_value=fake_kalshi_config,
         ), \
         patch("trading_platform.kalshi.client.KalshiClient") as mock_client_cls, \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        mock_client = object()
        mock_client_cls.return_value = mock_client
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    output = capsys.readouterr().out

    config = captured["config"]
    project_root = tmp_path

    assert config.raw_markets_dir == str(project_root / "data/kalshi/raw/markets")
    assert config.raw_trades_dir == str(project_root / "data/kalshi/raw/trades")
    assert config.raw_candles_dir == str(project_root / "data/kalshi/raw/candles")
    assert config.trades_parquet_dir == str(project_root / "data/kalshi/normalized/trades")
    assert config.normalized_candles_dir == str(project_root / "data/kalshi/normalized/candles")
    assert config.normalized_markets_path == str(project_root / "data/kalshi/normalized/markets.parquet")
    assert config.features_dir == str(project_root / "data/kalshi/features/real")
    assert config.resolution_csv_path == str(project_root / "data/kalshi/normalized/resolution.csv")
    assert config.legacy_resolution_csv_path == str(project_root / "data/kalshi/resolution.csv")
    assert config.manifest_path == str(project_root / "data/kalshi/raw/ingest_manifest.json")
    assert config.checkpoint_path == str(project_root / "data/kalshi/raw/ingest_checkpoint.json")
    assert config.summary_path == str(project_root / "data/kalshi/raw/ingest_summary.json")
    assert config.base_rate_db_path == str(project_root / "data/kalshi/base_rates/base_rate_db.json")
    assert config.status_artifacts_root == str(project_root / "artifacts/kalshi_ingest")
    assert config.checkpoint_backup_path == str(project_root / "data/kalshi/raw/ingest_checkpoint.bak.json")
    assert config.resume is True
    assert config.resume_mode == "latest"
    assert config.resume_checkpoint_path is None
    assert config.resume_recovery_mode == "automatic"
    assert config.lookback_days == 123
    assert config.min_trades == 5
    assert config.run_base_rate is True
    assert config.run_metaculus is False
    assert config.market_page_size == 1000
    assert config.trade_page_size == 1000
    assert config.request_sleep_sec == 0.05
    assert config.authenticated_request_sleep_sec == 0.072
    assert config.authenticated_rate_limit_max_retries == 5
    assert config.authenticated_rate_limit_backoff_base_sec == 0.5
    assert config.authenticated_rate_limit_backoff_max_sec == 8.0
    assert config.authenticated_rate_limit_jitter_max_sec == 0.25
    assert config.ticker_filter == []

    for path_str in (
        config.raw_markets_dir,
        config.raw_trades_dir,
        config.raw_candles_dir,
        config.trades_parquet_dir,
        config.normalized_candles_dir,
        config.features_dir,
        str(Path(config.normalized_markets_path).parent),
        str(Path(config.resolution_csv_path).parent),
        str(Path(config.legacy_resolution_csv_path).parent),
        str(Path(config.manifest_path).parent),
        str(Path(config.checkpoint_path).parent),
        str(Path(config.summary_path).parent),
        str(Path(config.base_rate_db_path).parent),
        str(Path(config.status_artifacts_root)),
    ):
        assert Path(path_str).exists()

    client_config = mock_client_cls.call_args.args[0]
    client_kwargs = mock_client_cls.call_args.kwargs
    assert client_config.demo is False
    assert client_config.api_key_id == "public-only"
    assert client_kwargs["historical_sleep_sec"] == 0.05
    assert client_kwargs["authenticated_sleep_sec"] == 0.072
    assert client_kwargs["authenticated_retry_policy"].max_retries == 5
    assert client_kwargs["authenticated_retry_policy"].backoff_base_sec == 0.5
    assert client_kwargs["authenticated_retry_policy"].backoff_max_sec == 8.0
    assert client_kwargs["authenticated_retry_policy"].jitter_max_sec == 0.25
    assert "lookback : 123 days" in output
    assert f"raw      : {project_root / 'data/kalshi/raw/markets'}" in output
    assert f"normalized: {project_root / 'data/kalshi/normalized/trades'}" in output
    assert f"features : {project_root / 'data/kalshi/features/real'}" in output
    assert f"status   : {project_root / 'artifacts/kalshi_ingest'}" in output
    assert "resume recovery: automatic" in output
    assert "Starting download... (this may take several minutes for 123 days of data)" in output


def test_cli_allows_flag_overrides_for_sleep_and_feature_toggles(tmp_path):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(yaml.safe_dump({"ingestion": {"backfill_days": 30}}), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        lookback_days=45,
        period="1d",
        sleep=0.1,
        output_dir="custom/features",
        tickers=["ABC-1"],
        no_base_rate=True,
        metaculus=True,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(captured["config"].manifest_path),
                summary_path=Path(captured["config"].summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", return_value=KalshiConfig("id", DUMMY_PEM, False)), \
         patch("trading_platform.kalshi.client.KalshiClient"), \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    config = captured["config"]
    project_root = tmp_path
    assert config.lookback_days == 45
    assert config.feature_period == "1d"
    assert config.request_sleep_sec == 0.1
    assert config.authenticated_request_sleep_sec == 0.072
    assert config.run_base_rate is False
    assert config.run_metaculus is True
    assert config.ticker_filter == ["ABC-1"]
    assert config.features_dir == str(project_root / "custom/features")


def test_cli_reads_authenticated_live_retry_config_from_yaml(tmp_path):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "historical_ingest": {
                    "authenticated_request_sleep_sec": 0.2,
                    "authenticated_rate_limit_max_retries": 7,
                    "authenticated_rate_limit_backoff_base_sec": 1.5,
                    "authenticated_rate_limit_backoff_max_sec": 30.0,
                    "authenticated_rate_limit_jitter_max_sec": 0.75,
                },
            }
        ),
        encoding="utf-8",
    )

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(config.manifest_path),
                summary_path=Path(config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", return_value=KalshiConfig("id", DUMMY_PEM, False)), \
         patch("trading_platform.kalshi.client.KalshiClient") as mock_client_cls, \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    config = captured["config"]
    client_kwargs = mock_client_cls.call_args.kwargs
    assert config.authenticated_request_sleep_sec == 0.2
    assert config.authenticated_rate_limit_max_retries == 7
    assert config.authenticated_rate_limit_backoff_base_sec == 1.5
    assert config.authenticated_rate_limit_backoff_max_sec == 30.0
    assert config.authenticated_rate_limit_jitter_max_sec == 0.75
    assert client_kwargs["authenticated_sleep_sec"] == 0.2
    assert client_kwargs["authenticated_retry_policy"].max_retries == 7
    assert client_kwargs["authenticated_retry_policy"].backoff_base_sec == 1.5
    assert client_kwargs["authenticated_retry_policy"].backoff_max_sec == 30.0
    assert client_kwargs["authenticated_retry_policy"].jitter_max_sec == 0.75


def test_cli_uses_yaml_auth_private_key_path_for_live_bridge(tmp_path, monkeypatch):
    key_path = tmp_path / "kalshi.pem"
    key_path.write_text(DUMMY_PEM, encoding="utf-8")
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "auth": {
                    "api_key_id": "yaml-key-id",
                    "private_key_path": str(key_path),
                },
            }
        ),
        encoding="utf-8",
    )

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )

    class FakePipeline:
        def __init__(self, client, config):
            self.config = config

        def run(self):
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(self.config.manifest_path),
                summary_path=Path(self.config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", side_effect=ValueError("missing")), \
         patch("trading_platform.kalshi.client.KalshiClient") as mock_client_cls, \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    client_config = mock_client_cls.call_args.args[0]
    assert client_config.api_key_id == "yaml-key-id"
    assert client_config.private_key_path == str(key_path)
    assert "BEGIN RSA PRIVATE KEY" in client_config.private_key_pem


def test_cli_supports_fresh_run_mode(tmp_path, capsys):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(yaml.safe_dump({"ingestion": {"backfill_days": 30}}), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=True,
    )

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(config.manifest_path),
                summary_path=Path(config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", return_value=KalshiConfig("id", DUMMY_PEM, False)), \
         patch("trading_platform.kalshi.client.KalshiClient"), \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    output = capsys.readouterr().out
    config = captured["config"]
    assert config.resume is False
    assert config.resume_mode == "fresh"
    assert "resume   : fresh" in output


def test_cli_supports_explicit_resume_recovery_mode(tmp_path, capsys):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(yaml.safe_dump({"ingestion": {"backfill_days": 30}}), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
        resume_recovery_mode="fail_fast",
    )

    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(config.manifest_path),
                summary_path=Path(config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", return_value=KalshiConfig("id", DUMMY_PEM, False)), \
         patch("trading_platform.kalshi.client.KalshiClient"), \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    output = capsys.readouterr().out
    assert captured["config"].resume_recovery_mode == "fail_fast"


def test_cli_direct_series_fetch_config_wired_from_yaml(tmp_path, capsys):
    """use_direct_series_fetch and direct_series_tickers are read from YAML and passed to config."""
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "ingestion": {"backfill_days": 30},
                "historical_ingest": {
                    "use_direct_series_fetch": True,
                    "direct_series_tickers": ["KXINFL", "KXFED"],
                },
            }
        ),
        encoding="utf-8",
    )
    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )
    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(config.manifest_path),
                summary_path=Path(config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    fake_kalshi_config = KalshiConfig(api_key_id="public-only", private_key_pem=DUMMY_PEM, demo=True)
    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", side_effect=ValueError("missing")), \
         patch(
             "trading_platform.cli.commands.kalshi_historical_ingest._build_public_only_config",
             return_value=fake_kalshi_config,
         ), \
         patch("trading_platform.kalshi.client.KalshiClient"), \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    config = captured["config"]
    assert config.use_direct_series_fetch is True
    assert config.direct_series_tickers == ["KXINFL", "KXFED"]


def test_cli_skip_historical_pagination_defaults_true(tmp_path, capsys):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "ingestion": {"backfill_days": 30},
                "historical_ingest": {},
            }
        ),
        encoding="utf-8",
    )
    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        period=None,
        sleep=None,
        output_dir=None,
        tickers=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume_from_checkpoint=None,
        fresh_run=False,
    )
    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, client, config):
            captured["config"] = config

        def run(self):
            config = captured["config"]
            return SimpleNamespace(
                markets_downloaded=0,
                markets_with_trades=0,
                markets_skipped_no_trades=0,
                markets_failed=0,
                total_trades=0,
                total_candlesticks=0,
                resolution_count=0,
                feature_files_written=0,
                normalized_markets_written=0,
                date_range_start=None,
                date_range_end=None,
                manifest_path=Path(config.manifest_path),
                summary_path=Path(config.summary_path),
                status_artifact_path=None,
                run_summary_artifact_path=None,
            )

    fake_kalshi_config = KalshiConfig(api_key_id="public-only", private_key_pem=DUMMY_PEM, demo=True)
    with patch("trading_platform.kalshi.auth.KalshiConfig.from_env", side_effect=ValueError("missing")), \
         patch(
             "trading_platform.cli.commands.kalshi_historical_ingest._build_public_only_config",
             return_value=fake_kalshi_config,
         ), \
         patch("trading_platform.kalshi.client.KalshiClient"), \
         patch("trading_platform.kalshi.historical_ingest.HistoricalIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_historical_ingest(args)

    config = captured["config"]
    assert config.skip_historical_pagination is True
