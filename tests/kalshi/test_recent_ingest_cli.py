from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.cli.commands import kalshi_recent_ingest as cli_module


DUMMY_PEM = "-----BEGIN RSA PRIVATE KEY-----\ndummy\n-----END RSA PRIVATE KEY-----"


def test_cli_builds_recent_ingest_config_from_yaml_and_flags(tmp_path, capsys):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "environment": {"demo": False},
                "ingestion": {"backfill_days": 21},
                "recent_ingest": {
                    "recent_ingest_enabled": True,
                    "recent_ingest_statuses": ["settled"],
                    "recent_ingest_categories": ["Economics", "Politics"],
                "recent_ingest_limit": 75,
                    "exclude_market_type_patterns": ["CROSSCATEGORY", "SPORTSMULTIGAME", "EXTENDED"],
                    "preferred_research_ingest_mode": "live_recent_filtered",
                    "direct_historical_tickers": ["OLD-1"],
                    "economics_series": ["KXINFL"],
                    "politics_series": ["KXSENATE"],
                },
            }
        ),
        encoding="utf-8",
    )

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        status=["open", "settled"],
        category=["Economics"],
        series=["KXINFL"],
        event=["KXINFL-2026"],
        limit=50,
        min_volume=250.0,
        disable_market_type_filter=True,
        direct_historical_tickers=["OLD-2"],
        period="1d",
        sleep=0.1,
        output_dir="custom/features",
        no_base_rate=True,
        metaculus=False,
        skip_validation=True,
        resume=True,
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

    with patch("trading_platform.kalshi.auth.KalshiConfig.from_mapping", return_value=KalshiConfig("id", DUMMY_PEM, False)), \
         patch("trading_platform.kalshi.client.KalshiClient") as mock_client_cls, \
         patch("trading_platform.kalshi.recent_ingest.RecentIngestPipeline", FakePipeline):
        mock_client_cls.return_value = object()
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_recent_ingest(args)

    output = capsys.readouterr().out
    config = captured["config"]
    client_kwargs = mock_client_cls.call_args.kwargs

    assert config.lookback_days == 21
    assert config.recent_ingest_statuses == ["open", "settled"]
    assert config.recent_ingest_categories == ["Economics"]
    assert config.preferred_categories == ["Economics"]
    assert config.recent_ingest_series_tickers == ["KXINFL"]
    assert config.recent_ingest_event_tickers == ["KXINFL-2026"]
    assert config.economics_series == ["KXINFL"]
    assert config.politics_series == ["KXSENATE"]
    assert config.recent_ingest_limit == 50
    assert config.min_volume == 250.0
    assert config.exclude_market_type_patterns == ["CROSSCATEGORY", "SPORTSMULTIGAME", "EXTENDED"]
    assert config.disable_market_type_filter is True
    assert config.direct_historical_tickers == ["OLD-2"]
    assert config.feature_period == "1d"
    assert config.request_sleep_sec == 0.1
    assert config.run_base_rate is False
    assert config.resume is True
    assert config.resume_mode == "latest"
    assert config.features_dir == str(tmp_path / "custom/features")
    assert config.summary_path == str(tmp_path / "data/kalshi/raw/recent_ingest_summary.json")
    assert client_kwargs["authenticated_sleep_sec"] == config.authenticated_request_sleep_sec
    assert "Kalshi Recent Ingest" in output
    assert "preferred mode : live_recent_filtered" in output
    assert "statuses       : open, settled" in output


def test_cli_recent_ingest_uses_public_fallback_config_when_auth_missing(tmp_path):
    config_path = tmp_path / "kalshi.yaml"
    config_path.write_text(yaml.safe_dump({"recent_ingest": {"recent_ingest_statuses": ["settled"]}}), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        lookback_days=None,
        status=None,
        category=None,
        series=None,
        event=None,
        limit=None,
        min_volume=None,
        disable_market_type_filter=False,
        direct_historical_tickers=None,
        period=None,
        sleep=None,
        output_dir=None,
        no_base_rate=False,
        metaculus=False,
        skip_validation=True,
        resume=False,
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

    fake_kalshi_config = KalshiConfig(api_key_id="public-only", private_key_pem=DUMMY_PEM, demo=True)
    with patch("trading_platform.kalshi.auth.KalshiConfig.from_mapping", return_value=None), \
         patch("trading_platform.cli.commands.kalshi_recent_ingest._build_public_only_config", return_value=fake_kalshi_config), \
         patch("trading_platform.kalshi.client.KalshiClient") as mock_client_cls, \
         patch("trading_platform.kalshi.recent_ingest.RecentIngestPipeline", FakePipeline):
        with patch.object(cli_module, "PROJECT_ROOT", tmp_path):
            cli_module.cmd_kalshi_recent_ingest(args)

    client_config = mock_client_cls.call_args.args[0]
    assert client_config.demo is False
    assert client_config.api_key_id == "public-only"
