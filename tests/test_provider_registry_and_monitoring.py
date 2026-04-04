from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.ops_monitor_providers_health import cmd_ops_monitor_providers_health
from trading_platform.cli.commands.ops_monitor_providers_summary import cmd_ops_monitor_providers_summary
from trading_platform.cli.commands.research_dataset_registry_list import cmd_research_dataset_registry_list
from trading_platform.cli.commands.research_dataset_registry_publish import cmd_research_dataset_registry_publish
from trading_platform.monitoring.provider_monitoring import build_cross_provider_monitoring_summary
from trading_platform.research.dataset_registry import list_dataset_registry_entries, upsert_dataset_registry_entry, ResearchDatasetRegistryEntry
from trading_platform.research.provider_dataset_registry import publish_shared_dataset_registry


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_parquet(path: Path, *, symbol: str, timestamp: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"timestamp": [pd.Timestamp(timestamp, tz="UTC")], "symbol": [symbol], "close": [55.0]}).to_parquet(path, index=False)


def _write_kalshi_config(root: Path) -> Path:
    config_path = root / "configs" / "kalshi.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    features_dir = root / "data" / "kalshi" / "features" / "real"
    _write_parquet(features_dir / "FED-001.parquet", symbol="FED-001", timestamp="2026-04-01T00:00:00Z")
    _write_json(root / "data" / "kalshi" / "raw" / "ingest_summary.json", {"date_range_end": "2026-04-01"})
    _write_json(root / "data" / "kalshi" / "raw" / "ingest_manifest.json", {"date_range_end": "2026-04-01"})
    _write_json(root / "data" / "kalshi" / "validation" / "kalshi_data_validation_summary.json", {"status": "PASS", "passed": True})
    _write_json(root / "artifacts" / "kalshi_ingest" / "run_1" / "ingest_status.json", {"overall_status": "completed"})
    _write_json(root / "artifacts" / "kalshi_ingest" / "run_1" / "ingest_run_summary.json", {"overall_status": "completed"})
    config_path.write_text(
        "\n".join(
            [
                "historical_ingest:",
                "  feature_period: 1h",
                "  features_dir: data/kalshi/features/real",
                "  manifest_path: data/kalshi/raw/ingest_manifest.json",
                "  summary_path: data/kalshi/raw/ingest_summary.json",
                "  status_artifacts_root: artifacts/kalshi_ingest",
                "data_validation:",
                "  output_dir: data/kalshi/validation",
                "research_registry:",
                "  enabled: true",
                "  path: data/research/dataset_registry.json",
                "  dataset_key: kalshi.prediction_market_features",
                "  dataset_name: prediction_market_features",
                "  asset_class: prediction_market",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _write_polymarket_config(root: Path) -> Path:
    config_path = root / "configs" / "polymarket.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    features_dir = root / "data" / "polymarket" / "features"
    _write_parquet(features_dir / "pm-001.parquet", symbol="pm-001", timestamp="2026-04-02T00:00:00Z")
    _write_json(root / "data" / "polymarket" / "raw" / "ingest_manifest.json", {"date_range_end": "2026-04-02", "markets_processed": 1})
    (root / "data" / "polymarket" / "resolution.csv").parent.mkdir(parents=True, exist_ok=True)
    (root / "data" / "polymarket" / "resolution.csv").write_text("ticker,resolution_price\npm-001,100\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "features_dir: data/polymarket/features",
                "resolution_csv_path: data/polymarket/resolution.csv",
                "manifest_path: data/polymarket/raw/ingest_manifest.json",
                "feature_period: 1h",
                "tag_slugs:",
                "  - politics",
                "research_registry:",
                "  enabled: true",
                "  path: data/research/dataset_registry.json",
                "  dataset_key: polymarket.prediction_market_features",
                "  dataset_name: prediction_market_features",
                "  asset_class: prediction_market",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_publish_shared_dataset_registry_adds_kalshi_and_polymarket(tmp_path: Path) -> None:
    kalshi_config = _write_kalshi_config(tmp_path)
    polymarket_config = _write_polymarket_config(tmp_path)
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"

    result = publish_shared_dataset_registry(
        project_root=tmp_path,
        registry_path=registry_path,
        kalshi_config_path=kalshi_config,
        polymarket_config_path=polymarket_config,
        include_providers=["kalshi", "polymarket"],
    )

    assert result.published_count == 2
    entries = list_dataset_registry_entries(registry_path=registry_path)
    assert {entry.provider for entry in entries} == {"kalshi", "polymarket"}
    kalshi_entry = next(entry for entry in entries if entry.provider == "kalshi")
    polymarket_entry = next(entry for entry in entries if entry.provider == "polymarket")
    assert kalshi_entry.storage_type == "parquet_directory"
    assert polymarket_entry.storage_type == "parquet_directory"
    assert kalshi_entry.health_references["validation_summary_path"].endswith("kalshi_data_validation_summary.json")
    assert polymarket_entry.manifest_references["ingest_manifest_path"].endswith("ingest_manifest.json")


def test_cross_provider_monitoring_summary_aggregates_statuses(tmp_path: Path) -> None:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.crypto_market_features",
                provider="binance",
                asset_class="crypto",
                dataset_name="crypto_market_features",
                dataset_path=str(tmp_path / "data" / "binance" / "research.parquet"),
                latest_materialized_at="2026-04-03T23:59:00+00:00",
            health_references={"health_summary_path": str(tmp_path / "data" / "binance" / "health.json")},
            manifest_references={"latest_sync_manifest_path": str(tmp_path / "data" / "binance" / "latest_sync_manifest.json")},
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(tmp_path / "data" / "kalshi" / "features"),
            storage_type="parquet_directory",
            latest_materialized_at="2026-04-01T00:00:00+00:00",
            health_references={"validation_summary_path": str(tmp_path / "data" / "kalshi" / "validation.json")},
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="polymarket.prediction_market_features",
            provider="polymarket",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(tmp_path / "data" / "polymarket" / "features"),
            storage_type="parquet_directory",
            latest_materialized_at="2026-01-01T00:00:00+00:00",
            manifest_references={"ingest_manifest_path": str(tmp_path / "missing_manifest.json")},
        ),
    )
    _write_json(tmp_path / "data" / "binance" / "health.json", {"status": "healthy"})
    _write_json(tmp_path / "data" / "binance" / "latest_sync_manifest.json", {"status": "completed"})
    _write_json(tmp_path / "data" / "kalshi" / "validation.json", {"status": "WARNING", "passed": True})

    result = build_cross_provider_monitoring_summary(
        registry_path=registry_path,
        output_root=tmp_path / "artifacts" / "provider_monitoring",
        staleness_threshold_hours=24,
    )

    assert result.record_count == 3
    assert result.provider_count == 3
    payload = json.loads(Path(result.health_summary_path).read_text(encoding="utf-8"))
    assert payload["overall_status"] == "critical"
    provider_status = {row["provider"]: row["status"] for row in payload["provider_summaries"]}
    assert provider_status["binance"] == "healthy"
    assert provider_status["kalshi"] == "warning"
    assert provider_status["polymarket"] == "critical"


def test_registry_publish_and_monitor_commands_render_summaries(tmp_path: Path, capsys) -> None:
    kalshi_config = _write_kalshi_config(tmp_path)
    polymarket_config = _write_polymarket_config(tmp_path)
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"

    cmd_research_dataset_registry_publish(
        Namespace(
            registry_path=str(registry_path),
            kalshi_config=str(kalshi_config),
            polymarket_config=str(polymarket_config),
            providers=["kalshi", "polymarket"],
            summary_path=str(tmp_path / "artifacts" / "provider_monitoring" / "latest_registry_summary.json"),
        )
    )
    publish_stdout = capsys.readouterr().out
    assert "published      : 2" in publish_stdout

    cmd_research_dataset_registry_list(
        Namespace(
            registry_path=str(registry_path),
            provider=None,
            asset_class=None,
            dataset_name=None,
            format="text",
        )
    )
    list_stdout = capsys.readouterr().out
    assert "kalshi" in list_stdout
    assert "polymarket" in list_stdout

    cmd_ops_monitor_providers_summary(
        Namespace(
            registry_path=str(registry_path),
            output_root=str(tmp_path / "artifacts" / "provider_monitoring"),
            providers=None,
            asset_class=None,
            staleness_threshold_hours=48,
            format="text",
        )
    )
    summary_stdout = capsys.readouterr().out
    assert "Cross-Provider Monitoring Summary" in summary_stdout

    cmd_ops_monitor_providers_health(
        Namespace(
            registry_path=str(registry_path),
            output_root=str(tmp_path / "artifacts" / "provider_monitoring"),
            providers=None,
            asset_class=None,
            staleness_threshold_hours=48,
            format="text",
        )
    )
    health_stdout = capsys.readouterr().out
    assert "Cross-Provider Health" in health_stdout
