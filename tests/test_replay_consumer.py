from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.monitoring.drilldown import load_dataset_timeline, load_provider_timeline
from trading_platform.monitoring.provider_monitoring import (
    build_cross_provider_monitoring_summary,
    read_monitoring_history,
    read_recent_transitions,
)
from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, upsert_dataset_registry_entry
from trading_platform.research.replay_assembly import ReplayAssemblyRequest, assemble_replay_dataset, write_replay_assembly_artifacts
from trading_platform.research.replay_consumer import ReplayConsumerRequest, load_replay_consumer_input, write_replay_consumer_summary


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _seed_registry(tmp_path: Path) -> Path:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    binance_path = tmp_path / "data" / "research" / "binance_features.parquet"
    kalshi_path = tmp_path / "data" / "research" / "kalshi_features.parquet"
    _write_parquet(
        binance_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m"],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"], utc=True),
                "feature_time": pd.to_datetime(["2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"], utc=True),
                "close": [42000.0, 42010.0],
                "target_return_1": [0.001, None],
            }
        ),
    )
    _write_parquet(
        kalshi_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m"],
                "timestamp": pd.to_datetime(["2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"], utc=True),
                "close": [61.0, 62.0],
            }
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.crypto_market_features",
            provider="binance",
            asset_class="crypto",
            dataset_name="crypto_market_features",
            dataset_path=str(binance_path),
            symbols=["BTCUSDT"],
            intervals=["1m"],
            latest_event_time="2024-01-01T00:02:00Z",
            metadata={"time_semantics": {"feature_time": "feature time"}, "keys": ["symbol", "interval", "timestamp"]},
            health_references={"health_summary_path": str(tmp_path / "artifacts" / "binance_health.json")},
            manifest_references={"latest_sync_manifest_path": str(tmp_path / "artifacts" / "binance_sync.json")},
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(kalshi_path),
            symbols=["BTCUSDT"],
            intervals=["1m"],
            latest_event_time="2024-01-01T00:02:00Z",
            metadata={"time_semantics": {"timestamp": "market time"}, "keys": ["symbol", "interval", "timestamp"]},
            health_references={"validation_summary_path": str(tmp_path / "artifacts" / "kalshi_validation.json")},
            manifest_references={"latest_run_summary_path": str(tmp_path / "artifacts" / "kalshi_run.json")},
        ),
    )
    _write_json(tmp_path / "artifacts" / "binance_health.json", {"status": "healthy"})
    _write_json(tmp_path / "artifacts" / "binance_sync.json", {"status": "completed"})
    _write_json(tmp_path / "artifacts" / "kalshi_validation.json", {"status": "warning"})
    _write_json(tmp_path / "artifacts" / "kalshi_run.json", {"overall_status": "completed_with_failures"})
    return registry_path


def test_replay_consumer_loads_on_demand_assembly_and_classifies_columns(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    result = load_replay_consumer_input(
        ReplayConsumerRequest(
            assembly_request=ReplayAssemblyRequest(
                registry_path=registry_path,
                dataset_keys=["binance.crypto_market_features", "kalshi.prediction_market_features"],
                alignment_mode="outer_union",
            )
        )
    )
    assert "binance__crypto_market_features__close" in result.feature_columns
    assert "binance__crypto_market_features__target_return_1" in result.target_columns
    assert result.metadata["alignment_mode"] == "outer_union"


def test_replay_consumer_loads_materialized_assembly_and_writes_summary(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    assembly = assemble_replay_dataset(
        ReplayAssemblyRequest(
            registry_path=registry_path,
            dataset_keys=["binance.crypto_market_features"],
        )
    )
    paths = write_replay_assembly_artifacts(
        result=assembly,
        output_path=tmp_path / "artifacts" / "replay" / "assembled.parquet",
        summary_path=tmp_path / "artifacts" / "replay" / "assembled.summary.json",
    )
    consumer = load_replay_consumer_input(
        ReplayConsumerRequest(
            assembly_dataset_path=paths["output_path"],
            assembly_summary_path=paths["summary_path"],
        )
    )
    assert consumer.metadata["assembly_source"] == "materialized_artifact"
    summary_path = write_replay_consumer_summary(
        result=consumer,
        output_path=tmp_path / "artifacts" / "replay" / "consumer.summary.json",
    )
    assert Path(summary_path).exists()


def test_monitoring_history_and_timeline_readers_capture_transitions(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    output_root = tmp_path / "artifacts" / "provider_monitoring"
    build_cross_provider_monitoring_summary(
        registry_path=registry_path,
        output_root=output_root,
        staleness_threshold_hours=100000,
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(tmp_path / "data" / "research" / "kalshi_features.parquet"),
            symbols=["BTCUSDT"],
            intervals=["1m"],
            latest_event_time="2023-01-01T00:00:00Z",
            metadata={"time_semantics": {"timestamp": "market time"}},
            health_references={"validation_summary_path": str(tmp_path / "artifacts" / "kalshi_validation.json")},
            manifest_references={"latest_run_summary_path": str(tmp_path / "artifacts" / "kalshi_run.json")},
        ),
    )
    build_cross_provider_monitoring_summary(
        registry_path=registry_path,
        output_root=output_root,
        staleness_threshold_hours=1,
    )

    history = read_monitoring_history(output_root=output_root)
    transitions = read_recent_transitions(output_root=output_root)
    assert len(history) == 2
    assert transitions["transition_count"] >= 1

    provider_timeline = load_provider_timeline(monitoring_output_root=output_root, provider="kalshi")
    dataset_timeline = load_dataset_timeline(
        monitoring_output_root=output_root,
        dataset_key="kalshi.prediction_market_features",
    )
    assert len(provider_timeline.history) == 2
    assert len(dataset_timeline.history) == 2
