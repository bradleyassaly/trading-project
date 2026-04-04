from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.monitoring.history_summary import summarize_dataset_history, summarize_provider_history
from trading_platform.monitoring.provider_monitoring import build_cross_provider_monitoring_summary
from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, upsert_dataset_registry_entry
from trading_platform.research.replay_evaluation import (
    build_replay_evaluation_request,
    run_replay_evaluation,
    write_replay_evaluation_artifacts,
)


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
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m", "1m", "1m"],
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01T00:00:00Z",
                        "2024-01-01T00:01:00Z",
                        "2024-01-01T00:02:00Z",
                        "2024-01-01T00:03:00Z",
                    ],
                    utc=True,
                ),
                "feature_time": pd.to_datetime(
                    [
                        "2024-01-01T00:00:30Z",
                        "2024-01-01T00:01:30Z",
                        "2024-01-01T00:02:30Z",
                        "2024-01-01T00:03:30Z",
                    ],
                    utc=True,
                ),
                "momentum_1": [1.0, 2.0, 3.0, 4.0],
                "target_return_1": [0.1, 0.2, 0.3, 0.4],
            }
        ),
    )
    _write_parquet(
        kalshi_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m", "1m"],
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01T00:01:00Z",
                        "2024-01-01T00:02:00Z",
                        "2024-01-01T00:03:00Z",
                    ],
                    utc=True,
                ),
                "price_signal": [0.2, None, 0.6],
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
            latest_event_time="2024-01-01T00:03:30Z",
            latest_materialized_at="2024-01-01T00:04:00Z",
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
            latest_event_time="2024-01-01T00:03:00Z",
            latest_materialized_at="2024-01-01T00:04:30Z",
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


def test_replay_evaluation_runs_from_registry_backed_consumer(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)

    result = run_replay_evaluation(
        build_replay_evaluation_request(
            registry_path=registry_path,
            dataset_keys=["binance.crypto_market_features", "kalshi.prediction_market_features"],
            alignment_mode="outer_union",
        )
    )

    assert result.request.consumer_request.assembly_request is not None
    assert result.consumer_summary["metadata"]["alignment_mode"] == "outer_union"
    assert result.metrics
    metric = next(
        item
        for item in result.metrics
        if item.feature_column == "binance__crypto_market_features__momentum_1"
        and item.target_column == "binance__crypto_market_features__target_return_1"
    )
    assert metric.pearson_correlation == pytest.approx(1.0)


def test_replay_evaluation_artifacts_capture_contract_and_metrics(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    result = run_replay_evaluation(
        build_replay_evaluation_request(
            registry_path=registry_path,
            providers=["binance"],
            feature_columns=["binance__crypto_market_features__momentum_1"],
            target_columns=["binance__crypto_market_features__target_return_1"],
            evaluation_name="binance_eval",
        )
    )

    paths = write_replay_evaluation_artifacts(result=result, output_dir=tmp_path / "artifacts" / "evaluation")
    summary = json.loads(Path(paths["summary_path"]).read_text(encoding="utf-8"))
    metrics = pd.read_csv(paths["metrics_path"])

    assert summary["evaluation_name"] == "binance_eval"
    assert summary["request"]["consumer_request"]["providers"] == ["binance"]
    assert summary["consumer_summary"]["row_count"] == 4
    assert list(metrics["feature_column"]) == ["binance__crypto_market_features__momentum_1"]


def test_monitoring_history_summaries_capture_status_trends(tmp_path: Path) -> None:
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
            latest_event_time="2022-01-01T00:00:00Z",
            latest_materialized_at="2022-01-01T00:10:00Z",
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

    provider_summary = summarize_provider_history(output_root=output_root, provider="kalshi")
    dataset_summary = summarize_dataset_history(
        output_root=output_root,
        dataset_key="kalshi.prediction_market_features",
    )

    assert provider_summary.snapshot_count == 2
    assert provider_summary.transition_count >= 1
    assert dataset_summary.snapshot_count == 2
    assert dataset_summary.stale_count == 1
