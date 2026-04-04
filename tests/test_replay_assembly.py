from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.monitoring.drilldown import load_dataset_drilldown, load_provider_drilldown
from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, upsert_dataset_registry_entry
from trading_platform.research.replay_assembly import (
    ReplayAssemblyRequest,
    assemble_replay_dataset,
    write_replay_assembly_artifacts,
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
    kalshi_dir = tmp_path / "data" / "kalshi" / "features"
    _write_parquet(
        binance_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "interval": ["1m", "1m"],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"], utc=True),
                "feature_time": pd.to_datetime(["2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"], utc=True),
                "close": [42000.0, 42020.0],
                "target_return_1": [0.001, None],
            }
        ),
    )
    _write_parquet(
        kalshi_dir / "FED-2024.parquet",
        pd.DataFrame(
            {
                "symbol": ["FED-2024", "FED-2024"],
                "timestamp": pd.to_datetime(["2024-01-01T00:01:00Z", "2024-01-01T00:03:00Z"], utc=True),
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
            health_references={"health_summary_path": "artifacts/binance/health.json"},
            manifest_references={"latest_sync_manifest_path": "artifacts/binance/latest_sync_manifest.json"},
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(kalshi_dir),
            storage_type="parquet_directory",
            symbols=["FED-2024"],
            latest_event_time="2024-01-01T00:03:00Z",
            metadata={"time_semantics": {"timestamp": "market timestamp"}},
            health_references={"validation_summary_path": "data/kalshi/validation/summary.json"},
            manifest_references={"ingest_summary_path": "data/kalshi/raw/ingest_summary.json"},
        ),
    )
    return registry_path


def test_replay_assembly_outer_union_namespaces_columns(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    result = assemble_replay_dataset(
        ReplayAssemblyRequest(
            registry_path=registry_path,
            dataset_keys=["binance.crypto_market_features", "kalshi.prediction_market_features"],
            alignment_mode="outer_union",
        )
    )
    assert "event_time" in result.frame.columns
    assert "binance__crypto_market_features__close" in result.frame.columns
    assert "kalshi__prediction_market_features__close" in result.frame.columns
    assert result.metadata["alignment_mode"] == "outer_union"
    assert len(result.components) == 2


def test_replay_assembly_anchor_mode_uses_backward_time_alignment(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    result = assemble_replay_dataset(
        ReplayAssemblyRequest(
            registry_path=registry_path,
            dataset_keys=["binance.crypto_market_features", "kalshi.prediction_market_features"],
            alignment_mode="anchor",
            anchor_dataset_key="kalshi.prediction_market_features",
            tolerance="2m",
        )
    )
    assert result.metadata["anchor_dataset_key"] == "kalshi.prediction_market_features"
    assert len(result.frame.index) == 2
    assert "binance__crypto_market_features__close" in result.frame.columns
    assert result.join_plan[0]["mode"] == "anchor"


def test_replay_assembly_writes_artifacts(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    result = assemble_replay_dataset(
        ReplayAssemblyRequest(
            registry_path=registry_path,
            dataset_keys=["binance.crypto_market_features"],
        )
    )
    paths = write_replay_assembly_artifacts(
        result=result,
        output_path=tmp_path / "artifacts" / "replay" / "assembled.parquet",
        summary_path=tmp_path / "artifacts" / "replay" / "assembled.summary.json",
    )
    assert Path(paths["output_path"]).exists()
    assert Path(paths["summary_path"]).exists()


def test_replay_assembly_raises_on_missing_dataset_selection(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    with pytest.raises(KeyError, match="No replay assembly datasets matched"):
        assemble_replay_dataset(
            ReplayAssemblyRequest(
                registry_path=registry_path,
                providers=["polymarket"],
                dataset_names=["missing"],
            )
        )


def test_provider_and_dataset_drilldown_use_shared_artifacts(tmp_path: Path) -> None:
    registry_path = _seed_registry(tmp_path)
    monitoring_root = tmp_path / "artifacts" / "provider_monitoring"
    _write_json(
        monitoring_root / "latest_registry_summary.json",
        {"generated_at": "2024-01-01T00:00:00Z", "entry_count": 2},
    )
    _write_json(
        monitoring_root / "latest_monitoring_summary.json",
        {
            "generated_at": "2024-01-01T00:00:00Z",
            "records": [
                {"provider": "binance", "dataset_key": "binance.crypto_market_features", "status": "healthy", "stale": False},
                {"provider": "kalshi", "dataset_key": "kalshi.prediction_market_features", "status": "warning", "stale": True},
            ],
        },
    )
    _write_json(
        monitoring_root / "cross_provider_health_summary.json",
        {
            "generated_at": "2024-01-01T00:00:00Z",
            "provider_summaries": [
                {"provider": "binance", "status": "healthy", "dataset_count": 1, "stale_dataset_count": 0},
                {"provider": "kalshi", "status": "warning", "dataset_count": 1, "stale_dataset_count": 1},
            ],
        },
    )

    provider_result = load_provider_drilldown(
        registry_path=registry_path,
        monitoring_output_root=monitoring_root,
        provider="kalshi",
    )
    assert provider_result.provider == "kalshi"
    assert len(provider_result.datasets) == 1
    assert provider_result.health_summary["status"] == "warning"

    dataset_result = load_dataset_drilldown(
        registry_path=registry_path,
        monitoring_output_root=monitoring_root,
        dataset_key="binance.crypto_market_features",
    )
    assert dataset_result.dataset.provider == "binance"
    assert dataset_result.monitoring_record["status"] == "healthy"
