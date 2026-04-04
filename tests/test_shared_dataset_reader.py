from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.dataset_reader import (
    ResearchDatasetReadRequest,
    list_research_datasets,
    load_research_dataset,
    resolve_research_dataset,
)
from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, upsert_dataset_registry_entry


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_shared_dataset_reader_resolves_and_filters_registry_entries(tmp_path: Path) -> None:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    dataset_path = tmp_path / "data" / "research" / "binance_features.parquet"
    _write_parquet(
        dataset_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
                "interval": ["1m", "5m", "1m"],
                "timestamp": pd.to_datetime(
                    ["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z", "2024-01-01T00:00:00Z"],
                    utc=True,
                ),
                "feature_time": pd.to_datetime(
                    ["2024-01-01T00:01:00Z", "2024-01-01T00:10:00Z", "2024-01-01T00:01:00Z"],
                    utc=True,
                ),
                "close": [42000.0, 42050.0, 2200.0],
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
            dataset_path=str(dataset_path),
            symbols=["BTCUSDT", "ETHUSDT"],
            intervals=["1m", "5m"],
            metadata={"time_semantics": {"feature_time": "close time", "timestamp": "bar time"}, "keys": ["symbol", "interval", "timestamp"]},
        ),
    )

    descriptors = list_research_datasets(registry_path=registry_path, provider="binance")
    assert len(descriptors) == 1
    assert descriptors[0].time_column == "feature_time"
    assert "close" in descriptors[0].schema_columns

    resolved = resolve_research_dataset(
        registry_path=registry_path,
        provider="binance",
        dataset_name="crypto_market_features",
    )
    assert resolved.dataset_key == "binance.crypto_market_features"

    result = load_research_dataset(
        ResearchDatasetReadRequest(
            registry_path=registry_path,
            dataset_key="binance.crypto_market_features",
            symbols=["BTCUSDT"],
            intervals=["1m"],
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T00:02:00Z",
        )
    )
    assert list(result.frame["symbol"]) == ["BTCUSDT"]
    assert list(result.frame["interval"]) == ["1m"]
    assert result.filters_applied["symbols"] == ["BTCUSDT"]


def test_shared_dataset_reader_handles_directory_backed_prediction_market_datasets(tmp_path: Path) -> None:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    features_dir = tmp_path / "data" / "kalshi" / "features"
    _write_parquet(
        features_dir / "FED-2024.parquet",
        pd.DataFrame(
            {
                "symbol": ["FED-2024"],
                "timestamp": pd.to_datetime(["2024-01-02T00:00:00Z"], utc=True),
                "close": [61.0],
            }
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(features_dir),
            storage_type="parquet_directory",
            symbols=["FED-2024"],
            metadata={"purpose": "prediction_market_research_features"},
        ),
    )

    result = load_research_dataset(
        ResearchDatasetReadRequest(
            registry_path=registry_path,
            dataset_key="kalshi.prediction_market_features",
            symbols=["FED-2024"],
        )
    )
    assert len(result.frame.index) == 1
    assert result.descriptor.storage_type == "parquet_directory"
    assert result.descriptor.provider == "kalshi"


def test_shared_dataset_reader_raises_on_ambiguous_resolution(tmp_path: Path) -> None:
    registry_path = tmp_path / "data" / "research" / "dataset_registry.json"
    first_path = tmp_path / "data" / "research" / "first.parquet"
    second_path = tmp_path / "data" / "research" / "second.parquet"
    _write_parquet(first_path, pd.DataFrame({"timestamp": []}))
    _write_parquet(second_path, pd.DataFrame({"timestamp": []}))
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.first",
            provider="binance",
            asset_class="crypto",
            dataset_name="shared_name",
            dataset_path=str(first_path),
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.second",
            provider="binance",
            asset_class="crypto",
            dataset_name="shared_name",
            dataset_path=str(second_path),
        ),
    )

    with pytest.raises(ValueError, match="Ambiguous research dataset selection"):
        resolve_research_dataset(
            registry_path=registry_path,
            provider="binance",
            dataset_name="shared_name",
        )
