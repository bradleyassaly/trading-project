from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.research.dataset_registry import (
    ResearchDatasetRegistryEntry,
    get_dataset_registry_entry,
    list_dataset_registry_entries,
    load_registered_dataset_frame,
    upsert_dataset_registry_entry,
)


def test_dataset_registry_upserts_and_filters_entries(tmp_path: Path) -> None:
    registry_path = tmp_path / "dataset_registry.json"
    dataset_path = tmp_path / "binance.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"], utc=True),
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "interval": ["1m", "1m"],
            "value": [1.0, 2.0],
        }
    ).to_parquet(dataset_path, index=False)

    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="binance.crypto_market_features",
            provider="binance",
            asset_class="crypto",
            dataset_name="crypto_market_features",
            dataset_path=str(dataset_path),
            symbols=["BTCUSDT", "ETHUSDT"],
            intervals=["1m"],
        ),
    )
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="example.equities.daily_features",
            provider="example",
            asset_class="equity",
            dataset_name="daily_features",
            dataset_path=str(dataset_path),
            symbols=["AAPL"],
            intervals=["1d"],
        ),
    )

    crypto_entries = list_dataset_registry_entries(
        registry_path=registry_path,
        asset_class="crypto",
    )
    assert len(crypto_entries) == 1
    assert crypto_entries[0].provider == "binance"
    assert get_dataset_registry_entry(
        registry_path=registry_path,
        dataset_key="binance.crypto_market_features",
    ).dataset_name == "crypto_market_features"


def test_load_registered_dataset_frame_applies_filters(tmp_path: Path) -> None:
    registry_path = tmp_path / "dataset_registry.json"
    dataset_path = tmp_path / "binance.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"],
                utc=True,
            ),
            "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "interval": ["1m", "5m", "1m"],
            "value": [1.0, 2.0, 3.0],
        }
    ).to_parquet(dataset_path, index=False)
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
        ),
    )

    frame = load_registered_dataset_frame(
        registry_path=registry_path,
        dataset_key="binance.crypto_market_features",
        symbols=["BTCUSDT"],
        intervals=["1m"],
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:01:00Z",
    )

    assert len(frame.index) == 1
    assert frame.iloc[0]["symbol"] == "BTCUSDT"
    assert frame.iloc[0]["interval"] == "1m"


def test_load_registered_dataset_frame_reads_parquet_directory(tmp_path: Path) -> None:
    registry_path = tmp_path / "dataset_registry.json"
    dataset_dir = tmp_path / "kalshi_features"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True),
            "symbol": ["FED-001"],
            "close": [55.0],
        }
    ).to_parquet(dataset_dir / "FED-001.parquet", index=False)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T01:00:00Z"], utc=True),
            "symbol": ["CPI-001"],
            "close": [57.0],
        }
    ).to_parquet(dataset_dir / "CPI-001.parquet", index=False)
    upsert_dataset_registry_entry(
        registry_path=registry_path,
        entry=ResearchDatasetRegistryEntry(
            dataset_key="kalshi.prediction_market_features",
            provider="kalshi",
            asset_class="prediction_market",
            dataset_name="prediction_market_features",
            dataset_path=str(dataset_dir),
            storage_type="parquet_directory",
            symbols=["FED-001", "CPI-001"],
        ),
    )

    frame = load_registered_dataset_frame(
        registry_path=registry_path,
        dataset_key="kalshi.prediction_market_features",
        symbols=["FED-001"],
    )

    assert len(frame.index) == 1
    assert frame.iloc[0]["symbol"] == "FED-001"
