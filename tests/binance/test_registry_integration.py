from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceDatasetRegistryConfig, BinanceFeatureConfig, BinanceResearchDatasetConfig
from trading_platform.binance.research import (
    load_binance_research_frame_from_registry,
    materialize_binance_research_dataset,
    resolve_binance_research_registry_entry,
)


def _write_projection_outputs(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2024-01-01T00:00:00Z"),
                "close_timestamp": pd.Timestamp("2024-01-01T00:00:59Z"),
                "event_time": pd.Timestamp("2024-01-01T00:00:59Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:01:00Z"),
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
                "quote_volume": 100.0,
                "trade_count": 2,
                "taker_buy_base_volume": 0.5,
                "taker_buy_quote_volume": 50.0,
            },
            {
                "timestamp": pd.Timestamp("2024-01-01T00:01:00Z"),
                "close_timestamp": pd.Timestamp("2024-01-01T00:01:59Z"),
                "event_time": pd.Timestamp("2024-01-01T00:01:59Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:02:00Z"),
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": 100.0,
                "high": 102.0,
                "low": 99.5,
                "close": 101.0,
                "volume": 2.0,
                "quote_volume": 202.0,
                "trade_count": 3,
                "taker_buy_base_volume": 1.0,
                "taker_buy_quote_volume": 101.0,
            },
        ]
    ).to_parquet(root / "crypto_ohlcv_bars.parquet", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2024-01-01T00:00:10Z"),
                "event_time": pd.Timestamp("2024-01-01T00:00:10Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:00:11Z"),
                "symbol": "BTCUSDT",
                "aggregate_trade_id": 1,
                "price": 100.0,
                "quantity": 0.2,
                "is_buyer_maker": False,
            }
        ]
    ).to_parquet(root / "crypto_agg_trades.parquet", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2024-01-01T00:00:20Z"),
                "event_time": pd.Timestamp("2024-01-01T00:00:20Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:00:21Z"),
                "symbol": "BTCUSDT",
                "bid_price": 99.9,
                "bid_quantity": 1.0,
                "ask_price": 100.1,
                "ask_quantity": 1.1,
                "dedupe_key": "BTCUSDT|1",
            }
        ]
    ).to_parquet(root / "crypto_top_of_book.parquet", index=False)


def test_materialized_binance_research_dataset_publishes_registry_entry(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    _write_projection_outputs(projection_root)
    build_binance_market_features(
        BinanceFeatureConfig(
            projection_root=str(projection_root),
            features_root=str(tmp_path / "features"),
            feature_store_root=str(tmp_path / "feature_store"),
            summary_path=str(tmp_path / "features" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        ),
        full_rebuild=True,
    )
    registry_path = tmp_path / "registry" / "datasets.json"

    result = materialize_binance_research_dataset(
        BinanceResearchDatasetConfig(
            feature_store_root=str(tmp_path / "feature_store"),
            output_root=str(tmp_path / "research"),
            summary_path=str(tmp_path / "research" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            target_horizons=(1,),
            registry=BinanceDatasetRegistryConfig(
                enabled=True,
                registry_path=str(registry_path),
                dataset_key="binance.crypto_market_features",
                dataset_name="crypto_market_features",
                asset_class="crypto",
            ),
            latest_sync_manifest_path=str(tmp_path / "sync" / "latest_sync_manifest.json"),
            status_summary_path=str(tmp_path / "status" / "binance_status.json"),
            alerts_summary_path=str(tmp_path / "monitoring" / "alerts_summary.json"),
            health_summary_path=str(tmp_path / "monitoring" / "health_check.json"),
        )
    )

    assert result.registry_path == str(registry_path)
    entry = resolve_binance_research_registry_entry(
        registry_path=registry_path,
        dataset_key="binance.crypto_market_features",
    )
    assert entry.provider == "binance"
    assert entry.dataset_path == result.dataset_path
    payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert payload["registry_path"] == str(registry_path)


def test_load_binance_research_frame_from_registry_filters_by_scope(tmp_path: Path) -> None:
    frame_path = tmp_path / "research" / "binance_research_dataset.parquet"
    frame_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"],
                utc=True,
            ),
            "feature_time": pd.to_datetime(
                ["2024-01-01T00:00:59Z", "2024-01-01T00:01:59Z", "2024-01-01T00:02:59Z"],
                utc=True,
            ),
            "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "interval": ["1m", "5m", "1m"],
            "close": [100.0, 101.0, 200.0],
        }
    ).to_parquet(frame_path, index=False)
    registry_path = tmp_path / "registry" / "datasets.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-04-03T00:00:00+00:00",
                "entry_count": 1,
                "entries": [
                    {
                        "dataset_key": "binance.crypto_market_features",
                        "provider": "binance",
                        "asset_class": "crypto",
                        "dataset_name": "crypto_market_features",
                        "dataset_path": str(frame_path),
                        "symbols": ["BTCUSDT", "ETHUSDT"],
                        "intervals": ["1m", "5m"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    frame = load_binance_research_frame_from_registry(
        registry_path=registry_path,
        dataset_key="binance.crypto_market_features",
        symbols=["BTCUSDT"],
        intervals=["1m"],
        end="2024-01-01T00:01:00Z",
    )

    assert len(frame.index) == 1
    assert frame.iloc[0]["symbol"] == "BTCUSDT"
    assert frame.iloc[0]["interval"] == "1m"
