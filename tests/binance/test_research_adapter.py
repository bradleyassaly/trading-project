from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceFeatureConfig, BinanceResearchDatasetConfig
from trading_platform.binance.research import materialize_binance_research_dataset


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


def test_materialize_binance_research_dataset_writes_contract_summary(tmp_path: Path) -> None:
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

    result = materialize_binance_research_dataset(
        BinanceResearchDatasetConfig(
            feature_store_root=str(tmp_path / "feature_store"),
            output_root=str(tmp_path / "research"),
            summary_path=str(tmp_path / "research" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            target_horizons=(1,),
        )
    )

    assert result.row_count == 2
    assert "target_return_1" in result.target_columns
    payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert payload["keys"] == ["symbol", "interval", "timestamp"]


def test_materialize_binance_research_dataset_handles_empty_store(tmp_path: Path) -> None:
    result = materialize_binance_research_dataset(
        BinanceResearchDatasetConfig(
            feature_store_root=str(tmp_path / "feature_store"),
            output_root=str(tmp_path / "research"),
            summary_path=str(tmp_path / "research" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            target_horizons=(1,),
        )
    )

    assert result.row_count == 0
    assert Path(result.dataset_path).exists()
