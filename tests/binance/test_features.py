from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceFeatureConfig


def _write_projection_inputs(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    bars = pd.DataFrame(
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
                "taker_buy_base_volume": 0.4,
                "taker_buy_quote_volume": 40.0,
                "source_mode": "historical_rest",
                "provider": "binance",
                "source": "binance_rest",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_klines_v1",
                "raw_artifact_path": "bars-1.json",
                "dedupe_key": "BTCUSDT|1m|1",
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
                "source_mode": "websocket_incremental",
                "provider": "binance",
                "source": "binance_websocket",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_klines_v1",
                "raw_artifact_path": "bars-2.jsonl",
                "dedupe_key": "BTCUSDT|1m|2",
            },
            {
                "timestamp": pd.Timestamp("2024-01-01T00:02:00Z"),
                "close_timestamp": pd.Timestamp("2024-01-01T00:02:59Z"),
                "event_time": pd.Timestamp("2024-01-01T00:02:59Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:03:00Z"),
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": 101.0,
                "high": 103.0,
                "low": 100.5,
                "close": 102.0,
                "volume": 3.0,
                "quote_volume": 306.0,
                "trade_count": 4,
                "taker_buy_base_volume": 1.6,
                "taker_buy_quote_volume": 163.2,
                "source_mode": "websocket_incremental",
                "provider": "binance",
                "source": "binance_websocket",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_klines_v1",
                "raw_artifact_path": "bars-3.jsonl",
                "dedupe_key": "BTCUSDT|1m|3",
            },
        ]
    )
    trades = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "aggregate_trade_id": 1,
                "timestamp": pd.Timestamp("2024-01-01T00:00:05Z"),
                "event_time": pd.Timestamp("2024-01-01T00:00:05Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:00:06Z"),
                "price": 100.0,
                "quantity": 0.2,
                "is_buyer_maker": False,
                "dedupe_key": "BTCUSDT|1",
            },
            {
                "symbol": "BTCUSDT",
                "aggregate_trade_id": 2,
                "timestamp": pd.Timestamp("2024-01-01T00:01:10Z"),
                "event_time": pd.Timestamp("2024-01-01T00:01:10Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:01:11Z"),
                "price": 101.0,
                "quantity": 0.3,
                "is_buyer_maker": True,
                "dedupe_key": "BTCUSDT|2",
            },
            {
                "symbol": "BTCUSDT",
                "aggregate_trade_id": 3,
                "timestamp": pd.Timestamp("2024-01-01T00:02:15Z"),
                "event_time": pd.Timestamp("2024-01-01T00:02:15Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:02:16Z"),
                "price": 102.0,
                "quantity": 0.4,
                "is_buyer_maker": False,
                "dedupe_key": "BTCUSDT|3",
            },
        ]
    )
    book = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": pd.Timestamp("2024-01-01T00:00:30Z"),
                "event_time": pd.Timestamp("2024-01-01T00:00:30Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:00:31Z"),
                "bid_price": 99.9,
                "bid_quantity": 1.0,
                "ask_price": 100.1,
                "ask_quantity": 1.2,
                "dedupe_key": "BTCUSDT|book1",
            },
            {
                "symbol": "BTCUSDT",
                "timestamp": pd.Timestamp("2024-01-01T00:01:30Z"),
                "event_time": pd.Timestamp("2024-01-01T00:01:30Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:01:31Z"),
                "bid_price": 100.9,
                "bid_quantity": 1.3,
                "ask_price": 101.1,
                "ask_quantity": 1.1,
                "dedupe_key": "BTCUSDT|book2",
            },
            {
                "symbol": "BTCUSDT",
                "timestamp": pd.Timestamp("2024-01-01T00:02:30Z"),
                "event_time": pd.Timestamp("2024-01-01T00:02:30Z"),
                "ingested_at": pd.Timestamp("2024-01-01T00:02:31Z"),
                "bid_price": 101.9,
                "bid_quantity": 1.5,
                "ask_price": 102.1,
                "ask_quantity": 1.0,
                "dedupe_key": "BTCUSDT|book3",
            },
        ]
    )
    bars.to_parquet(root / "crypto_ohlcv_bars.parquet", index=False)
    trades.to_parquet(root / "crypto_agg_trades.parquet", index=False)
    book.to_parquet(root / "crypto_top_of_book.parquet", index=False)


def test_feature_builder_consumes_projected_datasets(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    _write_projection_inputs(projection_root)
    config = BinanceFeatureConfig(
        projection_root=str(projection_root),
        features_root=str(tmp_path / "features"),
        feature_store_root=str(tmp_path / "feature_store"),
        summary_path=str(tmp_path / "features" / "summary.json"),
        symbols=("BTCUSDT",),
        intervals=("1m",),
        return_horizons=(1, 2),
        volatility_windows=(2,),
        volume_windows=(2,),
        order_book_windows=(2,),
        trade_intensity_windows=(2,),
        rebuild_lookback_rows=5,
        incremental_refresh=True,
    )

    result = build_binance_market_features(config)

    assert result.rows_written == 3
    assert result.artifacts_written == 1
    frame = pd.read_parquet(result.features_path)
    assert "return_1" in frame.columns
    assert "rolling_volatility_2" in frame.columns
    assert "spread_bps" in frame.columns
    assert "signed_flow_ratio" in frame.columns
    assert frame.loc[1, "return_1"] == pytest.approx(0.01)
    manifest = json.loads(Path(result.feature_store_manifest_paths[0]).read_text(encoding="utf-8"))
    assert manifest["symbol"] == "BTCUSDT"
    assert manifest["timeframe"] == "1m"


def test_feature_builder_incremental_refresh_replaces_overlapping_tail_without_duplicates(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    _write_projection_inputs(projection_root)
    config = BinanceFeatureConfig(
        projection_root=str(projection_root),
        features_root=str(tmp_path / "features"),
        feature_store_root=str(tmp_path / "feature_store"),
        summary_path=str(tmp_path / "features" / "summary.json"),
        symbols=("BTCUSDT",),
        intervals=("1m",),
        return_horizons=(1,),
        volatility_windows=(2,),
        volume_windows=(2,),
        order_book_windows=(2,),
        trade_intensity_windows=(2,),
        rebuild_lookback_rows=2,
        incremental_refresh=True,
    )

    first = build_binance_market_features(config)
    frame_before = pd.read_parquet(first.features_path)
    bars = pd.read_parquet(projection_root / "crypto_ohlcv_bars.parquet")
    bars = pd.concat(
        [
            bars,
            pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2024-01-01T00:03:00Z"),
                        "close_timestamp": pd.Timestamp("2024-01-01T00:03:59Z"),
                        "event_time": pd.Timestamp("2024-01-01T00:03:59Z"),
                        "ingested_at": pd.Timestamp("2024-01-01T00:04:00Z"),
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "open": 102.0,
                        "high": 104.0,
                        "low": 101.5,
                        "close": 103.0,
                        "volume": 4.0,
                        "quote_volume": 412.0,
                        "trade_count": 5,
                        "taker_buy_base_volume": 2.0,
                        "taker_buy_quote_volume": 206.0,
                        "source_mode": "websocket_incremental",
                        "provider": "binance",
                        "source": "binance_websocket",
                        "asset_class": "crypto",
                        "schema_version": "binance_crypto_klines_v1",
                        "raw_artifact_path": "bars-4.jsonl",
                        "dedupe_key": "BTCUSDT|1m|4",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    bars.to_parquet(projection_root / "crypto_ohlcv_bars.parquet", index=False)

    second = build_binance_market_features(config)
    frame_after = pd.read_parquet(second.features_path)

    assert len(frame_before.index) == 3
    assert len(frame_after.index) == 4
    assert frame_after["timestamp"].nunique() == 4
    assert frame_after.iloc[-1]["close"] == 103.0
