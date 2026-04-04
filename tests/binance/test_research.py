from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceFeatureConfig
from trading_platform.binance.research import assemble_binance_research_dataset, load_binance_feature_frame


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
                "close": 100.5,
                "volume": 1.0,
                "quote_volume": 100.5,
                "trade_count": 2,
                "taker_buy_base_volume": 0.5,
                "taker_buy_quote_volume": 50.0,
            }
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
                "price": 100.5,
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
                "bid_price": 100.4,
                "bid_quantity": 1.0,
                "ask_price": 100.6,
                "ask_quantity": 1.1,
                "dedupe_key": "BTCUSDT|1",
            }
        ]
    ).to_parquet(root / "crypto_top_of_book.parquet", index=False)


def test_load_binance_feature_frame_filters_symbol_interval_and_time(tmp_path: Path) -> None:
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

    frame = load_binance_feature_frame(
        feature_store_root=tmp_path / "feature_store",
        symbols=["BTCUSDT", "ETHUSDT"],
        intervals=["1m"],
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:01:00Z",
    )

    assert frame["symbol"].unique().tolist() == ["BTCUSDT"]
    assert frame["interval"].unique().tolist() == ["1m"]
    assert len(frame.index) == 1


def test_assemble_binance_research_dataset_adds_forward_target(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    _write_projection_outputs(projection_root)
    # Extend bars so forward target can be computed.
    bars = pd.read_parquet(projection_root / "crypto_ohlcv_bars.parquet")
    bars = pd.concat(
        [
            bars,
            pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2024-01-01T00:01:00Z"),
                        "close_timestamp": pd.Timestamp("2024-01-01T00:01:59Z"),
                        "event_time": pd.Timestamp("2024-01-01T00:01:59Z"),
                        "ingested_at": pd.Timestamp("2024-01-01T00:02:00Z"),
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "open": 100.5,
                        "high": 101.5,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 1.2,
                        "quote_volume": 121.2,
                        "trade_count": 3,
                        "taker_buy_base_volume": 0.6,
                        "taker_buy_quote_volume": 60.6,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    bars.to_parquet(projection_root / "crypto_ohlcv_bars.parquet", index=False)
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

    dataset = assemble_binance_research_dataset(
        feature_store_root=tmp_path / "feature_store",
        symbols=["BTCUSDT"],
        intervals=["1m"],
        target_horizon_bars=1,
    )

    assert "target_return_1" in dataset.columns
    assert len(dataset.index) == 2
    assert dataset.iloc[0]["target_return_1"] > 0


def test_load_binance_feature_frame_returns_empty_when_no_feature_artifacts_exist(tmp_path: Path) -> None:
    frame = load_binance_feature_frame(
        feature_store_root=tmp_path / "feature_store",
        symbols=["BTCUSDT"],
        intervals=["1m"],
    )

    assert frame.empty
