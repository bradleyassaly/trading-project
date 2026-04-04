from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceFeatureConfig, BinanceStatusConfig
from trading_platform.binance.status import build_binance_status


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


def test_build_binance_status_marks_records_stale_or_fresh(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    _write_projection_outputs(projection_root)
    features_root = tmp_path / "features"
    feature_store_root = tmp_path / "feature_store"
    build_binance_market_features(
        BinanceFeatureConfig(
            projection_root=str(projection_root),
            features_root=str(features_root),
            feature_store_root=str(feature_store_root),
            summary_path=str(features_root / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        ),
        full_rebuild=True,
    )
    latest_manifest = tmp_path / "sync" / "latest_sync_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text('{"sync_id": "binance-sync-test"}', encoding="utf-8")
    config = BinanceStatusConfig(
        projection_root=str(projection_root),
        features_root=str(features_root),
        feature_store_root=str(feature_store_root),
        latest_sync_manifest_path=str(latest_manifest),
        summary_path=str(tmp_path / "status" / "status.json"),
        symbols=("BTCUSDT",),
        intervals=("1m",),
        projection_staleness_threshold_sec=1,
        feature_staleness_threshold_sec=1,
    )

    result = build_binance_status(config)

    assert result.dataset_count >= 2
    payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert payload["latest_sync_id"] == "binance-sync-test"
    assert any(record["dataset_name"] == "crypto_market_features" for record in payload["records"])
    assert all(record["stale"] is True for record in payload["records"])


def test_build_binance_status_can_classify_records_as_fresh(tmp_path: Path) -> None:
    projection_root = tmp_path / "projections"
    now = pd.Timestamp.utcnow()
    projection_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "timestamp": now.floor("min"),
                "close_timestamp": now,
                "event_time": now,
                "ingested_at": now,
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1.0,
            }
        ]
    ).to_parquet(projection_root / "crypto_ohlcv_bars.parquet", index=False)
    config = BinanceStatusConfig(
        projection_root=str(projection_root),
        features_root=str(tmp_path / "features"),
        feature_store_root=str(tmp_path / "feature_store"),
        latest_sync_manifest_path=str(tmp_path / "sync" / "latest_sync_manifest.json"),
        summary_path=str(tmp_path / "status" / "status.json"),
        symbols=("BTCUSDT",),
        intervals=("1m",),
        projection_staleness_threshold_sec=3600,
        feature_staleness_threshold_sec=3600,
    )

    result = build_binance_status(config, latest_sync_id="fresh-sync")

    assert result.stale_dataset_count == 0
