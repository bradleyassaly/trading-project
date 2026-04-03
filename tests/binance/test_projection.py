from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.binance.models import BinanceProjectionConfig
from trading_platform.binance.projection import project_binance_market_data


def test_projection_merges_historical_and_incremental_sources(tmp_path: Path) -> None:
    historical_root = tmp_path / "normalized"
    incremental_root = tmp_path / "normalized_incremental"
    (historical_root / "klines" / "BTCUSDT").mkdir(parents=True)
    (historical_root / "agg_trades").mkdir(parents=True)
    (historical_root / "book_ticker").mkdir(parents=True)
    (incremental_root / "klines" / "BTCUSDT").mkdir(parents=True)
    (incremental_root / "agg_trades").mkdir(parents=True)
    (incremental_root / "book_ticker").mkdir(parents=True)

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
                "quote_volume": 100.0,
                "trade_count": 2,
                "taker_buy_base_volume": 0.5,
                "taker_buy_quote_volume": 50.0,
                "provider": "binance",
                "source": "binance_rest",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_klines_v1",
                "raw_artifact_path": "raw.json",
                "dedupe_key": "BTCUSDT|1m|1",
            }
        ]
    ).to_parquet(historical_root / "klines" / "BTCUSDT" / "1m.parquet", index=False)
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
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1.2,
                "quote_volume": 120.0,
                "trade_count": 3,
                "taker_buy_base_volume": 0.6,
                "taker_buy_quote_volume": 60.0,
                "provider": "binance",
                "source": "binance_websocket",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_klines_v1",
                "raw_artifact_path": "ws.jsonl",
                "dedupe_key": "BTCUSDT|1m|2",
            }
        ]
    ).to_parquet(incremental_root / "klines" / "BTCUSDT" / "1m.parquet", index=False)
    pd.DataFrame(
        [{"symbol": "BTCUSDT", "aggregate_trade_id": 1, "timestamp": pd.Timestamp("2024-01-01T00:00:00Z"), "event_time": pd.Timestamp("2024-01-01T00:00:00Z"), "ingested_at": pd.Timestamp("2024-01-01T00:00:10Z"), "dedupe_key": "BTCUSDT|1"}]
    ).to_parquet(historical_root / "agg_trades" / "BTCUSDT.parquet", index=False)
    pd.DataFrame(
        [{"symbol": "BTCUSDT", "aggregate_trade_id": 2, "timestamp": pd.Timestamp("2024-01-01T00:00:01Z"), "event_time": pd.Timestamp("2024-01-01T00:00:01Z"), "ingested_at": pd.Timestamp("2024-01-01T00:00:11Z"), "dedupe_key": "BTCUSDT|2"}]
    ).to_parquet(incremental_root / "agg_trades" / "BTCUSDT.parquet", index=False)
    pd.DataFrame(
        [{"symbol": "BTCUSDT", "timestamp": pd.Timestamp("2024-01-01T00:00:00Z"), "event_time": pd.Timestamp("2024-01-01T00:00:00Z"), "ingested_at": pd.Timestamp("2024-01-01T00:00:10Z"), "dedupe_key": "BTCUSDT|rest", "bid_price": 100.0, "bid_quantity": 1.0, "ask_price": 100.1, "ask_quantity": 1.2}]
    ).to_parquet(historical_root / "book_ticker" / "BTCUSDT.parquet", index=False)
    pd.DataFrame(
        [{"symbol": "BTCUSDT", "timestamp": pd.Timestamp("2024-01-01T00:00:01Z"), "event_time": pd.Timestamp("2024-01-01T00:00:01Z"), "ingested_at": pd.Timestamp("2024-01-01T00:00:11Z"), "dedupe_key": "BTCUSDT|2", "bid_price": 100.2, "bid_quantity": 1.1, "ask_price": 100.3, "ask_quantity": 1.3}]
    ).to_parquet(incremental_root / "book_ticker" / "BTCUSDT.parquet", index=False)

    result = project_binance_market_data(
        BinanceProjectionConfig(
            historical_normalized_root=str(historical_root),
            incremental_normalized_root=str(incremental_root),
            output_root=str(tmp_path / "projections"),
            summary_path=str(tmp_path / "projections" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
        )
    )

    assert result.row_counts["crypto_ohlcv_bars"] == 2
    assert result.row_counts["crypto_agg_trades"] == 2
    assert result.row_counts["crypto_top_of_book"] == 2
