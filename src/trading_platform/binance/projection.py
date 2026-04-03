from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.models import BinanceProjectionConfig, BinanceProjectionResult


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _collect_parquet_frames(root: Path, pattern: str) -> list[pd.DataFrame]:
    if not root.exists():
        return []
    frames: list[pd.DataFrame] = []
    for path in sorted(root.rglob(pattern)):
        if path.is_file():
            frame = pd.read_parquet(path)
            if not frame.empty:
                frames.append(frame)
    return frames


def _concat_dedup(
    frames: list[pd.DataFrame],
    *,
    dedupe_keys: list[str],
    sort_keys: list[str],
) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    available_sort = [column for column in sort_keys if column in combined.columns]
    if available_sort:
        combined = combined.sort_values(available_sort)
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    if available_sort:
        combined = combined.sort_values(available_sort)
    return combined.reset_index(drop=True)


def project_binance_market_data(config: BinanceProjectionConfig) -> BinanceProjectionResult:
    historical_root = Path(config.historical_normalized_root)
    incremental_root = Path(config.incremental_normalized_root)
    output_root = Path(config.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    symbol_filter = {symbol.upper() for symbol in config.symbols} if config.symbols else None
    interval_filter = {interval for interval in config.intervals} if config.intervals else None

    historical_kline_frames: list[pd.DataFrame] = []
    if (historical_root / "klines").exists():
        for frame in _collect_parquet_frames(historical_root / "klines", "*.parquet"):
            if symbol_filter is not None and not set(frame["symbol"].str.upper()).intersection(symbol_filter):
                continue
            if interval_filter is not None and not set(frame["interval"]).intersection(interval_filter):
                continue
            historical_kline_frames.append(frame.assign(source_mode="historical_rest"))

    incremental_kline_frames = [
        frame.assign(source_mode="websocket_incremental")
        for frame in _collect_parquet_frames(incremental_root / "klines", "*.parquet")
        if not frame.empty
        and (symbol_filter is None or set(frame["symbol"].str.upper()).intersection(symbol_filter))
        and (interval_filter is None or set(frame["interval"]).intersection(interval_filter))
    ]
    ohlcv = _concat_dedup(
        historical_kline_frames + incremental_kline_frames,
        dedupe_keys=["symbol", "interval", "timestamp"],
        sort_keys=["symbol", "interval", "timestamp", "ingested_at"],
    )

    historical_trade_frames: list[pd.DataFrame] = []
    if (historical_root / "agg_trades").exists():
        for frame in _collect_parquet_frames(historical_root / "agg_trades", "*.parquet"):
            if symbol_filter is not None and not set(frame["symbol"].str.upper()).intersection(symbol_filter):
                continue
            historical_trade_frames.append(frame.assign(source_mode="historical_rest"))
    incremental_trade_frames = [
        frame.assign(source_mode="websocket_incremental")
        for frame in _collect_parquet_frames(incremental_root / "agg_trades", "*.parquet")
        if not frame.empty and (symbol_filter is None or set(frame["symbol"].str.upper()).intersection(symbol_filter))
    ]
    agg_trades = _concat_dedup(
        historical_trade_frames + incremental_trade_frames,
        dedupe_keys=["symbol", "aggregate_trade_id"],
        sort_keys=["symbol", "aggregate_trade_id", "ingested_at"],
    )

    historical_book_frames: list[pd.DataFrame] = []
    if (historical_root / "book_ticker").exists():
        for frame in _collect_parquet_frames(historical_root / "book_ticker", "*.parquet"):
            if symbol_filter is not None and not set(frame["symbol"].str.upper()).intersection(symbol_filter):
                continue
            historical_book_frames.append(frame.assign(source_mode="historical_rest"))
    incremental_book_frames = [
        frame.assign(source_mode="websocket_incremental")
        for frame in _collect_parquet_frames(incremental_root / "book_ticker", "*.parquet")
        if not frame.empty and (symbol_filter is None or set(frame["symbol"].str.upper()).intersection(symbol_filter))
    ]
    top_of_book = _concat_dedup(
        historical_book_frames + incremental_book_frames,
        dedupe_keys=["symbol", "dedupe_key"],
        sort_keys=["symbol", "event_time", "ingested_at"],
    )

    output_paths: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    datasets = {
        "crypto_ohlcv_bars": ohlcv,
        "crypto_agg_trades": agg_trades,
        "crypto_top_of_book": top_of_book,
    }
    for dataset_name, frame in datasets.items():
        output_path = output_root / f"{dataset_name}.parquet"
        frame.to_parquet(output_path, index=False)
        output_paths[dataset_name] = str(output_path)
        row_counts[dataset_name] = int(len(frame.index))

    summary = {
        "output_paths": output_paths,
        "row_counts": row_counts,
        "uniqueness_rules": {
            "crypto_ohlcv_bars": ["symbol", "interval", "timestamp"],
            "crypto_agg_trades": ["symbol", "aggregate_trade_id"],
            "crypto_top_of_book": ["symbol", "dedupe_key"],
        },
        "ordering": {
            "crypto_ohlcv_bars": ["symbol", "interval", "timestamp", "ingested_at"],
            "crypto_agg_trades": ["symbol", "aggregate_trade_id", "ingested_at"],
            "crypto_top_of_book": ["symbol", "event_time", "ingested_at"],
        },
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return BinanceProjectionResult(summary_path=str(summary_path), output_paths=output_paths, row_counts=row_counts)
