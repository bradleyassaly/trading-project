from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.binance.models import BinanceNormalizeConfig, BinanceNormalizeResult

BINANCE_KLINE_SCHEMA_VERSION = "binance_crypto_klines_v1"
BINANCE_AGG_TRADE_SCHEMA_VERSION = "binance_crypto_agg_trades_v1"
BINANCE_BOOK_TICKER_SCHEMA_VERSION = "binance_crypto_book_ticker_v1"


def _iter_json_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.json") if path.is_file())


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_klines_for_symbol_interval(symbol: str, interval: str, files: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in files:
        payload = list(_read_json(path) or [])
        ingested_at = pd.to_datetime(path.stat().st_mtime, unit="s", utc=True)
        for entry in payload:
            if len(entry) < 11:
                continue
            rows.append(
                {
                    "timestamp": pd.to_datetime(int(entry[0]), unit="ms", utc=True),
                    "close_timestamp": pd.to_datetime(int(entry[6]), unit="ms", utc=True),
                    "event_time": pd.to_datetime(int(entry[6]), unit="ms", utc=True),
                    "ingested_at": ingested_at,
                    "symbol": symbol,
                    "interval": interval,
                    "open": float(entry[1]),
                    "high": float(entry[2]),
                    "low": float(entry[3]),
                    "close": float(entry[4]),
                    "volume": float(entry[5]),
                    "quote_volume": float(entry[7]),
                    "trade_count": int(entry[8]),
                    "taker_buy_base_volume": float(entry[9]),
                    "taker_buy_quote_volume": float(entry[10]),
                    "provider": "binance",
                    "source": "binance_rest",
                    "asset_class": "crypto",
                    "schema_version": BINANCE_KLINE_SCHEMA_VERSION,
                    "raw_artifact_path": str(path),
                    "dedupe_key": f"{symbol}|{interval}|{int(entry[0])}",
                }
            )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = frame.drop_duplicates(subset=["timestamp", "symbol", "interval"], keep="last")
    return frame.sort_values(["timestamp", "symbol", "interval"]).reset_index(drop=True)


def _normalize_agg_trades_for_symbol(symbol: str, files: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in files:
        payload = list(_read_json(path) or [])
        ingested_at = pd.to_datetime(path.stat().st_mtime, unit="s", utc=True)
        for entry in payload:
            rows.append(
                {
                    "timestamp": pd.to_datetime(int(entry["T"]), unit="ms", utc=True),
                    "event_time": pd.to_datetime(int(entry["T"]), unit="ms", utc=True),
                    "ingested_at": ingested_at,
                    "symbol": symbol,
                    "aggregate_trade_id": int(entry["a"]),
                    "first_trade_id": int(entry["f"]),
                    "last_trade_id": int(entry["l"]),
                    "price": float(entry["p"]),
                    "quantity": float(entry["q"]),
                    "is_buyer_maker": bool(entry["m"]),
                    "provider": "binance",
                    "source": "binance_rest",
                    "asset_class": "crypto",
                    "schema_version": BINANCE_AGG_TRADE_SCHEMA_VERSION,
                    "raw_artifact_path": str(path),
                    "dedupe_key": f"{symbol}|{int(entry['a'])}",
                }
            )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = frame.drop_duplicates(subset=["symbol", "aggregate_trade_id"], keep="last")
    return frame.sort_values(["timestamp", "aggregate_trade_id"]).reset_index(drop=True)


def _normalize_book_ticker_for_symbol(symbol: str, files: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in files:
        payload = dict(_read_json(path) or {})
        event_time = pd.to_datetime(payload.get("_snapshot_timestamp"), utc=True)
        rows.append(
            {
                "timestamp": event_time,
                "event_time": event_time,
                "ingested_at": pd.to_datetime(path.stat().st_mtime, unit="s", utc=True),
                "symbol": symbol,
                "bid_price": float(payload["bidPrice"]),
                "bid_quantity": float(payload["bidQty"]),
                "ask_price": float(payload["askPrice"]),
                "ask_quantity": float(payload["askQty"]),
                "provider": "binance",
                "source": "binance_rest",
                "asset_class": "crypto",
                "schema_version": BINANCE_BOOK_TICKER_SCHEMA_VERSION,
                "raw_artifact_path": str(path),
                "dedupe_key": f"{symbol}|{payload.get('_snapshot_timestamp')}",
            }
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = frame.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
    return frame.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def normalize_binance_artifacts(config: BinanceNormalizeConfig) -> BinanceNormalizeResult:
    raw_root = Path(config.raw_root)
    normalized_root = Path(config.normalized_root)
    normalized_root.mkdir(parents=True, exist_ok=True)

    kline_files_written = 0
    agg_trade_files_written = 0
    book_ticker_files_written = 0
    total_kline_rows = 0
    total_agg_trade_rows = 0
    total_book_ticker_rows = 0
    output_paths: dict[str, list[str]] = {"klines": [], "agg_trades": [], "book_ticker": []}

    kline_root = raw_root / "klines"
    agg_trade_root = raw_root / "agg_trades"
    book_ticker_root = raw_root / "book_ticker"
    symbol_filter = {symbol.upper() for symbol in config.symbols} if config.symbols else None
    interval_filter = {interval for interval in config.intervals} if config.intervals else None

    if kline_root.exists():
        for symbol_dir in sorted(path for path in kline_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if symbol_filter and symbol not in symbol_filter:
                continue
            for interval_dir in sorted(path for path in symbol_dir.iterdir() if path.is_dir()):
                interval = interval_dir.name
                if interval_filter and interval not in interval_filter:
                    continue
                frame = _normalize_klines_for_symbol_interval(symbol, interval, _iter_json_files(interval_dir))
                if frame.empty:
                    continue
                out_dir = normalized_root / "klines" / symbol
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{interval}.parquet"
                frame.to_parquet(out_path, index=False)
                kline_files_written += 1
                total_kline_rows += int(len(frame.index))
                output_paths["klines"].append(str(out_path))

    if agg_trade_root.exists():
        for symbol_dir in sorted(path for path in agg_trade_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if symbol_filter and symbol not in symbol_filter:
                continue
            frame = _normalize_agg_trades_for_symbol(symbol, _iter_json_files(symbol_dir))
            if frame.empty:
                continue
            out_dir = normalized_root / "agg_trades"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{symbol}.parquet"
            frame.to_parquet(out_path, index=False)
            agg_trade_files_written += 1
            total_agg_trade_rows += int(len(frame.index))
            output_paths["agg_trades"].append(str(out_path))

    if book_ticker_root.exists():
        for symbol_dir in sorted(path for path in book_ticker_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if symbol_filter and symbol not in symbol_filter:
                continue
            frame = _normalize_book_ticker_for_symbol(symbol, _iter_json_files(symbol_dir))
            if frame.empty:
                continue
            out_dir = normalized_root / "book_ticker"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{symbol}.parquet"
            frame.to_parquet(out_path, index=False)
            book_ticker_files_written += 1
            total_book_ticker_rows += int(len(frame.index))
            output_paths["book_ticker"].append(str(out_path))

    summary = {
        "kline_files_written": kline_files_written,
        "agg_trade_files_written": agg_trade_files_written,
        "book_ticker_files_written": book_ticker_files_written,
        "total_kline_rows": total_kline_rows,
        "total_agg_trade_rows": total_agg_trade_rows,
        "total_book_ticker_rows": total_book_ticker_rows,
        "output_paths": output_paths,
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return BinanceNormalizeResult(
        kline_files_written=kline_files_written,
        agg_trade_files_written=agg_trade_files_written,
        book_ticker_files_written=book_ticker_files_written,
        total_kline_rows=total_kline_rows,
        total_agg_trade_rows=total_agg_trade_rows,
        total_book_ticker_rows=total_book_ticker_rows,
        summary_path=str(summary_path),
        output_paths=output_paths,
    )
