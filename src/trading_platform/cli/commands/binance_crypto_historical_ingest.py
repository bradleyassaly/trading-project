from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.client import BinanceClient, BinanceClientConfig
from trading_platform.binance.historical_ingest import BinanceHistoricalIngestPipeline
from trading_platform.binance.models import BinanceHistoricalIngestConfig

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_historical_ingest(args: argparse.Namespace) -> None:
    config = BinanceHistoricalIngestConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceHistoricalIngestConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceHistoricalIngestConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})

    overrides = {
        "start": getattr(args, "start", None) or config.start,
        "end": getattr(args, "end", None) if getattr(args, "end", None) is not None else config.end,
        "kline_limit": getattr(args, "kline_limit", None) or config.kline_limit,
        "agg_trade_limit": getattr(args, "agg_trade_limit", None) or config.agg_trade_limit,
        "request_sleep_sec": getattr(args, "request_sleep_sec", None) or config.request_sleep_sec,
        "max_retries": getattr(args, "max_retries", None) or config.max_retries,
        "backoff_base_sec": getattr(args, "backoff_base_sec", None) or config.backoff_base_sec,
        "backoff_max_sec": getattr(args, "backoff_max_sec", None) or config.backoff_max_sec,
        "capture_book_ticker": config.capture_book_ticker
        if getattr(args, "capture_book_ticker", None) is None
        else bool(args.capture_book_ticker),
        "normalize_after_ingest": not bool(getattr(args, "skip_normalize", False)),
        "raw_root": _resolve_path(getattr(args, "raw_root", None), config.raw_root),
        "normalized_root": _resolve_path(getattr(args, "normalized_root", None), config.normalized_root),
        "checkpoint_path": _resolve_path(getattr(args, "checkpoint_path", None), config.checkpoint_path),
        "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
        "exchange_info_path": _resolve_path(getattr(args, "exchange_info_path", None), config.exchange_info_path),
    }
    config = BinanceHistoricalIngestConfig(**{**config.__dict__, **overrides})

    client = BinanceClient(
        BinanceClientConfig(
            request_sleep_sec=config.request_sleep_sec,
            max_retries=config.max_retries,
            backoff_base_sec=config.backoff_base_sec,
            backoff_max_sec=config.backoff_max_sec,
        )
    )
    print("Binance Crypto Historical Ingest")
    print(f"  config     : {args.config}")
    print(f"  symbols    : {', '.join(config.symbols)}")
    print(f"  intervals  : {', '.join(config.intervals)}")
    print(f"  start      : {config.start}")
    print(f"  end        : {config.end or 'now'}")
    print(f"  raw root   : {config.raw_root}")
    print(f"  normalized : {config.normalized_root}")
    print(f"  normalize  : {'enabled' if config.normalize_after_ingest else 'disabled'}")

    result = BinanceHistoricalIngestPipeline(client, config).run()
    print("\n[DONE] Binance historical ingest complete.")
    print(f"  Requests                 : {result.request_count}")
    print(f"  Retries                  : {result.retry_count}")
    print(f"  Pages fetched            : {result.pages_fetched}")
    print(f"  Raw artifacts written    : {result.raw_artifacts_written}")
    print(f"  Kline rows fetched       : {result.kline_rows_fetched}")
    print(f"  Agg trade rows fetched   : {result.agg_trade_rows_fetched}")
    print(f"  Book ticker snapshots    : {result.book_ticker_snapshots_fetched}")
    print(f"  Exchange info            : {result.exchange_info_path}")
    print(f"  Checkpoint               : {result.checkpoint_path}")
    print(f"  Summary                  : {result.summary_path}")
    if result.normalization_summary_path:
        print(f"  Normalization summary    : {result.normalization_summary_path}")
