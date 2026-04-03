from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.models import BinanceNormalizeConfig
from trading_platform.binance.normalize import normalize_binance_artifacts

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_normalize(args: argparse.Namespace) -> None:
    config = BinanceNormalizeConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceNormalizeConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceNormalizeConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    config = BinanceNormalizeConfig(
        raw_root=_resolve_path(getattr(args, "raw_root", None), config.raw_root),
        normalized_root=_resolve_path(getattr(args, "normalized_root", None), config.normalized_root),
        symbols=config.symbols,
        intervals=config.intervals,
        summary_path=_resolve_path(getattr(args, "summary_path", None), config.summary_path),
    )

    print("Binance Crypto Normalize")
    print(f"  config     : {args.config}")
    print(f"  raw root   : {config.raw_root}")
    print(f"  normalized : {config.normalized_root}")
    if config.symbols:
        print(f"  symbols    : {', '.join(config.symbols)}")
    if config.intervals:
        print(f"  intervals  : {', '.join(config.intervals)}")

    result = normalize_binance_artifacts(config)
    print("\n[DONE] Binance normalization complete.")
    print(f"  Kline files written      : {result.kline_files_written}")
    print(f"  Agg trade files written  : {result.agg_trade_files_written}")
    print(f"  Book ticker files written: {result.book_ticker_files_written}")
    print(f"  Summary                  : {result.summary_path}")
