from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.models import BinanceProjectionConfig
from trading_platform.binance.projection import project_binance_market_data

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_project(args: argparse.Namespace) -> None:
    config = BinanceProjectionConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceProjectionConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceProjectionConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    config = BinanceProjectionConfig(
        historical_normalized_root=_resolve_path(
            getattr(args, "historical_normalized_root", None),
            config.historical_normalized_root,
        ),
        incremental_normalized_root=_resolve_path(
            getattr(args, "incremental_normalized_root", None),
            config.incremental_normalized_root,
        ),
        output_root=_resolve_path(getattr(args, "output_root", None), config.output_root),
        summary_path=_resolve_path(getattr(args, "summary_path", None), config.summary_path),
        symbols=config.symbols,
        intervals=config.intervals,
    )

    print("Binance Crypto Projection")
    print(f"  config        : {args.config}")
    print(f"  historical    : {config.historical_normalized_root}")
    print(f"  incremental   : {config.incremental_normalized_root}")
    print(f"  output root   : {config.output_root}")

    result = project_binance_market_data(config)
    print("\n[DONE] Binance projection complete.")
    for dataset_name, row_count in result.row_counts.items():
        print(f"  {dataset_name:<22}: {row_count}")
    print(f"  Summary                  : {result.summary_path}")
