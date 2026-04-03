from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.features import build_binance_market_features
from trading_platform.binance.models import BinanceFeatureConfig

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_features(args: argparse.Namespace) -> None:
    config = BinanceFeatureConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceFeatureConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceFeatureConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    overrides = {
        "projection_root": _resolve_path(getattr(args, "projection_root", None), config.projection_root),
        "features_root": _resolve_path(getattr(args, "features_root", None), config.features_root),
        "feature_store_root": _resolve_path(getattr(args, "feature_store_root", None), config.feature_store_root),
        "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
        "incremental_refresh": config.incremental_refresh
        if getattr(args, "incremental_refresh", None) is None
        else bool(args.incremental_refresh),
    }
    config = BinanceFeatureConfig(**{**config.__dict__, **overrides})

    print("Binance Crypto Feature Refresh")
    print(f"  config        : {args.config}")
    print(f"  projections   : {config.projection_root}")
    print(f"  features root : {config.features_root}")
    print(f"  feature store : {config.feature_store_root}")
    print(f"  symbols       : {', '.join(config.symbols) if config.symbols else 'all projected symbols'}")
    print(f"  intervals     : {', '.join(config.intervals) if config.intervals else 'all projected intervals'}")
    print(f"  mode          : {'full rebuild' if bool(getattr(args, 'full_rebuild', False)) else 'incremental refresh'}")

    result = build_binance_market_features(config, full_rebuild=bool(getattr(args, "full_rebuild", False)))
    print("\n[DONE] Binance feature refresh complete.")
    print(f"  Rows written             : {result.rows_written}")
    print(f"  Artifacts written        : {result.artifacts_written}")
    print(f"  Features path            : {result.features_path}")
    print(f"  Summary                  : {result.summary_path}")
    print(f"  Feature-store manifests  : {len(result.feature_store_manifest_paths)}")
