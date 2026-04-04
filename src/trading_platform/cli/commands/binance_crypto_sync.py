from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.models import (
    BinanceFeatureConfig,
    BinanceProjectionConfig,
    BinanceStatusConfig,
    BinanceSyncConfig,
    BinanceWebsocketIngestConfig,
)
from trading_platform.binance.sync import run_binance_incremental_sync

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_sync(args: argparse.Namespace) -> None:
    config = BinanceSyncConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    symbols = tuple(args.symbols) if getattr(args, "symbols", None) else config.symbols
    intervals = tuple(args.intervals) if getattr(args, "intervals", None) else config.intervals
    stream_families = tuple(args.stream_families) if getattr(args, "stream_families", None) else config.stream_families
    websocket = BinanceWebsocketIngestConfig(
        **{
            **config.websocket.__dict__,
            "symbols": symbols,
            "intervals": intervals,
            "stream_families": stream_families,
            "max_runtime_seconds": getattr(args, "max_runtime_seconds", None)
            if getattr(args, "max_runtime_seconds", None) is not None
            else config.max_runtime_seconds,
            "max_messages": getattr(args, "max_messages", None)
            if getattr(args, "max_messages", None) is not None
            else config.max_messages,
            "raw_incremental_root": _resolve_path(
                getattr(args, "raw_incremental_root", None),
                config.websocket.raw_incremental_root,
            ),
            "normalized_incremental_root": _resolve_path(
                getattr(args, "normalized_incremental_root", None),
                config.websocket.normalized_incremental_root,
            ),
            "checkpoint_path": _resolve_path(getattr(args, "checkpoint_path", None), config.websocket.checkpoint_path),
            "summary_path": _resolve_path(getattr(args, "websocket_summary_path", None), config.websocket.summary_path),
            "projection_output_root": _resolve_path(
                getattr(args, "projection_output_root", None),
                config.websocket.projection_output_root,
            ),
        }
    )
    projection = BinanceProjectionConfig(
        historical_normalized_root=_resolve_path(
            getattr(args, "historical_normalized_root", None),
            config.projection.historical_normalized_root,
        ),
        incremental_normalized_root=_resolve_path(
            getattr(args, "normalized_incremental_root", None),
            config.projection.incremental_normalized_root,
        ),
        output_root=_resolve_path(getattr(args, "projection_output_root", None), config.projection.output_root),
        summary_path=_resolve_path(getattr(args, "projection_summary_path", None), config.projection.summary_path),
        symbols=symbols,
        intervals=intervals,
    )
    features = BinanceFeatureConfig(
        **{
            **config.features.__dict__,
            "projection_root": projection.output_root,
            "features_root": _resolve_path(getattr(args, "features_root", None), config.features.features_root),
            "feature_store_root": _resolve_path(
                getattr(args, "feature_store_root", None),
                config.features.feature_store_root,
            ),
            "summary_path": _resolve_path(getattr(args, "feature_summary_path", None), config.features.summary_path),
            "symbols": symbols,
            "intervals": intervals,
            "incremental_refresh": config.features.incremental_refresh
            if getattr(args, "incremental_refresh", None) is None
            else bool(args.incremental_refresh),
        }
    )
    status = BinanceStatusConfig(
        **{
            **config.status.__dict__,
            "projection_root": projection.output_root,
            "features_root": features.features_root,
            "feature_store_root": features.feature_store_root,
            "latest_sync_manifest_path": _resolve_path(
                getattr(args, "latest_sync_manifest_path", None),
                config.latest_sync_manifest_path,
            ),
            "summary_path": _resolve_path(getattr(args, "status_summary_path", None), config.status.summary_path),
            "symbols": symbols,
            "intervals": intervals,
        }
    )
    config = BinanceSyncConfig(
        websocket=websocket,
        projection=projection,
        features=features,
        status=status,
        symbols=symbols,
        intervals=intervals,
        stream_families=stream_families,
        skip_projection=bool(getattr(args, "skip_projection", False)),
        skip_features=bool(getattr(args, "skip_features", False)),
        max_runtime_seconds=getattr(args, "max_runtime_seconds", None)
        if getattr(args, "max_runtime_seconds", None) is not None
        else config.max_runtime_seconds,
        max_messages=getattr(args, "max_messages", None)
        if getattr(args, "max_messages", None) is not None
        else config.max_messages,
        full_feature_rebuild=bool(getattr(args, "full_feature_rebuild", False)),
        sync_summary_path=_resolve_path(getattr(args, "sync_summary_path", None), config.sync_summary_path),
        sync_manifest_root=_resolve_path(getattr(args, "sync_manifest_root", None), config.sync_manifest_root),
        latest_sync_manifest_path=_resolve_path(
            getattr(args, "latest_sync_manifest_path", None),
            config.latest_sync_manifest_path,
        ),
    )

    print("Binance Crypto Incremental Sync")
    print(f"  config        : {args.config}")
    print(f"  symbols       : {', '.join(config.symbols)}")
    print(f"  intervals     : {', '.join(config.intervals)}")
    print(f"  stream types  : {', '.join(config.stream_families)}")
    print(f"  max runtime   : {config.max_runtime_seconds}")
    print(f"  max messages  : {config.max_messages}")
    print(f"  skip project  : {config.skip_projection}")
    print(f"  skip features : {config.skip_features}")
    print(f"  summary path  : {config.sync_summary_path}")

    result = run_binance_incremental_sync(config)
    print("\n[DONE] Binance sync complete.")
    print(f"  Sync id                  : {result.sync_id}")
    print(f"  Status                   : {result.status}")
    print(f"  Sync manifest            : {result.manifest_path}")
    print(f"  Latest manifest          : {result.latest_manifest_path}")
    print(f"  Freshness summary        : {result.freshness_summary_path}")
    print(f"  Websocket summary        : {result.websocket_summary_path}")
    print(f"  Projection summary       : {result.projection_summary_path}")
    print(f"  Feature summary          : {result.feature_summary_path}")
    print(f"  Sync summary             : {result.summary_path}")
