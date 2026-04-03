from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.models import BinanceWebsocketIngestConfig
from trading_platform.binance.websocket import BinanceWebsocketIngestService

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_websocket_ingest(args: argparse.Namespace) -> None:
    config = BinanceWebsocketIngestConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceWebsocketIngestConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceWebsocketIngestConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    if getattr(args, "stream_families", None):
        config = BinanceWebsocketIngestConfig(**{**config.__dict__, "stream_families": tuple(args.stream_families)})
    overrides = {
        "enabled": True,
        "combined_stream": config.combined_stream if getattr(args, "combined_stream", None) is None else bool(args.combined_stream),
        "max_runtime_seconds": getattr(args, "max_runtime_seconds", None) if getattr(args, "max_runtime_seconds", None) is not None else config.max_runtime_seconds,
        "max_messages": getattr(args, "max_messages", None) if getattr(args, "max_messages", None) is not None else config.max_messages,
        "reconnect_backoff_base_sec": getattr(args, "reconnect_backoff_base_sec", None) or config.reconnect_backoff_base_sec,
        "reconnect_backoff_max_sec": getattr(args, "reconnect_backoff_max_sec", None) or config.reconnect_backoff_max_sec,
        "max_reconnect_attempts": getattr(args, "max_reconnect_attempts", None) or config.max_reconnect_attempts,
        "receive_timeout_sec": getattr(args, "receive_timeout_sec", None) or config.receive_timeout_sec,
        "raw_incremental_root": _resolve_path(getattr(args, "raw_incremental_root", None), config.raw_incremental_root),
        "normalized_incremental_root": _resolve_path(
            getattr(args, "normalized_incremental_root", None),
            config.normalized_incremental_root,
        ),
        "checkpoint_path": _resolve_path(getattr(args, "checkpoint_path", None), config.checkpoint_path),
        "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
        "projection_output_root": _resolve_path(
            getattr(args, "projection_output_root", None),
            config.projection_output_root,
        ),
    }
    config = BinanceWebsocketIngestConfig(**{**config.__dict__, **overrides})

    print("Binance Crypto Websocket Ingest")
    print(f"  config        : {args.config}")
    print(f"  symbols       : {', '.join(config.symbols)}")
    print(f"  stream types  : {', '.join(config.stream_families)}")
    if "kline" in config.stream_families:
        print(f"  intervals     : {', '.join(config.intervals)}")
    print(f"  combined      : {config.combined_stream}")
    print(f"  max runtime   : {config.max_runtime_seconds}")
    print(f"  max messages  : {config.max_messages}")
    print(f"  raw root      : {config.raw_incremental_root}")
    print(f"  normalized    : {config.normalized_incremental_root}")
    print(f"  projections   : {config.projection_output_root}")

    result = BinanceWebsocketIngestService(config).run()
    print("\n[DONE] Binance websocket ingest complete.")
    print(f"  Messages processed       : {result.messages_processed}")
    print(f"  Messages written         : {result.messages_written}")
    print(f"  Duplicates dropped       : {result.duplicates_dropped}")
    print(f"  Reconnect count          : {result.reconnect_count}")
    print(f"  Checkpoint               : {result.checkpoint_path}")
    print(f"  Summary                  : {result.summary_path}")
    if result.projection_summary_path:
        print(f"  Projection summary       : {result.projection_summary_path}")
    if result.warnings:
        print(f"  Warnings                 : {len(result.warnings)}")
    if result.failures:
        print(f"  Failures                 : {len(result.failures)}")
