from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_platform.binance.models import BinanceStatusConfig
from trading_platform.binance.status import build_binance_status

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_status(args: argparse.Namespace) -> None:
    config = BinanceStatusConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceStatusConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceStatusConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    config = BinanceStatusConfig(
        **{
            **config.__dict__,
            "projection_root": _resolve_path(getattr(args, "projection_root", None), config.projection_root),
            "features_root": _resolve_path(getattr(args, "features_root", None), config.features_root),
            "feature_store_root": _resolve_path(getattr(args, "feature_store_root", None), config.feature_store_root),
            "latest_sync_manifest_path": _resolve_path(
                getattr(args, "latest_sync_manifest_path", None),
                config.latest_sync_manifest_path,
            ),
            "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
        }
    )
    result = build_binance_status(config)
    if getattr(args, "format", "text") == "json":
        payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
        print(json.dumps(payload, indent=2))
        return

    print("Binance Crypto Status")
    print(f"  config        : {args.config}")
    print(f"  summary path  : {result.summary_path}")
    print(f"  datasets      : {result.dataset_count}")
    print(f"  stale         : {result.stale_dataset_count}")
    if result.latest_sync_manifest_path:
        print(f"  latest sync   : {result.latest_sync_manifest_path}")
    for record in result.records:
        scope = record["symbol"] if record["interval"] is None else f"{record['symbol']} {record['interval']}"
        freshness = "stale" if record["stale"] else "fresh"
        latest = record["latest_event_time"] or record["latest_materialized_at"] or "n/a"
        print(
            f"  {record['dataset_name']:<22} {scope:<16} {freshness:<5} "
            f"age={record['freshness_age_seconds']} latest={latest}"
        )
