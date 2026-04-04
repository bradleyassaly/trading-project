from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.health import evaluate_binance_health_check
from trading_platform.binance.models import BinanceHealthCheckConfig

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_health_check(args: argparse.Namespace) -> None:
    config = BinanceHealthCheckConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceHealthCheckConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceHealthCheckConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    config = BinanceHealthCheckConfig(
        **{
            **config.__dict__,
            "latest_sync_manifest_path": _resolve_path(
                getattr(args, "latest_sync_manifest_path", None),
                config.latest_sync_manifest_path,
            ),
            "status_summary_path": _resolve_path(
                getattr(args, "status_summary_path", None),
                config.status_summary_path,
            ),
            "output_root": _resolve_path(getattr(args, "output_root", None), config.output_root),
            "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
        }
    )
    result = evaluate_binance_health_check(config)
    if getattr(args, "format", "text") == "json":
        print(Path(result.summary_path).read_text(encoding="utf-8"))
        return
    print("Binance Crypto Health Check")
    print(f"  config        : {args.config}")
    print(f"  status        : {result.status}")
    print(f"  checks        : {result.check_count}")
    print(
        f"  alerts        : info={result.alert_counts['info']} warning={result.alert_counts['warning']} critical={result.alert_counts['critical']}"
    )
    print(f"  summary       : {result.summary_path}")
