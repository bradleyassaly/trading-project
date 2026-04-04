from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_platform.binance.health import evaluate_binance_alerts
from trading_platform.binance.models import BinanceAlertsConfig

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_alerts(args: argparse.Namespace) -> None:
    config = BinanceAlertsConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    if getattr(args, "symbols", None):
        config = BinanceAlertsConfig(**{**config.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        config = BinanceAlertsConfig(**{**config.__dict__, "intervals": tuple(args.intervals)})
    config = BinanceAlertsConfig(
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
    result = evaluate_binance_alerts(config)
    if getattr(args, "format", "text") == "json":
        print(Path(result.summary_path).read_text(encoding="utf-8"))
        return
    print("Binance Crypto Alerts")
    print(f"  config        : {args.config}")
    print(f"  status        : {result.status}")
    print(f"  alert count   : {result.alert_count}")
    print(f"  alerts json   : {result.alerts_json_path}")
    print(f"  alerts csv    : {result.alerts_csv_path}")
    print(f"  summary       : {result.summary_path}")
