from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.binance.models import BinanceAlertsConfig, BinanceHealthCheckConfig, BinanceNotifyConfig
from trading_platform.binance.notify import run_binance_monitor_notifications

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str | None) -> str | None:
    if path is None and default is None:
        return None
    candidate = Path(path or str(default))
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_binance_crypto_notify(args: argparse.Namespace) -> None:
    config = BinanceNotifyConfig.from_yaml(args.config, project_root=PROJECT_ROOT)
    alerts = config.alerts
    health = config.health
    if getattr(args, "symbols", None):
        alerts = BinanceAlertsConfig(**{**alerts.__dict__, "symbols": tuple(args.symbols)})
        health = BinanceHealthCheckConfig(**{**health.__dict__, "symbols": tuple(args.symbols)})
    if getattr(args, "intervals", None):
        alerts = BinanceAlertsConfig(**{**alerts.__dict__, "intervals": tuple(args.intervals)})
        health = BinanceHealthCheckConfig(**{**health.__dict__, "intervals": tuple(args.intervals)})
    alerts = BinanceAlertsConfig(
        **{
            **alerts.__dict__,
            "latest_sync_manifest_path": _resolve_path(
                getattr(args, "latest_sync_manifest_path", None),
                alerts.latest_sync_manifest_path,
            ),
            "status_summary_path": _resolve_path(
                getattr(args, "status_summary_path", None),
                alerts.status_summary_path,
            ),
            "output_root": _resolve_path(getattr(args, "alerts_output_root", None), alerts.output_root),
            "summary_path": _resolve_path(getattr(args, "alerts_summary_path", None), alerts.summary_path),
        }
    )
    health = BinanceHealthCheckConfig(
        **{
            **health.__dict__,
            "latest_sync_manifest_path": _resolve_path(
                getattr(args, "latest_sync_manifest_path", None),
                health.latest_sync_manifest_path,
            ),
            "status_summary_path": _resolve_path(
                getattr(args, "status_summary_path", None),
                health.status_summary_path,
            ),
            "output_root": _resolve_path(getattr(args, "health_output_root", None), health.output_root),
            "summary_path": _resolve_path(getattr(args, "health_summary_path", None), health.summary_path),
        }
    )
    config = BinanceNotifyConfig(
        **{
            **config.__dict__,
            "alerts": alerts,
            "health": health,
            "output_root": _resolve_path(getattr(args, "output_root", None), config.output_root),
            "summary_path": _resolve_path(getattr(args, "summary_path", None), config.summary_path),
            "state_path": _resolve_path(getattr(args, "state_path", None), config.state_path),
            "notification_config_path": _resolve_path(
                getattr(args, "notification_config_path", None),
                config.notification_config_path,
            ),
            "enabled": config.enabled if getattr(args, "enabled", None) is None else bool(args.enabled),
            "subject_prefix": getattr(args, "subject_prefix", None) or config.subject_prefix,
        }
    )
    result = run_binance_monitor_notifications(config, dry_run=bool(getattr(args, "dry_run", False)))
    if getattr(args, "format", "text") == "json":
        print(Path(result.summary_path).read_text(encoding="utf-8"))
        return
    print("Binance Crypto Notify")
    print(f"  config        : {args.config}")
    print(f"  status        : {result.status}")
    print(f"  transition    : {result.transition or 'none'}")
    print(f"  should notify : {result.should_notify}")
    print(f"  notified      : {result.notified}")
    print(f"  suppressed    : {result.suppressed}")
    print(f"  alert count   : {result.alert_count}")
    print(f"  summary       : {result.summary_path}")
    print(f"  state         : {result.state_path}")
