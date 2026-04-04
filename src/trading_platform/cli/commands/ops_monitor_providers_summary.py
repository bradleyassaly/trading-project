from __future__ import annotations

import json
from pathlib import Path

from trading_platform.monitoring.provider_monitoring import build_cross_provider_monitoring_summary

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_ops_monitor_providers_summary(args) -> None:
    providers = list(getattr(args, "providers", None) or [])
    result = build_cross_provider_monitoring_summary(
        registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json"),
        output_root=_resolve_path(getattr(args, "output_root", None), "artifacts/provider_monitoring"),
        providers=providers or None,
        asset_class=getattr(args, "asset_class", None),
        staleness_threshold_hours=int(getattr(args, "staleness_threshold_hours", 48)),
    )
    if getattr(args, "format", "text") == "json":
        payload = json.loads(Path(result.monitoring_summary_path).read_text(encoding="utf-8"))
        print(json.dumps(payload, indent=2))
        return

    print("Cross-Provider Monitoring Summary")
    print(f"  records        : {result.record_count}")
    print(f"  providers      : {result.provider_count}")
    print(f"  highest status : {result.highest_status}")
    print(f"  summary path   : {result.monitoring_summary_path}")
    print(f"  health path    : {result.health_summary_path}")
