from __future__ import annotations

import json
from pathlib import Path

from trading_platform.monitoring.drilldown import load_provider_timeline

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_ops_monitor_provider_timeline(args) -> None:
    result = load_provider_timeline(
        monitoring_output_root=_resolve_path(getattr(args, "output_root", None), "artifacts/provider_monitoring"),
        provider=str(getattr(args, "provider")),
    )
    payload = result.to_dict()
    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    print("Provider Timeline")
    print(f"  provider       : {payload['scope_value']}")
    print(f"  snapshots      : {len(payload['history'])}")
    print(f"  transitions    : {len(payload['transitions'])}")
