from __future__ import annotations

import json
from pathlib import Path

from trading_platform.monitoring.drilldown import load_dataset_timeline

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_ops_monitor_dataset_timeline(args) -> None:
    result = load_dataset_timeline(
        monitoring_output_root=_resolve_path(getattr(args, "output_root", None), "artifacts/provider_monitoring"),
        dataset_key=str(getattr(args, "dataset_key")),
    )
    payload = result.to_dict()
    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    print("Dataset Timeline")
    print(f"  dataset key    : {payload['scope_value']}")
    print(f"  snapshots      : {len(payload['history'])}")
    print(f"  transitions    : {len(payload['transitions'])}")
