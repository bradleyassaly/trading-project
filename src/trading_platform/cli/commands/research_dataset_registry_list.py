from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.dataset_registry import list_dataset_registry_entries

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_dataset_registry_list(args) -> None:
    entries = list_dataset_registry_entries(
        registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json"),
        provider=getattr(args, "provider", None),
        asset_class=getattr(args, "asset_class", None),
        dataset_name=getattr(args, "dataset_name", None),
    )
    if getattr(args, "format", "text") == "json":
        print(json.dumps([entry.to_dict() for entry in entries], indent=2))
        return

    print("Research Dataset Registry")
    print(f"  entries        : {len(entries)}")
    for entry in entries:
        latest = entry.latest_event_time or entry.latest_materialized_at or "n/a"
        print(
            f"  {entry.provider:<12} {entry.asset_class:<18} {entry.dataset_name:<28} "
            f"symbols={len(entry.symbols):<4} latest={latest}"
        )
