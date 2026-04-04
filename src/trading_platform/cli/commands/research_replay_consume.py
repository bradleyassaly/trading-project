from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_assembly import ReplayAssemblyRequest
from trading_platform.research.replay_consumer import (
    ReplayConsumerRequest,
    load_replay_consumer_input,
    write_replay_consumer_summary,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_consume(args) -> None:
    result = load_replay_consumer_input(
        ReplayConsumerRequest(
            assembly_request=ReplayAssemblyRequest(
                registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json"),
                dataset_keys=list(getattr(args, "dataset_keys", None) or []),
                providers=list(getattr(args, "providers", None) or []),
                asset_class=getattr(args, "asset_class", None),
                dataset_names=list(getattr(args, "dataset_names", None) or []),
                symbols=list(getattr(args, "symbols", None) or []),
                intervals=list(getattr(args, "intervals", None) or []),
                start=getattr(args, "start", None),
                end=getattr(args, "end", None),
                alignment_mode=str(getattr(args, "alignment_mode", "outer_union")),
                anchor_dataset_key=getattr(args, "anchor_dataset_key", None),
                tolerance=getattr(args, "tolerance", None),
            ),
            limit=getattr(args, "limit", None),
        )
    )
    summary_path = None
    if getattr(args, "summary_path", None):
        summary_path = write_replay_consumer_summary(
            result=result,
            output_path=_resolve_path(
                getattr(args, "summary_path", None),
                "artifacts/research_replay/latest_replay_consumer_summary.json",
            ),
        )

    payload = result.to_summary()
    if summary_path:
        payload["summary_path"] = summary_path
    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    print("Research Replay Consumer")
    print(f"  rows           : {payload['row_count']}")
    print(f"  features       : {len(payload['feature_columns'])}")
    print(f"  targets        : {len(payload['target_columns'])}")
    print(f"  alignment mode : {payload['metadata'].get('alignment_mode')}")
    if payload["warnings"]:
        print(f"  warnings       : {', '.join(payload['warnings'])}")
    if summary_path:
        print(f"  summary path   : {summary_path}")
