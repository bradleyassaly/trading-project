from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_assembly import (
    ReplayAssemblyRequest,
    assemble_replay_dataset,
    write_replay_assembly_artifacts,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_assemble(args) -> None:
    result = assemble_replay_dataset(
        ReplayAssemblyRequest(
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
        )
    )

    artifact_paths: dict[str, str] = {}
    output_path = getattr(args, "output_path", None)
    if output_path:
        artifact_paths = write_replay_assembly_artifacts(
            result=result,
            output_path=_resolve_path(output_path, output_path),
            summary_path=_resolve_path(getattr(args, "summary_path", None), "artifacts/research_replay/latest_replay_assembly_summary.json"),
        )

    summary = result.to_summary()
    if getattr(args, "format", "text") == "json":
        payload = dict(summary)
        payload["artifact_paths"] = artifact_paths
        print(json.dumps(payload, indent=2))
        return

    print("Research Replay Assembly")
    print(f"  rows           : {summary['row_count']}")
    print(f"  datasets       : {len(summary['components'])}")
    print(f"  alignment mode : {summary['request']['alignment_mode']}")
    if summary["metadata"].get("anchor_dataset_key"):
        print(f"  anchor         : {summary['metadata']['anchor_dataset_key']}")
    if artifact_paths:
        print(f"  output path    : {artifact_paths['output_path']}")
        print(f"  summary path   : {artifact_paths['summary_path']}")
