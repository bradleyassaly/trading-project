from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_evaluation import (
    build_replay_evaluation_request,
    run_replay_evaluation,
    write_replay_evaluation_artifacts,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_evaluate(args) -> None:
    result = run_replay_evaluation(
        build_replay_evaluation_request(
            registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json"),
            dataset_keys=list(getattr(args, "dataset_keys", None) or []),
            providers=list(getattr(args, "providers", None) or []),
            dataset_names=list(getattr(args, "dataset_names", None) or []),
            symbols=list(getattr(args, "symbols", None) or []),
            intervals=list(getattr(args, "intervals", None) or []),
            start=getattr(args, "start", None),
            end=getattr(args, "end", None),
            alignment_mode=str(getattr(args, "alignment_mode", "outer_union")),
            anchor_dataset_key=getattr(args, "anchor_dataset_key", None),
            tolerance=getattr(args, "tolerance", None),
            limit=getattr(args, "limit", None),
            feature_columns=list(getattr(args, "feature_columns", None) or []),
            target_columns=list(getattr(args, "target_columns", None) or []),
        )
    )
    artifact_paths = {}
    if getattr(args, "output_dir", None):
        artifact_paths = write_replay_evaluation_artifacts(
            result=result,
            output_dir=_resolve_path(getattr(args, "output_dir", None), "artifacts/research_replay/evaluation"),
        )
    payload = result.to_summary()
    if artifact_paths:
        payload["artifact_paths"] = artifact_paths
    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    print("Replay Evaluation")
    print(f"  metrics        : {len(payload['metrics'])}")
    print(f"  warnings       : {', '.join(payload['warnings']) if payload['warnings'] else 'none'}")
    if artifact_paths:
        print(f"  summary path   : {artifact_paths['summary_path']}")
        print(f"  metrics path   : {artifact_paths['metrics_path']}")
