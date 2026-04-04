from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_comparison import (
    ReplayComparisonRequest,
    run_replay_comparison,
    write_replay_comparison_artifacts,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_compare(args) -> None:
    result = run_replay_comparison(
        ReplayComparisonRequest(
            registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json")
            if not (getattr(args, "evaluation_summary_paths", None) or [])
            else None,
            evaluation_summary_paths=[
                _resolve_path(path, path)
                for path in list(getattr(args, "evaluation_summary_paths", None) or [])
            ],
            dataset_keys=list(getattr(args, "dataset_keys", None) or []),
            providers=list(getattr(args, "providers", None) or []),
            dataset_names=list(getattr(args, "dataset_names", None) or []),
            symbols=list(getattr(args, "symbols", None) or []),
            intervals=list(getattr(args, "intervals", None) or []),
            start=getattr(args, "start", None),
            end=getattr(args, "end", None),
            alignment_modes=list(getattr(args, "alignment_modes", None) or ["outer_union"]),
            anchor_dataset_key=getattr(args, "anchor_dataset_key", None),
            tolerance=getattr(args, "tolerance", None),
            limit=getattr(args, "limit", None),
            feature_columns=list(getattr(args, "feature_columns", None) or []),
            target_columns=list(getattr(args, "target_columns", None) or []),
            comparison_mode=str(getattr(args, "comparison_mode", "provider")),
            min_row_count=int(getattr(args, "min_row_count", 25)),
            max_candidates=int(getattr(args, "max_candidates", 10)),
            comparison_name=str(getattr(args, "comparison_name", "shared_replay_comparison")),
        )
    )
    artifact_paths = {}
    if getattr(args, "output_dir", None):
        artifact_paths = write_replay_comparison_artifacts(
            result=result,
            output_dir=_resolve_path(getattr(args, "output_dir", None), "artifacts/research_replay/comparison"),
        )
    payload = result.to_summary()
    if artifact_paths:
        payload["artifact_paths"] = artifact_paths
    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    print("Replay Comparison")
    print(f"  rankings       : {len(payload['rankings'])}")
    print(f"  candidates     : {len(payload['candidate_slices'])}")
    print(f"  warnings       : {', '.join(payload['warnings']) if payload['warnings'] else 'none'}")
    top_candidate = payload["candidate_slices"][0] if payload["candidate_slices"] else None
    if top_candidate is not None:
        provider_label = ",".join(top_candidate["providers"]) or "-"
        print(
            "  top candidate  : "
            f"{provider_label} | {top_candidate['alignment_mode']} | "
            f"{top_candidate['target_column']} | score={top_candidate['composite_score']}"
        )
    if artifact_paths:
        print(f"  summary path   : {artifact_paths['summary_path']}")
        print(f"  rankings path  : {artifact_paths['rankings_path']}")
        print(f"  candidates path: {artifact_paths['candidates_path']}")
