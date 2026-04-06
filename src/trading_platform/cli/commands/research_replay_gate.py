from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_gating import (
    ReplayResearchGateThresholds,
    evaluate_research_gates,
    write_research_gating_artifacts,
)
from trading_platform.research.replay_history import load_replay_history, update_shared_replay_history


PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_gate(args) -> None:
    history_output_dir = _resolve_path(getattr(args, "history_output_dir", None), "artifacts/research_replay/history")
    gating_output_dir = _resolve_path(getattr(args, "gating_output_dir", None), "artifacts/research_replay/gating")
    history_result = update_shared_replay_history(
        output_dir=history_output_dir,
        evaluation_summary_paths=[
            _resolve_path(path, path)
            for path in list(getattr(args, "evaluation_summary_paths", None) or [])
        ],
        comparison_summary_paths=[
            _resolve_path(path, path)
            for path in list(getattr(args, "comparison_summary_paths", None) or [])
        ],
        evaluation_root=_resolve_path(getattr(args, "evaluation_root", None), "artifacts/research_replay/evaluation"),
        comparison_root=_resolve_path(getattr(args, "comparison_root", None), "artifacts/research_replay/comparison"),
    )
    history_records = load_replay_history(Path(history_output_dir) / "shared_replay_history.jsonl")
    gating_result = evaluate_research_gates(
        history_records,
        thresholds=ReplayResearchGateThresholds(
            min_row_count=int(getattr(args, "min_row_count", 25)),
            min_replay_runs=int(getattr(args, "min_replay_runs", 2)),
            min_mean_abs_spearman=float(getattr(args, "min_mean_abs_spearman", 0.02)),
            min_mean_directional_accuracy=float(getattr(args, "min_mean_directional_accuracy", 0.51)),
            min_mean_composite_score=float(getattr(args, "min_mean_composite_score", 0.10)),
            max_score_stddev=float(getattr(args, "max_score_stddev", 0.20)),
            max_best_rank=int(getattr(args, "max_best_rank", 5)),
            max_best_rank_percentile=float(getattr(args, "max_best_rank_percentile", 0.50)),
            min_provider_count=int(getattr(args, "min_provider_count", 1)),
        ),
    )
    artifact_paths = write_research_gating_artifacts(result=gating_result, output_dir=gating_output_dir)
    payload = gating_result.to_summary()
    payload["history_update"] = history_result.to_summary()
    payload["artifact_paths"] = artifact_paths

    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    counts = payload["summary_counts"]
    print("Replay Research Gating")
    print(f"  history append : {history_result.appended_record_count} new records")
    print(f"  candidates     : {counts['candidate_count']}")
    print(f"  promotable     : {counts['promotable_count']}")
    print(f"  watchlist      : {counts['watchlist_count']}")
    print(f"  rejected       : {counts['rejected_count']}")
    top = payload["promotable_candidates"][0] if payload["promotable_candidates"] else None
    if top is not None:
        print(
            "  top promotable : "
            f"{top['candidate_id']} | providers={','.join(top['providers']) or '-'} | "
            f"target={top['supporting_metrics'].get('target_column') or '-'}"
        )
    elif payload["watchlist_candidates"]:
        top = payload["watchlist_candidates"][0]
        reasons = ",".join(top["watchlist_reasons"]) or "none"
        print(f"  top watchlist  : {top['candidate_id']} | reasons={reasons}")
    print(f"  history path   : {history_result.history_path}")
    print(f"  summary path   : {artifact_paths['summary_path']}")
