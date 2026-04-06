from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_gating import (
    ReplayResearchGateThresholds,
    evaluate_research_gates,
    write_research_gating_artifacts,
)
from trading_platform.research.replay_history import load_replay_history, update_shared_replay_history
from trading_platform.research.replay_review import (
    ReplayDriftThresholds,
    build_research_review_queue,
    evaluate_replay_drift,
    write_replay_review_artifacts,
)


PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_replay_review(args) -> None:
    history_output_dir = _resolve_path(getattr(args, "history_output_dir", None), "artifacts/research_replay/history")
    gating_output_dir = _resolve_path(getattr(args, "gating_output_dir", None), "artifacts/research_replay/gating")
    review_output_dir = _resolve_path(getattr(args, "review_output_dir", None), "artifacts/research_replay/review")

    history_result = update_shared_replay_history(
        output_dir=history_output_dir,
        evaluation_summary_paths=[_resolve_path(path, path) for path in list(getattr(args, "evaluation_summary_paths", None) or [])],
        comparison_summary_paths=[_resolve_path(path, path) for path in list(getattr(args, "comparison_summary_paths", None) or [])],
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
    gating_artifacts = write_research_gating_artifacts(result=gating_result, output_dir=gating_output_dir)
    gating_summary = gating_result.to_summary()

    drift_result = evaluate_replay_drift(
        history_records=history_records,
        gating_summary=gating_summary,
        thresholds=ReplayDriftThresholds(
            recent_window=int(getattr(args, "recent_window", 4)),
            min_recent_runs=int(getattr(args, "min_recent_runs", 2)),
            max_abs_spearman_drop=float(getattr(args, "max_abs_spearman_drop", 0.03)),
            max_directional_accuracy_drop=float(getattr(args, "max_directional_accuracy_drop", 0.03)),
            max_rank_percentile_worsening=float(getattr(args, "max_rank_percentile_worsening", 0.25)),
            max_recent_score_stddev=float(getattr(args, "max_recent_score_stddev", 0.20)),
            min_recent_provider_count=int(getattr(args, "min_recent_provider_count", 1)),
        ),
    )
    queue_result = build_research_review_queue(
        gating_summary=gating_summary,
        drift_result=drift_result,
        history_path=history_result.history_path,
        gating_summary_path=gating_artifacts["summary_path"],
        drift_summary_path=str(Path(review_output_dir) / "latest_replay_drift_summary.json"),
    )
    review_artifacts = write_replay_review_artifacts(
        drift_result=drift_result,
        queue_result=queue_result,
        output_dir=review_output_dir,
    )
    payload = {
        "history_update": history_result.to_summary(),
        "gating_summary": gating_summary,
        "drift_summary": drift_result.to_summary(),
        "review_queue_summary": queue_result.to_summary(),
        "artifact_paths": {
            **gating_artifacts,
            **review_artifacts,
        },
    }

    if getattr(args, "format", "text") == "json":
        print(json.dumps(payload, indent=2))
        return

    queue_counts = payload["review_queue_summary"]["summary_counts"]
    drift_counts = payload["drift_summary"]["summary_counts"]
    print("Replay Research Review")
    print(f"  history append     : {history_result.appended_record_count} new records")
    print(f"  promotable review  : {queue_counts['promotable_review_count']}")
    print(f"  watchlist review   : {queue_counts['watchlist_review_count']}")
    print(f"  needs rerun        : {queue_counts['needs_rerun_count']}")
    print(f"  rejected archive   : {queue_counts['rejected_archive_count']}")
    print(f"  drifted candidates : {drift_counts['drifted_count']}")
    print(f"  queue summary path : {review_artifacts['queue_summary_path']}")
    print(f"  drift summary path : {review_artifacts['drift_summary_path']}")
