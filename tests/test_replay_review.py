from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.research_replay_review import cmd_research_replay_review
from trading_platform.research.replay_review import (
    ReplayDriftThresholds,
    build_research_review_queue,
    evaluate_replay_drift,
)
from trading_platform.research.replay_history import ReplayHistoryRecord


def _record(
    *,
    record_id: str,
    candidate_id: str,
    source_type: str,
    generated_at: str,
    providers: list[str],
    spearman: float,
    directional_accuracy: float,
    score: float | None = None,
    rank_percentile: float | None = None,
) -> ReplayHistoryRecord:
    return ReplayHistoryRecord(
        record_id=record_id,
        history_run_id="history_run",
        recorded_at=generated_at,
        source_type=source_type,
        source_name=f"{source_type}_summary",
        source_summary_path=f"artifacts/{record_id}.json",
        source_generated_at=generated_at,
        candidate_id=candidate_id,
        providers=providers,
        dataset_keys=["dataset"],
        alignment_mode="outer_union",
        feature_column="feature",
        target_column="target_return_1",
        row_count=100,
        pearson_correlation=spearman,
        spearman_correlation=spearman,
        directional_accuracy=directional_accuracy,
        top_bottom_spread=0.05,
        composite_score=score,
        comparison_rank=1 if rank_percentile is not None else None,
        comparison_rank_percentile=rank_percentile,
        eligible=True,
        warnings=[],
        provenance={},
    )


def test_replay_drift_classifies_stable_warning_and_drifted() -> None:
    history = [
        _record(record_id="stable_old_eval", candidate_id="stable", source_type="evaluation", generated_at="2024-01-01T00:00:00Z", providers=["binance"], spearman=0.08, directional_accuracy=0.58),
        _record(record_id="stable_old_cmp", candidate_id="stable", source_type="comparison", generated_at="2024-01-02T00:00:00Z", providers=["binance"], spearman=0.08, directional_accuracy=0.58, score=0.30, rank_percentile=0.20),
        _record(record_id="stable_new_eval", candidate_id="stable", source_type="evaluation", generated_at="2024-01-03T00:00:00Z", providers=["binance"], spearman=0.07, directional_accuracy=0.57),
        _record(record_id="stable_new_cmp", candidate_id="stable", source_type="comparison", generated_at="2024-01-04T00:00:00Z", providers=["binance"], spearman=0.07, directional_accuracy=0.57, score=0.28, rank_percentile=0.25),
        _record(record_id="warn_old_eval", candidate_id="warning", source_type="evaluation", generated_at="2024-01-01T00:00:00Z", providers=["kalshi"], spearman=0.07, directional_accuracy=0.58),
        _record(record_id="warn_old_cmp", candidate_id="warning", source_type="comparison", generated_at="2024-01-02T00:00:00Z", providers=["kalshi"], spearman=0.07, directional_accuracy=0.58, score=0.35, rank_percentile=0.15),
        _record(record_id="warn_new_eval", candidate_id="warning", source_type="evaluation", generated_at="2024-01-03T00:00:00Z", providers=["kalshi"], spearman=0.05, directional_accuracy=0.56),
        _record(record_id="warn_new_cmp", candidate_id="warning", source_type="comparison", generated_at="2024-01-04T00:00:00Z", providers=["kalshi"], spearman=0.05, directional_accuracy=0.56, score=0.10, rank_percentile=0.45),
        _record(record_id="drift_old_eval", candidate_id="drifted", source_type="evaluation", generated_at="2024-01-01T00:00:00Z", providers=["polymarket"], spearman=0.08, directional_accuracy=0.57),
        _record(record_id="drift_old_cmp", candidate_id="drifted", source_type="comparison", generated_at="2024-01-02T00:00:00Z", providers=["polymarket"], spearman=0.08, directional_accuracy=0.57, score=0.30, rank_percentile=0.20),
        _record(record_id="drift_new_eval", candidate_id="drifted", source_type="evaluation", generated_at="2024-01-03T00:00:00Z", providers=["polymarket"], spearman=0.01, directional_accuracy=0.47),
    ]
    gating_summary = {
        "decisions": [
            {"candidate_id": "stable", "overall_status": "promotable", "providers": ["binance"]},
            {"candidate_id": "warning", "overall_status": "promotable", "providers": ["kalshi"]},
            {"candidate_id": "drifted", "overall_status": "promotable", "providers": ["polymarket"]},
        ]
    }

    result = evaluate_replay_drift(
        history_records=history,
        gating_summary=gating_summary,
        thresholds=ReplayDriftThresholds(recent_window=2, min_recent_runs=2, max_abs_spearman_drop=0.015),
    )
    statuses = {item.candidate_id: item.drift_status for item in result.decisions}
    assert statuses["stable"] == "stable"
    assert statuses["warning"] == "warning"
    assert statuses["drifted"] == "drifted"


def test_review_queue_moves_drifted_candidates_to_needs_rerun() -> None:
    gating_summary = {
        "decisions": [
            {
                "candidate_id": "candidate_a",
                "overall_status": "promotable",
                "providers": ["binance"],
                "watchlist_reasons": [],
                "rejection_reasons": [],
                "supporting_metrics": {"mean_composite_score": 0.4},
            },
            {
                "candidate_id": "candidate_b",
                "overall_status": "watchlist",
                "providers": ["kalshi"],
                "watchlist_reasons": ["weak_composite_score"],
                "rejection_reasons": [],
                "supporting_metrics": {"mean_composite_score": 0.1},
            },
        ]
    }
    drift_summary = evaluate_replay_drift(
        history_records=[
            _record(record_id="a1", candidate_id="candidate_a", source_type="evaluation", generated_at="2024-01-01T00:00:00Z", providers=["binance"], spearman=0.08, directional_accuracy=0.58),
            _record(record_id="a2", candidate_id="candidate_a", source_type="comparison", generated_at="2024-01-02T00:00:00Z", providers=["binance"], spearman=0.08, directional_accuracy=0.58, score=0.30, rank_percentile=0.20),
            _record(record_id="a3", candidate_id="candidate_a", source_type="evaluation", generated_at="2024-01-03T00:00:00Z", providers=["binance"], spearman=0.01, directional_accuracy=0.46),
            _record(record_id="b1", candidate_id="candidate_b", source_type="evaluation", generated_at="2024-01-01T00:00:00Z", providers=["kalshi"], spearman=0.04, directional_accuracy=0.53),
            _record(record_id="b2", candidate_id="candidate_b", source_type="comparison", generated_at="2024-01-02T00:00:00Z", providers=["kalshi"], spearman=0.04, directional_accuracy=0.53, score=0.12, rank_percentile=0.4),
        ],
        gating_summary=gating_summary,
        thresholds=ReplayDriftThresholds(recent_window=2, min_recent_runs=1, max_abs_spearman_drop=0.03),
    )

    queue = build_research_review_queue(
        gating_summary=gating_summary,
        drift_result=drift_summary,
        history_path="artifacts/research_replay/history/shared_replay_history.jsonl",
        gating_summary_path="artifacts/research_replay/gating/latest_research_gating_summary.json",
        drift_summary_path="artifacts/research_replay/review/latest_replay_drift_summary.json",
    )
    by_candidate = {entry.candidate_id: entry for entry in queue.entries}
    assert by_candidate["candidate_a"].queue_state == "needs_rerun"
    assert by_candidate["candidate_b"].queue_state == "watchlist_review"


def test_research_replay_review_command_writes_review_artifacts(tmp_path: Path, capsys) -> None:
    evaluation_root = tmp_path / "artifacts" / "research_replay" / "evaluation"
    comparison_root = tmp_path / "artifacts" / "research_replay" / "comparison"
    evaluation_root.mkdir(parents=True, exist_ok=True)
    comparison_root.mkdir(parents=True, exist_ok=True)
    (evaluation_root / "latest_replay_evaluation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2024-01-01T00:00:00Z",
                "evaluation_name": "eval_a",
                "request": {"consumer_request": {"providers": ["binance"], "dataset_keys": ["binance.crypto_market_features"], "alignment_mode": "outer_union"}},
                "consumer_summary": {"metadata": {"alignment_mode": "outer_union", "providers": ["binance"], "datasets": [{"provider": "binance", "dataset_key": "binance.crypto_market_features"}]}},
                "metrics": [{"feature_column": "feature_a", "target_column": "target_return_1", "row_count": 90, "pearson_correlation": 0.05, "spearman_correlation": 0.05, "directional_accuracy": 0.55, "top_bottom_spread": 0.08}],
                "warnings": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (comparison_root / "latest_replay_comparison_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2024-01-02T00:00:00Z",
                "comparison_name": "compare_a",
                "request": {"comparison_mode": "provider"},
                "rankings": [{"evaluation_name": "eval_a", "summary_path": str(evaluation_root / "latest_replay_evaluation_summary.json"), "providers": ["binance"], "dataset_keys": ["binance.crypto_market_features"], "alignment_mode": "outer_union", "feature_column": "feature_a", "target_column": "target_return_1", "row_count": 90, "pearson_correlation": 0.05, "spearman_correlation": 0.05, "directional_accuracy": 0.55, "top_bottom_spread": 0.08, "composite_score": 0.24, "eligible": True, "warnings": []}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    cmd_research_replay_review(
        SimpleNamespace(
            evaluation_root=str(evaluation_root),
            comparison_root=str(comparison_root),
            evaluation_summary_paths=None,
            comparison_summary_paths=None,
            history_output_dir=str(tmp_path / "artifacts" / "research_replay" / "history"),
            gating_output_dir=str(tmp_path / "artifacts" / "research_replay" / "gating"),
            review_output_dir=str(tmp_path / "artifacts" / "research_replay" / "review"),
            min_row_count=25,
            min_replay_runs=2,
            min_mean_abs_spearman=0.02,
            min_mean_directional_accuracy=0.51,
            min_mean_composite_score=0.10,
                max_score_stddev=0.20,
                max_best_rank=5,
                max_best_rank_percentile=1.00,
                min_provider_count=1,
            recent_window=4,
            min_recent_runs=1,
            max_abs_spearman_drop=0.03,
            max_directional_accuracy_drop=0.03,
            max_rank_percentile_worsening=0.25,
            max_recent_score_stddev=0.20,
            min_recent_provider_count=1,
            format="json",
        )
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["review_queue_summary"]["summary_counts"]["promotable_review_count"] == 1
    assert payload["drift_summary"]["summary_counts"]["candidate_count"] == 1
    assert (tmp_path / "artifacts" / "research_replay" / "review" / "latest_review_queue_summary.json").exists()
