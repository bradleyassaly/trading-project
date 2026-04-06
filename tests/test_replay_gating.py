from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.research_replay_gate import cmd_research_replay_gate
from trading_platform.research.replay_gating import (
    ReplayResearchGateThresholds,
    evaluate_research_gates,
)
from trading_platform.research.replay_history import ReplayHistoryRecord


def _history_record(
    *,
    record_id: str,
    source_type: str,
    candidate_id: str,
    providers: list[str],
    row_count: int,
    spearman: float,
    directional_accuracy: float,
    composite_score: float | None = None,
    comparison_rank: int | None = None,
    comparison_rank_percentile: float | None = None,
) -> ReplayHistoryRecord:
    return ReplayHistoryRecord(
        record_id=record_id,
        history_run_id="history_run",
        recorded_at="2024-01-01T00:00:00Z",
        source_type=source_type,
        source_name=f"{source_type}_summary",
        source_summary_path=f"artifacts/{source_type}.json",
        source_generated_at="2024-01-01T00:00:00Z",
        candidate_id=candidate_id,
        providers=providers,
        dataset_keys=["dataset"],
        alignment_mode="outer_union",
        feature_column="feature",
        target_column="target_return_1",
        row_count=row_count,
        pearson_correlation=spearman,
        spearman_correlation=spearman,
        directional_accuracy=directional_accuracy,
        top_bottom_spread=0.05,
        composite_score=composite_score,
        comparison_rank=comparison_rank,
        comparison_rank_percentile=comparison_rank_percentile,
        eligible=True,
        warnings=[],
        provenance={"summary_path": f"artifacts/{source_type}.json"},
    )


def test_evaluate_research_gates_returns_promotable_watchlist_and_rejected() -> None:
    records = [
        _history_record(record_id="p1", source_type="evaluation", candidate_id="promotable", providers=["binance", "kalshi"], row_count=120, spearman=0.06, directional_accuracy=0.58),
        _history_record(record_id="p2", source_type="comparison", candidate_id="promotable", providers=["binance", "kalshi"], row_count=120, spearman=0.06, directional_accuracy=0.58, composite_score=0.33, comparison_rank=1, comparison_rank_percentile=0.1),
        _history_record(record_id="p3", source_type="evaluation", candidate_id="promotable", providers=["binance", "kalshi"], row_count=110, spearman=0.05, directional_accuracy=0.56),
        _history_record(record_id="w1", source_type="evaluation", candidate_id="watchlist", providers=["binance"], row_count=90, spearman=0.04, directional_accuracy=0.52),
        _history_record(record_id="w2", source_type="comparison", candidate_id="watchlist", providers=["binance"], row_count=90, spearman=0.04, directional_accuracy=0.52, composite_score=0.04, comparison_rank=9, comparison_rank_percentile=0.9),
        _history_record(record_id="r1", source_type="evaluation", candidate_id="rejected", providers=["polymarket"], row_count=10, spearman=0.0, directional_accuracy=0.48),
    ]

    result = evaluate_research_gates(records, thresholds=ReplayResearchGateThresholds(min_provider_count=1))
    decisions = {decision.candidate_id: decision for decision in result.decisions}

    assert decisions["promotable"].overall_status == "promotable"
    assert decisions["watchlist"].overall_status == "watchlist"
    assert "weak_composite_score" in decisions["watchlist"].watchlist_reasons
    assert decisions["rejected"].overall_status == "rejected"
    assert "insufficient_replay_rows" in decisions["rejected"].rejection_reasons


def test_research_replay_gate_command_writes_history_and_gating_artifacts(tmp_path: Path, capsys) -> None:
    evaluation_root = tmp_path / "artifacts" / "research_replay" / "evaluation"
    comparison_root = tmp_path / "artifacts" / "research_replay" / "comparison"
    history_output_dir = tmp_path / "artifacts" / "research_replay" / "history"
    gating_output_dir = tmp_path / "artifacts" / "research_replay" / "gating"
    evaluation_root.mkdir(parents=True, exist_ok=True)
    comparison_root.mkdir(parents=True, exist_ok=True)
    (evaluation_root / "latest_replay_evaluation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2024-01-01T00:00:00Z",
                "evaluation_name": "binance_eval",
                "request": {"consumer_request": {"providers": ["binance"], "dataset_keys": ["binance.crypto_market_features"], "alignment_mode": "outer_union"}},
                "consumer_summary": {
                    "metadata": {
                        "alignment_mode": "outer_union",
                        "providers": ["binance"],
                        "datasets": [{"provider": "binance", "dataset_key": "binance.crypto_market_features"}],
                    }
                },
                "metrics": [
                    {
                        "feature_column": "feature_a",
                        "target_column": "target_return_1",
                        "row_count": 80,
                        "pearson_correlation": 0.04,
                        "spearman_correlation": 0.04,
                        "directional_accuracy": 0.55,
                        "top_bottom_spread": 0.08,
                    }
                ],
                "warnings": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (comparison_root / "latest_replay_comparison_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2024-01-01T00:05:00Z",
                "comparison_name": "shared_compare",
                "request": {"comparison_mode": "provider"},
                "rankings": [
                    {
                        "evaluation_name": "binance_eval",
                        "summary_path": str(evaluation_root / "latest_replay_evaluation_summary.json"),
                        "providers": ["binance"],
                        "dataset_keys": ["binance.crypto_market_features"],
                        "alignment_mode": "outer_union",
                        "feature_column": "feature_a",
                        "target_column": "target_return_1",
                        "row_count": 80,
                        "pearson_correlation": 0.04,
                        "spearman_correlation": 0.04,
                        "directional_accuracy": 0.55,
                        "top_bottom_spread": 0.08,
                        "composite_score": 0.22,
                        "eligible": True,
                        "warnings": [],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    cmd_research_replay_gate(
        SimpleNamespace(
            evaluation_root=str(evaluation_root),
            comparison_root=str(comparison_root),
            evaluation_summary_paths=None,
            comparison_summary_paths=None,
            history_output_dir=str(history_output_dir),
            gating_output_dir=str(gating_output_dir),
            min_row_count=25,
            min_replay_runs=2,
            min_mean_abs_spearman=0.02,
            min_mean_directional_accuracy=0.51,
            min_mean_composite_score=0.10,
            max_score_stddev=0.20,
            max_best_rank=5,
            max_best_rank_percentile=0.50,
            min_provider_count=1,
            format="json",
        )
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary_counts"]["candidate_count"] == 1
    assert payload["history_update"]["appended_record_count"] == 2
    assert (history_output_dir / "shared_replay_history.jsonl").exists()
    assert (gating_output_dir / "latest_research_gating_summary.json").exists()
