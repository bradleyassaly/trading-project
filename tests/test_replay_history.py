from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.replay_history import (
    filter_replay_history,
    load_replay_history,
    update_shared_replay_history,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_update_shared_replay_history_is_append_only(tmp_path: Path) -> None:
    evaluation_root = tmp_path / "artifacts" / "research_replay" / "evaluation"
    comparison_root = tmp_path / "artifacts" / "research_replay" / "comparison"
    history_root = tmp_path / "artifacts" / "research_replay" / "history"

    _write_json(
        evaluation_root / "latest_replay_evaluation_summary.json",
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
                    "feature_column": "binance__momentum_1",
                    "target_column": "binance__target_return_1",
                    "row_count": 120,
                    "pearson_correlation": 0.08,
                    "spearman_correlation": 0.06,
                    "directional_accuracy": 0.57,
                    "top_bottom_spread": 0.12,
                }
            ],
            "warnings": [],
        },
    )
    _write_json(
        comparison_root / "latest_replay_comparison_summary.json",
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
                    "feature_column": "binance__momentum_1",
                    "target_column": "binance__target_return_1",
                    "row_count": 120,
                    "pearson_correlation": 0.08,
                    "spearman_correlation": 0.06,
                    "directional_accuracy": 0.57,
                    "top_bottom_spread": 0.12,
                    "composite_score": 0.31,
                    "eligible": True,
                    "warnings": [],
                }
            ],
        },
    )

    first = update_shared_replay_history(
        output_dir=history_root,
        evaluation_root=evaluation_root,
        comparison_root=comparison_root,
    )
    second = update_shared_replay_history(
        output_dir=history_root,
        evaluation_root=evaluation_root,
        comparison_root=comparison_root,
    )

    records = load_replay_history(history_root / "shared_replay_history.jsonl")
    assert first.appended_record_count == 2
    assert second.appended_record_count == 0
    assert len(records) == 2
    assert {record.source_type for record in records} == {"evaluation", "comparison"}
    comparison_record = next(record for record in records if record.source_type == "comparison")
    assert comparison_record.comparison_rank == 1
    assert comparison_record.comparison_rank_percentile == 1.0


def test_filter_replay_history_by_provider_and_candidate(tmp_path: Path) -> None:
    evaluation_root = tmp_path / "evaluation"
    history_root = tmp_path / "history"
    _write_json(
        evaluation_root / "latest_replay_evaluation_summary.json",
        {
            "generated_at": "2024-01-02T00:00:00Z",
            "evaluation_name": "shared_eval",
            "request": {"consumer_request": {"providers": ["binance", "kalshi"], "dataset_keys": ["binance.crypto_market_features", "kalshi.prediction_market_features"], "alignment_mode": "anchor"}},
            "consumer_summary": {
                "metadata": {
                    "alignment_mode": "anchor",
                    "providers": ["binance", "kalshi"],
                    "datasets": [
                        {"provider": "binance", "dataset_key": "binance.crypto_market_features"},
                        {"provider": "kalshi", "dataset_key": "kalshi.prediction_market_features"},
                    ],
                }
            },
            "metrics": [
                {
                    "feature_column": "shared_feature",
                    "target_column": "shared_target",
                    "row_count": 80,
                    "pearson_correlation": 0.03,
                    "spearman_correlation": 0.02,
                    "directional_accuracy": 0.53,
                    "top_bottom_spread": 0.04,
                }
            ],
            "warnings": [],
        },
    )
    update_shared_replay_history(output_dir=history_root, evaluation_root=evaluation_root)
    records = load_replay_history(history_root / "shared_replay_history.jsonl")
    candidate_id = records[0].candidate_id

    by_provider = filter_replay_history(records, provider="kalshi")
    by_candidate = filter_replay_history(records, candidate_id=candidate_id)

    assert len(by_provider) == 1
    assert len(by_candidate) == 1
    assert by_provider[0].candidate_id == candidate_id
