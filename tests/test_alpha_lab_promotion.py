from __future__ import annotations

import pandas as pd

from trading_platform.governance.models import PromotionDecision
from trading_platform.research.alpha_lab.promotion import (
    PromotionThresholds,
    apply_promotion_rules,
    evaluate_promotion_gate,
)


def test_evaluate_promotion_gate_emits_named_gate_results_and_reason_codes() -> None:
    row = pd.Series(
        {
            "candidate_id": "momentum|10|1",
            "signal_family": "momentum",
            "lookback": 10,
            "horizon": 1,
            "symbols_tested": 1.0,
            "folds_tested": 1,
            "mean_dates_evaluated": 2.0,
            "mean_spearman_ic": 0.0,
            "mean_turnover": 0.9,
            "worst_fold_spearman_ic": -0.2,
            "total_obs": 20.0,
        }
    )

    evaluation = evaluate_promotion_gate(row)

    assert evaluation.candidate_id == "momentum|10|1"
    assert evaluation.passed is False
    assert evaluation.failed_gate_names == [
        "mean_rank_ic",
        "symbols_tested",
        "folds_tested",
        "dates_evaluated",
        "total_observations",
        "turnover",
        "worst_fold_rank_ic",
    ]
    assert evaluation.rejection_reasons == [
        "low_mean_rank_ic",
        "insufficient_symbols",
        "insufficient_folds",
        "insufficient_dates",
        "insufficient_observations",
        "high_turnover",
        "weak_worst_fold_rank_ic",
    ]


def test_apply_promotion_rules_preserves_threshold_behavior_and_emits_gate_payloads() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "symbols_tested": 3.0,
                "folds_tested": 3,
                "mean_dates_evaluated": 10.0,
                "mean_spearman_ic": 0.05,
                "mean_turnover": 0.2,
                "worst_fold_spearman_ic": 0.01,
                "total_obs": 300.0,
            },
            {
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "symbols_tested": 1.0,
                "folds_tested": 1,
                "mean_dates_evaluated": 2.0,
                "mean_spearman_ic": 0.0,
                "mean_turnover": 0.9,
                "worst_fold_spearman_ic": -0.2,
                "total_obs": 20.0,
            },
        ]
    )

    result = apply_promotion_rules(
        leaderboard_df,
        thresholds=PromotionThresholds(),
    )

    assert result.loc[0, "promotion_status"] == "promote"
    assert result.loc[0, "rejection_reason"] == "none"
    assert result.loc[1, "promotion_status"] == "reject"
    assert result.loc[1, "rejection_reason"] == (
        "low_mean_rank_ic;insufficient_symbols;insufficient_folds;"
        "insufficient_dates;insufficient_observations;high_turnover;weak_worst_fold_rank_ic"
    )
    summary = result.loc[1, "promotion_gate_summary"]
    assert summary["candidate_id"] == "momentum|10|1"
    assert summary["failed_gate_names"] == [
        "mean_rank_ic",
        "symbols_tested",
        "folds_tested",
        "dates_evaluated",
        "total_observations",
        "turnover",
        "worst_fold_rank_ic",
    ]
    assert len(result.loc[1, "promotion_gate_results"]) == 7
    decision = PromotionDecision.from_dict(result.loc[1, "promotion_decision"])
    assert decision.final_status == "reject"
    assert decision.failed_gate_names == [
        "mean_rank_ic",
        "symbols_tested",
        "folds_tested",
        "dates_evaluated",
        "total_observations",
        "turnover",
        "worst_fold_rank_ic",
    ]
    assert decision.summary_metadata["candidate_id"] == "momentum|10|1"
    assert decision.summary_metadata["gate_count"] == 7


def test_apply_promotion_rules_adds_empty_gate_columns_for_empty_frames() -> None:
    result = apply_promotion_rules(pd.DataFrame())

    assert "promotion_gate_results" in result.columns
    assert "promotion_gate_summary" in result.columns
    assert "failed_promotion_gates" in result.columns
    assert "passed_promotion_gates" in result.columns
    assert "promotion_decision" in result.columns
    assert result.empty
