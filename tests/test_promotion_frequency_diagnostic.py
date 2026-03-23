from __future__ import annotations

from trading_platform.diagnostics.equity_feature_expansion import _build_comparison_summary
from trading_platform.diagnostics.promotion_frequency import _build_summary


def test_build_summary_counts_drop_stages_and_rates() -> None:
    rows = [
        {
            "candidate_count": 2,
            "validation_pass_count": 1,
            "promotion_candidate_count": 1,
            "promoted_strategy_count": 1,
            "portfolio_stage_reached": True,
            "paper_stage_reached": True,
            "first_drop_stage": None,
        },
        {
            "candidate_count": 2,
            "validation_pass_count": 0,
            "promotion_candidate_count": 0,
            "promoted_strategy_count": 0,
            "portfolio_stage_reached": False,
            "paper_stage_reached": False,
            "first_drop_stage": "promotion",
        },
        {
            "candidate_count": 0,
            "validation_pass_count": 0,
            "promotion_candidate_count": 0,
            "promoted_strategy_count": 0,
            "portfolio_stage_reached": False,
            "paper_stage_reached": False,
            "first_drop_stage": "research",
        },
    ]

    summary = _build_summary(rows)

    assert summary["total_runs_attempted"] == 3
    assert summary["runs_with_candidates"] == 2
    assert summary["runs_passing_validation"] == 1
    assert summary["runs_with_promotion_candidates"] == 1
    assert summary["runs_with_promoted_strategies"] == 1
    assert summary["runs_reaching_portfolio_stage"] == 1
    assert summary["runs_reaching_paper_stage"] == 1
    assert summary["drop_stage_counts"] == {"none": 1, "promotion": 1, "research": 1}
    assert summary["rates"]["reaching_paper_stage"] == 1 / 3


def test_build_comparison_summary_flags_frequency_improvement() -> None:
    baseline = {
        "total_runs_attempted": 6,
        "runs_with_candidates": 6,
        "runs_passing_validation": 4,
        "runs_with_promotion_candidates": 4,
        "runs_with_promoted_strategies": 4,
        "runs_reaching_portfolio_stage": 4,
        "runs_reaching_paper_stage": 4,
        "rates": {
            "with_candidates": 1.0,
            "passing_validation": 4 / 6,
            "with_promotion_candidates": 4 / 6,
            "with_promoted_strategies": 4 / 6,
            "reaching_portfolio_stage": 4 / 6,
            "reaching_paper_stage": 4 / 6,
        },
    }
    expanded = {
        "total_runs_attempted": 6,
        "runs_with_candidates": 6,
        "runs_passing_validation": 5,
        "runs_with_promotion_candidates": 5,
        "runs_with_promoted_strategies": 5,
        "runs_reaching_portfolio_stage": 5,
        "runs_reaching_paper_stage": 5,
        "rates": {
            "with_candidates": 1.0,
            "passing_validation": 5 / 6,
            "with_promotion_candidates": 5 / 6,
            "with_promoted_strategies": 5 / 6,
            "reaching_portfolio_stage": 5 / 6,
            "reaching_paper_stage": 5 / 6,
        },
    }

    result = _build_comparison_summary(baseline=baseline, expanded=expanded)

    assert result["count_deltas"]["runs_with_promoted_strategies"] == 1
    assert result["rate_deltas"]["reaching_paper_stage"] == (5 / 6) - (4 / 6)
    assert result["promotion_frequency_improved"] is True
    assert result["downstream_activity_improved"] is True
