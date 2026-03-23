from __future__ import annotations

from trading_platform.diagnostics.feature_ablation import _build_comparison_payload, AblationMode
from trading_platform.diagnostics.equity_feature_expansion import _build_comparison_summary
from trading_platform.diagnostics.promotion_frequency import _build_summary, _resolve_scenarios


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


def test_resolve_richer_ablation_scenarios_includes_volume_context() -> None:
    scenarios = _resolve_scenarios("richer_ablation")

    assert len(scenarios) >= 8
    assert any(scenario.volume_by_symbol for scenario in scenarios)
    assert "late_volume_breakout" in {scenario.name for scenario in scenarios}


def test_feature_ablation_payload_prefers_best_funnel_then_sharpe() -> None:
    modes = [
        AblationMode("baseline_momentum", "momentum", False, False, "baseline"),
        AblationMode("momentum_with_context_features", "momentum", True, True, "control"),
        AblationMode("equity_context_momentum", "equity_context_momentum", True, True, "expanded"),
    ]
    mode_results = {
        "baseline_momentum": {
            "summary": {
                "runs_with_candidates": 10,
                "runs_passing_validation": 5,
                "runs_with_promotion_candidates": 5,
                "runs_with_promoted_strategies": 5,
                "runs_reaching_portfolio_stage": 5,
                "runs_reaching_paper_stage": 5,
            },
            "rows": [{"scenario_name": "s1", "portfolio_sharpe": 1.0}],
        },
        "momentum_with_context_features": {
            "summary": {
                "runs_with_candidates": 10,
                "runs_passing_validation": 5,
                "runs_with_promotion_candidates": 5,
                "runs_with_promoted_strategies": 5,
                "runs_reaching_portfolio_stage": 5,
                "runs_reaching_paper_stage": 5,
            },
            "rows": [{"scenario_name": "s1", "portfolio_sharpe": 1.0}],
        },
        "equity_context_momentum": {
            "summary": {
                "runs_with_candidates": 10,
                "runs_passing_validation": 6,
                "runs_with_promotion_candidates": 6,
                "runs_with_promoted_strategies": 6,
                "runs_reaching_portfolio_stage": 6,
                "runs_reaching_paper_stage": 6,
            },
            "rows": [{"scenario_name": "s1", "portfolio_sharpe": 1.5}],
        },
    }

    payload = _build_comparison_payload(mode_results=mode_results, modes=modes)

    assert payload["best_mode_by_funnel_then_sharpe"] == "equity_context_momentum"
    assert payload["decision"] == "promote equity-context features into broader testing because they show measurable benefit"
