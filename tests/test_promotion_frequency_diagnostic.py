from __future__ import annotations

from pathlib import Path

from trading_platform.diagnostics.feature_ablation import _build_comparison_payload, AblationMode
from trading_platform.diagnostics.equity_feature_expansion import _build_comparison_summary
from trading_platform.diagnostics.promotion_frequency import _build_summary, _resolve_scenarios
from trading_platform.diagnostics.signal_family_comparison import _build_comparison_payload as _build_family_payload
from trading_platform.diagnostics.signal_family_comparison import _build_family_summary
from trading_platform.diagnostics.strategy_weighting_comparison import _recommend_mode


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


def test_build_family_summary_aggregates_counts_and_sharpe() -> None:
    result = {
        "summary": {
            "runs_with_candidates": 2,
            "runs_passing_validation": 1,
            "runs_with_promotion_candidates": 1,
            "runs_with_promoted_strategies": 1,
            "runs_reaching_portfolio_stage": 1,
            "runs_reaching_paper_stage": 1,
        },
        "rows": [
            {
                "candidate_count": 2,
                "validation_pass_count": 1,
                "promotion_candidate_count": 1,
                "promoted_strategy_count": 1,
                "portfolio_selected_strategy_count": 1,
                "paper_stage_reached": True,
                "portfolio_sharpe": 1.2,
            },
            {
                "candidate_count": 1,
                "validation_pass_count": 0,
                "promotion_candidate_count": 0,
                "promoted_strategy_count": 0,
                "portfolio_selected_strategy_count": 0,
                "paper_stage_reached": False,
                "portfolio_sharpe": None,
            },
        ],
        "json_path": "family.json",
        "csv_path": "family.csv",
        "md_path": "family.md",
    }

    summary = _build_family_summary("momentum", result)

    assert summary["total_candidate_count"] == 3
    assert summary["promoted_strategy_count"] == 1
    assert summary["paper_stage_count"] == 1
    assert summary["portfolio_sharpe_summary"]["mean"] == 1.2


def test_signal_family_payload_prefers_better_funnel_then_sharpe() -> None:
    payload = _build_family_payload(
        family_summaries=[
            {
                "signal_family": "momentum",
                "summary": {
                    "runs_with_promoted_strategies": 3,
                    "runs_reaching_paper_stage": 3,
                    "runs_passing_validation": 3,
                },
                "portfolio_sharpe_summary": {"mean": 1.0},
            },
            {
                "signal_family": "momentum_acceleration",
                "summary": {
                    "runs_with_promoted_strategies": 4,
                    "runs_reaching_paper_stage": 4,
                    "runs_passing_validation": 4,
                },
                "portfolio_sharpe_summary": {"mean": 1.1},
            },
        ],
        commands=["python -m trading_platform.diagnostics.signal_family_comparison"],
        base_config_path=Path("configs/orchestration_signal_promotion_test.yaml"),
        output_root=Path("artifacts/diagnostics/signal_family_comparison"),
        scenario_set_name="default",
    )

    assert payload["best_family_by_funnel_then_sharpe"] == "momentum_acceleration"
    assert payload["decision"] == "promote winning new family into broader testing"


def test_strategy_weighting_recommendation_prefers_balanced_exportable_mode() -> None:
    recommendation = _recommend_mode(
        [
            {
                "weighting_mode": "metric_weighted",
                "run_bundle_exported": True,
                "effective_strategy_count": 2.0,
                "effective_family_count": 2.0,
                "weighted_metric_average": 1.2,
                "max_family_weight": 0.7,
                "max_strategy_weight": 0.7,
            },
            {
                "weighting_mode": "capped_metric_weighted",
                "run_bundle_exported": True,
                "effective_strategy_count": 2.6,
                "effective_family_count": 2.0,
                "weighted_metric_average": 1.1,
                "max_family_weight": 0.5,
                "max_strategy_weight": 0.45,
            },
        ]
    )

    assert recommendation == "capped_metric_weighted"
