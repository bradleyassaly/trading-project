from __future__ import annotations

import json
from pathlib import Path

from trading_platform.reporting.strategy_quality_report import (
    build_strategy_quality_report,
    write_strategy_quality_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_strategy_quality_report_writes_expected_artifacts(tmp_path: Path) -> None:
    promoted_dir = tmp_path / "promoted"
    portfolio_dir = tmp_path / "portfolio"
    activated_dir = portfolio_dir / "activated"
    paper_dir = tmp_path / "paper"
    output_root = tmp_path / "daily_trading"

    _write_json(
        promoted_dir / "promoted_strategies.json",
        {
            "promotion_candidates_path": str(promoted_dir / "promotion_candidates.json"),
            "strategies": [
                {
                    "preset_name": "generated_base",
                    "source_run_id": "run-1",
                    "signal_family": "momentum",
                    "ranking_metric": "portfolio_sharpe",
                    "ranking_value": 2.1,
                    "portfolio_sharpe": 2.1,
                    "runtime_score_validation_pass": True,
                    "runtime_score_validation_reason": "runtime_scores_available",
                    "runtime_computable_symbol_count": 42,
                },
                {
                    "preset_name": "generated_regime",
                    "source_run_id": "run-2",
                    "signal_family": "quality",
                    "promotion_variant": "conditional",
                    "condition_id": "regime::risk_on",
                    "condition_type": "regime",
                    "ranking_metric": "mean_spearman_ic",
                    "ranking_value": 0.08,
                    "runtime_score_validation_pass": True,
                    "runtime_score_validation_reason": "runtime_scores_available",
                    "runtime_computable_symbol_count": 18,
                },
            ],
        },
    )
    _write_json(
        promoted_dir / "promotion_candidates.json",
        {
            "rows": [
                {
                    "run_id": "run-1",
                    "signal_family": "momentum",
                    "mean_spearman_ic": 0.05,
                    "portfolio_sharpe": 2.1,
                    "runtime_computability_pass": True,
                    "runtime_computability_reason": "runtime_scores_available",
                    "runtime_computable_symbol_count": 42,
                },
                {
                    "run_id": "run-2",
                    "signal_family": "quality",
                    "mean_spearman_ic": 0.03,
                    "portfolio_sharpe": 1.4,
                    "runtime_computability_pass": True,
                    "runtime_computability_reason": "runtime_scores_available",
                    "runtime_computable_symbol_count": 18,
                },
            ],
            "conditional_rows": [
                {
                    "run_id": "run-2",
                    "signal_family": "quality",
                    "condition_id": "regime::risk_on",
                    "metric_value": 0.08,
                    "runtime_computable_symbol_count": 18,
                }
            ],
        },
    )
    _write_json(
        portfolio_dir / "strategy_portfolio.json",
        {
            "selected_strategies": [
                {
                    "preset_name": "generated_base",
                    "source_run_id": "run-1",
                    "signal_family": "momentum",
                    "allocation_weight": 0.6,
                    "target_capital_fraction": 0.6,
                    "selection_rank": 1,
                    "ranking_metric": "portfolio_sharpe",
                    "ranking_value": 2.1,
                },
                {
                    "preset_name": "generated_regime",
                    "source_run_id": "run-2",
                    "signal_family": "quality",
                    "promotion_variant": "conditional",
                    "condition_id": "regime::risk_on",
                    "condition_type": "regime",
                    "allocation_weight": 0.4,
                    "target_capital_fraction": 0.4,
                    "selection_rank": 2,
                    "ranking_metric": "mean_spearman_ic",
                    "ranking_value": 0.08,
                },
            ]
        },
    )
    _write_json(
        activated_dir / "activated_strategy_portfolio.json",
        {
            "summary": {
                "active_row_count": 1,
                "activated_unconditional_count": 1,
                "activated_conditional_count": 0,
                "inactive_conditional_count": 1,
            },
            "strategies": [
                {
                    "preset_name": "generated_base",
                    "activation_state": "active",
                    "portfolio_bucket": "primary",
                },
                {
                    "preset_name": "generated_regime",
                    "activation_state": "inactive",
                    "portfolio_bucket": "conditional",
                },
            ],
            "active_strategies": [{"preset_name": "generated_base"}],
        },
    )
    _write_json(
        paper_dir / "paper_run_summary_latest.json",
        {
            "timestamp": "2026-03-27T16:00:00+00:00",
            "turnover_estimate": 0.12,
        },
    )
    _write_json(
        paper_dir / "portfolio_performance_summary.json",
        {
            "total_pnl": 1250.0,
            "total_return": 0.0125,
            "max_drawdown": -0.04,
            "turnover": 0.12,
        },
    )
    _write_json(
        paper_dir / "strategy_contribution_summary.json",
        {
            "normalized_capital_weights": {"generated_base": 0.6, "generated_regime": 0.4},
            "sleeve_contribution": {"generated_base": 0.7, "generated_regime": 0.3},
        },
    )

    report = build_strategy_quality_report(
        promoted_dir=promoted_dir,
        portfolio_dir=portfolio_dir,
        activated_dir=activated_dir,
        paper_output_dir=paper_dir,
        output_root=output_root,
        run_name="run_current",
    )
    paths = write_strategy_quality_report(report=report, output_dir=paper_dir / "report")

    comparison_rows = report["strategy_comparison_rows"]
    assert len(comparison_rows) == 2
    assert comparison_rows[0]["strategy_id"] == "generated_base"
    assert comparison_rows[0]["runtime_computability_pass"] is True
    assert report["summary"]["active_strategy_count"] == 1
    assert paths["strategy_comparison_summary_path"].exists()
    assert paths["strategy_performance_history_path"].exists()
    assert paths["rolling_sharpe_by_strategy_path"].exists()
    assert paths["rolling_ic_by_signal_path"].exists()
    assert paths["drawdown_by_strategy_path"].exists()
    assert paths["strategy_quality_summary_path"].exists()
