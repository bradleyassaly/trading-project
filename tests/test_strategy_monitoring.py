from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.strategy_monitor_build import cmd_strategy_monitor_build
from trading_platform.cli.commands.strategy_monitor_recommend_kill_switch import (
    cmd_strategy_monitor_recommend_kill_switch,
)
from trading_platform.cli.commands.strategy_monitor_show import cmd_strategy_monitor_show
from trading_platform.portfolio.strategy_monitoring import (
    StrategyMonitoringPolicyConfig,
    build_strategy_monitoring_snapshot,
    load_strategy_monitoring,
)


def _write_strategy_portfolio(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "strategy_portfolio.json").write_text(
        json.dumps(
            {
                "summary": {
                    "total_selected_strategies": 2,
                    "total_active_weight": 1.0,
                },
                "selected_strategies": [
                    {
                        "preset_name": "generated_momentum_a",
                        "source_run_id": "run-a",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "promotion_status": "active",
                        "allocation_weight": 0.6,
                        "target_capital_fraction": 0.6,
                        "selection_metric_value": 1.4,
                    },
                    {
                        "preset_name": "generated_value_b",
                        "source_run_id": "run-b",
                        "signal_family": "value",
                        "universe": "sp500",
                        "promotion_status": "active",
                        "allocation_weight": 0.4,
                        "target_capital_fraction": 0.4,
                        "selection_metric_value": 0.9,
                    },
                ],
                "warnings": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return root


def _write_paper_artifacts(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"timestamp": "2026-03-17T00:00:00+00:00", "equity": 100000.0},
            {"timestamp": "2026-03-18T00:00:00+00:00", "equity": 98000.0},
            {"timestamp": "2026-03-19T00:00:00+00:00", "equity": 96000.0},
            {"timestamp": "2026-03-20T00:00:00+00:00", "equity": 93000.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "equity": 92000.0},
        ]
    ).to_csv(root / "paper_equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-21T00:00:00+00:00",
                "turnover_estimate": 0.42,
                "target_selected_count": 4,
            }
        ]
    ).to_csv(root / "paper_run_summary.csv", index=False)
    pd.DataFrame(
        [
            {"check_name": "data_loaded", "status": "pass", "message": "ok"},
            {"check_name": "gross_exposure", "status": "fail", "message": "breach"},
        ]
    ).to_csv(root / "paper_health_checks.csv", index=False)
    (root / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"current_equity": 92000.0, "gross_exposure": 0.95}}, indent=2),
        encoding="utf-8",
    )
    return root


def _write_execution_artifacts(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "execution_summary.json").write_text(
        json.dumps({"expected_total_cost": 25.0, "rejected_order_count": 2}, indent=2),
        encoding="utf-8",
    )
    return root


def _write_allocation_artifacts(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"sleeve_name": "generated_momentum_a", "gross_contribution": 0.6},
            {"sleeve_name": "generated_value_b", "gross_contribution": 0.4},
        ]
    ).to_csv(root / "sleeve_attribution.csv", index=False)
    pd.DataFrame(
        [
            {"sleeve_name": "generated_momentum_a", "symbol": "AAPL", "scaled_target_weight": 0.35},
            {"sleeve_name": "generated_momentum_a", "symbol": "MSFT", "scaled_target_weight": 0.25},
            {"sleeve_name": "generated_value_b", "symbol": "JPM", "scaled_target_weight": 0.25},
            {"sleeve_name": "generated_value_b", "symbol": "XOM", "scaled_target_weight": 0.15},
        ]
    ).to_csv(root / "sleeve_target_weights.csv", index=False)
    return root


def test_strategy_monitoring_build_computes_metrics_and_recommendations(tmp_path: Path) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    paper_dir = _write_paper_artifacts(tmp_path / "paper")
    execution_dir = _write_execution_artifacts(tmp_path / "execution")
    allocation_dir = _write_allocation_artifacts(tmp_path / "allocation")

    result = build_strategy_monitoring_snapshot(
        strategy_portfolio_path=portfolio_dir,
        paper_dir=paper_dir,
        execution_dir=execution_dir,
        allocation_dir=allocation_dir,
        output_dir=tmp_path / "monitoring",
        policy=StrategyMonitoringPolicyConfig(
            min_observations=3,
            warning_drawdown=0.05,
            deactivate_drawdown=0.07,
            warning_realized_sharpe=0.2,
            deactivate_realized_sharpe=0.0,
            max_drift_from_expected=0.5,
            max_underperformance_streak=1,
        ),
    )

    payload = load_strategy_monitoring(tmp_path / "monitoring")

    assert result["warning_strategy_count"] == 2
    assert result["deactivation_candidate_count"] == 2
    assert payload["summary"]["selected_strategy_count"] == 2
    assert payload["summary"]["aggregate_drawdown"] >= 0.07
    assert payload["strategies"][0]["attribution_method"] == "proxy_weight_scaled"
    assert payload["kill_switch_recommendations"][0]["recommendation"] == "deactivate"


def test_strategy_monitoring_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    paper_dir = _write_paper_artifacts(tmp_path / "paper")

    cmd_strategy_monitor_build(
        Namespace(
            portfolio=str(portfolio_dir),
            paper_dir=str(paper_dir),
            execution_dir=None,
            allocation_dir=None,
            policy_config=None,
            output_dir=str(tmp_path / "monitoring"),
        )
    )
    cmd_strategy_monitor_show(Namespace(monitoring=str(tmp_path / "monitoring")))
    cmd_strategy_monitor_recommend_kill_switch(
        Namespace(
            monitoring=str(tmp_path / "monitoring"),
            output_dir=str(tmp_path / "recommendations"),
            include_review=True,
        )
    )

    captured = capsys.readouterr().out
    assert "Strategy monitoring JSON" in captured
    assert "Selected strategies:" in captured
    assert "Recommendation count:" in captured


def test_strategy_monitoring_respects_inactive_filter(tmp_path: Path) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    payload = json.loads((portfolio_dir / "strategy_portfolio.json").read_text(encoding="utf-8"))
    payload["selected_strategies"][1]["promotion_status"] = "inactive"
    (portfolio_dir / "strategy_portfolio.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    paper_dir = _write_paper_artifacts(tmp_path / "paper")

    build_strategy_monitoring_snapshot(
        strategy_portfolio_path=portfolio_dir,
        paper_dir=paper_dir,
        execution_dir=None,
        allocation_dir=None,
        output_dir=tmp_path / "monitoring",
        policy=StrategyMonitoringPolicyConfig(include_inactive_strategies=False),
    )

    monitoring = load_strategy_monitoring(tmp_path / "monitoring")
    assert monitoring["summary"]["selected_strategy_count"] == 1
    assert monitoring["summary"]["inactive_skipped_count"] == 1
