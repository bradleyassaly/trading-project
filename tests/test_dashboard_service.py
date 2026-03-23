from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path

import pandas as pd

from trading_platform.dashboard.server import build_dashboard_static_data, create_dashboard_app
from trading_platform.dashboard.service import DashboardDataService
from trading_platform.governance.models import StrategyRegistry, StrategyRegistryEntry
from trading_platform.governance.persistence import save_strategy_registry


def _write_sample_artifacts(root: Path) -> None:
    run_dir = root / "orchestration" / "daily_governance" / "2026-03-22T00-00-00+00-00"
    (run_dir / "monitoring").mkdir(parents=True, exist_ok=True)
    (run_dir / "portfolio_allocation").mkdir(parents=True, exist_ok=True)
    (run_dir / "live_dry_run").mkdir(parents=True, exist_ok=True)
    (run_dir / "paper_trading").mkdir(parents=True, exist_ok=True)

    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_name": "daily_governance",
                "schedule_type": "daily",
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:10:00+00:00",
                "status": "succeeded",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"stage_name": "portfolio_allocation", "status": "succeeded"},
            {"stage_name": "paper_trading", "status": "succeeded"},
            {"stage_name": "live_dry_run", "status": "succeeded"},
        ]
    ).to_csv(run_dir / "stage_status.csv", index=False)
    (run_dir / "monitoring" / "run_health.json").write_text(
        json.dumps(
            {
                "status": "warning",
                "alert_counts": {"info": 0, "warning": 2, "critical": 0},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "monitoring" / "alerts.json").write_text(
        json.dumps(
            [{"severity": "warning", "code": "execution_cost", "message": "high cost"}],
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "portfolio_allocation" / "allocation_summary.json").write_text(
        json.dumps(
            {
                "summary": {
                    "gross_exposure_after_constraints": 0.9,
                    "net_exposure_after_constraints": 0.7,
                    "symbols_removed_or_clipped": [{"constraint_name": "max_position_weight", "symbol": "AAPL"}],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"symbol": "AAPL", "target_weight": 0.4, "side": "long", "latest_price": 100.0},
            {"symbol": "MSFT", "target_weight": 0.3, "side": "long", "latest_price": 200.0},
        ]
    ).to_csv(run_dir / "portfolio_allocation" / "combined_target_weights.csv", index=False)
    pd.DataFrame(
        [
            {"sleeve_name": "core", "symbol": "AAPL", "scaled_target_weight": 0.4},
            {"sleeve_name": "core", "symbol": "MSFT", "scaled_target_weight": 0.3},
        ]
    ).to_csv(run_dir / "portfolio_allocation" / "sleeve_target_weights.csv", index=False)
    pd.DataFrame([{"symbol": "AAPL", "sleeve_count": 1}]).to_csv(
        run_dir / "portfolio_allocation" / "symbol_overlap_report.csv",
        index=False,
    )

    (run_dir / "paper_trading" / "execution_summary.json").write_text(
        json.dumps(
            {
                "requested_order_count": 2,
                "executable_order_count": 1,
                "rejected_order_count": 1,
                "requested_notional": 10000.0,
                "executed_notional": 5000.0,
                "expected_total_cost": 25.0,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"symbol": "AAPL", "side": "BUY", "requested_shares": 100, "adjusted_shares": 50, "estimated_fill_price": 100.1, "commission": 1.0, "clipping_reason": "adv_cap"}]
    ).to_csv(run_dir / "paper_trading" / "executable_orders.csv", index=False)
    pd.DataFrame(
        [{"symbol": "MSFT", "side": "BUY", "requested_shares": 40, "rejection_reason": "min_trade_notional"}]
    ).to_csv(run_dir / "paper_trading" / "rejected_orders.csv", index=False)
    pd.DataFrame([{"symbol": "AAPL", "stale": False}]).to_csv(
        run_dir / "paper_trading" / "liquidity_constraints_report.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL", "requested_notional": 10000.0, "executed_notional": 5000.0}]).to_csv(
        run_dir / "paper_trading" / "turnover_summary.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL", "requested_shares": 100}]).to_csv(
        run_dir / "paper_trading" / "requested_orders.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"timestamp": "2026-03-20T00:00:00+00:00", "equity": 100000.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "equity": 101500.0},
            {"timestamp": "2026-03-22T00:00:00+00:00", "equity": 103000.0},
        ]
    ).to_csv(run_dir / "paper_trading" / "paper_equity_curve.csv", index=False)

    (run_dir / "live_dry_run" / "live_dry_run_summary.json").write_text(
        json.dumps(
            {
                "adjusted_order_count": 1,
                "health_checks": [{"check_name": "broker_connectivity", "status": "pass", "message": "ok"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    live_submit_dir = root / "live_submit"
    live_submit_dir.mkdir(parents=True, exist_ok=True)
    (live_submit_dir / "live_submission_summary.json").write_text(
        json.dumps(
            {
                "risk_passed": False,
                "submitted_order_count": 0,
                "duplicate_order_skip_count": 1,
                "risk_checks": [
                    {"check_name": "broker_health", "passed": True, "message": "healthy"},
                    {"check_name": "open_order_policy", "passed": False, "message": "open orders present"},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"symbol": "AAPL", "status": "skipped", "message": "materially identical open order already exists", "client_order_id": "cid-1"}]
    ).to_csv(live_submit_dir / "broker_order_results.csv", index=False)

    registry = StrategyRegistry(
        updated_at="2026-03-22T00:00:00+00:00",
        entries=[
            StrategyRegistryEntry(
                strategy_id="strat-a",
                strategy_name="Strategy A",
                family="momentum",
                version="v1",
                preset_name="xsec_nasdaq100_momentum_v1_deploy",
                research_artifact_paths=["artifacts/research/strat-a"],
                created_at="2026-03-20T00:00:00+00:00",
                status="approved",
                owner="qa",
                source="test",
                current_deployment_stage="approved",
                universe="nasdaq100",
                tags=["core"],
            ),
            StrategyRegistryEntry(
                strategy_id="strat-b",
                strategy_name="Strategy B",
                family="value",
                version="v2",
                preset_name="value_deploy",
                research_artifact_paths=["artifacts/research/strat-b"],
                created_at="2026-03-20T00:00:00+00:00",
                status="paper",
                owner="qa",
                source="test",
                current_deployment_stage="paper",
                universe="sp500",
                tags=["challenger"],
            ),
        ],
    )
    save_strategy_registry(registry, root / "strategy_registry.json")
    pd.DataFrame([{"family": "momentum", "champion_strategy_id": "strat-a", "challenger_strategy_id": "strat-b"}]).to_csv(
        root / "family_comparison.csv",
        index=False,
    )
    (root / "research_registry.json").write_text(
        json.dumps(
            {
                "summary": {"run_count": 2},
                "runs": [
                    {
                        "run_id": "research-run-1",
                        "timestamp": "2026-03-22T00:00:00+00:00",
                        "workflow_type": "alpha_research",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "candidate_count": 12,
                        "promoted_signal_count": 2,
                    },
                    {
                        "run_id": "research-run-2",
                        "timestamp": "2026-03-21T00:00:00+00:00",
                        "workflow_type": "alpha_research",
                        "signal_family": "value",
                        "universe": "sp500",
                        "candidate_count": 8,
                        "promoted_signal_count": 0,
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "research_leaderboard.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "rank": 1,
                        "run_id": "research-run-1",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "metric_name": "portfolio_sharpe",
                        "metric_value": 1.4,
                        "promotion_recommendation": "promotion_candidate",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "promotion_candidates.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "run_id": "research-run-1",
                        "eligible": True,
                        "promotion_recommendation": "promotion_candidate",
                        "mean_spearman_ic": 0.04,
                        "portfolio_sharpe": 1.4,
                        "reasons": "folds_tested >= 3",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "strategy_validation.json").write_text(
        json.dumps(
            {
                "summary": {"run_count": 2, "pass_count": 1, "weak_count": 1, "fail_count": 0},
                "rows": [
                    {
                        "run_id": "research-run-1",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "number_of_folds": 4,
                        "proxy_confidence_score": 0.81,
                        "validation_status": "pass",
                        "validation_reason": "validation_pass",
                        "out_of_sample_metrics": {"out_of_sample_sharpe": 1.1},
                    },
                    {
                        "run_id": "research-run-2",
                        "signal_family": "value",
                        "universe": "sp500",
                        "number_of_folds": 3,
                        "proxy_confidence_score": 0.52,
                        "validation_status": "weak",
                        "validation_reason": "out_of_sample_sharpe 0.2 < 0.5",
                        "out_of_sample_metrics": {"out_of_sample_sharpe": 0.2},
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "promoted_strategies.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "source_run_id": "research-run-1",
                        "status": "inactive",
                        "validation_status": "pass",
                        "ranking_metric": "portfolio_sharpe",
                        "ranking_value": 1.4,
                        "generated_preset_path": "configs/generated_strategies/generated_momentum_nasdaq100_research_run_1_paper.json",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "strategy_lifecycle.json").write_text(
        json.dumps(
            {
                "summary": {
                    "strategy_count": 2,
                    "active_count": 0,
                    "under_review_count": 1,
                    "degraded_count": 0,
                    "demoted_count": 1,
                    "state_counts": {
                        "candidate": 0,
                        "validated": 0,
                        "promoted": 0,
                        "active": 0,
                        "under_review": 1,
                        "degraded": 0,
                        "demoted": 1,
                    },
                },
                "strategies": [
                    {
                        "strategy_id": "generated_momentum_nasdaq100_research_run_1_paper",
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "current_state": "demoted",
                        "validation_status": "pass",
                        "monitoring_recommendation": "deactivate",
                        "adaptive_adjusted_weight": 0.0,
                        "latest_reasons": ["repeated_deactivate_recommendation"],
                    },
                    {
                        "strategy_id": "research-run-2",
                        "preset_name": None,
                        "current_state": "under_review",
                        "validation_status": "weak",
                        "monitoring_recommendation": None,
                        "adaptive_adjusted_weight": None,
                        "latest_reasons": ["weak_validation"],
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "strategy_governance_summary.json").write_text(
        json.dumps(
            {
                "summary": {
                    "strategy_count": 2,
                    "demoted_count": 1,
                    "degraded_count": 0,
                    "under_review_count": 1,
                    "active_count": 0,
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "strategy_portfolio.json").write_text(
        json.dumps(
            {
                "summary": {
                    "total_selected_strategies": 1,
                    "total_active_weight": 1.0,
                    "warning_count": 1,
                },
                "selected_strategies": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "allocation_weight": 1.0,
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "selection_rank": 1,
                    }
                ],
                "excluded_candidates": [
                    {"preset_name": "generated_value_sp500_research_run_2_paper", "reason": "signal_family_cap"}
                ],
                "warnings": ["underfilled_allocation_due_to_caps"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "strategy_monitoring.json").write_text(
        json.dumps(
            {
                "summary": {
                    "selected_strategy_count": 1,
                    "warning_strategy_count": 1,
                    "deactivation_candidate_count": 1,
                    "aggregate_return": -0.08,
                },
                "strategies": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "current_status": "active",
                        "portfolio_weight": 1.0,
                        "realized_sharpe": -0.6,
                        "drawdown": 0.12,
                        "recommendation": "deactivate",
                        "warning_flags": ["drawdown_breach"],
                    }
                ],
                "kill_switch_recommendations": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "recommendation": "deactivate",
                        "reasons": ["drawdown_breach"],
                        "portfolio_weight": 1.0,
                        "paper_observation_count": 10,
                    }
                ],
                "attribution_summary": {
                    "method": "proxy_weight_scaled",
                    "confidence": "low",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "kill_switch_recommendations.json").write_text(
        json.dumps(
            {
                "summary": {"recommendation_count": 1},
                "recommendations": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "recommendation": "deactivate",
                        "reasons": ["drawdown_breach"],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "adaptive_allocation.json").write_text(
        json.dumps(
            {
                "summary": {
                    "total_selected_strategies": 1,
                    "absolute_weight_change": 0.08,
                    "warning_count": 1,
                    "current_regime_label": "trend",
                },
                "strategies": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "prior_weight": 1.0,
                        "adjusted_weight": 0.92,
                        "current_regime_label": "trend",
                        "regime_compatibility": ["trend", "low_vol"],
                        "monitoring_recommendation": "deactivate",
                        "reason_for_adjustment": ["recommendation_penalty:deactivate"],
                        "capped_by_policy": True,
                    }
                ],
                "top_changes": [
                    {
                        "preset_name": "generated_momentum_nasdaq100_research_run_1_paper",
                        "prior_weight": 1.0,
                        "adjusted_weight": 0.92,
                        "delta_weight": -0.08,
                        "monitoring_recommendation": "deactivate",
                    }
                ],
                "warnings": ["generated_momentum_nasdaq100_research_run_1_paper:stale_monitoring"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "market_regime.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-22T00:00:00+00:00",
                "latest": {
                    "timestamp": "2026-03-22T00:00:00+00:00",
                    "regime_label": "trend",
                    "confidence_score": 0.82,
                    "realized_volatility": 0.18,
                    "long_return": 0.07,
                },
                "history": [
                    {
                        "timestamp": "2026-03-22T00:00:00+00:00",
                        "regime_label": "trend",
                        "confidence_score": 0.82,
                        "realized_volatility": 0.18,
                        "long_return": 0.07,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    orchestration_dir = root / "orchestration_runs" / "automation" / "2026-03-22T00-00-00+00-00"
    orchestration_dir.mkdir(parents=True, exist_ok=True)
    (orchestration_dir / "orchestration_run.json").write_text(
        json.dumps(
            {
                "run_id": "2026-03-22T00-00-00+00-00",
                "run_name": "automation",
                "experiment_name": "adaptive_vs_static",
                "feature_flags": {"regime": True, "adaptive": True},
                "schedule_frequency": "daily",
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:05:00+00:00",
                "status": "succeeded",
                "stage_records": [
                    {"stage_name": "research", "status": "succeeded"},
                    {"stage_name": "promotion", "status": "succeeded"},
                ],
                "outputs": {
                    "selected_strategy_count": 1,
                    "warning_strategy_count": 1,
                    "kill_switch_recommendation_count": 1,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "system_evaluation.json").write_text(
        json.dumps(
            {
                "row": {
                    "run_id": "2026-03-22T00-00-00+00-00",
                    "experiment_name": "adaptive_vs_static",
                    "status": "succeeded",
                    "total_return": 0.03,
                    "sharpe": 1.1,
                    "max_drawdown": 0.01,
                    "regime": "trend",
                },
                "metrics": {
                    "total_return": 0.03,
                    "volatility": 0.12,
                    "sharpe": 1.1,
                    "max_drawdown": 0.01,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (root / "system_evaluation_history.json").write_text(
        json.dumps(
            {
                "summary": {
                    "run_count": 2,
                    "best_run_id": "2026-03-22T00-00-00+00-00",
                    "worst_run_id": "2026-03-21T00-00-00+00-00",
                },
                "rows": [
                    {
                        "run_id": "2026-03-22T00-00-00+00-00",
                        "experiment_name": "adaptive_vs_static",
                        "total_return": 0.03,
                        "sharpe": 1.1,
                        "max_drawdown": 0.01,
                        "warning_count": 1,
                        "kill_switch_count": 1,
                        "regime": "trend",
                    },
                    {
                        "run_id": "2026-03-21T00-00-00+00-00",
                        "experiment_name": "baseline",
                        "total_return": -0.01,
                        "sharpe": -0.2,
                        "max_drawdown": 0.03,
                        "warning_count": 2,
                        "kill_switch_count": 0,
                        "regime": "low_vol",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _call_app(app, path: str) -> tuple[str, dict[str, str], dict]:
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "wsgi.input": BytesIO(b""),
    }
    body = b"".join(app(environ, start_response))
    return str(captured["status"]), captured["headers"], json.loads(body.decode("utf-8"))


def test_dashboard_data_loading_with_sample_artifacts(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    overview = service.overview_payload()
    strategies = service.strategies_payload()
    execution = service.execution_payload()

    assert overview["latest_run"]["run_name"] == "daily_governance"
    assert overview["registry"]["approved_strategy_count"] == 1
    assert overview["research"]["eligible_candidate_count"] == 1
    assert overview["research"]["promoted_strategy_count"] == 1
    assert overview["research"]["validated_pass_count"] == 1
    assert overview["research"]["strategy_portfolio_selected_count"] == 1
    assert overview["strategy_monitoring"]["warning_strategy_count"] == 1
    assert overview["strategy_lifecycle"]["demoted_count"] == 1
    assert overview["adaptive_allocation"]["absolute_weight_change"] == 0.08
    assert overview["market_regime"]["regime_label"] == "trend"
    assert overview["system_evaluation"]["total_return"] == 0.03
    assert overview["orchestration"]["status"] == "succeeded"
    assert strategies["summary"]["status_counts"]["approved"] == 1
    assert execution["summary"]["executable_order_count"] == 1


def test_dashboard_missing_artifacts_handled_gracefully(tmp_path: Path) -> None:
    service = DashboardDataService(tmp_path)

    overview = service.overview_payload()
    runs = service.runs_payload()
    live = service.live_payload()

    assert overview["latest_run"]["run_dir"] is None
    assert runs["runs"] == []
    assert live["risk_checks"] == []


def test_dashboard_latest_run_discovery(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    older = tmp_path / "orchestration" / "daily_governance" / "2026-03-20T00-00-00+00-00"
    older.mkdir(parents=True, exist_ok=True)
    older_summary = older / "run_summary.json"
    older_summary.write_text(json.dumps({"run_name": "older"}), encoding="utf-8")
    os.utime(older_summary, (1, 1))

    service = DashboardDataService(tmp_path)

    assert service.find_latest_run_dir() is not None
    assert service.latest_run_payload()["summary"]["run_name"] == "daily_governance"


def test_dashboard_registry_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.strategies_payload()

    assert len(payload["strategies"]) == 2
    assert payload["filters"]["families"] == ["momentum", "value"]
    assert payload["champion_challenger"][0]["champion_strategy_id"] == "strat-a"


def test_dashboard_execution_summary_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.execution_payload()

    assert payload["summary"]["requested_order_count"] == 2
    assert payload["rejected_orders"][0]["rejection_reason"] == "min_trade_notional"


def test_dashboard_research_summary_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.research_latest_payload()

    assert payload["summary"]["run_count"] == 2
    assert payload["leaderboard"][0]["run_id"] == "research-run-1"
    assert payload["promotion_candidates"][0]["eligible"] is True
    assert payload["promoted_strategies"][0]["preset_name"].startswith("generated_")
    assert payload["strategy_validation"]["summary"]["pass_count"] == 1
    assert payload["strategy_lifecycle"]["summary"]["demoted_count"] == 1
    assert payload["strategy_portfolio"]["selected_strategies"][0]["preset_name"].startswith("generated_")


def test_dashboard_strategy_monitoring_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.strategy_monitoring_payload()

    assert payload["summary"]["warning_strategy_count"] == 1
    assert payload["strategies"][0]["recommendation"] == "deactivate"
    assert payload["recommendations"][0]["recommendation"] == "deactivate"


def test_dashboard_adaptive_allocation_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.adaptive_allocation_payload()

    assert payload["summary"]["absolute_weight_change"] == 0.08
    assert payload["strategies"][0]["adjusted_weight"] == 0.92
    assert payload["top_changes"][0]["delta_weight"] == -0.08


def test_dashboard_market_regime_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.market_regime_payload()

    assert payload["summary"]["regime_label"] == "trend"
    assert payload["history"][0]["regime_label"] == "trend"


def test_dashboard_orchestration_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    payload = service.latest_automated_orchestration_payload()
    runs = service.runs_payload()

    assert payload["summary"]["run_name"] == "automation"
    assert runs["orchestration_runs"][0]["status"] == "succeeded"
    assert runs["orchestration_runs"][0]["experiment_name"] == "adaptive_vs_static"


def test_dashboard_system_evaluation_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    latest = service.system_evaluation_payload()
    history = service.system_evaluation_history_payload()

    assert latest["row"]["total_return"] == 0.03
    assert history["summary"]["best_run_id"] == "2026-03-22T00-00-00+00-00"


def test_dashboard_api_response_shapes(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    app = create_dashboard_app(tmp_path)

    status, headers, overview = _call_app(app, "/api/overview")
    assert status.startswith("200")
    assert headers["Content-Type"].startswith("application/json")
    assert {"generated_at", "latest_run", "monitoring", "registry", "research", "strategy_monitoring", "strategy_lifecycle", "adaptive_allocation", "market_regime", "orchestration", "system_evaluation", "portfolio", "execution", "broker_health", "quick_links"} <= set(overview)

    _status, _headers, strategies = _call_app(app, "/api/strategies")
    assert {"generated_at", "summary", "filters", "strategies", "champion_challenger"} <= set(strategies)

    _status, _headers, research = _call_app(app, "/api/research/latest")
    assert {"generated_at", "summary", "recent_runs", "leaderboard", "promotion_candidates", "promoted_strategies"} <= set(research)
    assert "strategy_portfolio" in research
    assert "strategy_validation" in research
    assert "strategy_lifecycle" in research

    _status, _headers, validation = _call_app(app, "/api/strategy-validation/latest")
    assert {"generated_at", "summary", "rows", "policy"} <= set(validation)

    _status, _headers, lifecycle = _call_app(app, "/api/strategy-lifecycle/latest")
    assert {"generated_at", "summary", "strategies", "governance_summary"} <= set(lifecycle)

    _status, _headers, strategy_monitor = _call_app(app, "/api/strategy-monitor/latest")
    assert {"generated_at", "summary", "strategies", "recommendations", "attribution_summary"} <= set(strategy_monitor)

    _status, _headers, adaptive = _call_app(app, "/api/adaptive-allocation/latest")
    assert {"generated_at", "summary", "strategies", "top_changes", "warnings"} <= set(adaptive)

    _status, _headers, regime = _call_app(app, "/api/regime/latest")
    assert {"generated_at", "summary", "history", "policy"} <= set(regime)

    _status, _headers, orchestration = _call_app(app, "/api/orchestration/latest")
    assert {"run_dir", "summary", "stage_records"} <= set(orchestration)

    _status, _headers, system_eval = _call_app(app, "/api/system-eval/latest")
    assert {"generated_at", "row", "metrics"} <= set(system_eval)

    _status, _headers, system_eval_history = _call_app(app, "/api/system-eval/history")
    assert {"generated_at", "summary", "rows"} <= set(system_eval_history)

    _status, _headers, live = _call_app(app, "/api/live/latest")
    assert {"generated_at", "dry_run_summary", "submission_summary", "risk_checks", "blocked_checks", "duplicate_events", "broker_health"} <= set(live)


def test_dashboard_static_data_build(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)

    paths = build_dashboard_static_data(
        artifacts_root=tmp_path,
        output_dir=tmp_path / "dashboard_data",
    )

    assert paths["overview_json"].exists()
    assert paths["runs_json"].exists()
    assert paths["research_latest_json"].exists()
    assert paths["strategy_validation_latest_json"].exists()
    assert paths["strategy_lifecycle_latest_json"].exists()
    assert paths["strategy_monitoring_latest_json"].exists()
    assert paths["adaptive_allocation_latest_json"].exists()
    assert paths["regime_latest_json"].exists()
    assert paths["orchestration_latest_json"].exists()
    assert paths["system_evaluation_latest_json"].exists()
    assert paths["system_evaluation_history_json"].exists()
