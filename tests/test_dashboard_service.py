from __future__ import annotations

import json
import os
from collections import namedtuple
from io import BytesIO
from pathlib import Path

import pandas as pd

from trading_platform.dashboard.chart_service import build_trade_records_from_fills
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
        [
            {
                "symbol": "AAPL",
                "side": "BUY",
                "requested_shares": 100,
                "adjusted_shares": 50,
                "estimated_fill_price": 100.1,
                "commission": 1.0,
                "clipping_reason": "adv_cap",
            }
        ]
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
        [
            {
                "symbol": "AAPL",
                "status": "skipped",
                "message": "materially identical open order already exists",
                "client_order_id": "cid-1",
            }
        ]
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
    pd.DataFrame(
        [{"family": "momentum", "champion_strategy_id": "strat-a", "challenger_strategy_id": "strat-b"}]
    ).to_csv(
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
                    "variant_names": ["adaptive_on", "adaptive_off"],
                },
                "rows": [
                    {
                        "run_id": "2026-03-22T00-00-00+00-00",
                        "experiment_name": "adaptive_vs_static",
                        "variant_name": "adaptive_on",
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
                        "variant_name": "adaptive_off",
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
    experiment_dir = root / "experiments" / "adaptive_vs_static" / "2026-03-22T00-00-00+00-00"
    experiment_dir.mkdir(parents=True, exist_ok=True)
    (experiment_dir / "experiment_run.json").write_text(
        json.dumps(
            {
                "experiment_name": "adaptive_vs_static",
                "experiment_run_id": "2026-03-22T00-00-00+00-00",
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:05:00+00:00",
                "status": "succeeded",
                "summary": {
                    "variant_count": 2,
                    "variant_run_count": 2,
                    "succeeded_count": 2,
                    "failed_count": 0,
                },
                "variants": [
                    {
                        "variant_name": "adaptive_on",
                        "repeat_index": 1,
                        "status": "succeeded",
                        "run_dir": str(orchestration_dir),
                    },
                    {
                        "variant_name": "adaptive_off",
                        "repeat_index": 1,
                        "status": "succeeded",
                        "run_dir": str(orchestration_dir),
                    },
                ],
                "system_evaluation": {
                    "system_evaluation_history_json_path": str(root / "system_evaluation_history.json"),
                },
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

    path_info = path
    query_string = ""
    if "?" in path:
        path_info, query_string = path.split("?", 1)
    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "wsgi.input": BytesIO(b""),
    }
    body = b"".join(app(environ, start_response))
    return str(captured["status"]), captured["headers"], json.loads(body.decode("utf-8"))


def _call_app_raw(app, path: str) -> tuple[str, dict[str, str], str]:
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    path_info = path
    query_string = ""
    if "?" in path:
        path_info, query_string = path.split("?", 1)
    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "wsgi.input": BytesIO(b""),
    }
    body = b"".join(app(environ, start_response))
    return str(captured["status"]), captured["headers"], body.decode("utf-8")


def _write_chart_artifacts(root: Path, feature_dir: Path) -> None:
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-18T00:00:00+00:00",
                "symbol": "AAPL",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000.0,
                "sma_20": 99.5,
            },
            {
                "timestamp": "2026-03-19T00:00:00+00:00",
                "symbol": "AAPL",
                "open": 101.0,
                "high": 104.0,
                "low": 100.0,
                "close": 103.0,
                "volume": 1100.0,
                "sma_20": 100.2,
            },
            {
                "timestamp": "2026-03-20T00:00:00+00:00",
                "symbol": "AAPL",
                "open": 103.0,
                "high": 105.0,
                "low": 102.0,
                "close": 104.0,
                "volume": 1200.0,
                "sma_20": 101.1,
            },
            {
                "timestamp": "2026-03-21T00:00:00+00:00",
                "symbol": "AAPL",
                "open": 104.0,
                "high": 106.0,
                "low": 103.0,
                "close": 105.0,
                "volume": 1250.0,
                "sma_20": 102.4,
            },
        ]
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    alternate_research_dir = root / "research_alt" / "alternate_run"
    alternate_research_dir.mkdir(parents=True, exist_ok=True)
    (alternate_research_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "run_id": "manifest-alt-run",
                "source": "research_alt_manifest",
                "mode": "paper",
                "strategy_id": "breakout-alt",
                "timeframe": "1d",
                "lookback": 63,
                "artifact_group": "research",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"timestamp": "2026-03-18T00:00:00+00:00", "close": 101.0, "score": 0.0, "position": 0.0},
            {"timestamp": "2026-03-19T00:00:00+00:00", "close": 103.0, "score": 0.2, "position": -1.0},
            {"timestamp": "2026-03-20T00:00:00+00:00", "close": 104.0, "score": 0.3, "position": -1.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "close": 105.0, "score": 0.0, "position": 0.0},
        ]
    ).to_csv(alternate_research_dir / "AAPL_breakout_hold_signals.csv", index=False)
    research_dir = root / "research" / "sample_run"
    research_dir.mkdir(parents=True, exist_ok=True)
    (research_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "run_id": "manifest-research-run",
                "source": "research_manifest",
                "mode": "paper",
                "strategy_id": "momentum-core",
                "timeframe": "1d",
                "lookback": 84,
                "artifact_group": "research",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"timestamp": "2026-03-18T00:00:00+00:00", "close": 101.0, "score": 0.0, "position": 0.0},
            {"timestamp": "2026-03-19T00:00:00+00:00", "close": 103.0, "score": 0.9, "position": 1.0},
            {"timestamp": "2026-03-20T00:00:00+00:00", "close": 104.0, "score": 0.7, "position": 1.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "close": 105.0, "score": 0.1, "position": 0.0},
        ]
    ).to_csv(research_dir / "AAPL_sma_cross_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-19T00:00:00+00:00",
                "symbol": "AAPL",
                "trade_id": "ledger-1",
                "strategy_id": "momentum-core",
                "signal_type": "entry_long_signal",
                "signal_value": 1.0,
                "ranking_score": 0.91,
                "universe_rank": 4,
                "selection_included": True,
                "target_weight": 0.12,
                "sizing_rationale": "top-ranked sleeve allocation",
                "constraint_hits": "max_symbol_weight",
                "order_intent_summary": "Open long rebalance order",
            },
            {
                "timestamp": "2026-03-21T00:00:00+00:00",
                "symbol": "AAPL",
                "trade_id": "ledger-1",
                "strategy_id": "momentum-core",
                "signal_type": "exit_long_signal",
                "signal_value": 0.0,
                "ranking_score": 0.10,
                "universe_rank": 32,
                "selection_included": False,
                "exclusion_reason": "signal decayed below threshold",
                "target_weight": 0.0,
                "order_intent_summary": "Close long rebalance order",
            },
        ]
    ).to_csv(research_dir / "decision_provenance.csv", index=False)

    paper_dir = root / "paper_trading"
    paper_dir.mkdir(parents=True, exist_ok=True)
    (paper_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "run_id": "manifest-paper-run",
                "source": "paper_manifest",
                "mode": "paper",
                "strategy_id": "momentum-core",
                "timeframe": "1d",
                "lookback": 84,
                "artifact_group": "paper",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "as_of": "2026-03-19T00:00:00+00:00",
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 10,
                "fill_price": 103.0,
                "order_id": "ord-1",
                "status": "filled",
            },
            {
                "as_of": "2026-03-21T00:00:00+00:00",
                "symbol": "AAPL",
                "side": "SELL",
                "quantity": 10,
                "fill_price": 105.0,
                "order_id": "ord-2",
                "status": "filled",
            },
        ]
    ).to_csv(paper_dir / "paper_fills.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 10,
                "estimated_fill_price": 103.0,
                "client_order_id": "ord-1",
                "reason": "rebalance",
            },
            {
                "symbol": "AAPL",
                "side": "SELL",
                "quantity": 10,
                "estimated_fill_price": 105.0,
                "client_order_id": "ord-2",
                "reason": "rebalance",
            },
        ]
    ).to_csv(paper_dir / "paper_orders.csv", index=False)
    pd.DataFrame(
        [
            {"symbol": "AAPL", "quantity": 0, "avg_price": 103.0, "market_value": 0.0},
        ]
    ).to_csv(paper_dir / "paper_positions.csv", index=False)
    (paper_dir / "paper_summary.json").write_text(
        json.dumps(
            {
                "preset_name": "momentum-core",
                "strategy": "xsec_momentum_topn",
                "equity": 100250.0,
                "cash": 5010.0,
                "gross_market_value": 95240.0,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "trade_id": "ledger-1",
                "side": "long",
                "qty": 10,
                "entry_ts": "2026-03-19T00:00:00+00:00",
                "entry_price": 103.0,
                "exit_ts": "2026-03-21T00:00:00+00:00",
                "exit_price": 105.0,
                "realized_pnl": 20.0,
                "status": "closed",
            },
            {
                "symbol": "MSFT",
                "trade_id": "ledger-2",
                "side": "long",
                "qty": 5,
                "entry_ts": "2026-03-20T00:00:00+00:00",
                "entry_price": 210.0,
                "exit_ts": None,
                "exit_price": None,
                "realized_pnl": 0.0,
                "status": "open",
            },
        ]
    ).to_csv(paper_dir / "paper_trades.csv", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-19T00:00:00+00:00",
                "symbol": "AAPL",
                "trade_id": "ledger-1",
                "strategy_id": "momentum-core",
                "selection_status": "included",
                "target_weight": 0.12,
                "order_intent_summary": "Submit opening buy order",
                "constraint_hits": '["max_symbol_weight"]',
            }
        ]
    ).to_csv(paper_dir / "order_intents.csv", index=False)


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
    assert overview["experiments"]["experiment_count"] == 1
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
    assert runs["orchestration_runs"][0]["variant_name"] is None
    assert runs["experiments"]["rows"][0]["experiment_name"] == "adaptive_vs_static"


def test_dashboard_system_evaluation_normalization(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    service = DashboardDataService(tmp_path)

    latest = service.system_evaluation_payload()
    history = service.system_evaluation_history_payload()
    experiments = service.experiments_payload()

    assert latest["row"]["total_return"] == 0.03
    assert history["summary"]["best_run_id"] == "2026-03-22T00-00-00+00-00"
    assert history["rows"][0]["variant_name"] == "adaptive_on"
    assert experiments["summary"]["experiment_count"] == 1


def test_trade_reconstruction_from_fills_handles_closed_trade() -> None:
    trades = build_trade_records_from_fills(
        [
            {"ts": "2026-03-19T00:00:00+00:00", "side": "buy", "qty": 10, "price": 103.0},
            {"ts": "2026-03-21T00:00:00+00:00", "side": "sell", "qty": 10, "price": 105.0},
        ]
    )

    assert len(trades) == 1
    assert trades[0]["status"] == "closed"
    assert trades[0]["realized_pnl"] == 20.0


def test_dashboard_chart_payload_from_artifacts(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.chart_payload("AAPL", lookback=3)

    assert payload["symbol"] == "AAPL"
    assert len(payload["bars"]) == 3
    assert payload["signals"][0]["type"] == "entry_long_signal"
    assert payload["signals"][-1]["type"] == "exit_long_signal"
    assert payload["fills"][0]["side"] == "buy"
    assert payload["orders"][0]["ts"] is None
    assert payload["trades"][0]["status"] == "closed"
    assert payload["trades"][0]["trade_id"] == "ledger-1"
    assert payload["position"]["qty"] == 0
    assert payload["provenance"][0]["order_intent_summary"] is not None
    assert payload["provenance"][0]["ranking_score"] is not None
    assert payload["meta"]["has_indicators"] is True
    assert payload["meta"]["has_ohlc"] is True
    assert payload["meta"]["trade_source_mode"] == "explicit_ledger"
    assert payload["meta"]["selected_source"] is None


def test_dashboard_chart_payload_missing_artifacts_returns_empty(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.chart_payload("AAPL")

    assert payload["bars"] == []
    assert payload["signals"] == []
    assert payload["fills"] == []
    assert payload["orders"] == []
    assert payload["trades"] == []
    assert payload["position"]["qty"] == 0


def test_dashboard_trade_payload_falls_back_to_fill_reconstruction_without_ledger(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    (tmp_path / "paper_trading" / "paper_trades.csv").unlink()
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.trades_payload("AAPL")

    assert payload["trades"][0]["trade_id"] == "T1"
    assert payload["meta"]["trade_source_mode"] == "reconstructed_from_fills"


def test_dashboard_source_selector_prefers_matching_run_and_source(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.chart_payload("AAPL", source="research_alt_manifest", run_id="manifest-alt-run")

    assert payload["signals"][0]["type"] == "entry_short_signal"
    assert payload["meta"]["selected_source"] == "research_alt_manifest"
    assert payload["meta"]["selected_run_id"] == "manifest-alt-run"
    assert len(payload["meta"]["available_chart_sources"]) >= 2


def test_dashboard_manifest_preferred_source_inference(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.chart_payload("AAPL")

    signal_sources = payload["meta"]["available_signal_sources"]
    assert any(source["run_id"] == "manifest-research-run" for source in signal_sources)
    assert any(source["source"] == "research_manifest" for source in signal_sources)


def test_portfolio_overview_payload_generation(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    _write_chart_artifacts(tmp_path, tmp_path / "features")
    service = DashboardDataService(tmp_path)

    payload = service.portfolio_overview_payload()

    assert payload["summary"]["equity"] is not None
    assert len(payload["equity_curve"]) == 3
    assert len(payload["drawdown_curve"]) == 3
    assert payload["recent_activity"] != []
    assert "best_trades" in payload
    assert set(payload["best_trades"][0]) == {
        "trade_id",
        "symbol",
        "side",
        "realized_pnl",
        "entry_ts",
        "exit_ts",
        "strategy_id",
    }
    assert set(payload["worst_trades"][0]) == {
        "trade_id",
        "symbol",
        "side",
        "realized_pnl",
        "entry_ts",
        "exit_ts",
        "strategy_id",
    }
    assert "pnl_by_symbol" in payload


def test_strategy_detail_payload_generation(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.strategy_detail_payload("momentum-core")

    assert payload["summary"]["closed_trade_count"] == 1
    assert payload["summary"]["open_trade_count"] == 1
    assert payload["summary"]["win_rate"] == 1.0
    assert "AAPL" in payload["summary"]["recent_symbols"]
    assert payload["pnl_by_symbol"][0]["symbol"] == "AAPL"
    assert payload["comparisons"][0]["source"] is not None


def test_discovery_payload_generation(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.discovery_payload()

    assert payload["summary"]["recent_trade_count"] >= 1
    assert any(row["symbol"] == "AAPL" for row in payload["recent_symbols"])
    assert (
        payload["recent_trades"][0]["trade_id"] == "ledger-2" or payload["recent_trades"][0]["trade_id"] == "ledger-1"
    )
    assert any(row["strategy_id"] == "momentum-core" for row in payload["recent_strategies"])
    assert payload["recent_run_contexts"] != []


def test_trade_detail_payload_generation(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.trade_detail_payload("ledger-1")

    assert payload["trade"]["trade_id"] == "ledger-1"
    assert payload["trade"]["symbol"] == "AAPL"
    assert payload["meta"]["strategy_id"] == "momentum-core"
    assert payload["explain"]["signal"] is not None
    assert payload["provenance"]["latest"]["order_intent_summary"] is not None
    assert payload["lifecycle"][0]["kind"] in {"signal", "decision", "order", "fill", "trade_open", "trade_close"}
    assert "related_trades" in payload["comparison"]
    assert payload["chart"]["bars"] != []


def test_execution_diagnostics_payload_generation(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    service = DashboardDataService(tmp_path, feature_dir=feature_dir)

    payload = service.execution_diagnostics_payload()

    assert payload["summary"]["filled_order_count"] == 2
    assert payload["summary"]["average_signal_to_fill_latency_seconds"] is not None
    assert payload["summary"]["average_slippage_bps"] is not None


def test_dashboard_service_normalizes_malformed_payload_shapes(tmp_path: Path, monkeypatch) -> None:
    _write_chart_artifacts(tmp_path, tmp_path / "features")
    service = DashboardDataService(tmp_path, feature_dir=tmp_path / "features")

    class TradeObject:
        def to_dict(self) -> dict[str, object]:
            return {
                "trade_id": "obj-1",
                "symbol": "AAPL",
                "side": "long",
                "realized_pnl": 12.5,
                "entry_ts": "2026-03-19T00:00:00+00:00",
                "exit_ts": "2026-03-21T00:00:00+00:00",
                "strategy_id": "momentum-core",
            }

    monkeypatch.setattr(
        "trading_platform.dashboard.service.build_portfolio_overview_payload",
        lambda **_: {
            "summary": None,
            "equity_curve": [None, {"ts": pd.Timestamp("2026-03-20T00:00:00+00:00"), "equity": 100000.0}],
            "drawdown_curve": "bad",
            "positions": [TradeObject(), None],
            "exposure": None,
            "recent_activity": [TradeObject(), "bad-row"],
            "pnl_by_symbol": [TradeObject()],
            "recent_realized_pnl": [namedtuple("PnlRow", ["period", "realized_pnl"])("2026-03-20", 10.0)],
            "best_trades": [TradeObject(), None],
            "worst_trades": ["bad-row"],
            "meta": None,
        },
    )
    monkeypatch.setattr(
        "trading_platform.dashboard.service.build_trade_detail_payload",
        lambda **_: {
            "trade": TradeObject(),
            "chart": {
                "bars": [
                    namedtuple("BarRow", ["ts", "open", "high", "low", "close", "volume"])(
                        "2026-03-19T00:00:00+00:00", 100.0, 101.0, 99.0, 100.5, 1000.0
                    )
                ],
                "signals": ["bad"],
                "fills": None,
                "orders": None,
                "trades": [TradeObject()],
                "provenance": [TradeObject()],
                "meta": None,
            },
            "signals": [TradeObject()],
            "fills": None,
            "orders": ["bad"],
            "provenance": {"latest": TradeObject(), "rows": ["bad"]},
            "lifecycle": [
                namedtuple("LifeRow", ["ts", "kind", "label", "detail", "status"])(
                    "2026-03-19T00:00:00+00:00", "signal", "entry", "detail", "ok"
                )
            ],
            "comparison": {
                "related_trades": [TradeObject()],
                "available_chart_sources": None,
                "available_provenance_sources": ["bad"],
            },
            "explain": None,
            "meta": None,
        },
    )
    monkeypatch.setattr(
        "trading_platform.dashboard.service.build_discovery_payload",
        lambda **_: {
            "summary": None,
            "recent_symbols": [TradeObject(), None],
            "recent_trades": [TradeObject()],
            "recent_strategies": ["bad"],
            "recent_run_contexts": [
                namedtuple(
                    "RunRow",
                    ["source", "run_id", "mode", "trade_count", "strategy_count", "symbol_count", "latest_entry_ts"],
                )("paper", "run-1", "paper", 1, 1, 1, "2026-03-19T00:00:00+00:00")
            ],
        },
    )
    monkeypatch.setattr(
        "trading_platform.dashboard.service.build_execution_diagnostics_payload",
        lambda **_: {"summary": None, "rows": ["bad"], "meta": None},
    )

    portfolio = service.portfolio_overview_payload()
    trade_detail = service.trade_detail_payload("obj-1")
    discovery = service.discovery_payload()
    execution = service.execution_diagnostics_payload()

    assert isinstance(portfolio["summary"], dict)
    assert portfolio["best_trades"][0]["trade_id"] == "obj-1"
    assert portfolio["worst_trades"][0]["trade_id"] is None
    assert portfolio["equity_curve"][1]["ts"] == "2026-03-20T00:00:00+00:00"
    assert trade_detail["trade"]["trade_id"] == "obj-1"
    assert trade_detail["chart"]["signals"][0]["ts"] is None
    assert trade_detail["provenance"]["rows"][0]["trade_id"] is None
    assert discovery["recent_symbols"][0]["symbol"] == "AAPL"
    assert discovery["recent_strategies"][0]["strategy_id"] is None
    assert isinstance(execution["summary"], dict)
    assert execution["rows"][0]["symbol"] is None


def test_dashboard_api_response_shapes(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    (tmp_path / "daily_trading_summary.json").write_text(
        json.dumps(
            {"status": "succeeded", "active_strategy_count": 2, "fill_count": 3, "executable_order_count": 3}, indent=2
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "strategy_id": "generated_base",
                "signal_family": "momentum",
                "is_active": True,
                "normalized_capital_weight": 0.6,
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
            },
            {
                "strategy_id": "generated_alt",
                "signal_family": "value",
                "is_active": False,
                "normalized_capital_weight": 0.4,
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.04,
                "condition_id": None,
            },
        ]
    ).to_csv(tmp_path / "strategy_comparison_summary.csv", index=False)
    pd.DataFrame(
        [{"as_of": "2026-03-22", "strategy_id": "generated_base", "signal_family": "momentum", "rolling_sharpe": 1.1}]
    ).to_csv(tmp_path / "rolling_sharpe_by_strategy.csv", index=False)
    pd.DataFrame([{"as_of": "2026-03-22", "signal_family": "momentum", "rolling_ic": 0.03}]).to_csv(
        tmp_path / "rolling_ic_by_signal.csv", index=False
    )
    (tmp_path / "strategy_quality_summary.json").write_text(
        json.dumps({"summary": {"strategy_count": 2, "active_strategy_count": 1}}, indent=2),
        encoding="utf-8",
    )
    app = create_dashboard_app(tmp_path)

    status, headers, overview = _call_app(app, "/api/overview")
    assert status.startswith("200")
    assert headers["Content-Type"].startswith("application/json")
    assert {
        "generated_at",
        "latest_run",
        "monitoring",
        "registry",
        "research",
        "strategy_monitoring",
        "strategy_lifecycle",
        "adaptive_allocation",
        "market_regime",
        "orchestration",
        "daily_trading",
        "strategy_quality",
        "experiments",
        "system_evaluation",
        "portfolio",
        "execution",
        "broker_health",
        "quick_links",
    } <= set(overview)

    _status, _headers, strategies = _call_app(app, "/api/strategies")
    assert {"generated_at", "summary", "filters", "strategies", "champion_challenger"} <= set(strategies)
    assert "strategy_comparison" in strategies
    assert "rolling_sharpe" in strategies
    assert "rolling_ic" in strategies
    assert strategies["strategy_comparison"][1]["condition_id"] is None

    _status, _headers, research = _call_app(app, "/api/research/latest")
    assert {
        "generated_at",
        "summary",
        "recent_runs",
        "leaderboard",
        "promotion_candidates",
        "promoted_strategies",
    } <= set(research)
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

    _status, _headers, experiments = _call_app(app, "/api/experiments/latest")
    assert {"generated_at", "summary", "latest", "rows"} <= set(experiments)

    _status, _headers, daily_trading = _call_app(app, "/api/daily-trading/latest")
    assert daily_trading["summary"]["status"] == "succeeded"

    _status, _headers, strategy_quality = _call_app(app, "/api/strategy-quality/latest")
    assert strategy_quality["summary"]["strategy_count"] == 2

    _status, _headers, discovery = _call_app(app, "/api/discovery/overview")
    assert {
        "generated_at",
        "summary",
        "recent_symbols",
        "recent_trades",
        "recent_strategies",
        "recent_run_contexts",
    } <= set(discovery)

    _status, _headers, recent_trades = _call_app(app, "/api/discovery/recent-trades")
    assert {"generated_at", "recent_trades", "summary"} <= set(recent_trades)

    _status, _headers, recent_symbols = _call_app(app, "/api/discovery/recent-symbols")
    assert {"generated_at", "recent_symbols", "summary"} <= set(recent_symbols)

    _status, _headers, live = _call_app(app, "/api/live/latest")
    assert {
        "generated_at",
        "dry_run_summary",
        "submission_summary",
        "risk_checks",
        "blocked_checks",
        "duplicate_events",
        "broker_health",
    } <= set(live)

    _status, _headers, portfolio_overview = _call_app(app, "/api/portfolio/overview")
    assert {
        "generated_at",
        "summary",
        "equity_curve",
        "drawdown_curve",
        "positions",
        "exposure",
        "recent_activity",
        "meta",
    } <= set(portfolio_overview)

    _status, _headers, portfolio_equity = _call_app(app, "/api/portfolio/equity")
    assert {"equity_curve", "drawdown_curve", "meta"} <= set(portfolio_equity)

    _status, _headers, portfolio_activity = _call_app(app, "/api/portfolio/activity")
    assert {"recent_activity", "meta"} <= set(portfolio_activity)

    _status, _headers, execution_diagnostics = _call_app(app, "/api/execution/diagnostics")
    assert {"generated_at", "summary", "rows", "meta"} <= set(execution_diagnostics)

    _status, _headers, trade_detail = _call_app(app, "/api/trade/ledger-1")
    assert {
        "generated_at",
        "trade",
        "chart",
        "signals",
        "fills",
        "orders",
        "explain",
        "provenance",
        "lifecycle",
        "comparison",
        "meta",
    } <= set(trade_detail)


def test_dashboard_chart_api_and_symbol_page(tmp_path: Path, monkeypatch) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    monkeypatch.setattr("trading_platform.dashboard.service.FEATURES_DIR", feature_dir)
    app = create_dashboard_app(tmp_path)

    status, headers, payload = _call_app(app, "/api/chart/AAPL?lookback=2")
    assert status.startswith("200")
    assert headers["Content-Type"].startswith("application/json")
    assert payload["symbol"] == "AAPL"
    assert len(payload["bars"]) == 2
    assert {"bars", "signals", "fills", "trades", "position", "meta"} <= set(payload)
    assert payload["meta"]["chart_style_default"] == "candlestick"

    _status, _headers, trades = _call_app(app, "/api/trades/AAPL")
    assert {"symbol", "trades", "fills", "meta"} <= set(trades)
    assert trades["meta"]["trade_source_mode"] == "explicit_ledger"

    _status, _headers, signals = _call_app(app, "/api/signals/AAPL?lookback=2")
    assert {"symbol", "signals", "meta"} <= set(signals)

    html_status, html_headers, html_body = _call_app_raw(app, "/symbols/AAPL?lookback=2")
    assert html_status.startswith("200")
    assert html_headers["Content-Type"].startswith("text/html")
    assert "Symbol Detail: AAPL" in html_body
    assert "/api/chart/AAPL" in html_body
    assert "trade-table" in html_body
    assert "hasOhlc" in html_body
    assert "toggle-signals" in html_body
    assert "chart-readout" in html_body
    assert "50 bars" in html_body

    selected_status, _selected_headers, selected_payload = _call_app(
        app,
        "/api/chart/AAPL?lookback=2&source=research_alt_manifest&run_id=manifest-alt-run",
    )
    assert selected_status.startswith("200")
    assert selected_payload["signals"][0]["type"] == "entry_short_signal"
    assert selected_payload["meta"]["selected_source"] == "research_alt_manifest"

    selected_html_status, _selected_html_headers, selected_html = _call_app_raw(
        app,
        "/symbols/AAPL?lookback=2&source=research_alt_manifest&run_id=manifest-alt-run",
    )
    assert selected_html_status.startswith("200")
    assert "manifest-alt-run" in selected_html


def test_dashboard_portfolio_and_strategy_pages(tmp_path: Path, monkeypatch) -> None:
    feature_dir = tmp_path / "features"
    _write_chart_artifacts(tmp_path, feature_dir)
    _write_sample_artifacts(tmp_path)
    monkeypatch.setattr("trading_platform.dashboard.service.FEATURES_DIR", feature_dir)
    app = create_dashboard_app(tmp_path)

    portfolio_status, portfolio_headers, portfolio_html = _call_app_raw(app, "/portfolio")
    assert portfolio_status.startswith("200")
    assert portfolio_headers["Content-Type"].startswith("text/html")
    assert "Current Open Positions" in portfolio_html
    assert "Recent Activity" in portfolio_html
    assert "Portfolio Summary" in portfolio_html

    overview_status, overview_headers, overview_html = _call_app_raw(app, "/")
    assert overview_status.startswith("200")
    assert overview_headers["Content-Type"].startswith("text/html")
    assert "Recent Symbols" in overview_html
    assert "Recent Trades" in overview_html
    assert "/symbols/AAPL" in overview_html
    assert "/trades/ledger-1" in overview_html
    assert "/strategies/momentum-core" in overview_html

    strategy_status, strategy_headers, strategy_html = _call_app_raw(app, "/strategies/momentum-core")
    assert strategy_status.startswith("200")
    assert strategy_headers["Content-Type"].startswith("text/html")
    assert "Strategy Detail: momentum-core" in strategy_html
    assert "/symbols/AAPL" in strategy_html
    assert "/trades/ledger-1" in strategy_html

    execution_status, execution_headers, execution_html = _call_app_raw(app, "/execution")
    assert execution_status.startswith("200")
    assert execution_headers["Content-Type"].startswith("text/html")
    assert "Execution Diagnostics" in execution_html
    assert "Orders Source" in execution_html

    trade_status, trade_headers, trade_html = _call_app_raw(app, "/trades/ledger-1")
    assert trade_status.startswith("200")
    assert trade_headers["Content-Type"].startswith("text/html")
    assert "Trade Detail: ledger-1" in trade_html
    assert "Associated Signals" in trade_html
    assert "Order Lifecycle" in trade_html
    assert "Decision Provenance" in trade_html

    symbol_status, symbol_headers, symbol_html = _call_app_raw(app, "/symbols/AAPL")
    assert symbol_status.startswith("200")
    assert symbol_headers["Content-Type"].startswith("text/html")
    assert "Related Source Comparison" in symbol_html
    assert "Decision Provenance Rows" in symbol_html

    strategy_compare_status, strategy_compare_headers, strategy_compare_html = _call_app_raw(
        app, "/strategies/momentum-core"
    )
    assert strategy_compare_status.startswith("200")
    assert strategy_compare_headers["Content-Type"].startswith("text/html")
    assert "Run / Source Comparison" in strategy_compare_html


def test_portfolio_page_tolerates_malformed_trade_rows(tmp_path: Path, monkeypatch) -> None:
    _write_sample_artifacts(tmp_path)
    _write_chart_artifacts(tmp_path, tmp_path / "features")

    TradeTuple = namedtuple(
        "TradeTuple", ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"]
    )

    class TradeObject:
        def to_dict(self) -> dict[str, object]:
            return {
                "trade_id": "obj-1",
                "symbol": "MSFT",
                "side": "long",
                "realized_pnl": -5.0,
                "entry_ts": "2026-03-18T00:00:00+00:00",
                "exit_ts": "2026-03-19T00:00:00+00:00",
                "strategy_id": "momentum-core",
            }

    original = DashboardDataService.portfolio_overview_payload

    def broken_overview(self: DashboardDataService) -> dict:
        payload = original(self)
        payload["best_trades"] = [
            TradeTuple(
                "tuple-1",
                "AAPL",
                "long",
                10.0,
                "2026-03-19T00:00:00+00:00",
                "2026-03-21T00:00:00+00:00",
                "momentum-core",
            ),
            TradeObject(),
        ]
        payload["worst_trades"] = [None, "unexpected-row-type", 123]
        return payload

    monkeypatch.setattr(DashboardDataService, "portfolio_overview_payload", broken_overview)
    app = create_dashboard_app(tmp_path)

    status, headers, body = _call_app_raw(app, "/portfolio")

    assert status.startswith("200")
    assert headers["Content-Type"].startswith("text/html")
    assert "Portfolio Summary" in body
    assert "Best Recent Trades" in body
    assert "Worst Recent Trades" in body


def test_portfolio_page_renders_normal_best_and_worst_trade_rows(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    _write_chart_artifacts(tmp_path, tmp_path / "features")
    app = create_dashboard_app(tmp_path)

    status, headers, body = _call_app_raw(app, "/portfolio")

    assert status.startswith("200")
    assert headers["Content-Type"].startswith("text/html")
    assert "Best Recent Trades" in body
    assert "Worst Recent Trades" in body
    assert "/trades/ledger-1" in body
    assert "/strategies/momentum-core" in body


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
    assert paths["experiments_latest_json"].exists()
    assert paths["system_evaluation_latest_json"].exists()
    assert paths["system_evaluation_history_json"].exists()


def test_dashboard_cost_payloads(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "strategy_id": "alpha",
                "gross_total_pnl": 100.0,
                "net_total_pnl": 93.7,
                "total_pnl": 93.7,
                "total_execution_cost": 6.3,
                "turnover": 1_000.0,
            }
        ]
    ).to_csv(paper_dir / "strategy_pnl_attribution.csv", index=False)
    (paper_dir / "pnl_attribution_summary.json").write_text(
        json.dumps(
            {
                "total_gross_pnl": 100.0,
                "total_net_pnl": 93.7,
                "total_execution_cost": 6.3,
            }
        ),
        encoding="utf-8",
    )
    (paper_dir / "paper_run_summary_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "gross_total_pnl": 100.0,
                    "net_total_pnl": 93.7,
                    "total_execution_cost": 6.3,
                    "total_slippage_cost": 2.0,
                    "total_commission_cost": 3.0,
                    "total_spread_cost": 1.3,
                    "cost_drag_pct": 0.063,
                }
            }
        ),
        encoding="utf-8",
    )

    service = DashboardDataService(tmp_path)

    execution_costs = service.execution_costs_latest_payload()
    strategy_costs = service.strategy_costs_latest_payload()
    cost_drag = service.cost_drag_latest_payload()

    assert execution_costs["summary"]["gross_total_pnl"] == 100.0
    assert execution_costs["summary"]["net_total_pnl"] == 93.7
    assert execution_costs["summary"]["total_execution_cost"] == 6.3
    assert strategy_costs["rows"][0]["strategy_id"] == "alpha"
    assert strategy_costs["rows"][0]["total_execution_cost"] == 6.3
    assert cost_drag["summary"]["total_execution_cost"] == 6.3
