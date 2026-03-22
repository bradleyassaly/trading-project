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


def test_dashboard_api_response_shapes(tmp_path: Path) -> None:
    _write_sample_artifacts(tmp_path)
    app = create_dashboard_app(tmp_path)

    status, headers, overview = _call_app(app, "/api/overview")
    assert status.startswith("200")
    assert headers["Content-Type"].startswith("application/json")
    assert {"generated_at", "latest_run", "monitoring", "registry", "portfolio", "execution", "broker_health", "quick_links"} <= set(overview)

    _status, _headers, strategies = _call_app(app, "/api/strategies")
    assert {"generated_at", "summary", "filters", "strategies", "champion_challenger"} <= set(strategies)

    _status, _headers, research = _call_app(app, "/api/research/latest")
    assert {"generated_at", "summary", "recent_runs", "leaderboard", "promotion_candidates"} <= set(research)

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
