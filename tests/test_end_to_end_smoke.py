from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.broker.models import BrokerConfig
from trading_platform.broker.service import resolve_broker_adapter
from trading_platform.config.loader import load_execution_config, load_multi_strategy_portfolio_config
from trading_platform.dashboard.server import build_dashboard_static_data
from trading_platform.execution.service import (
    build_execution_requests_from_target_weights,
    simulate_execution,
    write_execution_artifacts,
)
from trading_platform.governance.models import (
    RegistrySelectionOptions,
    StrategyRegistry,
    StrategyRegistryEntry,
)
from trading_platform.governance.persistence import load_strategy_registry, save_strategy_registry
from trading_platform.governance.service import (
    build_multi_strategy_config_from_registry,
    write_registry_backed_multi_strategy_artifacts,
)
from trading_platform.live.preview import (
    LivePreviewConfig,
    run_live_dry_run_preview_for_targets,
    write_live_dry_run_artifacts,
)
from trading_platform.live.submission import submit_live_orders
from trading_platform.monitoring.models import MonitoringConfig
from trading_platform.monitoring.service import evaluate_run_health
from trading_platform.portfolio.multi_strategy import allocate_multi_strategy_portfolio, write_multi_strategy_artifacts
from trading_platform.services.target_construction_service import TargetConstructionResult


def test_end_to_end_smoke_workflow(tmp_path: Path, monkeypatch) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
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
                created_at="2026-03-21T00:00:00+00:00",
                status="approved",
                owner="qa",
                source="test",
                current_deployment_stage="approved",
                universe="nasdaq100",
            )
        ],
    )
    registry_path = artifacts_root / "strategy_registry.json"
    save_strategy_registry(registry, registry_path)
    loaded_registry = load_strategy_registry(registry_path)
    config, comparison_rows = build_multi_strategy_config_from_registry(
        registry=loaded_registry,
        options=RegistrySelectionOptions(include_statuses=["approved"], weighting_scheme="equal"),
    )
    generated_dir = artifacts_root / "generated"
    generated_paths = write_registry_backed_multi_strategy_artifacts(
        config=config,
        family_rows=comparison_rows,
        output_path=generated_dir / "multi_strategy.json",
    )
    loaded_multi = load_multi_strategy_portfolio_config(generated_paths["multi_strategy_config_path"])

    def fake_build_target_construction_result(config):
        return TargetConstructionResult(
            as_of="2026-03-22",
            scheduled_target_weights={"AAPL": 0.6, "MSFT": 0.4},
            effective_target_weights={"AAPL": 0.6, "MSFT": 0.4},
            latest_prices={"AAPL": 100.0, "MSFT": 200.0},
            latest_scores={"AAPL": 1.0, "MSFT": 0.9},
            target_diagnostics={"target_selected_count": 2, "realized_holdings_count": 2, "average_gross_exposure": 1.0},
            skipped_symbols=[],
            extra_diagnostics={},
        )

    monkeypatch.setattr("trading_platform.portfolio.multi_strategy.build_target_construction_result", fake_build_target_construction_result)

    allocation_result = allocate_multi_strategy_portfolio(loaded_multi)
    allocation_dir = artifacts_root / "portfolio_allocation"
    allocation_paths = write_multi_strategy_artifacts(allocation_result, allocation_dir)

    execution_config_path = tmp_path / "execution.yaml"
    execution_config_path.write_text("commission_model_type: bps\ncommission_bps: 1.0\nslippage_model_type: fixed_bps\nfixed_slippage_bps: 2.0\n", encoding="utf-8")
    execution_config = load_execution_config(execution_config_path)
    execution_requests = build_execution_requests_from_target_weights(
        target_weights=allocation_result.combined_target_weights,
        current_positions={},
        latest_prices=allocation_result.latest_prices,
        portfolio_equity=100_000.0,
        reserve_cash_pct=loaded_multi.cash_reserve_pct,
    )
    execution_result = simulate_execution(
        requests=execution_requests,
        config=execution_config,
        current_cash=100_000.0,
        current_equity=100_000.0,
    )
    execution_dir = artifacts_root / "execution"
    execution_paths = write_execution_artifacts(execution_result, execution_dir)

    preview_config = LivePreviewConfig(
        symbols=sorted(allocation_result.combined_target_weights),
        preset_name="multi_strategy",
        universe_name="smoke",
        strategy="multi_strategy",
        broker="mock",
        output_dir=artifacts_root / "live_dry_run",
    )
    preview_result = run_live_dry_run_preview_for_targets(
        config=preview_config,
        as_of=allocation_result.as_of,
        target_weights=allocation_result.combined_target_weights,
        latest_prices=allocation_result.latest_prices,
        target_diagnostics={"average_gross_exposure": 1.0, "target_selected_count": 2, "realized_holdings_count": 2},
        execution_config=execution_config,
    )
    live_dry_paths = write_live_dry_run_artifacts(preview_result)
    broker_config = BrokerConfig(
        broker_name="mock",
        live_trading_enabled=True,
        require_manual_enable_flag=False,
        skip_submission_if_existing_open_orders=False,
    )
    submit_result = submit_live_orders(
        preview_result=preview_result,
        broker_config=broker_config,
        broker_adapter=resolve_broker_adapter(broker_config),
        validate_only=True,
        output_dir=artifacts_root / "live_submit",
    )

    run_dir = artifacts_root / "orchestration" / "smoke" / "2026-03-22T00-00-00+00-00"
    (run_dir / "portfolio_allocation").mkdir(parents=True, exist_ok=True)
    (run_dir / "live_dry_run").mkdir(parents=True, exist_ok=True)
    (run_dir / "monitoring").mkdir(parents=True, exist_ok=True)
    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_name": "smoke",
                "schedule_type": "ad_hoc",
                "status": "succeeded",
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:05:00+00:00",
                "summary_type": "pipeline_run",
                "timestamp": "2026-03-22T00:05:00+00:00",
                "key_counts": {"stage_count": 2, "failed_stage_count": 0, "error_count": 0},
                "key_metrics": {"total_stage_duration_seconds": 300.0},
                "warnings": [],
                "errors": [],
                "artifact_paths": {},
                "stage_records": [
                    {"stage_name": "portfolio_allocation", "status": "succeeded"},
                    {"stage_name": "live_dry_run", "status": "succeeded"},
                ],
                "errors": [],
                "outputs": {"multi_strategy_selected_strategies": ["strat-a"]},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "run_summary.md").write_text("# smoke\n", encoding="utf-8")
    pd.DataFrame(
        [
            {"stage_name": "portfolio_allocation", "status": "succeeded"},
            {"stage_name": "live_dry_run", "status": "succeeded"},
        ]
    ).to_csv(run_dir / "stage_status.csv", index=False)
    (run_dir / "pipeline_config_snapshot.json").write_text(json.dumps({"run_name": "smoke"}, indent=2), encoding="utf-8")
    for path in Path(allocation_dir).iterdir():
        if path.is_file():
            (run_dir / "portfolio_allocation" / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    for path in (artifacts_root / "live_dry_run").iterdir():
        if path.is_file():
            target = run_dir / "live_dry_run" / path.name
            target.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    (run_dir / "live_submission_summary.json").write_text(
        Path(submit_result.artifacts["live_submission_summary_json_path"]).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    report, monitor_paths = evaluate_run_health(
        run_dir=run_dir,
        config=MonitoringConfig(minimum_generated_position_count=1, minimum_approved_strategy_count=1),
        output_dir=run_dir / "monitoring",
    )
    dashboard_paths = build_dashboard_static_data(
        artifacts_root=artifacts_root,
        output_dir=artifacts_root / "dashboard_data",
    )

    assert report.status in {"healthy", "warning"}
    assert generated_paths["multi_strategy_config_path"].exists()
    assert allocation_paths["allocation_summary_json_path"].exists()
    assert execution_paths["execution_summary_json_path"].exists()
    assert live_dry_paths["summary_json_path"].exists()
    assert submit_result.artifacts["live_submission_summary_json_path"].exists()
    assert monitor_paths["run_health_json_path"].exists()
    assert dashboard_paths["overview_json"].exists()

    for path in [
        run_dir / "run_summary.json",
        execution_paths["execution_summary_json_path"],
        live_dry_paths["summary_json_path"],
        submit_result.artifacts["live_submission_summary_json_path"],
        monitor_paths["run_health_json_path"],
    ]:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        assert "status" in payload
        assert "timestamp" in payload or "evaluated_at" in payload
        if path.name != "run_health.json":
            assert "key_counts" in payload
            assert "key_metrics" in payload
            assert "warnings" in payload
            assert "errors" in payload
            assert "artifact_paths" in payload
