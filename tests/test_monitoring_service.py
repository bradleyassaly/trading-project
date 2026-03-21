from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.governance.models import StrategyRegistry, StrategyRegistryEntry
from trading_platform.governance.persistence import save_strategy_registry
from trading_platform.monitoring.models import MonitoringConfig
from trading_platform.monitoring.service import (
    build_dashboard_data,
    evaluate_portfolio_health,
    evaluate_run_health,
    evaluate_strategy_health,
    find_latest_pipeline_run_dir,
)
from trading_platform.orchestration.models import OrchestrationStageToggles, PipelineRunConfig
from trading_platform.orchestration.service import run_orchestration_pipeline


def _write_run_dir(
    base_dir: Path,
    *,
    stage_records: list[dict[str, object]] | None = None,
    outputs: dict[str, object] | None = None,
    combined_rows: list[dict[str, object]] | None = None,
    allocation_summary: dict[str, object] | None = None,
) -> Path:
    run_dir = base_dir / "daily_governance" / "2026-03-21T00-00-00+00-00"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_name": "daily_governance",
        "schedule_type": "daily",
        "started_at": "2026-03-21T00:00:00+00:00",
        "ended_at": "2026-03-21T00:05:00+00:00",
        "status": "succeeded",
        "run_dir": str(run_dir),
        "stage_records": stage_records
        or [
            {"stage_name": "portfolio_allocation", "status": "succeeded"},
            {"stage_name": "paper_trading", "status": "succeeded"},
            {"stage_name": "live_dry_run", "status": "succeeded"},
        ],
        "errors": [],
        "outputs": outputs or {"multi_strategy_selected_strategies": ["strat-a", "strat-b"]},
    }
    (run_dir / "run_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (run_dir / "run_summary.md").write_text("# run\n", encoding="utf-8")
    pd.DataFrame(payload["stage_records"]).to_csv(run_dir / "stage_status.csv", index=False)
    (run_dir / "pipeline_config_snapshot.json").write_text(json.dumps({"run_name": "daily_governance"}, indent=2), encoding="utf-8")

    allocation_dir = run_dir / "portfolio_allocation"
    allocation_dir.mkdir(parents=True, exist_ok=True)
    combined = combined_rows if combined_rows is not None else [
        {"symbol": "AAPL", "target_weight": 0.5, "side": "long", "latest_price": 100.0},
        {"symbol": "MSFT", "target_weight": 0.4, "side": "long", "latest_price": 200.0},
    ]
    pd.DataFrame(combined).to_csv(allocation_dir / "combined_target_weights.csv", index=False)
    pd.DataFrame(
        [
            {"sleeve_name": "strat-a", "symbol": row["symbol"], "scaled_target_weight": row["target_weight"]}
            for row in combined
        ]
    ).to_csv(allocation_dir / "sleeve_target_weights.csv", index=False)
    summary = allocation_summary or {
        "summary": {
            "gross_exposure_after_constraints": 0.9,
            "net_exposure_after_constraints": 0.9,
            "turnover_estimate": 0.1,
            "overlap_concentration": 0.0,
            "effective_number_of_sleeves": 2.0,
            "effective_number_of_positions": 2.0,
            "symbols_removed_or_clipped": [],
        }
    }
    (allocation_dir / "allocation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    pd.DataFrame([{"sleeve_name": "strat-a"}]).to_csv(allocation_dir / "sleeve_attribution.csv", index=False)
    pd.DataFrame([{"symbol": "AAPL", "sleeve_count": 1}]).to_csv(allocation_dir / "symbol_overlap_report.csv", index=False)

    paper_dir = run_dir / "paper_trading"
    paper_dir.mkdir(parents=True, exist_ok=True)
    (paper_dir / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"current_equity": 100000.0, "gross_exposure": 0.9}}, indent=2),
        encoding="utf-8",
    )
    live_dir = run_dir / "live_dry_run"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "live_dry_run_summary.json").write_text(
        json.dumps({"adjusted_order_count": 2, "gross_exposure": 0.9}, indent=2),
        encoding="utf-8",
    )
    return run_dir


def _write_strategy_registry(tmp_path: Path, *, drawdown: float = 0.05, sharpe: float = 1.0) -> tuple[Path, Path]:
    research_dir = tmp_path / "research" / "strat_a"
    paper_dir = tmp_path / "paper" / "strat_a"
    live_dir = tmp_path / "live" / "strat_a"
    research_dir.mkdir(parents=True, exist_ok=True)
    paper_dir.mkdir(parents=True, exist_ok=True)
    live_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([{"portfolio_total_return": 0.1, "portfolio_sharpe": sharpe, "portfolio_max_drawdown": drawdown}]).to_csv(
        research_dir / "portfolio_metrics.csv",
        index=False,
    )
    pd.DataFrame([{"folds_tested": 3, "mean_fold_return": 0.1, "mean_fold_sharpe": sharpe, "mean_turnover": 0.1}]).to_csv(
        research_dir / "robustness_report.csv",
        index=False,
    )
    pd.DataFrame([{"mean_spearman_ic": 0.05}]).to_csv(research_dir / "leaderboard.csv", index=False)
    pd.DataFrame([{"score_corr": 0.1}]).to_csv(research_dir / "redundancy_report.csv", index=False)
    pd.DataFrame([{"mean_turnover": 0.1}]).to_csv(research_dir / "implementability_report.csv", index=False)
    (research_dir / "signal_diagnostics.json").write_text(json.dumps({}), encoding="utf-8")

    pd.DataFrame(
        [
            {"timestamp": "2026-03-20", "equity": 100000.0},
            {"timestamp": "2026-03-21", "equity": 101000.0 if drawdown < 0.2 else 80000.0},
        ]
    ).to_csv(paper_dir / "paper_equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"timestamp": "2026-03-20", "target_names": "AAPL,MSFT", "target_selected_count": 2, "turnover_estimate": 0.1},
            {"timestamp": "2026-03-21", "target_names": "AAPL,NVDA", "target_selected_count": 2, "turnover_estimate": 0.1},
        ]
    ).to_csv(paper_dir / "paper_run_summary.csv", index=False)
    pd.DataFrame([{"timestamp": "2026-03-21", "status": "pass"}]).to_csv(paper_dir / "paper_health_checks.csv", index=False)
    pd.DataFrame([{"timestamp": "2026-03-21", "order_index": 0}]).to_csv(paper_dir / "paper_orders_history.csv", index=False)
    (paper_dir / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"turnover_estimate": 0.1, "current_equity": 100000.0}, "health_checks": []}, indent=2),
        encoding="utf-8",
    )
    (live_dir / "live_dry_run_summary.json").write_text(json.dumps({"health_checks": []}, indent=2), encoding="utf-8")

    registry = StrategyRegistry(
        updated_at="2026-03-21T00:00:00+00:00",
        entries=[
            StrategyRegistryEntry(
                strategy_id="strat-a",
                strategy_name="Strategy A",
                family="momentum",
                version="v1",
                preset_name="xsec_nasdaq100_momentum_v1_deploy",
                research_artifact_paths=[str(research_dir)],
                created_at="2026-03-20T00:00:00+00:00",
                status="approved",
                owner="qa",
                source="test",
                current_deployment_stage="approved",
                paper_artifact_path=str(paper_dir),
                live_artifact_path=str(live_dir),
            )
        ],
    )
    registry_path = tmp_path / "registry.json"
    save_strategy_registry(registry, registry_path)
    return registry_path, tmp_path


def test_run_health_healthy_case(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path)
    report, paths = evaluate_run_health(
        run_dir=run_dir,
        config=MonitoringConfig(maximum_symbol_concentration=0.6),
    )

    assert report.status == "healthy"
    assert report.alert_counts["critical"] == 0
    assert paths["run_health_json_path"].exists()


def test_run_health_failed_stage_case(tmp_path: Path) -> None:
    run_dir = _write_run_dir(
        tmp_path,
        stage_records=[{"stage_name": "portfolio_allocation", "status": "failed"}],
    )
    report, _paths = evaluate_run_health(run_dir=run_dir, config=MonitoringConfig())

    assert report.status == "critical"
    assert any(alert.code == "failed_stages" for alert in report.alerts)


def test_run_health_missing_artifact_case(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path)
    (run_dir / "portfolio_allocation" / "combined_target_weights.csv").unlink()

    report, _paths = evaluate_run_health(run_dir=run_dir, config=MonitoringConfig())

    assert any(alert.code == "missing_artifact" for alert in report.alerts)


def test_run_health_empty_portfolio_case(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path, combined_rows=[])
    report, _paths = evaluate_run_health(
        run_dir=run_dir,
        config=MonitoringConfig(minimum_generated_position_count=1),
    )

    assert any(alert.code == "empty_or_small_portfolio" for alert in report.alerts)


def test_portfolio_health_concentration_breach_case(tmp_path: Path) -> None:
    allocation_dir = _write_run_dir(tmp_path).parent / "2026-03-21T00-00-00+00-00" / "portfolio_allocation"
    report, _paths = evaluate_portfolio_health(
        allocation_dir=allocation_dir,
        config=MonitoringConfig(maximum_symbol_concentration=0.3),
        output_dir=tmp_path / "portfolio_health",
    )

    assert report.status == "critical"
    assert any(alert.code == "portfolio_concentration" for alert in report.alerts)


def test_portfolio_health_turnover_breach_case(tmp_path: Path) -> None:
    allocation_dir = _write_run_dir(
        tmp_path,
        allocation_summary={"summary": {"gross_exposure_after_constraints": 0.9, "net_exposure_after_constraints": 0.9, "turnover_estimate": 0.5, "overlap_concentration": 0.0, "effective_number_of_sleeves": 2.0, "effective_number_of_positions": 2.0, "symbols_removed_or_clipped": []}},
    ).parent / "2026-03-21T00-00-00+00-00" / "portfolio_allocation"
    report, _paths = evaluate_portfolio_health(
        allocation_dir=allocation_dir,
        config=MonitoringConfig(maximum_turnover=0.25),
        output_dir=tmp_path / "portfolio_health",
    )

    assert any(alert.code == "portfolio_turnover" for alert in report.alerts)


def test_strategy_health_degradation_case(tmp_path: Path) -> None:
    registry_path, artifacts_root = _write_strategy_registry(tmp_path, drawdown=0.3, sharpe=0.1)
    report, _paths = evaluate_strategy_health(
        registry_path=registry_path,
        artifacts_root=artifacts_root,
        config=MonitoringConfig(maximum_drawdown=0.2, minimum_rolling_sharpe=0.5),
        output_dir=tmp_path / "strategy_health",
    )

    assert report.status in {"warning", "critical"}
    assert any(alert.code in {"strategy_drawdown", "rolling_sharpe"} for alert in report.alerts)


def test_history_file_append_behavior(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path)
    config = MonitoringConfig()

    _report_1, paths_1 = evaluate_run_health(run_dir=run_dir, config=config)
    _report_2, paths_2 = evaluate_run_health(run_dir=run_dir, config=config)

    history_df = pd.read_csv(paths_2["run_history_path"])
    assert paths_1["run_history_path"] == paths_2["run_history_path"]
    assert len(history_df) == 2


def test_latest_run_discovery_and_dashboard(tmp_path: Path) -> None:
    first = _write_run_dir(tmp_path / "a")
    second = _write_run_dir(tmp_path / "b")
    second_summary = second / "run_summary.json"
    second_summary.write_text(second_summary.read_text(encoding="utf-8"), encoding="utf-8")

    latest = find_latest_pipeline_run_dir(tmp_path)
    dashboard_paths = build_dashboard_data(pipeline_root=tmp_path, output_dir=tmp_path / "dashboard")

    assert latest.name == second.name
    assert dashboard_paths["dashboard_runs_json_path"].exists()


def test_monitoring_pipeline_stage_integration(monkeypatch, tmp_path: Path) -> None:
    monitor_config_path = tmp_path / "monitoring.json"
    monitor_config_path.write_text(json.dumps({"maximum_failed_stages": 0}), encoding="utf-8")
    notification_config_path = tmp_path / "notifications.json"
    notification_config_path.write_text(
        json.dumps(
            {
                "smtp_host": "smtp.example.com",
                "smtp_port": 587,
                "from_address": "alerts@example.com",
                "min_severity": "warning",
                "channels": [{"channel_type": "email", "recipients": ["ops@example.com"]}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2026-03-21T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])
    monkeypatch.setattr(
        "trading_platform.orchestration.service.send_notifications",
        lambda **kwargs: {"sent": True, "filtered_alert_count": 0, "channel_results": [], "subject": None, "body": None},
    )

    config = PipelineRunConfig(
        run_name="monitoring_pipeline",
        schedule_type="daily",
        universes=["nasdaq100"],
        output_root_dir=str(tmp_path / "runs"),
        monitoring_config_path=str(monitor_config_path),
        notification_config_path=str(notification_config_path),
        stage_order=["reporting", "monitoring"],
        stages=OrchestrationStageToggles(reporting=True, monitoring=True),
    )

    result, artifact_paths = run_orchestration_pipeline(config)

    assert result.outputs["monitoring_health_status"] == "healthy"
    assert result.outputs["notification_sent"] is True
    assert artifact_paths["run_summary_json_path"].exists()
