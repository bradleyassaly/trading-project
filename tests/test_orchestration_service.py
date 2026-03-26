from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.config.loader import load_pipeline_run_config
from trading_platform.governance.models import StrategyRegistry, StrategyRegistryEntry
from trading_platform.governance.persistence import save_strategy_registry
from trading_platform.orchestration.models import (
    OrchestrationStageToggles,
    PipelineRunConfig,
)
from trading_platform.orchestration.service import run_orchestration_pipeline
from trading_platform.portfolio.strategy_execution_handoff import StrategyExecutionHandoff


def _pipeline_config(tmp_path: Path, **overrides) -> PipelineRunConfig:
    base = {
        "run_name": "daily_governance",
        "schedule_type": "daily",
        "universes": ["nasdaq100"],
        "output_root_dir": str(tmp_path / "runs"),
        "stage_order": [
            "data_refresh",
            "feature_generation",
            "research",
            "promotion_evaluation",
            "registry_mutation",
            "multi_strategy_config_generation",
            "portfolio_allocation",
            "paper_trading",
            "live_dry_run",
            "reporting",
        ],
        "stages": OrchestrationStageToggles(
            data_refresh=True,
            feature_generation=True,
            research=True,
            promotion_evaluation=True,
            registry_mutation=True,
            multi_strategy_config_generation=True,
            portfolio_allocation=True,
            paper_trading=True,
            live_dry_run=True,
            reporting=True,
        ),
        "registry_path": str(tmp_path / "registry.json"),
        "governance_config_path": str(tmp_path / "governance.json"),
        "multi_strategy_output_path": str(tmp_path / "generated_multi_strategy.json"),
        "paper_state_path": str(tmp_path / "paper_state.json"),
        "continue_on_stage_error": False,
        "fail_fast": False,
        "auto_promote_qualifying_candidates": False,
    }
    base.update(overrides)
    return PipelineRunConfig(**base)


def _write_governance_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "promotion": {
                    "minimum_walk_forward_folds": 1,
                    "minimum_sharpe": 0.1,
                },
                "degradation": {},
            }
        ),
        encoding="utf-8",
    )


def _write_research_dir(base_dir: Path, *, sharpe: float = 1.0, total_return: float = 0.2) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"portfolio_total_return": total_return, "portfolio_sharpe": sharpe, "portfolio_max_drawdown": 0.1}]
    ).to_csv(base_dir / "portfolio_metrics.csv", index=False)
    pd.DataFrame([{"folds_tested": 3, "mean_fold_return": total_return, "mean_fold_sharpe": sharpe, "mean_turnover": 0.1}]).to_csv(
        base_dir / "robustness_report.csv", index=False
    )
    pd.DataFrame([{"mean_spearman_ic": 0.05}]).to_csv(base_dir / "leaderboard.csv", index=False)
    pd.DataFrame([{"score_corr": 0.1}]).to_csv(base_dir / "redundancy_report.csv", index=False)
    pd.DataFrame([{"return_drag": 0.0, "mean_turnover": 0.1}]).to_csv(
        base_dir / "implementability_report.csv", index=False
    )
    (base_dir / "signal_diagnostics.json").write_text(json.dumps({}), encoding="utf-8")
    return base_dir


def _write_registry(tmp_path: Path, *, status: str = "candidate") -> Path:
    research_dir = _write_research_dir(tmp_path / "research")
    registry = StrategyRegistry(
        updated_at="2025-01-01T00:00:00Z",
        entries=[
            StrategyRegistryEntry(
                strategy_id="strat-a",
                strategy_name="Strategy A",
                family="momentum",
                version="v1",
                preset_name="xsec_nasdaq100_momentum_v1_deploy",
                research_artifact_paths=[str(research_dir)],
                created_at="2025-01-01T00:00:00Z",
                status=status,
                owner="qa",
                source="test",
                current_deployment_stage="candidate" if status == "candidate" else status,
                universe="nasdaq100",
            )
        ],
    )
    registry_path = tmp_path / "registry.json"
    save_strategy_registry(registry, registry_path)
    return registry_path


def test_load_pipeline_run_config_from_json(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "run_name": "daily",
                "schedule_type": "daily",
                "universes": ["nasdaq100"],
                "output_root_dir": "artifacts/orchestration",
                "registry_path": "artifacts/registry.json",
                "governance_config_path": "configs/governance.json",
                "multi_strategy_output_path": "artifacts/generated_multi_strategy.json",
                "paper_state_path": "artifacts/paper_state.json",
                "stages": {
                    "data_refresh": True,
                    "feature_generation": False,
                    "research": False,
                    "promotion_evaluation": True,
                    "registry_mutation": False,
                    "multi_strategy_config_generation": True,
                    "portfolio_allocation": False,
                    "paper_trading": False,
                    "live_dry_run": False,
                    "reporting": True,
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_run_config(config_path)

    assert config.run_name == "daily"
    assert config.stages.data_refresh is True
    assert config.stages.feature_generation is False


def test_pipeline_run_happy_path_full_pipeline(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(tmp_path, research_fast=20, research_slow=100)
    _write_governance_config(Path(config.governance_config_path))
    _write_registry(tmp_path, status="approved")
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL", "MSFT"])
    monkeypatch.setattr("trading_platform.orchestration.service.run_ingest", lambda config: tmp_path / f"{config.symbol}_normalized.parquet")
    monkeypatch.setattr("trading_platform.orchestration.service.run_feature_build", lambda config: tmp_path / f"{config.symbol}_features.parquet")
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_universe_research_workflow",
        lambda **kwargs: {"results": {"AAPL": {"experiment_id": "exp-aapl"}}, "errors": {}},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.build_multi_strategy_config_from_registry",
        lambda **kwargs: (
            type(
                "Cfg",
                (),
                {
                    "sleeves": [type("Sleeve", (), {"sleeve_name": "strat-a"})()],
                    "gross_leverage_cap": 1.0,
                    "net_exposure_cap": 1.0,
                    "max_position_weight": 1.0,
                    "max_symbol_concentration": 1.0,
                    "sector_caps": [],
                    "turnover_cap": None,
                    "cash_reserve_pct": 0.0,
                    "group_map_path": None,
                    "rebalance_timestamp": None,
                    "notes": None,
                    "tags": [],
                },
            )(),
            [{"family": "momentum", "strategy_id": "strat-a", "is_champion": True}],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_registry_backed_multi_strategy_artifacts",
        lambda **kwargs: (
            Path(config.multi_strategy_output_path).write_text(
                json.dumps({"sleeves": [], "cash_reserve_pct": 0.0}),
                encoding="utf-8",
            ),
            {"multi_strategy_config_path": Path(config.multi_strategy_output_path)},
        )[1],
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=type("PortfolioCfg", (), {"cash_reserve_pct": 0.0})(),
            summary={"active_strategy_count": 1},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    allocation_result = type(
        "AllocationResult",
        (),
        {
            "as_of": "2025-01-02",
            "combined_target_weights": {"AAPL": 0.5, "MSFT": 0.5},
            "latest_prices": {"AAPL": 100.0, "MSFT": 200.0},
            "sleeve_rows": [{"symbol": "AAPL"}, {"symbol": "MSFT"}],
            "sleeve_bundles": [],
            "summary": {
                "enabled_sleeve_count": 1,
                "gross_exposure_after_constraints": 1.0,
                "turnover_estimate": 0.1,
                "turnover_cap_binding": False,
                "symbols_removed_or_clipped": [],
            },
        },
    )()
    monkeypatch.setattr("trading_platform.orchestration.service.allocate_multi_strategy_portfolio", lambda cfg: allocation_result)
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_paper_trading_cycle_for_targets",
        lambda **kwargs: type(
            "PaperResult",
            (),
            {
                "as_of": "2025-01-02",
                "state": type("State", (), {"cash": 100000.0, "equity": 100000.0, "gross_market_value": 0.0, "positions": {}})(),
                "orders": [],
                "fills": [],
                "latest_prices": kwargs["latest_prices"],
                "latest_scores": {},
                "latest_target_weights": kwargs["latest_effective_weights"],
                "scheduled_target_weights": kwargs["latest_scheduled_weights"],
                "skipped_symbols": [],
                "diagnostics": {},
            },
        )(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_paper_trading_artifacts",
        lambda **kwargs: {"summary_path": Path(kwargs["output_dir"]) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {"current_equity": 100000.0, "gross_exposure": 1.0, "turnover_estimate": 0.1},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": str(tracker_dir / "experiment_registry.csv")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.build_paper_experiment_record",
        lambda output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_live_dry_run_preview_for_targets",
        lambda **kwargs: type(
            "LiveResult",
            (),
            {
                "config": kwargs["config"],
            },
        )(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_live_dry_run_artifacts",
        lambda result: {"summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json"},
    )
    live_dir = Path(config.output_root_dir) / config.run_name / "2025-01-02T00-00-00Z" / "live_dry_run"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "live_dry_run_summary.json").write_text(
        json.dumps({"adjusted_order_count": 0, "gross_exposure": 1.0}),
        encoding="utf-8",
    )

    result, artifact_paths = run_orchestration_pipeline(config)

    assert result.status == "succeeded"
    assert all(record.status == "succeeded" for record in result.stage_records if getattr(config.stages, record.stage_name))
    assert artifact_paths["run_summary_json_path"].exists()


def test_pipeline_stage_failure_with_fail_fast(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(
            data_refresh=True,
            feature_generation=True,
            reporting=True,
        ),
        stage_order=["data_refresh", "feature_generation", "reporting"],
        fail_fast=True,
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])
    monkeypatch.setattr("trading_platform.orchestration.service.run_ingest", lambda config: tmp_path / "a.parquet")
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_feature_build",
        lambda config: (_ for _ in ()).throw(ValueError("feature boom")),
    )

    result, _artifact_paths = run_orchestration_pipeline(config)

    assert result.status == "failed"
    assert result.stage_records[1].status == "failed"
    assert result.stage_records[2].status == "pending"


def test_pipeline_stage_failure_with_continue_on_stage_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(
            data_refresh=True,
            feature_generation=True,
            reporting=True,
        ),
        stage_order=["data_refresh", "feature_generation", "reporting"],
        continue_on_stage_error=True,
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])
    monkeypatch.setattr("trading_platform.orchestration.service.run_ingest", lambda config: tmp_path / "a.parquet")
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_feature_build",
        lambda config: (_ for _ in ()).throw(ValueError("feature boom")),
    )

    result, _artifact_paths = run_orchestration_pipeline(config)

    assert result.status == "failed"
    assert result.stage_records[2].status == "succeeded"


def test_pipeline_skipped_stages(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(reporting=True),
        stage_order=["data_refresh", "reporting"],
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])

    result, _artifact_paths = run_orchestration_pipeline(config)

    assert result.stage_records[0].status == "skipped"
    assert result.stage_records[1].status == "succeeded"


def test_pipeline_artifacts_are_deterministic(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(reporting=True),
        stage_order=["reporting"],
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])

    first, paths1 = run_orchestration_pipeline(config)
    second, paths2 = run_orchestration_pipeline(config)

    assert Path(paths1["run_summary_json_path"]).read_text(encoding="utf-8") == Path(
        paths2["run_summary_json_path"]
    ).read_text(encoding="utf-8")


def test_pipeline_summary_uses_standard_fields(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(reporting=True),
        stage_order=["reporting"],
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])

    _result, paths = run_orchestration_pipeline(config)
    payload = json.loads(Path(paths["run_summary_json_path"]).read_text(encoding="utf-8"))

    assert payload["summary_type"] == "pipeline_run"
    assert payload["timestamp"] == "2025-01-02T00:00:00Z"
    assert "status" in payload
    assert "key_counts" in payload
    assert "key_metrics" in payload
    assert "warnings" in payload
    assert "errors" in payload
    assert "artifact_paths" in payload


def test_promotion_batch_and_multi_strategy_generation_integration(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    registry_path = _write_registry(tmp_path)
    governance_path = tmp_path / "governance.json"
    _write_governance_config(governance_path)
    config = _pipeline_config(
        tmp_path,
        registry_path=str(registry_path),
        governance_config_path=str(governance_path),
        stages=OrchestrationStageToggles(
            promotion_evaluation=True,
            registry_mutation=True,
            multi_strategy_config_generation=True,
            reporting=True,
        ),
        stage_order=[
            "promotion_evaluation",
            "registry_mutation",
            "multi_strategy_config_generation",
            "reporting",
        ],
        auto_promote_qualifying_candidates=True,
        registry_include_paper_strategies=True,
    )
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])

    result, artifact_paths = run_orchestration_pipeline(config)

    assert result.outputs["promoted_strategy_ids"] == ["strat-a"]
    generated_config = Path(config.multi_strategy_output_path)
    assert generated_config.exists()
    assert (generated_config.parent / "family_comparison.csv").exists()
    assert artifact_paths["run_summary_json_path"].exists()


def test_pipeline_paper_and_live_integration_through_orchestrator(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    config = _pipeline_config(
        tmp_path,
        stages=OrchestrationStageToggles(
            multi_strategy_config_generation=True,
            paper_trading=True,
            live_dry_run=True,
            reporting=True,
        ),
        stage_order=[
            "multi_strategy_config_generation",
            "paper_trading",
            "live_dry_run",
            "reporting",
        ],
    )
    _write_governance_config(Path(config.governance_config_path))
    _write_registry(tmp_path, status="approved")
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_paper_trading_cycle_for_targets",
        lambda **kwargs: type(
            "PaperResult",
            (),
            {
                "as_of": "2025-01-02",
                "state": type("State", (), {"cash": 100000.0, "equity": 100000.0, "gross_market_value": 0.0, "positions": {}})(),
                "orders": [],
                "fills": [],
                "latest_prices": kwargs["latest_prices"],
                "latest_scores": {},
                "latest_target_weights": kwargs["latest_effective_weights"],
                "scheduled_target_weights": kwargs["latest_scheduled_weights"],
                "skipped_symbols": [],
                "diagnostics": {},
            },
        )(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_paper_trading_artifacts",
        lambda **kwargs: {"summary_path": Path(kwargs["output_dir"]) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {"current_equity": 100000.0, "gross_exposure": 1.0, "turnover_estimate": 0.1},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": str(tracker_dir / "experiment_registry.csv")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.build_paper_experiment_record",
        lambda output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_live_dry_run_preview_for_targets",
        lambda **kwargs: type("LiveResult", (), {"config": kwargs["config"]})(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_live_dry_run_artifacts",
        lambda result: {"summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json"},
    )
    live_dir = Path(config.output_root_dir) / config.run_name / "2025-01-02T00-00-00Z" / "live_dry_run"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "live_dry_run_summary.json").write_text(
        json.dumps({"adjusted_order_count": 0, "gross_exposure": 1.0}),
        encoding="utf-8",
    )

    result, _artifact_paths = run_orchestration_pipeline(config)

    assert "paper_summary" in result.outputs
    assert "live_summary" in result.outputs


def test_pipeline_passes_execution_config_to_paper_and_live(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.orchestration.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.orchestration.service.perf_counter", lambda: 1.0)
    execution_config = object()
    captured: dict[str, object] = {}
    config = _pipeline_config(
        tmp_path,
        execution_config_path=str(tmp_path / "execution.json"),
        stages=OrchestrationStageToggles(
            multi_strategy_config_generation=True,
            paper_trading=True,
            live_dry_run=True,
            reporting=True,
        ),
        stage_order=[
            "multi_strategy_config_generation",
            "paper_trading",
            "live_dry_run",
            "reporting",
        ],
    )
    _write_governance_config(Path(config.governance_config_path))
    _write_registry(tmp_path, status="approved")
    monkeypatch.setattr("trading_platform.orchestration.service._union_symbols", lambda universes: ["AAPL"])
    monkeypatch.setattr("trading_platform.orchestration.service.load_execution_config", lambda path: execution_config)

    def fake_paper_run(**kwargs):
        captured["paper_execution_config"] = kwargs["execution_config"]
        return type(
            "PaperResult",
            (),
            {
                "as_of": "2025-01-02",
                "state": type("State", (), {"cash": 100000.0, "equity": 100000.0, "gross_market_value": 0.0, "positions": {}})(),
                "orders": [],
                "fills": [],
                "latest_prices": kwargs["latest_prices"],
                "latest_scores": {},
                "latest_target_weights": kwargs["latest_effective_weights"],
                "scheduled_target_weights": kwargs["latest_scheduled_weights"],
                "skipped_symbols": [],
                "diagnostics": {},
            },
        )()

    def fake_live_run(**kwargs):
        captured["live_execution_config"] = kwargs["execution_config"]
        return type("LiveResult", (), {"config": kwargs["config"]})()

    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_paper_trading_cycle_for_targets",
        fake_paper_run,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_paper_trading_artifacts",
        lambda **kwargs: {"summary_path": Path(kwargs["output_dir"]) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {"current_equity": 100000.0, "gross_exposure": 1.0, "turnover_estimate": 0.1},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": str(tracker_dir / "experiment_registry.csv")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.build_paper_experiment_record",
        lambda output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.run_live_dry_run_preview_for_targets",
        fake_live_run,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.service.write_live_dry_run_artifacts",
        lambda result: {"summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json"},
    )
    live_dir = Path(config.output_root_dir) / config.run_name / "2025-01-02T00-00-00Z" / "live_dry_run"
    live_dir.mkdir(parents=True, exist_ok=True)
    (live_dir / "live_dry_run_summary.json").write_text(
        json.dumps({"adjusted_order_count": 0, "gross_exposure": 1.0}),
        encoding="utf-8",
    )

    run_orchestration_pipeline(config)

    assert captured["paper_execution_config"] is execution_config
    assert captured["live_execution_config"] is execution_config


def test_pipeline_config_rejects_invalid_stage_ordering(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must come before"):
        _pipeline_config(
            tmp_path,
            stage_order=["paper_trading", "multi_strategy_config_generation"],
            stages=OrchestrationStageToggles(
                multi_strategy_config_generation=True,
                paper_trading=True,
            ),
        )
