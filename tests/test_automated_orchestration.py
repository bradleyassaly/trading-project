from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.orchestrate_run import cmd_orchestrate_run
from trading_platform.cli.commands.orchestrate_show_run import cmd_orchestrate_show_run
from trading_platform.config.loader import load_automated_orchestration_config
from trading_platform.orchestration.pipeline_runner import (
    AutomatedOrchestrationConfig,
    AutomatedOrchestrationStageToggles,
    run_automated_orchestration,
)


def _config(tmp_path: Path, **overrides) -> AutomatedOrchestrationConfig:
    base = {
        "run_name": "automation",
        "schedule_frequency": "manual",
        "research_artifacts_root": str(tmp_path / "research"),
        "output_root_dir": str(tmp_path / "runs"),
        "promotion_policy_config_path": str(tmp_path / "promotion.yaml"),
        "strategy_validation_policy_config_path": str(tmp_path / "strategy_validation.yaml"),
        "strategy_portfolio_policy_config_path": str(tmp_path / "strategy_portfolio.yaml"),
        "strategy_monitoring_policy_config_path": str(tmp_path / "strategy_monitoring.yaml"),
        "market_regime_policy_config_path": str(tmp_path / "market_regime.yaml"),
        "adaptive_allocation_policy_config_path": str(tmp_path / "adaptive_allocation.yaml"),
        "strategy_governance_policy_config_path": str(tmp_path / "strategy_governance.yaml"),
        "strategy_lifecycle_path": str(tmp_path / "strategy_lifecycle.json"),
        "paper_state_path": str(tmp_path / "paper_state.json"),
        "market_regime_input_path": str(tmp_path / "prices.csv"),
    }
    base.update(overrides)
    return AutomatedOrchestrationConfig(**base)


def _write_policy_files(tmp_path: Path) -> None:
    (tmp_path / "promotion.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "strategy_validation.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "strategy_portfolio.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "strategy_monitoring.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "market_regime.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "adaptive_allocation.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "strategy_governance.yaml").write_text("schema_version: 1\n", encoding="utf-8")


def test_load_automated_orchestration_config_from_yaml(tmp_path: Path) -> None:
    path = tmp_path / "orchestration.yaml"
    path.write_text(
        """
run_name: auto
schedule_frequency: daily
research_artifacts_root: artifacts
experiment_name: ab_test
feature_flags:
  regime: true
  adaptive: true
output_root_dir: artifacts/orchestration_runs
promotion_policy_config_path: configs/promotion.yaml
strategy_validation_policy_config_path: configs/strategy_validation.yaml
strategy_portfolio_policy_config_path: configs/strategy_portfolio.yaml
strategy_monitoring_policy_config_path: configs/strategy_monitoring.yaml
market_regime_policy_config_path: configs/market_regime.yaml
adaptive_allocation_policy_config_path: configs/adaptive_allocation.yaml
strategy_governance_policy_config_path: configs/strategy_governance.yaml
strategy_lifecycle_path: artifacts/governance/strategy_lifecycle.json
paper_state_path: artifacts/paper_state.json
max_promotions_per_run: 2
stages:
  research: true
  registry: true
  validation: true
  promotion: true
  portfolio: true
  allocation: true
  paper: true
  monitoring: true
  regime: true
  adaptive_allocation: true
  governance: true
  kill_switch: true
""".strip(),
        encoding="utf-8",
    )

    config = load_automated_orchestration_config(path)

    assert config.run_name == "auto"
    assert config.experiment_name == "ab_test"
    assert config.feature_flags["adaptive"] is True
    assert config.schedule_frequency == "daily"
    assert config.max_promotions_per_run == 2
    assert config.stages.validation is True
    assert config.stages.regime is True
    assert config.stages.adaptive_allocation is True
    assert config.stages.governance is True
    assert config.stages.kill_switch is True


def test_automated_orchestration_stage_sequencing_and_artifact_passing(monkeypatch, tmp_path: Path) -> None:
    _write_policy_files(tmp_path)
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.perf_counter", lambda: 1.0)
    config = _config(
        tmp_path,
        stages=AutomatedOrchestrationStageToggles(
            research=True,
            registry=True,
            validation=True,
            promotion=True,
            portfolio=True,
            allocation=True,
            paper=True,
            monitoring=True,
            regime=True,
            adaptive_allocation=True,
            governance=True,
            kill_switch=True,
        ),
    )

    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.load_research_manifests",
        lambda root: [{"run_id": "run-a"}],
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_research_registry",
        lambda **kwargs: {"registry_json_path": str(Path(kwargs["output_dir"]) / "research_registry.json"), "run_count": 1},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_research_leaderboard",
        lambda **kwargs: {"leaderboard_json_path": str(Path(kwargs["output_dir"]) / "research_leaderboard.json"), "row_count": 1},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_strategy_validation",
        lambda **kwargs: {"strategy_validation_json_path": str(Path(kwargs["output_dir"]) / "strategy_validation.json"), "strategy_validation_csv_path": str(Path(kwargs["output_dir"]) / "strategy_validation.csv"), "pass_count": 1, "weak_count": 0, "fail_count": 0},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_promotion_candidates",
        lambda **kwargs: (
            (Path(kwargs["output_dir"]) / "promotion_candidates.json").write_text(json.dumps({"rows": [{"run_id": "run-a"}]}), encoding="utf-8"),
            {"promotion_candidates_json_path": str(Path(kwargs["output_dir"]) / "promotion_candidates.json"), "eligible_count": 1},
        )[1],
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.apply_research_promotions",
        lambda **kwargs: {
            "selected_count": 1,
            "dry_run": False,
            "promoted_index_path": str(Path(kwargs["output_dir"]) / "promoted_strategies.json"),
            "promoted_rows": [{"preset_name": "generated_a"}],
        },
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_strategy_portfolio",
        lambda **kwargs: {"selected_count": 1, "warning_count": 0, "strategy_portfolio_json_path": str(Path(kwargs["output_dir"]) / "strategy_portfolio.json"), "strategy_portfolio_csv_path": str(Path(kwargs["output_dir"]) / "strategy_portfolio.csv")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.load_strategy_portfolio",
        lambda path: {"summary": {"total_selected_strategies": 1}, "warnings": []},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.export_strategy_portfolio_run_config",
        lambda **kwargs: {"multi_strategy_config_path": str(Path(kwargs["output_dir"]) / "multi_strategy.json"), "pipeline_config_path": str(Path(kwargs["output_dir"]) / "pipeline.yaml"), "run_bundle_path": str(Path(kwargs["output_dir"]) / "bundle.json")},
    )
    _fake_portfolio_cfg = type("Cfg", (), {
        "cash_reserve_pct": 0.0,
        "activation_applied": False,
        "source_portfolio_path": None,
        "fail_if_no_active_strategies": False,
        "active_strategy_count": 1,
        "active_unconditional_count": 1,
        "active_conditional_count": 0,
        "inactive_conditional_count": 0,
    })()
    _fake_handoff = type("Handoff", (), {
        "source_kind": "multi_strategy_config",
        "source_path": "fake",
        "warnings": [],
        "summary": {"activation_applied": False, "source_portfolio_path": None, "fail_if_no_active_strategies": False, "active_strategy_count": 1},
        "portfolio_config": _fake_portfolio_cfg,
    })()
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.resolve_strategy_execution_handoff",
        lambda path_or_dir, config=None: _fake_handoff,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    allocation_result = type(
        "AllocationResult",
        (),
        {
            "as_of": "2026-03-22",
            "combined_target_weights": {"AAPL": 1.0},
            "latest_prices": {"AAPL": 100.0},
            "sleeve_rows": [{"symbol": "AAPL"}],
            "sleeve_bundles": [],
            "summary": {"enabled_sleeve_count": 1, "gross_exposure_after_constraints": 1.0, "turnover_estimate": 0.1, "turnover_cap_binding": False, "symbols_removed_or_clipped": []},
        },
    )()
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.allocate_multi_strategy_portfolio", lambda cfg: allocation_result)
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.run_paper_trading_cycle_for_targets",
        lambda **kwargs: type("PaperResult", (), {"orders": [], "as_of": "2026-03-22"})(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.write_paper_trading_artifacts",
        lambda **kwargs: {"paper_summary_json_path": Path(kwargs["output_dir"]) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {"current_equity": 100000.0},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.register_experiment",
        lambda *args, **kwargs: {"experiment_registry_path": str(tmp_path / "experiment_registry.csv")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_paper_experiment_record",
        lambda output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_strategy_monitoring_snapshot",
        lambda **kwargs: {"strategy_monitoring_json_path": str(Path(kwargs["output_dir"]) / "strategy_monitoring.json"), "warning_strategy_count": 1, "deactivation_candidate_count": 1, "kill_switch_recommendations_json_path": str(Path(kwargs["output_dir"]) / "kill_switch_recommendations.json")},
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.detect_market_regime",
        lambda **kwargs: {
            "market_regime_json_path": str(Path(kwargs["output_dir"]) / "market_regime.json"),
            "market_regime_csv_path": str(Path(kwargs["output_dir"]) / "market_regime.csv"),
            "regime_label": "trend",
            "confidence_score": 0.7,
            "latest": {"regime_label": "trend", "confidence_score": 0.7},
        },
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_adaptive_allocation",
        lambda **kwargs: (
            (Path(kwargs["output_dir"]) / "adaptive_allocation.json").write_text(
                json.dumps({"summary": {"total_selected_strategies": 1, "warning_count": 0}}),
                encoding="utf-8",
            ),
            {
                "adaptive_allocation_json_path": str(Path(kwargs["output_dir"]) / "adaptive_allocation.json"),
                "adaptive_allocation_csv_path": str(Path(kwargs["output_dir"]) / "adaptive_allocation.csv"),
                "selected_count": 1,
                "warning_count": 0,
                "absolute_weight_change": 0.05,
            },
        )[1],
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.export_adaptive_allocation_run_config",
        lambda **kwargs: {
            "multi_strategy_config_path": str(Path(kwargs["output_dir"]) / "adaptive_multi_strategy.json"),
            "pipeline_config_path": str(Path(kwargs["output_dir"]) / "adaptive_pipeline.yaml"),
            "run_bundle_path": str(Path(kwargs["output_dir"]) / "adaptive_bundle.json"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.apply_strategy_governance",
        lambda **kwargs: {
            "strategy_lifecycle_json_path": str(Path(kwargs["output_dir"]) / "strategy_lifecycle.json"),
            "strategy_lifecycle_csv_path": str(Path(kwargs["output_dir"]) / "strategy_lifecycle.csv"),
            "strategy_governance_summary_json_path": str(Path(kwargs["output_dir"]) / "strategy_governance_summary.json"),
            "under_review_count": 1,
            "degraded_count": 0,
            "demoted_count": 0,
        },
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.recommend_kill_switch_actions",
        lambda **kwargs: {"kill_switch_recommendations_json_path": str(Path(kwargs["output_dir"]) / "kill_switch_recommendations.json"), "kill_switch_recommendations_csv_path": str(Path(kwargs["output_dir"]) / "kill_switch_recommendations.csv"), "recommendation_count": 1},
    )

    result, paths = run_automated_orchestration(config)

    assert result.status == "succeeded"
    assert result.experiment_name is None
    assert result.outputs["validated_pass_count"] == 1
    assert result.outputs["selected_strategy_count"] == 1
    assert result.outputs["current_regime_label"] == "trend"
    assert result.outputs["adaptive_selected_strategy_count"] == 1
    assert result.outputs["under_review_count"] == 1
    assert result.outputs["kill_switch_recommendation_count"] == 1
    assert paths["orchestration_run_json_path"].exists()


def test_automated_orchestration_skips_on_empty_promotions(monkeypatch, tmp_path: Path) -> None:
    _write_policy_files(tmp_path)
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.perf_counter", lambda: 1.0)
    config = _config(
        tmp_path,
        stages=AutomatedOrchestrationStageToggles(research=True, registry=True, validation=True, promotion=True, portfolio=True, allocation=False, paper=False, monitoring=False, regime=False, governance=False, kill_switch=False),
        stage_order=["research", "registry", "validation", "promotion", "portfolio"],
    )
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.load_research_manifests", lambda root: [{"run_id": "run-a"}])
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_research_registry", lambda **kwargs: {"registry_json_path": "x", "run_count": 1})
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_research_leaderboard", lambda **kwargs: {"leaderboard_json_path": "x", "row_count": 1})
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_strategy_validation", lambda **kwargs: {"strategy_validation_json_path": "x", "strategy_validation_csv_path": "x", "pass_count": 1, "weak_count": 0, "fail_count": 0})
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_promotion_candidates",
        lambda **kwargs: (
            (Path(kwargs["output_dir"]) / "promotion_candidates.json").write_text(json.dumps({"rows": [{"run_id": "run-a"}]}), encoding="utf-8"),
            {"promotion_candidates_json_path": str(Path(kwargs["output_dir"]) / "promotion_candidates.json"), "eligible_count": 1},
        )[1],
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.apply_research_promotions",
        lambda **kwargs: {"selected_count": 0, "dry_run": False, "promoted_index_path": "x", "promoted_rows": []},
    )

    result, _ = run_automated_orchestration(config)

    assert result.status == "succeeded"
    assert result.stage_records[3].status == "skipped"
    assert result.stage_records[3].outputs["promoted_strategy_count"] == 0
    assert result.stage_records[3].outputs["skip_reason"] == "no strategies were promoted"
    assert result.stage_records[4].status == "skipped"
    assert result.stage_records[4].outputs["skip_reason"] == "no strategies were promoted"
    assert result.outputs["promoted_strategy_count"] == 0
    assert result.outputs["no_op"] is True
    assert result.outputs["no_op_reason"] == "no strategies were promoted"


def test_automated_orchestration_skips_on_empty_promotion_candidates(monkeypatch, tmp_path: Path) -> None:
    _write_policy_files(tmp_path)
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.perf_counter", lambda: 1.0)
    config = _config(
        tmp_path,
        stages=AutomatedOrchestrationStageToggles(research=True, registry=True, validation=True, promotion=True, portfolio=True, allocation=False, paper=False, monitoring=False, regime=False, governance=False, kill_switch=False),
        stage_order=["research", "registry", "validation", "promotion", "portfolio"],
    )
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.load_research_manifests", lambda root: [{"run_id": "run-a"}])
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_research_registry", lambda **kwargs: {"registry_json_path": "x", "run_count": 1})
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_research_leaderboard", lambda **kwargs: {"leaderboard_json_path": "x", "row_count": 1})
    monkeypatch.setattr("trading_platform.orchestration.pipeline_runner.build_strategy_validation", lambda **kwargs: {"strategy_validation_json_path": "x", "strategy_validation_csv_path": "x", "pass_count": 1, "weak_count": 0, "fail_count": 0})
    monkeypatch.setattr(
        "trading_platform.orchestration.pipeline_runner.build_promotion_candidates",
        lambda **kwargs: (
            (Path(kwargs["output_dir"]) / "promotion_candidates.json").write_text(json.dumps({"rows": []}), encoding="utf-8"),
            {"promotion_candidates_json_path": str(Path(kwargs["output_dir"]) / "promotion_candidates.json"), "eligible_count": 0},
        )[1],
    )

    result, _ = run_automated_orchestration(config)

    assert result.status == "succeeded"
    assert result.stage_records[3].status == "skipped"
    assert result.stage_records[3].outputs["promotion_candidate_count"] == 0
    assert result.stage_records[4].status == "skipped"


def test_orchestrate_cli_commands(tmp_path: Path, monkeypatch, capsys) -> None:
    _write_policy_files(tmp_path)
    config_path = tmp_path / "orchestration.yaml"
    config_path.write_text(
        json.dumps(_config(tmp_path).to_dict(), indent=2),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.orchestrate_run.run_automated_orchestration",
        lambda config: (
            type(
                "Result",
                (),
                {
                    "run_id": "2026-03-22T00-00-00+00-00",
                    "run_name": config.run_name,
                    "schedule_frequency": config.schedule_frequency,
                    "status": "succeeded",
                    "stage_records": [],
                },
            )(),
            {"orchestration_run_json_path": tmp_path / "run.json"},
        ),
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "orchestration_run.json").write_text(json.dumps({"run_id": "abc", "run_name": "auto", "status": "succeeded", "stage_records": []}), encoding="utf-8")

    cmd_orchestrate_run(Namespace(config=str(config_path)))
    cmd_orchestrate_show_run(Namespace(run=str(run_dir)))

    captured = capsys.readouterr().out
    assert "Run id:" in captured
    assert "Status:" in captured
