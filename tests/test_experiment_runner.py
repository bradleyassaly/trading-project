from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.experiment_compare import cmd_experiment_compare
from trading_platform.cli.commands.experiment_run import cmd_experiment_run
from trading_platform.cli.commands.experiment_show import cmd_experiment_show
from trading_platform.experiments.runner import (
    compare_experiment_variants,
    load_experiment_run,
    load_experiment_spec_config,
    materialize_variant_orchestration_config,
    run_experiment,
)


def _base_orchestration_config(tmp_path: Path) -> Path:
    path = tmp_path / "orchestration.json"
    path.write_text(
        json.dumps(
            {
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
                "stages": {
                    "research": False,
                    "registry": False,
                    "validation": False,
                    "promotion": False,
                    "portfolio": False,
                    "allocation": False,
                    "paper": False,
                    "monitoring": False,
                    "regime": False,
                    "adaptive_allocation": False,
                    "governance": False,
                    "kill_switch": False,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _experiment_spec(tmp_path: Path, orchestration_path: Path) -> Path:
    path = tmp_path / "experiments.yaml"
    path.write_text(
        f"""
experiment_name: ab_feature_test
base_orchestration_config_path: {orchestration_path.as_posix()}
output_root_dir: {(tmp_path / "experiments").as_posix()}
repeat_count: 1
run_label_metadata:
  owner: qa
variants:
  - name: regime_on
    feature_flags:
      regime: true
      adaptive: true
    stage_overrides:
      regime: true
      adaptive_allocation: true
  - name: regime_off
    feature_flags:
      regime: false
      adaptive: false
    stage_overrides:
      regime: false
      adaptive_allocation: false
""".strip(),
        encoding="utf-8",
    )
    return path


def _fake_orchestration_run(config) -> tuple[object, dict[str, Path]]:
    run_dir = Path(config.output_root_dir) / config.run_name / "2026-03-22T00-00-00+00-00"
    (run_dir / "paper").mkdir(parents=True, exist_ok=True)
    (run_dir / "governance").mkdir(parents=True, exist_ok=True)
    (run_dir / "kill_switch").mkdir(parents=True, exist_ok=True)
    (run_dir / "orchestration_run.json").write_text(
        json.dumps(
            {
                "run_id": "2026-03-22T00-00-00+00-00",
                "run_name": config.run_name,
                "schedule_frequency": config.schedule_frequency,
                "experiment_name": config.experiment_name,
                "variant_name": config.variant_name,
                "experiment_run_id": config.experiment_run_id,
                "feature_flags": config.feature_flags,
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:05:00+00:00",
                "status": "succeeded",
                "warnings": [],
                "stage_records": [],
                "outputs": {"selected_strategy_count": 1, "promoted_strategy_count": 1},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "orchestration_config_snapshot.json").write_text(
        json.dumps(config.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"timestamp": "2026-03-20T00:00:00+00:00", "equity": 100000.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "equity": 101000.0 if config.feature_flags.get("regime") else 100200.0},
            {"timestamp": "2026-03-22T00:00:00+00:00", "equity": 103000.0 if config.feature_flags.get("regime") else 100500.0},
        ]
    ).to_csv(run_dir / "paper" / "paper_equity_curve.csv", index=False)
    (run_dir / "paper" / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"turnover_after_execution_constraints": 0.1, "current_equity": 103000.0}}),
        encoding="utf-8",
    )
    (run_dir / "governance" / "strategy_lifecycle.json").write_text(
        json.dumps({"summary": {"active_count": 1, "demoted_count": 0}}),
        encoding="utf-8",
    )
    (run_dir / "kill_switch" / "kill_switch_recommendations.json").write_text(
        json.dumps({"summary": {"recommendation_count": 0}}),
        encoding="utf-8",
    )
    result = type(
        "Result",
        (),
        {
            "run_id": "2026-03-22T00-00-00+00-00",
            "run_name": config.run_name,
            "schedule_frequency": config.schedule_frequency,
            "experiment_name": config.experiment_name,
            "variant_name": config.variant_name,
            "experiment_run_id": config.experiment_run_id,
            "feature_flags": config.feature_flags,
            "run_label_metadata": config.run_label_metadata,
            "started_at": "2026-03-22T00:00:00+00:00",
            "ended_at": "2026-03-22T00:05:00+00:00",
            "status": "succeeded",
            "run_dir": str(run_dir),
            "stage_records": [],
            "warnings": [],
            "errors": [],
            "outputs": {"selected_strategy_count": 1},
        },
    )()
    return result, {
        "orchestration_run_json_path": run_dir / "orchestration_run.json",
        "system_evaluation_json_path": run_dir / "system_evaluation.json",
    }


def test_experiment_spec_loading_and_materialization(tmp_path: Path) -> None:
    orchestration_path = _base_orchestration_config(tmp_path)
    spec_path = _experiment_spec(tmp_path, orchestration_path)

    spec = load_experiment_spec_config(spec_path)
    config, config_path = materialize_variant_orchestration_config(
        spec=spec,
        variant=spec.variants[0],
        experiment_run_id="2026-03-22T00-00-00+00-00",
        experiment_run_dir=tmp_path / "materialized",
        repeat_index=1,
    )

    assert spec.experiment_name == "ab_feature_test"
    assert config.experiment_name == "ab_feature_test"
    assert config.variant_name == "regime_on"
    assert config.feature_flags["regime"] is True
    assert config.stages.regime is True
    assert config_path.exists()


def test_experiment_runner_and_compare(monkeypatch, tmp_path: Path) -> None:
    orchestration_path = _base_orchestration_config(tmp_path)
    spec_path = _experiment_spec(tmp_path, orchestration_path)
    spec = load_experiment_spec_config(spec_path)

    monkeypatch.setattr("trading_platform.experiments.runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.experiments.runner.run_automated_orchestration", _fake_orchestration_run)

    result = run_experiment(spec=spec)
    payload = load_experiment_run(result["run_dir"])
    compare = compare_experiment_variants(
        experiment_run_path=result["run_dir"],
        output_dir=tmp_path / "compare",
        variant_a="regime_on",
        variant_b="regime_off",
    )
    history_payload = json.loads((Path(result["run_dir"]) / "system_evaluation" / "system_evaluation_history.json").read_text(encoding="utf-8"))

    assert result["status"] == "succeeded"
    assert payload["summary"]["variant_count"] == 2
    assert payload["variants"][0]["experiment_run_id"] == "2026-03-22T00-00-00+00-00"
    assert history_payload["summary"]["run_count"] == 2
    assert "regime_on" in history_payload["summary"]["variant_names"]
    assert compare["group_a_count"] == 1
    assert (tmp_path / "compare" / "system_evaluation_compare.json").exists()


def test_experiment_cli_commands(monkeypatch, tmp_path: Path, capsys) -> None:
    orchestration_path = _base_orchestration_config(tmp_path)
    spec_path = _experiment_spec(tmp_path, orchestration_path)
    monkeypatch.setattr("trading_platform.experiments.runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.experiments.runner.run_automated_orchestration", _fake_orchestration_run)

    cmd_experiment_run(Namespace(config=str(spec_path), variants=None, dry_run=False))
    run_dir = tmp_path / "experiments" / "ab_feature_test" / "2026-03-22T00-00-00+00-00"
    cmd_experiment_show(Namespace(run=str(run_dir)))
    cmd_experiment_compare(
        Namespace(
            run=str(run_dir),
            output_dir=str(tmp_path / "compare"),
            variant_a="regime_on",
            variant_b="regime_off",
        )
    )

    captured = capsys.readouterr().out
    assert "Experiment:" in captured
    assert "Run id:" in captured
    assert "Comparison JSON:" in captured


def test_experiment_runner_records_no_op_variant(monkeypatch, tmp_path: Path) -> None:
    orchestration_path = _base_orchestration_config(tmp_path)
    spec_path = _experiment_spec(tmp_path, orchestration_path)
    spec = load_experiment_spec_config(spec_path)

    def _fake_noop_run(config):
        run_dir = Path(config.output_root_dir) / config.run_name / "2026-03-22T00-00-00+00-00"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "orchestration_run.json").write_text(
            json.dumps(
                {
                    "run_id": "2026-03-22T00-00-00+00-00",
                    "run_name": config.run_name,
                    "schedule_frequency": config.schedule_frequency,
                    "experiment_name": config.experiment_name,
                    "variant_name": config.variant_name,
                    "experiment_run_id": config.experiment_run_id,
                    "feature_flags": config.feature_flags,
                    "started_at": "2026-03-22T00:00:00+00:00",
                    "ended_at": "2026-03-22T00:05:00+00:00",
                    "status": "succeeded",
                    "warnings": ["promotion:no_strategies_promoted"],
                    "stage_records": [
                        {"stage_name": "promotion", "status": "skipped", "outputs": {"promoted_strategy_count": 0, "skip_reason": "no strategies were promoted"}}
                    ],
                    "outputs": {
                        "promoted_strategy_count": 0,
                        "no_op": True,
                        "no_op_reason": "no strategies were promoted",
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (run_dir / "orchestration_config_snapshot.json").write_text(
            json.dumps(config.to_dict(), indent=2, default=str),
            encoding="utf-8",
        )
        result = type(
            "Result",
            (),
            {
                "run_id": "2026-03-22T00-00-00+00-00",
                "run_name": config.run_name,
                "schedule_frequency": config.schedule_frequency,
                "experiment_name": config.experiment_name,
                "variant_name": config.variant_name,
                "experiment_run_id": config.experiment_run_id,
                "feature_flags": config.feature_flags,
                "run_label_metadata": config.run_label_metadata,
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:05:00+00:00",
                "status": "succeeded",
                "run_dir": str(run_dir),
                "stage_records": [],
                "warnings": ["promotion:no_strategies_promoted"],
                "errors": [],
                "outputs": {
                    "promoted_strategy_count": 0,
                    "no_op": True,
                    "no_op_reason": "no strategies were promoted",
                },
            },
        )()
        return result, {
            "orchestration_run_json_path": run_dir / "orchestration_run.json",
            "system_evaluation_json_path": run_dir / "system_evaluation.json",
        }

    monkeypatch.setattr("trading_platform.experiments.runner._now_utc", lambda: "2026-03-22T00:00:00+00:00")
    monkeypatch.setattr("trading_platform.experiments.runner.run_automated_orchestration", _fake_noop_run)

    result = run_experiment(spec=spec, selected_variants=["regime_on"])
    payload = load_experiment_run(result["run_dir"])

    assert result["status"] == "succeeded"
    assert payload["summary"]["no_op_count"] == 1
    assert payload["variants"][0]["no_op"] is True
    assert payload["variants"][0]["insufficient_output_reason"] == "no strategies were promoted"
