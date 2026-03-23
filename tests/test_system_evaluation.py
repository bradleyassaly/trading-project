from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.system_eval_build import cmd_system_eval_build
from trading_platform.cli.commands.system_eval_compare import cmd_system_eval_compare
from trading_platform.cli.commands.system_eval_show import cmd_system_eval_show
from trading_platform.system_evaluation.service import (
    build_system_evaluation_history,
    compare_system_evaluations,
    evaluate_orchestration_run,
    load_system_evaluation,
)


def _write_run(root: Path, *, run_id: str, total_return_scale: float, adaptive: bool, regime: bool, variant_name: str | None = None) -> Path:
    run_dir = root / "automation" / run_id
    (run_dir / "paper").mkdir(parents=True, exist_ok=True)
    (run_dir / "governance").mkdir(parents=True, exist_ok=True)
    (run_dir / "kill_switch").mkdir(parents=True, exist_ok=True)
    config_snapshot = {
        "run_name": "automation",
        "experiment_name": "ab_test",
        "variant_name": variant_name or ("adaptive_on" if adaptive else "adaptive_off"),
        "experiment_run_id": "experiment-1",
        "feature_flags": {"adaptive": adaptive, "regime": regime},
        "stages": {"adaptive_allocation": adaptive, "regime": regime},
    }
    (run_dir / "orchestration_config_snapshot.json").write_text(json.dumps(config_snapshot, indent=2), encoding="utf-8")
    orchestration = {
        "run_id": run_id,
        "run_name": "automation",
        "schedule_frequency": "daily",
        "experiment_name": "ab_test",
        "variant_name": variant_name or ("adaptive_on" if adaptive else "adaptive_off"),
        "experiment_run_id": "experiment-1",
        "feature_flags": {"adaptive": adaptive, "regime": regime},
        "started_at": f"{run_id.replace('T00-00-00+00-00', 'T00:00:00+00:00')}",
        "ended_at": f"{run_id.replace('T00-00-00+00-00', 'T00:05:00+00:00')}",
        "status": "succeeded",
        "warnings": ["monitoring:review"] if not adaptive else [],
        "stage_records": [{"stage_name": "paper", "status": "succeeded", "warnings": []}],
        "outputs": {
            "selected_strategy_count": 2,
            "promoted_strategy_count": 1,
            "demoted_count": 0 if adaptive else 1,
            "kill_switch_recommendation_count": 1 if not adaptive else 0,
            "current_regime_label": "trend" if regime else None,
        },
    }
    (run_dir / "orchestration_run.json").write_text(json.dumps(orchestration, indent=2), encoding="utf-8")
    pd.DataFrame(
        [
            {"timestamp": "2026-03-20T00:00:00+00:00", "equity": 100000.0},
            {"timestamp": "2026-03-21T00:00:00+00:00", "equity": 100000.0 * (1 + total_return_scale / 2)},
            {"timestamp": "2026-03-22T00:00:00+00:00", "equity": 100000.0 * (1 + total_return_scale)},
        ]
    ).to_csv(run_dir / "paper" / "paper_equity_curve.csv", index=False)
    (run_dir / "paper" / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"turnover_after_execution_constraints": 0.12, "current_equity": 100000.0 * (1 + total_return_scale)}}),
        encoding="utf-8",
    )
    (run_dir / "governance" / "strategy_lifecycle.json").write_text(
        json.dumps({"summary": {"active_count": 2, "demoted_count": 0 if adaptive else 1}}),
        encoding="utf-8",
    )
    (run_dir / "kill_switch" / "kill_switch_recommendations.json").write_text(
        json.dumps({"summary": {"recommendation_count": 1 if not adaptive else 0}}),
        encoding="utf-8",
    )
    if regime:
        (run_dir / "regime" / "market_regime.json").parent.mkdir(parents=True, exist_ok=True)
        (run_dir / "regime" / "market_regime.json").write_text(
            json.dumps({"latest": {"regime_label": "trend"}}),
            encoding="utf-8",
        )
    return run_dir


def test_system_evaluation_metric_computation(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path / "runs", run_id="2026-03-22T00-00-00+00-00", total_return_scale=0.04, adaptive=True, regime=True)

    payload = evaluate_orchestration_run(run_dir=run_dir, output_dir=tmp_path / "eval")
    latest = load_system_evaluation(tmp_path / "eval")

    assert payload["row"]["run_id"] == "2026-03-22T00-00-00+00-00"
    assert payload["row"]["total_return"] is not None
    assert payload["row"]["regime"] == "trend"
    assert latest["row"]["experiment_name"] == "ab_test"
    assert latest["row"]["variant_name"] == "adaptive_on"


def test_system_evaluation_history_and_compare(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_run(runs_root, run_id="2026-03-22T00-00-00+00-00", total_return_scale=0.04, adaptive=True, regime=True)
    _write_run(runs_root, run_id="2026-03-21T00-00-00+00-00", total_return_scale=0.01, adaptive=False, regime=False)
    _write_run(runs_root, run_id="2026-03-20T00-00-00+00-00", total_return_scale=0.03, adaptive=True, regime=False)
    _write_run(runs_root, run_id="2026-03-19T00-00-00+00-00", total_return_scale=-0.01, adaptive=False, regime=False)

    history = build_system_evaluation_history(runs_root=runs_root, output_dir=tmp_path / "history")
    compare = compare_system_evaluations(
        history_path_or_root=tmp_path / "history",
        output_dir=tmp_path / "compare",
        feature_flag="adaptive",
        value_a="true",
        value_b="false",
    )
    variant_compare = compare_system_evaluations(
        history_path_or_root=tmp_path / "history",
        output_dir=tmp_path / "compare_variant",
        group_by_field="variant_name",
        value_a="adaptive_on",
        value_b="adaptive_off",
    )

    history_payload = json.loads((tmp_path / "history" / "system_evaluation_history.json").read_text(encoding="utf-8"))
    compare_payload = json.loads((tmp_path / "compare" / "system_evaluation_compare.json").read_text(encoding="utf-8"))

    assert history["run_count"] == 4
    assert history_payload["summary"]["best_run_id"] == "2026-03-22T00-00-00+00-00"
    assert "adaptive_on" in history_payload["summary"]["variant_names"]
    assert compare["group_a_count"] == 2
    assert variant_compare["group_a_count"] == 2
    assert compare_payload["comparison"]["feature_flag"] == "adaptive"


def test_system_eval_cli_commands(tmp_path: Path, capsys) -> None:
    runs_root = tmp_path / "runs"
    run_dir = _write_run(runs_root, run_id="2026-03-22T00-00-00+00-00", total_return_scale=0.02, adaptive=True, regime=True)
    _write_run(runs_root, run_id="2026-03-21T00-00-00+00-00", total_return_scale=-0.01, adaptive=False, regime=False)

    evaluate_orchestration_run(run_dir=run_dir, output_dir=tmp_path / "eval")
    cmd_system_eval_show(Namespace(evaluation=str(tmp_path / "eval")))
    cmd_system_eval_build(Namespace(runs_root=str(runs_root), output_dir=str(tmp_path / "history")))
    cmd_system_eval_compare(
        Namespace(
            history=str(runs_root),
            output_dir=str(tmp_path / "compare"),
            latest_count=1,
            previous_count=1,
            feature_flag=None,
            value_a="true",
            value_b="false",
        )
    )

    captured = capsys.readouterr().out
    assert "Run id:" in captured
    assert "History JSON:" in captured
    assert "Comparison JSON:" in captured
