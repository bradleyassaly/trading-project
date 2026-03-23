from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.experiment_summarize_campaign import cmd_experiment_summarize_campaign
from trading_platform.experiments.campaign import build_experiment_campaign_summary


def _write_experiment_run(
    root: Path,
    *,
    experiment_name: str,
    experiment_run_id: str,
    rows: list[dict],
) -> Path:
    run_dir = root / experiment_name / experiment_run_id
    (run_dir / "system_evaluation").mkdir(parents=True, exist_ok=True)
    (run_dir / "experiment_run.json").write_text(
        json.dumps(
            {
                "experiment_name": experiment_name,
                "experiment_run_id": experiment_run_id,
                "status": "succeeded",
                "summary": {
                    "variant_count": len({row["variant_name"] for row in rows}),
                    "variant_run_count": len(rows),
                    "succeeded_count": len(rows),
                    "failed_count": 0,
                },
                "variants": [
                    {
                        "variant_name": row["variant_name"],
                        "repeat_index": 1,
                        "status": "succeeded",
                        "run_dir": str(run_dir / "variants" / row["variant_name"]),
                    }
                    for row in rows
                ],
                "system_evaluation": {
                    "system_evaluation_history_json_path": str(run_dir / "system_evaluation" / "system_evaluation_history.json"),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "system_evaluation" / "system_evaluation_history.json").write_text(
        json.dumps({"summary": {"run_count": len(rows)}, "rows": rows}, indent=2),
        encoding="utf-8",
    )
    return run_dir


def test_experiment_campaign_summary_and_winners(tmp_path: Path) -> None:
    run_dir = _write_experiment_run(
        tmp_path,
        experiment_name="campaign_regime_on_off",
        experiment_run_id="2026-03-22T00-00-00+00-00",
        rows=[
            {
                "run_id": "run-a",
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_on",
                "total_return": 0.05,
                "sharpe": 1.2,
                "max_drawdown": 0.04,
                "turnover": 0.15,
                "promoted_strategy_count": 2,
                "demoted_count": 0,
                "active_strategy_count": 3,
                "warning_count": 1,
                "kill_switch_count": 0,
                "adaptive_enabled": True,
                "regime_enabled": True,
            },
            {
                "run_id": "run-b",
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_off",
                "total_return": 0.02,
                "sharpe": 0.6,
                "max_drawdown": 0.06,
                "turnover": 0.11,
                "promoted_strategy_count": 1,
                "demoted_count": 1,
                "active_strategy_count": 2,
                "warning_count": 3,
                "kill_switch_count": 1,
                "adaptive_enabled": True,
                "regime_enabled": False,
            },
        ],
    )

    result = build_experiment_campaign_summary(
        experiment_runs=[run_dir],
        output_dir=tmp_path / "campaign_summary",
    )
    payload = json.loads((tmp_path / "campaign_summary" / "experiment_campaign_summary.json").read_text(encoding="utf-8"))

    assert result["variant_count"] == 2
    assert payload["metric_winners"]["total_return"] == ["regime_on"]
    assert payload["metric_winners"]["max_drawdown"] == ["regime_on"]
    assert payload["metric_winners"]["turnover"] == ["regime_off"]
    assert payload["variants"][0]["variant_name"] == "regime_off"


def test_experiment_campaign_cli(tmp_path: Path, capsys) -> None:
    run_dir = _write_experiment_run(
        tmp_path,
        experiment_name="campaign_adaptive_on_off",
        experiment_run_id="2026-03-22T00-00-00+00-00",
        rows=[
            {
                "run_id": "run-a",
                "experiment_name": "campaign_adaptive_on_off",
                "variant_name": "adaptive_on",
                "total_return": 0.03,
                "sharpe": 0.9,
                "max_drawdown": 0.05,
                "turnover": 0.16,
                "promoted_strategy_count": 2,
                "demoted_count": 0,
                "active_strategy_count": 3,
                "warning_count": 1,
                "kill_switch_count": 0,
                "adaptive_enabled": True,
                "regime_enabled": True,
            },
            {
                "run_id": "run-b",
                "experiment_name": "campaign_adaptive_on_off",
                "variant_name": "adaptive_off",
                "total_return": 0.01,
                "sharpe": 0.4,
                "max_drawdown": 0.08,
                "turnover": 0.09,
                "promoted_strategy_count": 1,
                "demoted_count": 1,
                "active_strategy_count": 2,
                "warning_count": 2,
                "kill_switch_count": 1,
                "adaptive_enabled": False,
                "regime_enabled": True,
            },
        ],
    )

    cmd_experiment_summarize_campaign(
        Namespace(
            runs=[str(run_dir)],
            output_dir=str(tmp_path / "summary"),
        )
    )

    captured = capsys.readouterr().out
    assert "Variants summarized:" in captured
    assert "total_return:" in captured
    assert (tmp_path / "summary" / "experiment_campaign_summary.md").exists()
