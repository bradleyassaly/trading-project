from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.experiment_recommend_defaults import cmd_experiment_recommend_defaults
from trading_platform.experiments.decision_support import recommend_experiment_defaults


def _write_campaign_summary(tmp_path: Path, variants: list[dict]) -> Path:
    summary_dir = tmp_path / "campaign_summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    (summary_dir / "experiment_campaign_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-22T00:00:00+00:00",
                "summary": {"experiment_run_count": 3, "variant_count": len(variants)},
                "included_runs": [],
                "variants": variants,
                "metric_winners": {},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return summary_dir


def test_recommend_defaults_prefers_sharpe_winner_without_penalty(tmp_path: Path) -> None:
    summary_dir = _write_campaign_summary(
        tmp_path,
        [
            {
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_on",
                "run_count": 3,
                "total_return": 0.05,
                "sharpe": 1.1,
                "max_drawdown": 0.05,
                "turnover": 0.14,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
            {
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_off",
                "run_count": 3,
                "total_return": 0.03,
                "sharpe": 0.8,
                "max_drawdown": 0.05,
                "turnover": 0.12,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
        ],
    )

    result = recommend_experiment_defaults(
        campaign_summary_path=summary_dir,
        output_dir=tmp_path / "decisions",
    )

    assert result["decisions"][0]["overall_recommended_default"] == "regime_on"
    assert result["decisions"][0]["confidence_level"] == "high"


def test_recommend_defaults_prefers_baseline_when_differences_negligible(tmp_path: Path) -> None:
    summary_dir = _write_campaign_summary(
        tmp_path,
        [
            {
                "experiment_name": "campaign_adaptive_on_off",
                "variant_name": "adaptive_on",
                "run_count": 2,
                "total_return": 0.021,
                "sharpe": 0.62,
                "max_drawdown": 0.06,
                "turnover": 0.20,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
            {
                "experiment_name": "campaign_adaptive_on_off",
                "variant_name": "adaptive_off",
                "run_count": 2,
                "total_return": 0.02,
                "sharpe": 0.58,
                "max_drawdown": 0.059,
                "turnover": 0.17,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
        ],
    )

    result = recommend_experiment_defaults(
        campaign_summary_path=summary_dir,
        output_dir=tmp_path / "decisions",
    )

    assert result["decisions"][0]["overall_recommended_default"] == "adaptive_off"
    assert "negligible_sharpe_difference" in result["decisions"][0]["caveats"]


def test_recommend_defaults_marks_insufficient_evidence_and_writes_config(tmp_path: Path) -> None:
    summary_dir = _write_campaign_summary(
        tmp_path,
        [
            {
                "experiment_name": "campaign_governance_strict_vs_loose",
                "variant_name": "governance_strict",
                "run_count": 1,
                "total_return": 0.02,
                "sharpe": 0.7,
                "max_drawdown": 0.05,
                "turnover": 0.10,
                "warning_count": 0,
                "kill_switch_count": 0,
            },
            {
                "experiment_name": "campaign_governance_strict_vs_loose",
                "variant_name": "governance_loose",
                "run_count": 1,
                "total_return": 0.018,
                "sharpe": 0.69,
                "max_drawdown": 0.051,
                "turnover": 0.09,
                "warning_count": 0,
                "kill_switch_count": 0,
            },
        ],
    )
    base_config = tmp_path / "base.yaml"
    base_config.write_text(
        """
run_name: experiment_baseline
schedule_frequency: manual
research_artifacts_root: artifacts
feature_flags:
  regime: true
  adaptive: true
  governance: true
stages:
  regime: true
  adaptive_allocation: true
strategy_governance_policy_config_path: configs/strategy_governance_strict.yaml
""".strip(),
        encoding="utf-8",
    )

    result = recommend_experiment_defaults(
        campaign_summary_path=summary_dir,
        output_dir=tmp_path / "decisions",
        write_config_path=tmp_path / "recommended.yaml",
        base_config_path=base_config,
    )
    written = (tmp_path / "recommended.yaml").read_text(encoding="utf-8")

    assert result["decisions"][0]["confidence_level"] == "low"
    assert "insufficient_evidence:min_run_count_below_2" in result["decisions"][0]["caveats"]
    assert "strategy_governance_loose.yaml" in written


def test_recommend_defaults_cli(tmp_path: Path, capsys) -> None:
    summary_dir = _write_campaign_summary(
        tmp_path,
        [
            {
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_on",
                "run_count": 3,
                "total_return": 0.05,
                "sharpe": 1.1,
                "max_drawdown": 0.05,
                "turnover": 0.14,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
            {
                "experiment_name": "campaign_regime_on_off",
                "variant_name": "regime_off",
                "run_count": 3,
                "total_return": 0.03,
                "sharpe": 0.8,
                "max_drawdown": 0.05,
                "turnover": 0.12,
                "warning_count": 1,
                "kill_switch_count": 0,
            },
        ],
    )

    cmd_experiment_recommend_defaults(
        Namespace(
            summary=str(summary_dir),
            output_dir=str(tmp_path / "decisions"),
            write_config=None,
            base_config=None,
        )
    )

    captured = capsys.readouterr().out
    assert "campaign_regime_on_off:" in captured
    assert "Decision JSON:" in captured
