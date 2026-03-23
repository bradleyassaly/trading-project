from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.strategy_validation_build import cmd_strategy_validation_build
from trading_platform.research.registry import write_research_run_manifest
from trading_platform.research.strategy_validation import (
    StrategyValidationPolicyConfig,
    build_strategy_validation,
    load_strategy_validation,
)


def _write_research_run(
    root: Path,
    *,
    run_name: str,
    signal_family: str,
    universe: str,
    mean_spearman_ic: float,
    portfolio_sharpe: float,
    fold_values: list[float],
) -> None:
    run_dir = root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = run_dir / "leaderboard.csv"
    fold_results_path = run_dir / "fold_results.csv"
    pd.DataFrame(
        [
            {
                "signal_family": signal_family,
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": mean_spearman_ic,
                "mean_hit_rate": 0.55,
                "mean_turnover": 0.12,
                "promotion_status": "promote",
                "rejection_reason": "",
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    pd.DataFrame(
        [{"fold_id": idx + 1, "spearman_ic": value} for idx, value in enumerate(fold_values)]
    ).to_csv(fold_results_path, index=False)
    manifest_path = write_research_run_manifest(
        output_dir=run_dir,
        workflow_type="alpha_research",
        command="test",
        feature_dir=root / "features",
        signal_family=signal_family,
        universe=universe,
        symbols_requested=["AAPL", "MSFT"],
        lookbacks=[20],
        horizons=[5],
        min_rows=250,
        train_size=756,
        test_size=63,
        step_size=63,
        min_train_size=252,
        artifact_paths={
            "leaderboard_path": leaderboard_path,
            "fold_results_path": fold_results_path,
        },
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["top_metrics"] = {
        "mean_spearman_ic": mean_spearman_ic,
        "portfolio_sharpe": portfolio_sharpe,
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_strategy_validation_build_computes_statuses(tmp_path: Path) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.03,
        portfolio_sharpe=1.1,
        fold_values=[0.03, 0.04, 0.02, 0.05],
    )
    _write_research_run(
        tmp_path,
        run_name="run_b",
        signal_family="value",
        universe="sp500",
        mean_spearman_ic=0.0,
        portfolio_sharpe=0.1,
        fold_values=[-0.01, 0.0, 0.01],
    )

    result = build_strategy_validation(
        artifacts_root=tmp_path,
        output_dir=tmp_path / "validation",
        policy=StrategyValidationPolicyConfig(),
    )
    payload = load_strategy_validation(tmp_path / "validation")

    assert result["pass_count"] == 1
    assert result["weak_count"] >= 0
    assert payload["rows"][0]["run_id"] == "run_a"
    assert payload["rows"][0]["validation_status"] == "pass"
    assert (tmp_path / "validation" / "strategy_validation.csv").exists()


def test_strategy_validation_cli_writes_outputs(tmp_path: Path, capsys) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.03,
        portfolio_sharpe=1.1,
        fold_values=[0.03, 0.04, 0.02, 0.05],
    )

    cmd_strategy_validation_build(
        Namespace(
            artifacts_root=str(tmp_path),
            policy_config=None,
            output_dir=str(tmp_path / "validation"),
        )
    )

    captured = capsys.readouterr().out
    assert "Strategy validation JSON" in captured
    assert (tmp_path / "validation" / "strategy_validation.json").exists()
