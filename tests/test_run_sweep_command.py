from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.run_sweep import cmd_run_sweep
from trading_platform.config.models import ParameterSweepConfig


def make_sweep_config() -> ParameterSweepConfig:
    return ParameterSweepConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast_values=[10, 20],
        slow_values=[50, 100],
        cash=10000,
        commission=0.001,
    )


def test_cmd_run_sweep_executes_and_saves_artifacts(monkeypatch) -> None:
    fake_config = make_sweep_config()
    captured: dict[str, object] = {}

    def fake_load(path):
        captured["config_path"] = path
        return fake_config

    def fake_run_parameter_sweep(*, config, continue_on_error):
        captured["continue_on_error"] = continue_on_error
        return {
            "config": {"symbol": "AAPL"},
            "results": [{"fast": 10, "slow": 50}],
            "errors": [],
            "leaderboard": pd.DataFrame(
                [
                    {
                        "fast": 10,
                        "slow": 50,
                        "lookback": None,
                        "return_pct": 12.0,
                        "sharpe_ratio": 1.1,
                        "max_drawdown_pct": -8.0,
                        "experiment_id": "exp-1",
                    }
                ]
            ),
        }

    def fake_make_sweep_artifact_stem():
        return "sweep_test"

    def fake_save_sweep_leaderboard_csv(leaderboard, stem):
        captured["leaderboard_stem"] = stem
        return Path("artifacts/jobs/sweep_test.leaderboard.csv")

    def fake_save_sweep_summary_json(payload, stem):
        captured["summary_payload"] = payload
        captured["summary_stem"] = stem
        return Path("artifacts/jobs/sweep_test.json")

    monkeypatch.setattr(
        "trading_platform.cli.commands.run_sweep.load_parameter_sweep_config",
        fake_load,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_sweep.run_parameter_sweep",
        fake_run_parameter_sweep,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_sweep.make_sweep_artifact_stem",
        fake_make_sweep_artifact_stem,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_sweep.save_sweep_leaderboard_csv",
        fake_save_sweep_leaderboard_csv,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_sweep.save_sweep_summary_json",
        fake_save_sweep_summary_json,
    )

    args = argparse.Namespace(
        config="configs/sweeps/test.yaml",
        fail_fast=False,
    )

    cmd_run_sweep(args)

    assert captured["config_path"] == "configs/sweeps/test.yaml"
    assert captured["continue_on_error"] is True
    assert captured["leaderboard_stem"] == "sweep_test"
    assert captured["summary_stem"] == "sweep_test"