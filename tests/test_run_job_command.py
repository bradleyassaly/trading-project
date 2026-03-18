from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.cli.commands.run_job import cmd_run_job
from trading_platform.config.models import ResearchWorkflowConfig


def make_config() -> ResearchWorkflowConfig:
    return ResearchWorkflowConfig(
        symbol="AAPL",
        start="2024-01-01",
        interval="1d",
        strategy="sma_cross",
        fast=20,
        slow=50,
        cash=10000,
        commission=0.001,
    )


def test_cmd_run_job_uses_config_symbol_when_no_override(monkeypatch) -> None:
    fake_config = make_config()
    captured: dict[str, object] = {}

    def fake_load(path):
        captured["config_path"] = path
        return fake_config

    def fake_run_universe_research_workflow(*, symbols, base_config, continue_on_error):
        captured["symbols"] = symbols
        captured["base_config"] = base_config
        captured["continue_on_error"] = continue_on_error
        return {"results": {}, "errors": {}}

    def fake_make_job_artifact_stem():
        return "job_test"

    def fake_build_universe_leaderboard(outputs):
        captured["leaderboard_outputs"] = outputs
        class DummyLeaderboard:
            pass
        return DummyLeaderboard()

    def fake_save_leaderboard_csv(leaderboard, stem):
        captured["leaderboard_stem"] = stem
        return Path("artifacts/jobs/job_test.leaderboard.csv")

    def fake_build_job_summary(*, config, symbols, outputs, leaderboard_csv_path):
        captured["summary_inputs"] = {
            "config": config,
            "symbols": symbols,
            "outputs": outputs,
            "leaderboard_csv_path": leaderboard_csv_path,
        }
        return {"ok": True}

    def fake_save_job_summary(summary, stem):
        captured["summary_stem"] = stem
        captured["summary"] = summary
        return Path("artifacts/jobs/job_test.json")

    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.load_research_workflow_config",
        fake_load,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.run_universe_research_workflow",
        fake_run_universe_research_workflow,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.make_job_artifact_stem",
        fake_make_job_artifact_stem,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.build_universe_leaderboard",
        fake_build_universe_leaderboard,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.save_leaderboard_csv",
        fake_save_leaderboard_csv,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.build_job_summary",
        fake_build_job_summary,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.save_job_summary",
        fake_save_job_summary,
    )

    args = argparse.Namespace(
        config="configs/research/test.yaml",
        symbols=None,
        fail_fast=False,
    )

    cmd_run_job(args)

    assert captured["config_path"] == "configs/research/test.yaml"
    assert captured["symbols"] == ["AAPL"]
    assert captured["continue_on_error"] is True
    assert captured["leaderboard_stem"] == "job_test"
    assert captured["summary_stem"] == "job_test"


def test_cmd_run_job_uses_symbol_override(monkeypatch) -> None:
    fake_config = make_config()
    captured: dict[str, object] = {}

    def fake_load(path):
        return fake_config

    def fake_run_universe_research_workflow(*, symbols, base_config, continue_on_error):
        captured["symbols"] = symbols
        return {"results": {}, "errors": {}}

    def fake_make_job_artifact_stem():
        return "job_test"

    def fake_build_universe_leaderboard(outputs):
        class DummyLeaderboard:
            pass
        return DummyLeaderboard()

    def fake_save_leaderboard_csv(leaderboard, stem):
        return Path("artifacts/jobs/job_test.leaderboard.csv")

    def fake_build_job_summary(*, config, symbols, outputs, leaderboard_csv_path):
        return {"ok": True}

    def fake_save_job_summary(summary, stem):
        return Path("artifacts/jobs/job_test.json")

    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.load_research_workflow_config",
        fake_load,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.run_universe_research_workflow",
        fake_run_universe_research_workflow,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.make_job_artifact_stem",
        fake_make_job_artifact_stem,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.build_universe_leaderboard",
        fake_build_universe_leaderboard,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.save_leaderboard_csv",
        fake_save_leaderboard_csv,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.build_job_summary",
        fake_build_job_summary,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.run_job.save_job_summary",
        fake_save_job_summary,
    )

    args = argparse.Namespace(
        config="configs/research/test.yaml",
        symbols=["MSFT", "NVDA"],
        fail_fast=False,
    )

    cmd_run_job(args)

    assert captured["symbols"] == ["MSFT", "NVDA"]