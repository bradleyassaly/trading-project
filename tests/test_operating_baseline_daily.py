from __future__ import annotations

import json
from pathlib import Path

from trading_platform.system.operating_baseline_daily import run_operating_baseline_daily


class _Record:
    def __init__(self, stage_name: str, status: str, outputs: dict[str, object] | None = None) -> None:
        self.stage_name = stage_name
        self.status = status
        self.outputs = outputs or {}
        self.warnings: list[str] = []
        self.error_message = None


class _Result:
    def __init__(self, run_dir: Path) -> None:
        self.run_id = "2026-03-23T23-30-00+00-00"
        self.run_name = "operating_baseline"
        self.status = "succeeded"
        self.run_dir = str(run_dir)
        self.warnings = ["monitoring:review"]
        self.outputs = {
            "promoted_strategy_count": 1,
            "selected_strategy_count": 1,
            "warning_strategy_count": 2,
        }
        self.stage_records = [
            _Record("paper", "succeeded", {"paper_order_count": 3}),
            _Record("monitoring", "succeeded", {"warning_strategy_count": 2}),
        ]


class _Config:
    def __init__(self, output_root_dir: Path) -> None:
        self.output_root_dir = str(output_root_dir)
        self.run_name = "operating_baseline"


def test_run_operating_baseline_daily_writes_summary(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "orchestration_operating_baseline.yaml"
    config_path.write_text("run_name: operating_baseline\nschedule_frequency: manual\nresearch_artifacts_root: artifacts\n", encoding="utf-8")
    output_root = tmp_path / "orchestration_runs_operating_baseline"
    run_dir = output_root / "operating_baseline" / "2026-03-23T23-30-00+00-00"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_dir = tmp_path / "daily_summary"
    system_eval_output_dir = output_root / "system_eval_history"

    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.load_automated_orchestration_config",
        lambda path: _Config(output_root),
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.run_automated_orchestration",
        lambda config: (
            _Result(run_dir),
            {
                "orchestration_run_json_path": run_dir / "orchestration_run.json",
                "orchestration_run_md_path": run_dir / "orchestration_run.md",
            },
        ),
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.build_system_evaluation_history",
        lambda *, runs_root, output_dir: {
            "run_count": 4,
            "system_evaluation_json_path": str(system_eval_output_dir / "system_evaluation.json"),
            "system_evaluation_history_json_path": str(system_eval_output_dir / "system_evaluation_history.json"),
            "system_evaluation_history_csv_path": str(system_eval_output_dir / "system_evaluation_history.csv"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.load_system_evaluation",
        lambda path: {
            "row": {"turnover": 0.12},
            "metrics": {
                "total_return": None,
                "volatility": None,
                "sharpe": None,
                "max_drawdown": None,
                "observation_count": 1,
                "return_observation_count": 0,
            },
            "history_metrics": {
                "total_return": 0.0,
                "volatility": None,
                "sharpe": None,
                "max_drawdown": 0.0,
                "observation_count": 5,
                "return_observation_count": 4,
            },
            "diagnostic": {
                "metric_warnings": ["insufficient_history_for_sharpe"],
                "history_metric_warnings": ["flat_equity_curve"],
            },
        },
    )

    summary = run_operating_baseline_daily(
        config_path=config_path,
        summary_dir=summary_dir,
        log_path=tmp_path / "logs" / "2026-03-23.log",
    )

    summary_json = json.loads((summary_dir / "daily_baseline_summary.json").read_text(encoding="utf-8"))
    summary_md = (summary_dir / "daily_baseline_summary.md").read_text(encoding="utf-8")

    assert summary["run_status"] == "succeeded"
    assert summary_json["promoted_strategy_count"] == 1
    assert summary_json["selected_strategy_count"] == 1
    assert summary_json["paper_order_count"] == 3
    assert summary_json["monitoring_warning_count"] == 2
    assert summary_json["system_evaluation"]["history_metrics"]["total_return"] == 0.0
    assert summary_json["paths"]["daily_baseline_summary_json_path"].endswith("daily_baseline_summary.json")
    assert "Daily Operating Baseline Summary" in summary_md


def test_run_operating_baseline_daily_persists_alert_results(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "orchestration_operating_baseline.yaml"
    alerts_config_path = tmp_path / "alerts.yaml"
    config_path.write_text("run_name: operating_baseline\nschedule_frequency: manual\nresearch_artifacts_root: artifacts\n", encoding="utf-8")
    alerts_config_path.write_text(
        "email_enabled: false\nsms_enabled: false\nsend_daily_success_summary: true\nsend_on_monitoring_warnings: false\n",
        encoding="utf-8",
    )
    output_root = tmp_path / "orchestration_runs_operating_baseline"
    run_dir = output_root / "operating_baseline" / "2026-03-23T23-30-00+00-00"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_dir = tmp_path / "daily_summary"
    system_eval_output_dir = output_root / "system_eval_history"

    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.load_automated_orchestration_config",
        lambda path: _Config(output_root),
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.run_automated_orchestration",
        lambda config: (
            _Result(run_dir),
            {
                "orchestration_run_json_path": run_dir / "orchestration_run.json",
                "orchestration_run_md_path": run_dir / "orchestration_run.md",
            },
        ),
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.build_system_evaluation_history",
        lambda *, runs_root, output_dir: {
            "run_count": 4,
            "system_evaluation_json_path": str(system_eval_output_dir / "system_evaluation.json"),
            "system_evaluation_history_json_path": str(system_eval_output_dir / "system_evaluation_history.json"),
            "system_evaluation_history_csv_path": str(system_eval_output_dir / "system_evaluation_history.csv"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.system.operating_baseline_daily.load_system_evaluation",
        lambda path: {
            "row": {"turnover": 0.12},
            "metrics": {"total_return": None, "volatility": None, "sharpe": None, "max_drawdown": None, "observation_count": 1, "return_observation_count": 0},
            "history_metrics": {"total_return": 0.0, "volatility": None, "sharpe": None, "max_drawdown": 0.0, "observation_count": 5, "return_observation_count": 4},
            "diagnostic": {"metric_warnings": [], "history_metric_warnings": []},
        },
    )

    summary = run_operating_baseline_daily(
        config_path=config_path,
        summary_dir=summary_dir,
        alerts_config_path=alerts_config_path,
    )

    assert summary["alerts"]["alert_count"] == 1
    assert (summary_dir / "daily_alerts.json").exists()
