from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CompletedProcess

from trading_platform.reporting.daily_validation_runner import (
    ALERT_FAILURE_EXIT_CODE,
    LATEST_POINTER_FILENAME,
    SUMMARY_FILENAME,
    detect_validation_config_routing,
    resolve_daily_validation_paths,
    run_daily_validation,
)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_detect_validation_config_routing_for_multi_strategy_filename(tmp_path: Path) -> None:
    config_path = tmp_path / "activated_strategy_portfolio.json"
    config_path.write_text("{}", encoding="utf-8")

    routing = detect_validation_config_routing(config_path)

    assert routing.config_type == "multi_strategy"
    assert routing.paper_command_used == "run-multi-strategy"
    assert routing.execution_mode == "cli_subprocess"


def test_detect_validation_config_routing_for_workflow_json(tmp_path: Path) -> None:
    config_path = tmp_path / "paper_workflow.json"
    config_path.write_text(json.dumps({"preset": "demo", "strategy": "sma_cross"}), encoding="utf-8")

    routing = detect_validation_config_routing(config_path)

    assert routing.config_type == "workflow"
    assert routing.paper_command_used == "run"


def test_run_daily_validation_success_for_workflow_config(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "validation"
    report_json = "daily_system_report.json"
    report_md = "daily_system_report.md"
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo", "strategy": "sma_cross"}), encoding="utf-8")
    fixed_now = datetime(2026, 4, 1, 9, 30, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._now_local", lambda: fixed_now)

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if "paper" in command and "run" in command:
            assert "run-multi-strategy" not in command
            return CompletedProcess(command, 0)
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text(json.dumps({"report_date": "2026-04-01"}), encoding="utf-8")
            report_md_path = Path(command[command.index("--output-md") + 1])
            report_md_path.write_text("# Daily System Report\n", encoding="utf-8")
            return CompletedProcess(command, 0)
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=artifact_root,
        report_json=report_json,
        report_md=report_md,
        strict_report=True,
    )

    assert exit_code == 0
    assert summary["overall_status"] == "success"
    assert summary["paper_run_exit_status"] == 0
    assert summary["report_exit_status"] == 0
    assert summary["warnings"] == []
    assert summary["config_type"] == "workflow"
    assert summary["paper_command_used"] == "run"
    assert summary["execution_mode"] == "cli_subprocess"

    summary_path = artifact_root / SUMMARY_FILENAME
    assert summary_path.exists()
    persisted = _read_json(summary_path)
    assert persisted == summary
    assert persisted["report_json_path"] == str(artifact_root / report_json)
    assert persisted["report_md_path"] == str(artifact_root / report_md)
    assert persisted["alerting"]["alerting_enabled"] is False
    assert persisted["alerting"]["alert_evaluated"] is False


def test_run_daily_validation_success_for_multi_strategy_config(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "activated_strategy_portfolio.json"
    config_path.write_text(
        json.dumps(
            {
                "summary": {"active_row_count": 1},
                "active_strategies": [{"preset_name": "generated_a", "is_active": True}],
                "strategies": [{"preset_name": "generated_a", "is_active": True}],
            }
        ),
        encoding="utf-8",
    )

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if "paper" in command:
            assert "run-multi-strategy" in command
            return CompletedProcess(command, 0)
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text("{}", encoding="utf-8")
            return CompletedProcess(command, 0)
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
    )

    assert exit_code == 0
    assert summary["config_type"] == "multi_strategy"
    assert summary["paper_command_used"] == "run-multi-strategy"
    assert summary["execution_mode"] == "cli_subprocess"


def test_run_daily_validation_handles_paper_run_failure(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")

    monkeypatch.setattr(
        "trading_platform.reporting.daily_validation_runner._run_command",
        lambda command: CompletedProcess(command, 7),
    )

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
    )

    assert exit_code == 1
    assert summary["overall_status"] == "paper_run_failed"
    assert summary["paper_run_exit_status"] == 7
    assert summary["report_exit_status"] is None
    assert summary["warnings"] == ["paper run failed; daily system report was not executed"]
    assert summary["paper_command_used"] == "run"
    assert summary["config_type"] == "workflow"


def test_run_daily_validation_strict_report_failure(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")
    calls: list[list[str]] = []

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        calls.append(command)
        if len(calls) == 1:
            return CompletedProcess(command, 0)
        return CompletedProcess(command, 3)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
    )

    assert exit_code == 1
    assert summary["overall_status"] == "report_failed"
    assert summary["paper_run_exit_status"] == 0
    assert summary["report_exit_status"] == 3
    assert summary["warnings"] == ["daily system report failed in strict mode"]


def test_run_daily_validation_non_strict_report_failure(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")
    calls: list[list[str]] = []

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        calls.append(command)
        return CompletedProcess(command, 0 if len(calls) == 1 else 5)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=False,
    )

    assert exit_code == 0
    assert summary["overall_status"] == "success_with_warnings"
    assert summary["warnings"] == ["daily system report failed in non-strict mode"]


def test_run_daily_validation_timestamped_output_writes_latest_pointer(monkeypatch, tmp_path: Path) -> None:
    fixed_now = datetime(2026, 4, 1, 9, 30, 0, tzinfo=timezone.utc)
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")
    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._now_local", lambda: fixed_now)

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text("{}", encoding="utf-8")
        return CompletedProcess(command, 0)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    output_root = tmp_path / "validation"
    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=output_root,
        report_json="daily_system_report.json",
        strict_report=True,
        timestamp_run_dir=True,
    )

    assert exit_code == 0
    run_dir = output_root / "2026-04-01T09-30-00"
    assert summary["artifact_dir"] == str(run_dir)
    assert (run_dir / SUMMARY_FILENAME).exists()
    latest_pointer = output_root / LATEST_POINTER_FILENAME
    assert latest_pointer.exists()
    latest_payload = _read_json(latest_pointer)
    assert latest_payload["artifact_dir"] == str(run_dir)
    assert latest_payload["summary_path"] == str(run_dir / SUMMARY_FILENAME)


def test_run_daily_validation_with_alerting_enabled_and_no_trigger(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text(json.dumps({"report_date": "2026-04-01"}), encoding="utf-8")
        return CompletedProcess(command, 0)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)
    monkeypatch.setattr(
        "trading_platform.reporting.daily_validation_runner.send_validation_alerts",
        lambda **kwargs: (
            0,
            {
                "alert_triggered": False,
                "alert_types": [],
                "sent": False,
                "mode": "no_trigger",
                "timestamp": "2026-04-01T09:30:00+00:00",
            },
            None,
        ),
    )

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
        send_alerts=True,
    )

    assert exit_code == 0
    assert summary["overall_status"] == "success"
    assert summary["alerting"]["alerting_enabled"] is True
    assert summary["alerting"]["alert_evaluated"] is True
    assert summary["alerting"]["alert_triggered"] is False
    assert summary["alerting"]["alert_mode"] == "no_trigger"


def test_run_daily_validation_with_alerting_enabled_and_trigger(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text(json.dumps({"report_date": "2026-04-01"}), encoding="utf-8")
        return CompletedProcess(command, 0)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)
    monkeypatch.setattr(
        "trading_platform.reporting.daily_validation_runner.send_validation_alerts",
        lambda **kwargs: (
            0,
            {
                "alert_triggered": True,
                "alert_types": ["daily_concerning_status"],
                "sent": True,
                "mode": "send",
                "timestamp": "2026-04-01T09:30:00+00:00",
            },
            "body",
        ),
    )

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
        send_alerts=True,
    )

    assert exit_code == 0
    assert summary["alerting"]["alert_triggered"] is True
    assert summary["alerting"]["alert_types"] == ["daily_concerning_status"]
    assert summary["alerting"]["alert_sent"] is True
    assert summary["alerting"]["alert_mode"] == "send"


def test_run_daily_validation_records_alerting_failure(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")

    def _fake_run(command: list[str]) -> CompletedProcess[str]:
        if any(str(item).endswith("daily_system_report.py") for item in command):
            report_json_path = Path(command[command.index("--output-json") + 1])
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_path.write_text(json.dumps({"report_date": "2026-04-01"}), encoding="utf-8")
        return CompletedProcess(command, 0)

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner._run_command", _fake_run)

    def _raise_alert_error(**kwargs):
        raise RuntimeError("smtp unavailable")

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner.send_validation_alerts", _raise_alert_error)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
        send_alerts=True,
    )

    assert exit_code == ALERT_FAILURE_EXIT_CODE
    assert summary["overall_status"] == "success"
    assert summary["alerting"]["alerting_enabled"] is True
    assert summary["alerting"]["alert_evaluated"] is True
    assert summary["alerting"]["alert_mode"] == "error"
    assert summary["alerting"]["alert_error"] == "smtp unavailable"


def test_run_daily_validation_paper_failure_still_evaluates_alerting(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "paper_config.json"
    config_path.write_text(json.dumps({"preset": "demo"}), encoding="utf-8")
    called: dict[str, object] = {}

    monkeypatch.setattr(
        "trading_platform.reporting.daily_validation_runner._run_command",
        lambda command: CompletedProcess(command, 9),
    )

    def _fake_alerts(**kwargs):
        called.update(kwargs)
        return (
            0,
            {
                "alert_triggered": True,
                "alert_types": ["run_failure"],
                "sent": False,
                "mode": "no_send",
                "timestamp": "2026-04-01T09:30:00+00:00",
            },
            "body",
        )

    monkeypatch.setattr("trading_platform.reporting.daily_validation_runner.send_validation_alerts", _fake_alerts)

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
        send_alerts=True,
        alert_no_send=True,
    )

    assert exit_code == 1
    assert summary["overall_status"] == "paper_run_failed"
    assert summary["alerting"]["alert_triggered"] is True
    assert summary["alerting"]["alert_types"] == ["run_failure"]
    assert called["daily_report_path"] is None
    assert Path(str(called["daily_run_summary_path"])).exists()


def test_run_daily_validation_fails_when_config_type_is_unknown(tmp_path: Path) -> None:
    config_path = tmp_path / "ambiguous.json"
    config_path.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")

    exit_code, summary = run_daily_validation(
        config_path=config_path,
        state_path=tmp_path / "paper_state.json",
        output_dir=tmp_path / "validation",
        report_json="daily_system_report.json",
        strict_report=True,
    )

    assert exit_code == 1
    assert summary["overall_status"] == "config_detection_failed"
    assert summary["paper_run_exit_status"] is None
    assert summary["report_exit_status"] is None
    assert summary["config_type"] is None
    assert summary["paper_command_used"] is None
    assert "Unable to determine validation config type" in summary["warnings"][0]


def test_resolve_daily_validation_paths_keeps_relative_reports_under_artifact_dir(tmp_path: Path) -> None:
    paths = resolve_daily_validation_paths(
        output_dir=tmp_path / "validation",
        report_json="reports/daily_system_report.json",
        report_md="reports/daily_system_report.md",
        timestamp_run_dir=False,
    )

    assert paths.report_json_path == tmp_path / "validation" / "reports" / "daily_system_report.json"
    assert paths.report_md_path == tmp_path / "validation" / "reports" / "daily_system_report.md"
