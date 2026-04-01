from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_platform.reporting.validation_alerting import (
    load_validation_notification_config,
    main,
    resolve_smtp_config,
    send_validation_alerts,
)


class FakeSMTP:
    sent_messages = []

    def __init__(self, host, port, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def starttls(self):
        return None

    def login(self, username, password):
        return None

    def send_message(self, message):
        self.__class__.sent_messages.append(message)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _daily_run_summary(*, overall_status: str = "success", paper_exit: int = 0, report_exit: int = 0) -> dict:
    return {
        "run_timestamp": "2026-04-01T09:30:00+00:00",
        "config_path": "config.json",
        "state_path": "state.json",
        "artifact_dir": "artifacts/paper/run_live_validation/2026-04-01T09-30-00",
        "report_json_path": "artifacts/paper/run_live_validation/2026-04-01T09-30-00/daily_system_report.json",
        "report_md_path": None,
        "paper_run_exit_status": paper_exit,
        "report_exit_status": report_exit,
        "paper_command_used": "run-multi-strategy",
        "config_type": "multi_strategy",
        "execution_mode": "cli_subprocess",
        "warnings": [],
        "overall_status": overall_status,
    }


def _daily_report(*, overall_status: str = "healthy", extra_concerning_flags: int = 0) -> dict:
    flags = {
        "ev_alignment_flag": "healthy",
        "calibration_signal_flag": "healthy",
        "drift_noise_flag": "healthy",
        "decay_churn_flag": "healthy",
        "lifecycle_churn_flag": "healthy",
        "overall_status": overall_status,
    }
    keys = ["ev_alignment_flag", "drift_noise_flag", "lifecycle_churn_flag"]
    for key in keys[:extra_concerning_flags]:
        flags[key] = "concerning"
    return {
        "report_date": "2026-04-01T09:30:00+00:00",
        "warnings": [],
        "evaluation_flags": flags,
        "risk_summary": {"risk_control_state": "healthy"},
    }


def _window_review(*, overall_status: str = "healthy", next_step: str = "continue_validation", concerning_count: int = 0) -> dict:
    checkpoint = {
        "ev_alignment_status": "healthy",
        "calibration_usefulness_status": "healthy",
        "drift_signal_quality_status": "healthy",
        "decay_signal_quality_status": "healthy",
        "lifecycle_churn_status": "healthy",
        "risk_control_status": "healthy",
        "overall_validation_status": overall_status,
        "recommended_next_step": next_step,
    }
    keys = ["ev_alignment_status", "drift_signal_quality_status", "risk_control_status"]
    for key in keys[:concerning_count]:
        checkpoint[key] = "concerning"
    return {"evaluation_checkpoint": checkpoint}


def test_send_validation_alerts_run_failure_case(tmp_path: Path) -> None:
    summary_path = tmp_path / "daily_validation_run_summary.json"
    _write_json(summary_path, _daily_run_summary(overall_status="paper_run_failed", paper_exit=2))
    decision_path = tmp_path / "validation_alert_decision.json"

    exit_code, artifact, body = send_validation_alerts(
        daily_run_summary_path=summary_path,
        decision_output_path=decision_path,
        no_send=True,
    )

    assert exit_code == 0
    assert artifact["alert_triggered"] is True
    assert artifact["alert_type"] == "run_failure"
    assert "paper_run_exit_status=2" in artifact["decision_reasons"]
    assert body is not None
    assert decision_path.exists()


def test_send_validation_alerts_daily_concerning_case(tmp_path: Path) -> None:
    report_path = tmp_path / "daily_system_report.json"
    _write_json(report_path, _daily_report(overall_status="concerning", extra_concerning_flags=2))

    exit_code, artifact, body = send_validation_alerts(
        daily_report_path=report_path,
        no_send=True,
    )

    assert exit_code == 0
    assert artifact["alert_triggered"] is True
    assert artifact["alert_type"] == "daily_concerning_status"
    assert "overall_status=concerning" in artifact["decision_reasons"]
    assert body is not None


def test_send_validation_alerts_window_concerning_case(tmp_path: Path) -> None:
    review_path = tmp_path / "validation_window_review.json"
    _write_json(review_path, _window_review(overall_status="concerning", next_step="inspect_execution", concerning_count=2))

    exit_code, artifact, _body = send_validation_alerts(
        window_review_path=review_path,
        no_send=True,
    )

    assert exit_code == 0
    assert artifact["alert_triggered"] is True
    assert artifact["alert_type"] == "window_concerning_status"
    assert "recommended_next_step=inspect_execution" in artifact["decision_reasons"]


def test_send_validation_alerts_no_alert_for_healthy_case(tmp_path: Path) -> None:
    summary_path = tmp_path / "daily_validation_run_summary.json"
    report_path = tmp_path / "daily_system_report.json"
    review_path = tmp_path / "validation_window_review.json"
    _write_json(summary_path, _daily_run_summary())
    _write_json(report_path, _daily_report())
    _write_json(review_path, _window_review())

    exit_code, artifact, body = send_validation_alerts(
        daily_run_summary_path=summary_path,
        daily_report_path=report_path,
        window_review_path=review_path,
        no_send=True,
    )

    assert exit_code == 0
    assert artifact["alert_triggered"] is False
    assert artifact["triggered_alert_count"] == 0
    assert body is None


def test_send_validation_alerts_dedupe_skips_repeat_send(tmp_path: Path) -> None:
    FakeSMTP.sent_messages = []
    report_path = tmp_path / "daily_system_report.json"
    registry_path = tmp_path / "validation_alert_registry.json"
    _write_json(report_path, _daily_report(overall_status="concerning", extra_concerning_flags=2))

    kwargs = {
        "daily_report_path": report_path,
        "registry_path": registry_path,
        "smtp_host": "smtp.example.com",
        "smtp_port": 587,
        "from_address": "alerts@example.com",
        "to_addresses": ["ops@example.com"],
        "smtp_client_factory": FakeSMTP,
    }
    first_exit, first_artifact, _ = send_validation_alerts(**kwargs)
    second_exit, second_artifact, _ = send_validation_alerts(**kwargs)

    assert first_exit == 0
    assert second_exit == 0
    assert first_artifact["alert_triggered"] is True
    assert second_artifact["alert_triggered"] is False
    assert len(FakeSMTP.sent_messages) == 1


def test_send_validation_alerts_dry_run_and_no_send_do_not_touch_registry(tmp_path: Path) -> None:
    report_path = tmp_path / "daily_system_report.json"
    registry_path = tmp_path / "validation_alert_registry.json"
    _write_json(report_path, _daily_report(overall_status="concerning", extra_concerning_flags=2))

    dry_exit, dry_artifact, dry_body = send_validation_alerts(
        daily_report_path=report_path,
        registry_path=registry_path,
        dry_run=True,
    )
    no_send_exit, no_send_artifact, no_send_body = send_validation_alerts(
        daily_report_path=report_path,
        registry_path=registry_path,
        no_send=True,
    )

    assert dry_exit == 0
    assert no_send_exit == 0
    assert dry_artifact["mode"] == "dry_run"
    assert no_send_artifact["mode"] == "no_send"
    assert dry_body is not None
    assert no_send_body is not None
    assert not registry_path.exists()


def test_load_validation_notification_config_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("TP_ALERT_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PORT", "587")
    monkeypatch.setenv("TP_ALERT_SMTP_USERNAME", "alerts@example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PASSWORD", "secret")
    monkeypatch.setenv("TP_ALERT_SMTP_USE_TLS", "true")
    monkeypatch.setenv("TP_ALERT_FROM", "alerts@example.com")
    monkeypatch.setenv("TP_ALERT_TO", "ops1@example.com,ops2@example.com")

    config = load_validation_notification_config()

    assert config.smtp_host == "smtp.example.com"
    assert config.smtp_port == 587
    assert config.smtp_username == "alerts@example.com"
    assert config.smtp_password == "secret"
    assert config.smtp_use_tls is True
    assert config.from_address == "alerts@example.com"
    assert config.channels[0].recipients == ["ops1@example.com", "ops2@example.com"]


def test_resolve_smtp_config_cli_overrides_environment(monkeypatch) -> None:
    monkeypatch.setenv("TP_ALERT_SMTP_HOST", "smtp.env.example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PORT", "2525")
    monkeypatch.setenv("TP_ALERT_FROM", "env@example.com")
    monkeypatch.setenv("TP_ALERT_TO", "env1@example.com,env2@example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_USE_TLS", "false")

    resolved = resolve_smtp_config(
        smtp_host="smtp.cli.example.com",
        smtp_port=587,
        from_address="cli@example.com",
        to_addresses=["cli@example.com"],
        smtp_use_tls=True,
    )

    assert resolved.config.smtp_host == "smtp.cli.example.com"
    assert resolved.config.smtp_port == 587
    assert resolved.config.from_address == "cli@example.com"
    assert resolved.config.channels[0].recipients == ["cli@example.com"]
    assert resolved.config.smtp_use_tls is True


def test_resolve_smtp_config_missing_required_fields_raises_clear_error(monkeypatch) -> None:
    monkeypatch.delenv("TP_ALERT_SMTP_HOST", raising=False)
    monkeypatch.delenv("TP_ALERT_SMTP_PORT", raising=False)
    monkeypatch.delenv("TP_ALERT_FROM", raising=False)
    monkeypatch.delenv("TP_ALERT_TO", raising=False)

    with pytest.raises(ValueError, match="Missing SMTP configuration fields:"):
        resolve_smtp_config()


def test_resolve_smtp_config_parses_tls_false_and_multiple_recipients(monkeypatch) -> None:
    monkeypatch.setenv("TP_ALERT_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PORT", "587")
    monkeypatch.setenv("TP_ALERT_FROM", "alerts@example.com")
    monkeypatch.setenv("TP_ALERT_TO", "ops1@example.com, ops2@example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_USE_TLS", "no")

    resolved = resolve_smtp_config()

    assert resolved.config.smtp_use_tls is False
    assert resolved.config.channels[0].recipients == ["ops1@example.com", "ops2@example.com"]


def test_send_validation_alerts_decision_artifact_is_deterministic(tmp_path: Path) -> None:
    report_path = tmp_path / "daily_system_report.json"
    decision_path = tmp_path / "validation_alert_decision.json"
    _write_json(report_path, _daily_report(overall_status="concerning", extra_concerning_flags=2))

    exit_code, artifact, _ = send_validation_alerts(
        daily_report_path=report_path,
        decision_output_path=decision_path,
        no_send=True,
    )

    assert exit_code == 0
    assert json.loads(decision_path.read_text(encoding="utf-8")) == artifact
    assert artifact["timestamp"]
    assert artifact["email_subject"]
    assert "smtp_config_summary" in artifact
    assert artifact["smtp_config_summary"] is None


def test_send_validation_alerts_validation_root_mode_uses_latest_successful_run(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    failed_dir = root / "2026-04-01T09-30-00"
    failed_dir.mkdir(parents=True)
    _write_json(failed_dir / "daily_validation_run_summary.json", _daily_run_summary(overall_status="paper_run_failed", paper_exit=2))
    _write_json(failed_dir / "daily_system_report.json", _daily_report(overall_status="concerning", extra_concerning_flags=2))

    valid_dir = root / "2026-04-02T09-30-00"
    valid_dir.mkdir(parents=True)
    summary = _daily_run_summary()
    summary["run_timestamp"] = "2026-04-02T09:30:00+00:00"
    summary["artifact_dir"] = str(valid_dir)
    summary["report_json_path"] = str(valid_dir / "daily_system_report.json")
    _write_json(valid_dir / "daily_validation_run_summary.json", summary)
    _write_json(valid_dir / "daily_system_report.json", _daily_report(overall_status="concerning", extra_concerning_flags=2))

    exit_code, artifact, body = send_validation_alerts(
        validation_root=root,
        latest_successful_run=True,
        no_send=True,
    )

    assert exit_code == 0
    assert artifact["alert_triggered"] is True
    assert artifact["alert_type"] == "daily_concerning_status"
    assert body is not None


def test_send_validation_alerts_rejects_mixed_explicit_and_root_modes(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    summary_path = tmp_path / "daily_validation_run_summary.json"
    _write_json(summary_path, _daily_run_summary())

    with pytest.raises(ValueError, match="Cannot combine --validation-root"):
        send_validation_alerts(
            validation_root=root,
            latest_successful_run=True,
            daily_run_summary_path=summary_path,
            no_send=True,
        )


def test_main_dry_run_prints_resolved_smtp_config_without_password(monkeypatch, tmp_path: Path, capsys) -> None:
    report_path = tmp_path / "daily_system_report.json"
    _write_json(report_path, _daily_report(overall_status="concerning", extra_concerning_flags=2))
    monkeypatch.setenv("TP_ALERT_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PORT", "587")
    monkeypatch.setenv("TP_ALERT_SMTP_USERNAME", "alerts@example.com")
    monkeypatch.setenv("TP_ALERT_SMTP_PASSWORD", "super-secret")
    monkeypatch.setenv("TP_ALERT_FROM", "alerts@example.com")
    monkeypatch.setenv("TP_ALERT_TO", "ops@example.com")

    exit_code = main(["--daily-report", str(report_path), "--dry-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert '"smtp_config_summary"' in output
    assert '"host": "smtp.example.com"' in output
    assert '"username": "alerts@example.com"' in output
    assert "super-secret" not in output
