from __future__ import annotations

from trading_platform.monitoring.daily_alerts import (
    build_daily_summary_body,
    derive_daily_alerts,
    send_daily_alerts,
)
from trading_platform.monitoring.models import DailyAlertsConfig
from trading_platform.monitoring.notification_service import send_sms_stub


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


def _config() -> DailyAlertsConfig:
    return DailyAlertsConfig(
        email_enabled=True,
        sms_enabled=True,
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_username="alerts@example.com",
        smtp_password_env_var="TRADING_PLATFORM_SMTP_PASSWORD",
        email_from="alerts@example.com",
        email_to=["ops@example.com"],
        email_min_severity="info",
        sms_provider="stub",
        sms_target=["+15555550123"],
        sms_min_severity="critical",
        send_daily_success_summary=True,
        send_on_failure=True,
        send_on_zero_promotions=True,
        send_on_monitoring_warnings=True,
        send_on_kill_switch_recommendations=True,
        monitoring_warning_threshold=1,
    )


def _summary(run_status: str = "succeeded", *, promoted: int = 1, warnings: int = 0, kill_switch: int = 0) -> dict:
    return {
        "generated_at": "2026-03-24T00:00:00+00:00",
        "run_status": run_status,
        "run_id": "run-1",
        "run_dir": "artifacts/orchestration_runs_operating_baseline/operating_baseline/run-1",
        "promoted_strategy_count": promoted,
        "selected_strategy_count": 1,
        "paper_order_count": 3,
        "monitoring_warning_count": warnings,
        "kill_switch_recommendation_count": kill_switch,
        "paths": {"daily_baseline_summary_json_path": "artifacts/operating_baseline_daily/daily_baseline_summary.json"},
        "system_evaluation": {
            "latest_metrics": {"total_return": None, "sharpe": None, "max_drawdown": None, "turnover": 0.12},
            "history_metrics": {"total_return": 0.0, "sharpe": None, "max_drawdown": 0.0},
        },
    }


def test_daily_alert_body_contains_key_fields() -> None:
    body = build_daily_summary_body(_summary())

    assert "run_status=succeeded" in body
    assert "promoted_strategy_count=1" in body
    assert "paper_order_count=3" in body


def test_daily_alert_trigger_logic() -> None:
    alerts = derive_daily_alerts(_summary(promoted=0, warnings=2, kill_switch=1), _config())

    assert [alert.code for alert in alerts] == [
        "baseline_daily_summary",
        "baseline_zero_promotions",
        "baseline_monitoring_warnings",
        "baseline_kill_switch_recommendations",
    ]


def test_daily_alert_sending_routes_email_and_sms(monkeypatch) -> None:
    FakeSMTP.sent_messages = []
    monkeypatch.setenv("TRADING_PLATFORM_SMTP_PASSWORD", "secret")
    result = send_daily_alerts(
        summary=_summary(run_status="failed", promoted=0, warnings=2, kill_switch=1),
        config=_config(),
        smtp_client_factory=FakeSMTP,
        sms_sender=send_sms_stub,
    )

    assert result["alert_count"] >= 3
    assert result["email_result"]["sent"] is True
    assert result["sms_result"]["sent"] is True
    assert result["sms_result"]["channel_results"][0]["sent"] is False
    assert len(FakeSMTP.sent_messages) == 1
