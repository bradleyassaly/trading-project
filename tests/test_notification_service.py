from __future__ import annotations

from trading_platform.monitoring.models import Alert, NotificationChannel, NotificationConfig
from trading_platform.monitoring.notification_service import (
    aggregate_alerts,
    filter_alerts_by_severity,
    send_notifications,
)


class FakeSMTP:
    sent_messages = []

    def __init__(self, host, port, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started_tls = False
        self.logged_in = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def starttls(self):
        self.started_tls = True

    def login(self, username, password):
        self.logged_in = True

    def send_message(self, message):
        self.__class__.sent_messages.append(message)


def _alerts() -> list[Alert]:
    return [
        Alert(
            code="info_a",
            severity="info",
            message="informational",
            timestamp="2026-03-21T00:00:00+00:00",
            entity_type="run",
            entity_id="run-a",
        ),
        Alert(
            code="warn_a",
            severity="warning",
            message="warning message",
            timestamp="2026-03-21T00:00:00+00:00",
            entity_type="run",
            entity_id="run-a",
        ),
        Alert(
            code="crit_a",
            severity="critical",
            message="critical message",
            timestamp="2026-03-21T00:00:00+00:00",
            entity_type="run",
            entity_id="run-a",
        ),
    ]


def _config(min_severity: str = "warning") -> NotificationConfig:
    return NotificationConfig(
        smtp_host="smtp.example.com",
        smtp_port=587,
        from_address="alerts@example.com",
        min_severity=min_severity,
        channels=[NotificationChannel(channel_type="email", recipients=["ops@example.com"])],
    )


def test_notification_severity_filtering() -> None:
    filtered = filter_alerts_by_severity(_alerts(), "warning")

    assert [alert.code for alert in filtered] == ["warn_a", "crit_a"]


def test_notification_aggregation_correctness() -> None:
    subject, body = aggregate_alerts(_alerts()[1:], subject_prefix="Trading Platform")

    assert "critical alerts (2)" in subject
    assert "[warning] warn_a" in body
    assert "[critical] crit_a" in body


def test_notification_email_formatting_mocked() -> None:
    FakeSMTP.sent_messages = []
    result = send_notifications(
        alerts=_alerts(),
        config=_config("warning"),
        smtp_client_factory=FakeSMTP,
    )

    assert result["sent"] is True
    assert len(FakeSMTP.sent_messages) == 1
    message = FakeSMTP.sent_messages[0]
    assert message["Subject"].startswith("Trading Platform")
    assert message["To"] == "ops@example.com"
    assert "critical message" in message.get_content()


def test_no_notification_when_below_threshold() -> None:
    FakeSMTP.sent_messages = []
    result = send_notifications(
        alerts=[_alerts()[0]],
        config=_config("warning"),
        smtp_client_factory=FakeSMTP,
    )

    assert result["sent"] is False
    assert result["filtered_alert_count"] == 0
    assert FakeSMTP.sent_messages == []
