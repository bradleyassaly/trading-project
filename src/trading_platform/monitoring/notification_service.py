from __future__ import annotations

import json
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable

from trading_platform.monitoring.models import Alert, NotificationConfig


SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}


def load_alerts(path: str | Path) -> list[Alert]:
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    return [Alert(**row) for row in rows]


def filter_alerts_by_severity(alerts: list[Alert], min_severity: str) -> list[Alert]:
    minimum = SEVERITY_ORDER[min_severity]
    return [alert for alert in alerts if SEVERITY_ORDER[alert.severity] >= minimum]


def aggregate_alerts(alerts: list[Alert], *, subject_prefix: str) -> tuple[str, str]:
    counts = {
        "info": sum(1 for alert in alerts if alert.severity == "info"),
        "warning": sum(1 for alert in alerts if alert.severity == "warning"),
        "critical": sum(1 for alert in alerts if alert.severity == "critical"),
    }
    highest = "critical" if counts["critical"] else "warning" if counts["warning"] else "info"
    subject = f"{subject_prefix}: {highest} alerts ({len(alerts)})"
    lines = [
        subject,
        "",
        f"info={counts['info']} warning={counts['warning']} critical={counts['critical']}",
        "",
    ]
    for alert in alerts:
        lines.append(f"[{alert.severity}] {alert.code} {alert.entity_type}:{alert.entity_id} - {alert.message}")
    return subject, "\n".join(lines) + "\n"


def send_email_notification(
    *,
    subject: str,
    body: str,
    recipients: list[str],
    config: NotificationConfig,
    smtp_client_factory: Callable[..., Any] = smtplib.SMTP,
) -> dict[str, Any]:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.from_address
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    with smtp_client_factory(config.smtp_host, config.smtp_port, timeout=10) as client:
        if config.smtp_use_tls:
            client.starttls()
        if config.smtp_username and config.smtp_password:
            client.login(config.smtp_username, config.smtp_password)
        client.send_message(message)
    return {"channel": "email", "recipient_count": len(recipients), "sent": True}


def send_sms_stub(*, message: str, recipients: list[str]) -> dict[str, Any]:
    return {
        "channel": "sms",
        "recipient_count": len(recipients),
        "sent": False,
        "message": "sms_stub_not_configured",
        "preview": message[:160],
    }


def send_notifications(
    *,
    alerts: list[Alert],
    config: NotificationConfig,
    smtp_client_factory: Callable[..., Any] = smtplib.SMTP,
    sms_sender: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    filtered_alerts = filter_alerts_by_severity(alerts, config.min_severity)
    if not filtered_alerts:
        return {"sent": False, "filtered_alert_count": 0, "channel_results": [], "subject": None, "body": None}

    subject, body = aggregate_alerts(filtered_alerts, subject_prefix=config.subject_prefix)
    channel_results: list[dict[str, Any]] = []
    for channel in config.channels:
        if channel.channel_type == "email":
            channel_results.append(
                send_email_notification(
                    subject=subject,
                    body=body,
                    recipients=channel.recipients,
                    config=config,
                    smtp_client_factory=smtp_client_factory,
                )
            )
        elif channel.channel_type == "sms":
            sender = sms_sender or send_sms_stub
            channel_results.append(sender(message=body, recipients=channel.recipients))
    return {
        "sent": True,
        "filtered_alert_count": len(filtered_alerts),
        "channel_results": channel_results,
        "subject": subject,
        "body": body,
    }
