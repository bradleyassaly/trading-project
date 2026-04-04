from __future__ import annotations

import json
import smtplib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.binance.health import evaluate_binance_alerts, evaluate_binance_health_check
from trading_platform.binance.models import BinanceNotifyConfig, BinanceNotifyResult
from trading_platform.config.loader import load_notification_config
from trading_platform.monitoring.models import Alert, NotificationConfig
from trading_platform.monitoring.notification_service import send_notifications


STATUS_ORDER = {"healthy": 0, "warning": 1, "critical": 2}
ALERT_SEVERITY_BY_STATUS = {"healthy": "info", "warning": "warning", "critical": "critical"}


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _read_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return dict(json.loads(file_path.read_text(encoding="utf-8")) or {})
    except json.JSONDecodeError:
        return {}


def _read_alerts(path: str | Path) -> list[Alert]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    try:
        payload = list(json.loads(file_path.read_text(encoding="utf-8")) or [])
    except json.JSONDecodeError:
        return []
    alerts: list[Alert] = []
    for row in payload:
        try:
            alerts.append(Alert(**dict(row)))
        except (TypeError, ValueError):
            continue
    return alerts


def _coalesce_status(*statuses: str) -> str:
    chosen = "healthy"
    for status in statuses:
        normalized = str(status or "healthy")
        if STATUS_ORDER.get(normalized, 0) > STATUS_ORDER[chosen]:
            chosen = normalized
    return chosen


def _dedupe_alerts(alerts: list[Alert]) -> list[Alert]:
    deduped: dict[tuple[str, str, str, str, str], Alert] = {}
    for alert in alerts:
        deduped[(alert.code, alert.severity, alert.entity_type, alert.entity_id, alert.message)] = alert
    return list(deduped.values())


def _iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _transition(previous: str | None, current: str) -> str | None:
    if previous is None:
        return f"initial->{current}"
    if previous == current:
        return None
    return f"{previous}->{current}"


def _should_notify(
    *,
    config: BinanceNotifyConfig,
    current_status: str,
    previous_status: str | None,
    previous_notified_at: str | None,
) -> tuple[bool, bool, str]:
    transition = _transition(previous_status, current_status)
    if current_status == "critical" and not config.notify_on_critical:
        return False, False, "critical_notifications_disabled"
    if current_status == "warning" and not config.notify_on_warning:
        return False, False, "warning_notifications_disabled"
    if current_status == "healthy":
        if previous_status in {"warning", "critical"} and config.notify_on_recovery:
            return True, False, "recovered"
        return False, False, "healthy_without_recovery"
    if config.transition_only and transition is None:
        previous_dt = _iso_to_dt(previous_notified_at)
        if previous_dt is not None:
            age_seconds = (datetime.now(UTC) - previous_dt).total_seconds()
            if age_seconds < config.duplicate_suppression_window_sec:
                return False, True, "duplicate_suppressed"
        return True, False, "suppression_window_elapsed"
    return True, False, "status_transition" if transition is not None else "status_requires_notification"


def _load_notification(path: str | None, subject_prefix: str) -> NotificationConfig | None:
    if not path:
        return None
    config = load_notification_config(path)
    return NotificationConfig(
        smtp_host=config.smtp_host,
        smtp_port=config.smtp_port,
        from_address=config.from_address,
        channels=list(config.channels),
        min_severity=config.min_severity,
        smtp_username=config.smtp_username,
        smtp_password=config.smtp_password,
        smtp_use_tls=config.smtp_use_tls,
        subject_prefix=subject_prefix or config.subject_prefix,
    )


def run_binance_monitor_notifications(
    config: BinanceNotifyConfig,
    *,
    smtp_client_factory: Any = smtplib.SMTP,
    dry_run: bool = False,
) -> BinanceNotifyResult:
    alerts_result = evaluate_binance_alerts(config.alerts)
    health_result = evaluate_binance_health_check(config.health)
    alerts_summary = _read_json(alerts_result.summary_path)
    health_summary = _read_json(health_result.summary_path)
    alerts = _read_alerts(alerts_result.alerts_json_path)

    for row in list(health_summary.get("alerts") or []):
        try:
            alerts.append(Alert(**dict(row)))
        except (TypeError, ValueError):
            continue
    if health_result.status != "healthy":
        alerts.append(
            Alert(
                code="binance_health_status",
                severity=ALERT_SEVERITY_BY_STATUS[health_result.status],
                message=f"Binance health-check status is {health_result.status}",
                timestamp=_now_utc(),
                entity_type="binance_health",
                entity_id="latest",
                artifact_path=health_result.summary_path,
            )
        )
    alerts = _dedupe_alerts(alerts)

    current_status = _coalesce_status(alerts_result.status, health_result.status)
    state_path = Path(config.state_path)
    previous_state = _read_json(state_path)
    previous_status = str(previous_state.get("current_status")) if previous_state.get("current_status") else None
    transition = _transition(previous_status, current_status)
    should_notify, suppressed, notify_reason = _should_notify(
        config=config,
        current_status=current_status,
        previous_status=previous_status,
        previous_notified_at=previous_state.get("last_notified_at"),
    )
    if current_status == "healthy" and should_notify and transition is not None:
        alerts.append(
            Alert(
                code="binance_recovered",
                severity="warning",
                message=f"Binance monitoring recovered ({transition})",
                timestamp=_now_utc(),
                entity_type="binance_monitoring",
                entity_id="latest",
                artifact_path=str(config.health.summary_path),
            )
        )
        alerts = _dedupe_alerts(alerts)
    notification_config = _load_notification(config.notification_config_path, config.subject_prefix)
    would_send = bool(config.enabled and should_notify)
    notified = False
    delivery: dict[str, Any] = {
        "sent": False,
        "filtered_alert_count": 0,
        "channel_results": [],
        "subject": None,
        "body": None,
    }
    if would_send and notification_config is not None and not dry_run:
        delivery = send_notifications(
            alerts=alerts,
            config=notification_config,
            smtp_client_factory=smtp_client_factory,
        )
        notified = bool(delivery.get("sent"))

    evaluated_at = _now_utc()
    output_root = Path(config.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    summary = {
        "evaluated_at": evaluated_at,
        "current_status": current_status,
        "alert_status": alerts_result.status,
        "health_status": health_result.status,
        "transition": transition,
        "notify_reason": notify_reason,
        "enabled": config.enabled,
        "should_notify": should_notify,
        "suppressed": suppressed,
        "would_send": would_send,
        "notified": notified,
        "dry_run": dry_run,
        "notification_config_path": config.notification_config_path,
        "alerts_summary_path": alerts_result.summary_path,
        "alerts_json_path": alerts_result.alerts_json_path,
        "health_summary_path": health_result.summary_path,
        "state_path": str(state_path),
        "alert_count": len(alerts),
        "delivery": delivery,
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    updated_state = {
        "evaluated_at": evaluated_at,
        "current_status": current_status,
        "alert_status": alerts_result.status,
        "health_status": health_result.status,
        "transition": transition,
        "last_notified_at": evaluated_at if would_send else previous_state.get("last_notified_at"),
        "last_notification_status": current_status if would_send else previous_state.get("last_notification_status"),
        "last_delivery_sent_at": evaluated_at if notified else previous_state.get("last_delivery_sent_at"),
        "last_delivery_status": current_status if notified else previous_state.get("last_delivery_status"),
        "last_notify_reason": notify_reason,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(updated_state, indent=2), encoding="utf-8")

    return BinanceNotifyResult(
        summary_path=str(summary_path),
        state_path=str(state_path),
        status=current_status,
        transition=transition,
        should_notify=should_notify,
        notified=notified,
        suppressed=suppressed,
        alert_count=len(alerts),
    )
