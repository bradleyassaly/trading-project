from __future__ import annotations

import json
import smtplib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from trading_platform.monitoring.models import Alert, DailyAlertsConfig
from trading_platform.monitoring.notification_service import (
    send_email_notification,
    send_notifications,
    send_sms_stub,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_daily_summary_subject(summary: dict[str, Any], *, prefix: str) -> str:
    status = summary.get("run_status") or "unknown"
    run_id = summary.get("run_id") or "n/a"
    promoted = _safe_int(summary.get("promoted_strategy_count"))
    selected = _safe_int(summary.get("selected_strategy_count"))
    warnings = _safe_int(summary.get("monitoring_warning_count"))
    return f"{prefix}: baseline {status} run={run_id} promoted={promoted} selected={selected} warnings={warnings}"


def build_daily_summary_body(summary: dict[str, Any]) -> str:
    metrics = (summary.get("system_evaluation") or {}).get("latest_metrics", {}) or {}
    history_metrics = (summary.get("system_evaluation") or {}).get("history_metrics", {}) or {}
    paths = summary.get("paths", {}) or {}
    lines = [
        "Daily operating baseline summary",
        "",
        f"generated_at={summary.get('generated_at')}",
        f"run_status={summary.get('run_status')}",
        f"run_id={summary.get('run_id')}",
        f"run_dir={summary.get('run_dir')}",
        f"promoted_strategy_count={summary.get('promoted_strategy_count')}",
        f"selected_strategy_count={summary.get('selected_strategy_count')}",
        f"paper_order_count={summary.get('paper_order_count')}",
        f"monitoring_warning_count={summary.get('monitoring_warning_count')}",
        f"kill_switch_recommendation_count={summary.get('kill_switch_recommendation_count')}",
        "",
        "latest_metrics:",
        f"  total_return={metrics.get('total_return')}",
        f"  sharpe={metrics.get('sharpe')}",
        f"  max_drawdown={metrics.get('max_drawdown')}",
        f"  turnover={metrics.get('turnover')}",
        "",
        "history_metrics:",
        f"  total_return={history_metrics.get('total_return')}",
        f"  sharpe={history_metrics.get('sharpe')}",
        f"  max_drawdown={history_metrics.get('max_drawdown')}",
        "",
        "paths:",
        f"  orchestration_run_json_path={paths.get('orchestration_run_json_path')}",
        f"  system_evaluation_json_path={paths.get('system_evaluation_json_path')}",
        f"  system_evaluation_history_json_path={paths.get('system_evaluation_history_json_path')}",
        f"  daily_baseline_summary_json_path={paths.get('daily_baseline_summary_json_path')}",
    ]
    return "\n".join(lines) + "\n"


def derive_daily_alerts(summary: dict[str, Any], config: DailyAlertsConfig) -> list[Alert]:
    alerts: list[Alert] = []
    timestamp = summary.get("generated_at") or _now_utc()
    run_id = str(summary.get("run_id") or "operating_baseline")
    run_dir = summary.get("run_dir")
    promoted = _safe_int(summary.get("promoted_strategy_count"))
    warning_count = _safe_int(summary.get("monitoring_warning_count"))
    kill_switch_count = _safe_int(summary.get("kill_switch_recommendation_count"))

    if config.send_daily_success_summary and summary.get("run_status") == "succeeded":
        alerts.append(
            Alert(
                code="baseline_daily_summary",
                severity="info",
                message=(
                    f"baseline succeeded promoted={promoted} selected={_safe_int(summary.get('selected_strategy_count'))} "
                    f"paper_orders={_safe_int(summary.get('paper_order_count'))}"
                ),
                timestamp=timestamp,
                entity_type="baseline_run",
                entity_id=run_id,
                artifact_path=run_dir,
                context={"kind": "daily_success_summary"},
            )
        )
    if config.send_on_failure and summary.get("run_status") == "failed":
        alerts.append(
            Alert(
                code="baseline_run_failed",
                severity="critical",
                message=str(summary.get("error") or "operating baseline run failed"),
                timestamp=timestamp,
                entity_type="baseline_run",
                entity_id=run_id,
                artifact_path=run_dir,
                context={"kind": "failure"},
            )
        )
    if config.send_on_zero_promotions and summary.get("run_status") == "succeeded" and promoted <= 0:
        alerts.append(
            Alert(
                code="baseline_zero_promotions",
                severity="warning",
                message="baseline run completed with zero promoted strategies",
                timestamp=timestamp,
                entity_type="baseline_run",
                entity_id=run_id,
                metric_value=promoted,
                threshold_value=1,
                artifact_path=run_dir,
                context={"kind": "zero_promotions"},
            )
        )
    if config.send_on_monitoring_warnings and warning_count >= config.monitoring_warning_threshold:
        alerts.append(
            Alert(
                code="baseline_monitoring_warnings",
                severity="warning",
                message=f"monitoring warnings reached {warning_count}",
                timestamp=timestamp,
                entity_type="baseline_run",
                entity_id=run_id,
                metric_value=warning_count,
                threshold_value=config.monitoring_warning_threshold,
                artifact_path=run_dir,
                context={"kind": "monitoring_warning_threshold"},
            )
        )
    if config.send_on_kill_switch_recommendations and kill_switch_count > 0:
        alerts.append(
            Alert(
                code="baseline_kill_switch_recommendations",
                severity="critical",
                message=f"kill-switch recommendations present: {kill_switch_count}",
                timestamp=timestamp,
                entity_type="baseline_run",
                entity_id=run_id,
                metric_value=kill_switch_count,
                threshold_value=0,
                artifact_path=run_dir,
                context={"kind": "kill_switch_recommendations"},
            )
        )
    return alerts


def send_daily_alerts(
    *,
    summary: dict[str, Any],
    config: DailyAlertsConfig,
    smtp_client_factory: Callable[..., Any] = smtplib.SMTP,
    sms_sender: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    alerts = derive_daily_alerts(summary, config)
    email_config = config.to_email_notification_config()
    sms_config = config.to_sms_notification_config()
    results: dict[str, Any] = {
        "generated_at": _now_utc(),
        "alert_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
        "email_result": None,
        "sms_result": None,
    }

    if email_config is not None and any(alert.severity in {"info", "warning", "critical"} for alert in alerts):
        results["email_result"] = send_notifications(
            alerts=alerts,
            config=email_config,
            smtp_client_factory=smtp_client_factory,
        )

    if sms_config is not None:
        if config.sms_provider == "email_gateway":
            critical_alerts = [alert for alert in alerts if alert.severity == "critical"]
            if critical_alerts:
                subject = f"{config.subject_prefix}: critical baseline alert ({len(critical_alerts)})"
                body = "\n".join(f"{alert.code}: {alert.message}" for alert in critical_alerts)
                results["sms_result"] = send_email_notification(
                    subject=subject,
                    body=body,
                    recipients=sms_config.channels[0].recipients,
                    config=sms_config,
                    smtp_client_factory=smtp_client_factory,
                )
            else:
                results["sms_result"] = {"sent": False, "filtered_alert_count": 0, "channel_results": []}
        else:
            results["sms_result"] = send_notifications(
                alerts=alerts,
                config=sms_config,
                smtp_client_factory=smtp_client_factory,
                sms_sender=sms_sender or send_sms_stub,
            )
    return results


def persist_daily_alert_result(*, summary_dir: str | Path, payload: dict[str, Any]) -> dict[str, str]:
    output_dir = Path(summary_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "daily_alerts.json"
    md_path = output_dir / "daily_alerts.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_lines = [
        "# Daily Alerts",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- alert_count: `{payload.get('alert_count')}`",
        f"- email_sent: `{(payload.get('email_result') or {}).get('sent')}`",
        f"- sms_sent: `{(payload.get('sms_result') or {}).get('sent')}`",
        "",
        "## Alerts",
    ]
    for alert in payload.get("alerts", []):
        md_lines.append(f"- `{alert['severity']}` `{alert['code']}` {alert['message']}")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return {
        "daily_alerts_json_path": str(json_path),
        "daily_alerts_md_path": str(md_path),
    }
