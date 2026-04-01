from __future__ import annotations

import argparse
import hashlib
import json
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from trading_platform.monitoring.models import NotificationChannel, NotificationConfig
from trading_platform.monitoring.notification_service import send_email_notification
from trading_platform.reporting.latest_validation_run import resolve_latest_validation_run


DEFAULT_REGISTRY_PATH = Path("artifacts/validation_alert_registry.json")
DEFAULT_DECISION_FILENAME = "validation_alert_decision.json"
DEFAULT_SUBJECT_PREFIX = "Trading Platform Validation"
SMTP_ENV_MAP = {
    "smtp_host": "TP_ALERT_SMTP_HOST",
    "smtp_port": "TP_ALERT_SMTP_PORT",
    "smtp_username": "TP_ALERT_SMTP_USERNAME",
    "smtp_password": "TP_ALERT_SMTP_PASSWORD",
    "smtp_use_tls": "TP_ALERT_SMTP_USE_TLS",
    "from_address": "TP_ALERT_FROM",
    "to_addresses": "TP_ALERT_TO",
    "subject_prefix": "TP_ALERT_SUBJECT_PREFIX",
}


@dataclass(frozen=True)
class ValidationAlertDecision:
    alert_type: str
    artifact_path: str
    subject_suffix: str
    status_signature: str
    severity: str
    decision_reasons: list[str]
    status_fields: dict[str, Any]


@dataclass(frozen=True)
class ResolvedSMTPConfig:
    config: NotificationConfig
    summary: dict[str, Any]


def _now_local() -> datetime:
    return datetime.now().astimezone()


def _safe_bool(value: str | bool | None, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _split_recipients(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _redacted_smtp_summary(config: NotificationConfig) -> dict[str, Any]:
    return {
        "host": config.smtp_host,
        "port": config.smtp_port,
        "username": config.smtp_username,
        "from": config.from_address,
        "to": list(config.channels[0].recipients) if config.channels else [],
        "use_tls": config.smtp_use_tls,
    }


def _has_any_smtp_config_source(
    *,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_username: str | None = None,
    smtp_password: str | None = None,
    smtp_use_tls: bool | None = None,
    from_address: str | None = None,
    to_addresses: list[str] | None = None,
    subject_prefix: str | None = None,
) -> bool:
    if any(
        value is not None and value != []
        for value in [
            smtp_host,
            smtp_port,
            smtp_username,
            smtp_password,
            smtp_use_tls,
            from_address,
            to_addresses,
            subject_prefix,
        ]
    ):
        return True
    return any(os.environ.get(env_name) for env_name in SMTP_ENV_MAP.values())


def _resolve_subject_prefix(subject_prefix: str | None = None) -> str:
    return subject_prefix or os.environ.get(SMTP_ENV_MAP["subject_prefix"]) or DEFAULT_SUBJECT_PREFIX


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _load_json_if_present(path: str | Path | None) -> tuple[dict[str, Any] | None, Path | None]:
    if path is None:
        return None, None
    resolved = Path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"Artifact not found: {resolved}")
    return _read_json(resolved), resolved


def _count_concerning(values: list[str]) -> int:
    return sum(1 for value in values if str(value) == "concerning")


def _daily_run_failure_decision(summary: dict[str, Any], artifact_path: Path) -> ValidationAlertDecision | None:
    reasons: list[str] = []
    overall_status = str(summary.get("overall_status") or "")
    paper_exit = summary.get("paper_run_exit_status")
    report_exit = summary.get("report_exit_status")
    if paper_exit not in (None, 0):
        reasons.append(f"paper_run_exit_status={paper_exit}")
    if overall_status in {"paper_run_failed", "report_failed", "config_detection_failed"}:
        reasons.append(f"overall_status={overall_status}")
    if report_exit not in (None, 0) and overall_status == "report_failed":
        reasons.append(f"report_exit_status={report_exit}")
    if not reasons:
        return None
    return ValidationAlertDecision(
        alert_type="run_failure",
        artifact_path=str(artifact_path),
        subject_suffix="validation run failure",
        status_signature="|".join(sorted(reasons)),
        severity="critical",
        decision_reasons=reasons,
        status_fields={
            "overall_status": overall_status,
            "paper_run_exit_status": paper_exit,
            "report_exit_status": report_exit,
            "paper_command_used": summary.get("paper_command_used"),
        },
    )


def _daily_concerning_decision(report: dict[str, Any], artifact_path: Path) -> ValidationAlertDecision | None:
    flags = dict(report.get("evaluation_flags") or {})
    reasons: list[str] = []
    overall_status = str(flags.get("overall_status") or "")
    if overall_status == "concerning":
        reasons.append("overall_status=concerning")
    checkpoint_values = [
        str(flags.get("ev_alignment_flag") or ""),
        str(flags.get("calibration_signal_flag") or ""),
        str(flags.get("drift_noise_flag") or ""),
        str(flags.get("decay_churn_flag") or ""),
        str(flags.get("lifecycle_churn_flag") or ""),
    ]
    concerning_count = _count_concerning(checkpoint_values)
    if concerning_count >= 2:
        reasons.append(f"concerning_checkpoint_flags={concerning_count}")
    risk_state = str(dict(report.get("risk_summary") or {}).get("risk_control_state") or "")
    if risk_state in {"halted", "restricted"}:
        reasons.append(f"risk_control_state={risk_state}")
    warnings = list(report.get("warnings") or [])
    if len(warnings) >= 3:
        reasons.append(f"warnings={len(warnings)}")
    if not reasons:
        return None
    return ValidationAlertDecision(
        alert_type="daily_concerning_status",
        artifact_path=str(artifact_path),
        subject_suffix="daily validation concern",
        status_signature="|".join(sorted(reasons)),
        severity="warning" if overall_status != "concerning" else "critical",
        decision_reasons=reasons,
        status_fields={
            "overall_status": overall_status,
            "ev_alignment_flag": flags.get("ev_alignment_flag"),
            "drift_noise_flag": flags.get("drift_noise_flag"),
            "lifecycle_churn_flag": flags.get("lifecycle_churn_flag"),
            "risk_control_state": risk_state or None,
        },
    )


def _window_concerning_decision(review: dict[str, Any], artifact_path: Path) -> ValidationAlertDecision | None:
    checkpoint = dict(review.get("evaluation_checkpoint") or {})
    reasons: list[str] = []
    overall_status = str(checkpoint.get("overall_validation_status") or "")
    next_step = str(checkpoint.get("recommended_next_step") or "")
    if overall_status == "concerning":
        reasons.append("overall_validation_status=concerning")
    if next_step in {"inspect_execution", "review_thresholds", "refine_alpha_generation"}:
        reasons.append(f"recommended_next_step={next_step}")
    concerning_count = _count_concerning(
        [
            str(checkpoint.get("ev_alignment_status") or ""),
            str(checkpoint.get("calibration_usefulness_status") or ""),
            str(checkpoint.get("drift_signal_quality_status") or ""),
            str(checkpoint.get("decay_signal_quality_status") or ""),
            str(checkpoint.get("lifecycle_churn_status") or ""),
            str(checkpoint.get("risk_control_status") or ""),
        ]
    )
    if concerning_count >= 2:
        reasons.append(f"concerning_checkpoint_statuses={concerning_count}")
    if not reasons:
        return None
    return ValidationAlertDecision(
        alert_type="window_concerning_status",
        artifact_path=str(artifact_path),
        subject_suffix="validation window concern",
        status_signature="|".join(sorted(reasons)),
        severity="critical" if overall_status == "concerning" else "warning",
        decision_reasons=reasons,
        status_fields={
            "overall_validation_status": overall_status,
            "recommended_next_step": next_step,
            "drift_signal_quality_status": checkpoint.get("drift_signal_quality_status"),
            "risk_control_status": checkpoint.get("risk_control_status"),
        },
    )


def evaluate_validation_alerts(
    *,
    daily_run_summary_path: str | Path | None = None,
    daily_report_path: str | Path | None = None,
    window_review_path: str | Path | None = None,
) -> list[ValidationAlertDecision]:
    decisions: list[ValidationAlertDecision] = []
    summary, summary_path = _load_json_if_present(daily_run_summary_path)
    if summary is not None and summary_path is not None:
        decision = _daily_run_failure_decision(summary, summary_path)
        if decision is not None:
            decisions.append(decision)
    report, report_path = _load_json_if_present(daily_report_path)
    if report is not None and report_path is not None:
        decision = _daily_concerning_decision(report, report_path)
        if decision is not None:
            decisions.append(decision)
    review, review_path = _load_json_if_present(window_review_path)
    if review is not None and review_path is not None:
        decision = _window_concerning_decision(review, review_path)
        if decision is not None:
            decisions.append(decision)
    return decisions


def build_dedupe_key(decision: ValidationAlertDecision) -> str:
    signature = "|".join([decision.alert_type, decision.artifact_path, decision.status_signature])
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def load_alert_registry(path: str | Path) -> dict[str, Any]:
    registry_path = Path(path)
    if not registry_path.exists():
        return {"sent_alerts": {}}
    return _read_json(registry_path)


def save_alert_registry(path: str | Path, registry: dict[str, Any]) -> None:
    _write_json(Path(path), registry)


def filter_unsent_alerts(
    decisions: list[ValidationAlertDecision],
    *,
    registry: dict[str, Any],
) -> tuple[list[ValidationAlertDecision], list[str]]:
    sent = dict(registry.get("sent_alerts") or {})
    unsent: list[ValidationAlertDecision] = []
    dedupe_keys: list[str] = []
    for decision in decisions:
        dedupe_key = build_dedupe_key(decision)
        if dedupe_key in sent:
            continue
        unsent.append(decision)
        dedupe_keys.append(dedupe_key)
    return unsent, dedupe_keys


def resolve_smtp_config(
    *,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_username: str | None = None,
    smtp_password: str | None = None,
    smtp_use_tls: bool | None = None,
    from_address: str | None = None,
    to_addresses: list[str] | None = None,
    subject_prefix: str | None = None,
) -> ResolvedSMTPConfig:
    env = os.environ
    resolved_host = smtp_host or env.get(SMTP_ENV_MAP["smtp_host"])
    resolved_port_value = smtp_port if smtp_port is not None else env.get(SMTP_ENV_MAP["smtp_port"])
    resolved_port: int | None = None
    if resolved_port_value is not None and str(resolved_port_value).strip():
        try:
            resolved_port = int(resolved_port_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("smtp_port must be an integer from CLI or TP_ALERT_SMTP_PORT") from exc
    resolved_username = smtp_username if smtp_username is not None else env.get(SMTP_ENV_MAP["smtp_username"])
    resolved_password = smtp_password if smtp_password is not None else env.get(SMTP_ENV_MAP["smtp_password"])
    resolved_use_tls = (
        smtp_use_tls if smtp_use_tls is not None else _safe_bool(env.get(SMTP_ENV_MAP["smtp_use_tls"]), True)
    )
    resolved_from = from_address or env.get(SMTP_ENV_MAP["from_address"])
    resolved_to = to_addresses if to_addresses is not None else _split_recipients(env.get(SMTP_ENV_MAP["to_addresses"]))
    resolved_subject_prefix = subject_prefix or env.get(SMTP_ENV_MAP["subject_prefix"]) or DEFAULT_SUBJECT_PREFIX
    missing: list[str] = []
    if not resolved_host:
        missing.append(f"smtp_host ({SMTP_ENV_MAP['smtp_host']})")
    if resolved_port is None:
        missing.append(f"smtp_port ({SMTP_ENV_MAP['smtp_port']})")
    if not resolved_from:
        missing.append(f"from_address ({SMTP_ENV_MAP['from_address']})")
    if not resolved_to:
        missing.append(f"to_addresses ({SMTP_ENV_MAP['to_addresses']})")
    if missing:
        raise ValueError(
            "Missing SMTP configuration fields: " + ", ".join(missing) + ". CLI arguments override environment variables."
        )
    config = NotificationConfig(
        smtp_host=str(resolved_host or ""),
        smtp_port=int(resolved_port),
        from_address=str(resolved_from or ""),
        channels=[NotificationChannel(channel_type="email", recipients=resolved_to)],
        smtp_username=resolved_username,
        smtp_password=resolved_password,
        smtp_use_tls=bool(resolved_use_tls),
        subject_prefix=resolved_subject_prefix,
    )
    return ResolvedSMTPConfig(config=config, summary=_redacted_smtp_summary(config))


def load_validation_notification_config(
    *,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_username: str | None = None,
    smtp_password: str | None = None,
    smtp_use_tls: bool | None = None,
    from_address: str | None = None,
    to_addresses: list[str] | None = None,
    subject_prefix: str | None = None,
) -> NotificationConfig:
    return resolve_smtp_config(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_username=smtp_username,
        smtp_password=smtp_password,
        smtp_use_tls=smtp_use_tls,
        from_address=from_address,
        to_addresses=to_addresses,
        subject_prefix=subject_prefix,
    ).config


def compose_validation_alert_email(
    *,
    decisions: list[ValidationAlertDecision],
    subject_prefix: str,
    timestamp: str,
) -> tuple[str, str]:
    highest = "critical" if any(decision.severity == "critical" for decision in decisions) else "warning"
    subject = f"{subject_prefix}: {highest} validation alert ({len(decisions)})"
    lines = [
        subject,
        "",
        f"timestamp={timestamp}",
        f"alert_count={len(decisions)}",
        "",
    ]
    for decision in decisions:
        lines.extend(
            [
                f"alert_type={decision.alert_type}",
                f"artifact_path={decision.artifact_path}",
                f"severity={decision.severity}",
                f"subject_suffix={decision.subject_suffix}",
                f"decision_reasons={', '.join(decision.decision_reasons)}",
            ]
        )
        for key, value in sorted(decision.status_fields.items()):
            lines.append(f"{key}={value}")
        lines.append("")
    return subject, "\n".join(lines).strip() + "\n"


def build_validation_alert_decision_artifact(
    *,
    triggered_decisions: list[ValidationAlertDecision],
    all_decisions: list[ValidationAlertDecision],
    dedupe_keys: list[str],
    email_subject: str | None,
    timestamp: str,
    sent: bool,
    mode: str,
    smtp_config_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    alert_types = [decision.alert_type for decision in triggered_decisions]
    primary_type = alert_types[0] if len(alert_types) == 1 else "multiple" if alert_types else None
    return {
        "alert_triggered": bool(triggered_decisions),
        "alert_type": primary_type,
        "alert_types": alert_types,
        "decision_reasons": [reason for decision in triggered_decisions for reason in decision.decision_reasons],
        "dedupe_key": dedupe_keys[0] if len(dedupe_keys) == 1 else None,
        "dedupe_keys": dedupe_keys,
        "email_subject": email_subject,
        "timestamp": timestamp,
        "mode": mode,
        "sent": sent,
        "smtp_config_summary": smtp_config_summary,
        "evaluated_alert_count": len(all_decisions),
        "triggered_alert_count": len(triggered_decisions),
    }


def send_validation_alerts(
    *,
    validation_root: str | Path | None = None,
    latest_successful_run: bool = False,
    daily_run_summary_path: str | Path | None = None,
    daily_report_path: str | Path | None = None,
    window_review_path: str | Path | None = None,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    decision_output_path: str | Path | None = None,
    dry_run: bool = False,
    no_send: bool = False,
    require_alert: bool = False,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    smtp_username: str | None = None,
    smtp_password: str | None = None,
    smtp_use_tls: bool | None = None,
    from_address: str | None = None,
    to_addresses: list[str] | None = None,
    subject_prefix: str | None = None,
    smtp_client_factory: Callable[..., Any] = smtplib.SMTP,
) -> tuple[int, dict[str, Any], str | None]:
    timestamp = _now_local().isoformat(timespec="seconds")
    if validation_root is not None:
        if daily_run_summary_path is not None or daily_report_path is not None:
            raise ValueError("Cannot combine --validation-root with explicit --daily-run-summary or --daily-report paths")
        if not latest_successful_run:
            raise ValueError("--validation-root currently requires --latest-successful-run")
        resolved = resolve_latest_validation_run(validation_root, require_success=True)
        daily_run_summary_path = resolved.summary_path
        daily_report_path = resolved.report_path
    decisions = evaluate_validation_alerts(
        daily_run_summary_path=daily_run_summary_path,
        daily_report_path=daily_report_path,
        window_review_path=window_review_path,
    )
    registry = load_alert_registry(registry_path)
    unsent_decisions, dedupe_keys = filter_unsent_alerts(decisions, registry=registry)
    subject: str | None = None
    body: str | None = None
    sent = False
    mode = "send"
    smtp_config_summary: dict[str, Any] | None = None
    if unsent_decisions:
        config: NotificationConfig | None = None
        should_resolve_smtp = not (dry_run or no_send) or _has_any_smtp_config_source(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            smtp_use_tls=smtp_use_tls,
            from_address=from_address,
            to_addresses=to_addresses,
            subject_prefix=subject_prefix,
        )
        if should_resolve_smtp:
            resolved_smtp = resolve_smtp_config(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_username=smtp_username,
                smtp_password=smtp_password,
                smtp_use_tls=smtp_use_tls,
                from_address=from_address,
                to_addresses=to_addresses,
                subject_prefix=subject_prefix,
            )
            config = resolved_smtp.config
            smtp_config_summary = resolved_smtp.summary
        subject, body = compose_validation_alert_email(
            decisions=unsent_decisions,
            subject_prefix=config.subject_prefix if config is not None else _resolve_subject_prefix(subject_prefix),
            timestamp=timestamp,
        )
        if dry_run or no_send:
            mode = "dry_run" if dry_run else "no_send"
        else:
            assert config is not None
            recipients = config.channels[0].recipients
            send_email_notification(
                subject=subject,
                body=body,
                recipients=recipients,
                config=config,
                smtp_client_factory=smtp_client_factory,
            )
            sent = True
            registry.setdefault("sent_alerts", {})
            for dedupe_key, decision in zip(dedupe_keys, unsent_decisions, strict=False):
                registry["sent_alerts"][dedupe_key] = {
                    "alert_type": decision.alert_type,
                    "artifact_path": decision.artifact_path,
                    "timestamp": timestamp,
                    "status_signature": decision.status_signature,
                }
            save_alert_registry(registry_path, registry)
    else:
        mode = "no_trigger"

    artifact = build_validation_alert_decision_artifact(
        triggered_decisions=unsent_decisions,
        all_decisions=decisions,
        dedupe_keys=dedupe_keys,
        email_subject=subject,
        timestamp=timestamp,
        sent=sent,
        mode=mode,
        smtp_config_summary=smtp_config_summary,
    )
    if decision_output_path is not None:
        _write_json(Path(decision_output_path), artifact)
    if require_alert and not unsent_decisions:
        return 1, artifact, body
    return 0, artifact, body


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send SMTP email alerts for failed or concerning Phase 1.5 validation artifacts."
    )
    parser.add_argument("--validation-root", help="Validation root containing timestamped run folders.")
    parser.add_argument("--latest-successful-run", action="store_true", help="Resolve daily artifacts from the latest successful timestamped validation run under --validation-root.")
    parser.add_argument("--daily-run-summary", help="Path to daily_validation_run_summary.json.")
    parser.add_argument("--daily-report", help="Path to daily_system_report.json.")
    parser.add_argument("--window-review", help="Path to validation_window_review.json.")
    parser.add_argument("--registry-path", default=str(DEFAULT_REGISTRY_PATH), help="Path to dedupe registry JSON.")
    parser.add_argument("--decision-output", default=None, help="Optional path to write validation_alert_decision.json.")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate and print the composed alert without sending.")
    parser.add_argument("--no-send", action="store_true", help="Evaluate and print the alert decision without SMTP.")
    parser.add_argument("--require-alert", action="store_true", help="Exit nonzero if no alert is triggered.")
    parser.add_argument("--subject-prefix", default=None, help="Optional email subject prefix.")
    parser.add_argument("--smtp-host", default=None, help="SMTP host. Overrides TP_ALERT_SMTP_HOST.")
    parser.add_argument("--smtp-port", type=int, default=None, help="SMTP port. Overrides TP_ALERT_SMTP_PORT.")
    parser.add_argument("--smtp-username", default=None, help="SMTP username. Overrides TP_ALERT_SMTP_USERNAME.")
    parser.add_argument("--smtp-password", default=None, help="SMTP password. Overrides TP_ALERT_SMTP_PASSWORD.")
    parser.add_argument("--smtp-use-tls", action=argparse.BooleanOptionalAction, default=None, help="Use STARTTLS for SMTP.")
    parser.add_argument("--from", dest="from_address", default=None, help="From address. Overrides TP_ALERT_FROM.")
    parser.add_argument("--to", dest="to_addresses", action="append", default=None, help="Recipient email. Repeatable; overrides TP_ALERT_TO.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    exit_code, artifact, body = send_validation_alerts(
        validation_root=args.validation_root,
        latest_successful_run=bool(args.latest_successful_run),
        daily_run_summary_path=args.daily_run_summary,
        daily_report_path=args.daily_report,
        window_review_path=args.window_review,
        registry_path=args.registry_path,
        decision_output_path=args.decision_output,
        dry_run=bool(args.dry_run),
        no_send=bool(args.no_send),
        require_alert=bool(args.require_alert),
        smtp_host=args.smtp_host,
        smtp_port=args.smtp_port,
        smtp_username=args.smtp_username,
        smtp_password=args.smtp_password,
        smtp_use_tls=args.smtp_use_tls,
        from_address=args.from_address,
        to_addresses=args.to_addresses,
        subject_prefix=args.subject_prefix,
    )
    print(json.dumps(artifact, indent=2))
    if body:
        print()
        print(body)
    return exit_code
