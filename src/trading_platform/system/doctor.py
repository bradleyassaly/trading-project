from __future__ import annotations

import importlib.util
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.config.loader import (
    load_broker_config,
    load_dashboard_config,
    load_execution_config,
    load_monitoring_config,
    load_notification_config,
    load_pipeline_run_config,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class DoctorCheck:
    check_name: str
    status: str
    message: str
    context: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _status_from_checks(checks: list[DoctorCheck]) -> str:
    statuses = [check.status for check in checks]
    if any(status == "fail" for status in statuses):
        return "failed"
    if any(status == "warn" for status in statuses):
        return "warning"
    return "succeeded"


def run_system_doctor(
    *,
    artifacts_root: str | Path,
    output_dir: str | Path,
    pipeline_config: str | None = None,
    monitoring_config: str | None = None,
    notification_config: str | None = None,
    execution_config: str | None = None,
    broker_config: str | None = None,
    dashboard_config: str | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    checks: list[DoctorCheck] = []
    artifacts_path = Path(artifacts_root)
    checks.append(
        DoctorCheck(
            check_name="artifacts_root",
            status="pass" if artifacts_path.exists() else "warn",
            message=f"artifacts_root={artifacts_path}",
            context={"path": str(artifacts_path)},
        )
    )

    for name, module_name in [("pandas", "pandas"), ("numpy", "numpy"), ("pyarrow", "pyarrow")]:
        present = importlib.util.find_spec(module_name) is not None
        checks.append(
            DoctorCheck(
                check_name=f"dependency_{name}",
                status="pass" if present else "warn",
                message=f"{name} {'available' if present else 'not installed'}",
                context={},
            )
        )

    if pipeline_config:
        try:
            config = load_pipeline_run_config(pipeline_config)
        except Exception as exc:
            checks.append(DoctorCheck("pipeline_config", "fail", f"{type(exc).__name__}: {exc}", {"path": pipeline_config}))
        else:
            checks.append(DoctorCheck("pipeline_config", "pass", f"loaded {config.run_name}", {"path": pipeline_config}))
    if monitoring_config:
        try:
            load_monitoring_config(monitoring_config)
        except Exception as exc:
            checks.append(DoctorCheck("monitoring_config", "fail", f"{type(exc).__name__}: {exc}", {"path": monitoring_config}))
        else:
            checks.append(DoctorCheck("monitoring_config", "pass", "loaded monitoring config", {"path": monitoring_config}))
    if notification_config:
        try:
            load_notification_config(notification_config)
        except Exception as exc:
            checks.append(DoctorCheck("notification_config", "fail", f"{type(exc).__name__}: {exc}", {"path": notification_config}))
        else:
            checks.append(DoctorCheck("notification_config", "pass", "loaded notification config", {"path": notification_config}))
    if execution_config:
        try:
            config = load_execution_config(execution_config)
        except Exception as exc:
            checks.append(DoctorCheck("execution_config", "fail", f"{type(exc).__name__}: {exc}", {"path": execution_config}))
        else:
            checks.append(DoctorCheck("execution_config", "pass", f"loaded execution config enabled={config.enabled}", {"path": execution_config}))
    if dashboard_config:
        try:
            config = load_dashboard_config(dashboard_config)
        except Exception as exc:
            checks.append(DoctorCheck("dashboard_config", "fail", f"{type(exc).__name__}: {exc}", {"path": dashboard_config}))
        else:
            checks.append(DoctorCheck("dashboard_config", "pass", f"loaded dashboard config port={config.port}", {"path": dashboard_config}))
    if broker_config:
        try:
            config = load_broker_config(broker_config)
        except Exception as exc:
            checks.append(DoctorCheck("broker_config", "fail", f"{type(exc).__name__}: {exc}", {"path": broker_config}))
        else:
            checks.append(DoctorCheck("broker_config", "pass", f"loaded broker config for {config.broker_name}", {"path": broker_config}))
            if config.require_manual_enable_flag:
                manual_flag_ok = bool(config.manual_enable_flag_path and Path(config.manual_enable_flag_path).exists())
                checks.append(
                    DoctorCheck(
                        "broker_manual_enable_flag",
                        "pass" if manual_flag_ok else "warn",
                        "manual enable flag present" if manual_flag_ok else "manual enable flag missing",
                        {"path": config.manual_enable_flag_path},
                    )
                )
            if config.global_kill_switch_path:
                kill_switch_active = Path(config.global_kill_switch_path).exists()
                checks.append(
                    DoctorCheck(
                        "broker_kill_switch",
                        "warn" if kill_switch_active else "pass",
                        "kill switch active" if kill_switch_active else "kill switch clear",
                        {"path": config.global_kill_switch_path},
                    )
                )
            if config.monitoring_status_path:
                monitoring_exists = Path(config.monitoring_status_path).exists()
                checks.append(
                    DoctorCheck(
                        "broker_monitoring_status_path",
                        "pass" if monitoring_exists else "warn",
                        "monitoring status artifact found" if monitoring_exists else "monitoring status artifact missing",
                        {"path": config.monitoring_status_path},
                    )
                )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report = {
        "timestamp": _now_utc(),
        "status": _status_from_checks(checks),
        "check_count": len(checks),
        "warning_count": sum(1 for check in checks if check.status == "warn"),
        "error_count": sum(1 for check in checks if check.status == "fail"),
        "checks": [check.to_dict() for check in checks],
    }
    json_path = output_path / "doctor_report.json"
    md_path = output_path / "doctor_report.md"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    lines = [
        "# System Doctor",
        "",
        f"- Timestamp: `{report['timestamp']}`",
        f"- Status: `{report['status']}`",
        f"- Checks: `{report['check_count']}`",
        f"- Warnings: `{report['warning_count']}`",
        f"- Errors: `{report['error_count']}`",
        "",
        "## Checks",
    ]
    for check in checks:
        lines.append(f"- `{check.status}` {check.check_name}: {check.message}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report, {"doctor_report_json_path": json_path, "doctor_report_md_path": md_path}
