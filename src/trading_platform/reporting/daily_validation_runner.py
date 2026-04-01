from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from trading_platform.reporting.validation_alerting import DEFAULT_DECISION_FILENAME, send_validation_alerts


SUMMARY_FILENAME = "daily_validation_run_summary.json"
LATEST_POINTER_FILENAME = "latest_validation_run.json"
EXECUTION_MODE = "cli_subprocess"
ALERT_FAILURE_EXIT_CODE = 2


@dataclass(frozen=True)
class DailyValidationPaths:
    base_output_dir: Path
    artifact_dir: Path
    report_json_path: Path
    report_md_path: Path | None
    summary_path: Path
    latest_pointer_path: Path | None


@dataclass(frozen=True)
class ValidationConfigRouting:
    config_type: str
    paper_command_used: str
    execution_mode: str = EXECUTION_MODE


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _src_root() -> Path:
    return _repo_root() / "src"


def _now_local() -> datetime:
    return datetime.now().astimezone()


def _format_run_timestamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def _format_run_dir_name(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H-%M-%S")


def _resolve_report_path(path_value: str | Path, artifact_dir: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return artifact_dir / path


def resolve_daily_validation_paths(
    *,
    output_dir: str | Path,
    report_json: str | Path,
    report_md: str | Path | None = None,
    timestamp_run_dir: bool = False,
    moment: datetime | None = None,
) -> DailyValidationPaths:
    base_output_dir = Path(output_dir)
    if timestamp_run_dir:
        current_moment = moment or _now_local()
        artifact_dir = base_output_dir / _format_run_dir_name(current_moment)
        latest_pointer_path: Path | None = base_output_dir / LATEST_POINTER_FILENAME
    else:
        artifact_dir = base_output_dir
        latest_pointer_path = None
    report_json_path = _resolve_report_path(report_json, artifact_dir)
    report_md_path = _resolve_report_path(report_md, artifact_dir) if report_md is not None else None
    return DailyValidationPaths(
        base_output_dir=base_output_dir,
        artifact_dir=artifact_dir,
        report_json_path=report_json_path,
        report_md_path=report_md_path,
        summary_path=artifact_dir / SUMMARY_FILENAME,
        latest_pointer_path=latest_pointer_path,
    )


def _python_executable(path_value: str | None) -> str:
    return str(Path(path_value)) if path_value else sys.executable


def detect_validation_config_routing(config_path: str | Path) -> ValidationConfigRouting:
    path = Path(config_path)
    lower_name = path.name.lower()
    if "activated_strategy_portfolio" in lower_name:
        return ValidationConfigRouting(config_type="multi_strategy", paper_command_used="run-multi-strategy")

    suffix = path.suffix.lower()
    if suffix not in {".json", ".yaml", ".yml"}:
        raise ValueError(f"Unsupported validation config file type for routing: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Validation config file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8")) if suffix == ".json" else None
    if isinstance(payload, dict):
        if payload.get("active_strategies") is not None or payload.get("strategies") is not None:
            return ValidationConfigRouting(config_type="multi_strategy", paper_command_used="run-multi-strategy")
        if payload.get("sleeves") is not None:
            return ValidationConfigRouting(config_type="multi_strategy", paper_command_used="run-multi-strategy")
        if any(key in payload for key in ("symbols", "universe", "preset")):
            return ValidationConfigRouting(config_type="workflow", paper_command_used="run")
        raise ValueError(f"Unable to determine validation config type from JSON payload: {path}")

    if suffix in {".yaml", ".yml"}:
        return ValidationConfigRouting(config_type="workflow", paper_command_used="run")
    raise ValueError(f"Unable to determine validation config type: {path}")


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    src_root = str(_src_root())
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src_root if not existing else os.pathsep.join([src_root, existing])
    return env


def build_paper_run_command(
    *,
    python_executable: str,
    paper_command_used: str,
    config_path: str | Path,
    state_path: str | Path,
    artifact_dir: str | Path,
) -> list[str]:
    return [
        python_executable,
        "-m",
        "trading_platform.cli",
        "paper",
        paper_command_used,
        "--config",
        str(config_path),
        "--state-path",
        str(state_path),
        "--output-dir",
        str(artifact_dir),
    ]


def build_daily_report_command(
    *,
    python_executable: str,
    artifact_dir: str | Path,
    report_json: str | Path,
    report_md: str | Path | None = None,
    strict: bool = False,
) -> list[str]:
    command = [
        python_executable,
        str(_repo_root() / "scripts" / "daily_system_report.py"),
        "--artifact-dir",
        str(artifact_dir),
        "--output-json",
        str(report_json),
    ]
    if report_md is not None:
        command.extend(["--output-md", str(report_md)])
    if strict:
        command.append("--strict")
    return command


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(_repo_root()),
        env=_subprocess_env(),
        check=False,
        text=True,
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _build_alerting_payload(
    *,
    enabled: bool,
    evaluated: bool = False,
    triggered: bool = False,
    alert_types: list[str] | None = None,
    sent: bool = False,
    mode: str | None = None,
    decision_output_path: Path | None = None,
    error: str | None = None,
    timestamp: str | None = None,
) -> dict[str, object]:
    resolved_alert_types = list(alert_types or [])
    return {
        "alerting_enabled": enabled,
        "alert_evaluated": evaluated,
        "alert_triggered": triggered,
        "alert_type": resolved_alert_types[0] if len(resolved_alert_types) == 1 else None,
        "alert_types": resolved_alert_types,
        "alert_sent": sent,
        "alert_mode": mode,
        "alert_decision_output_path": str(decision_output_path) if decision_output_path is not None else None,
        "alert_error": error,
        "timestamp": timestamp,
    }


def _build_summary_payload(
    *,
    run_timestamp: str,
    config_path: str | Path,
    state_path: str | Path,
    artifact_dir: Path,
    report_json_path: Path,
    report_md_path: Path | None,
    paper_run_exit_status: int | None,
    report_exit_status: int | None,
    config_type: str | None,
    paper_command_used: str | None,
    execution_mode: str,
    warnings: list[str],
    overall_status: str,
    alerting: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "run_timestamp": run_timestamp,
        "config_path": str(Path(config_path)),
        "state_path": str(Path(state_path)),
        "artifact_dir": str(artifact_dir),
        "report_json_path": str(report_json_path),
        "report_md_path": str(report_md_path) if report_md_path is not None else None,
        "paper_run_exit_status": paper_run_exit_status,
        "report_exit_status": report_exit_status,
        "paper_command_used": paper_command_used,
        "config_type": config_type,
        "execution_mode": execution_mode,
        "warnings": warnings,
        "overall_status": overall_status,
        "alerting": dict(alerting or _build_alerting_payload(enabled=False, timestamp=run_timestamp)),
    }


def _write_latest_pointer(path: Path, *, summary_path: Path, artifact_dir: Path, run_timestamp: str) -> None:
    _write_json(
        path,
        {
            "run_timestamp": run_timestamp,
            "artifact_dir": str(artifact_dir),
            "summary_path": str(summary_path),
        },
    )


def run_daily_validation(
    *,
    config_path: str | Path,
    state_path: str | Path,
    output_dir: str | Path,
    report_json: str | Path,
    report_md: str | Path | None = None,
    python_executable: str | None = None,
    strict_report: bool = False,
    timestamp_run_dir: bool = False,
    send_alerts: bool = False,
    alert_dry_run: bool = False,
    alert_no_send: bool = False,
    alert_registry_path: str | Path | None = None,
    alert_decision_output: str | Path | None = None,
    alert_subject_prefix: str | None = None,
) -> tuple[int, dict[str, object]]:
    python_path = _python_executable(python_executable)
    moment = _now_local()
    run_timestamp = _format_run_timestamp(moment)
    paths = resolve_daily_validation_paths(
        output_dir=output_dir,
        report_json=report_json,
        report_md=report_md,
        timestamp_run_dir=timestamp_run_dir,
        moment=moment,
    )
    paths.artifact_dir.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []
    alerting = _build_alerting_payload(enabled=send_alerts, timestamp=run_timestamp)
    decision_output_path = (
        _resolve_report_path(alert_decision_output, paths.artifact_dir)
        if alert_decision_output is not None
        else paths.artifact_dir / DEFAULT_DECISION_FILENAME if send_alerts else None
    )
    try:
        routing = detect_validation_config_routing(config_path)
    except Exception as exc:
        warnings.append(str(exc))
        summary = _build_summary_payload(
            run_timestamp=run_timestamp,
            config_path=config_path,
            state_path=state_path,
            artifact_dir=paths.artifact_dir,
            report_json_path=paths.report_json_path,
            report_md_path=paths.report_md_path,
            paper_run_exit_status=None,
            report_exit_status=None,
            config_type=None,
            paper_command_used=None,
            execution_mode=EXECUTION_MODE,
            warnings=warnings,
            overall_status="config_detection_failed",
            alerting=alerting,
        )
        _write_json(paths.summary_path, summary)
        summary, exit_code = _evaluate_alerting(
            summary=summary,
            summary_path=paths.summary_path,
            daily_report_path=None,
            send_alerts=send_alerts,
            alert_dry_run=alert_dry_run,
            alert_no_send=alert_no_send,
            alert_registry_path=alert_registry_path,
            alert_decision_output_path=decision_output_path,
            alert_subject_prefix=alert_subject_prefix,
            current_exit_code=1,
        )
        _write_json(paths.summary_path, summary)
        if paths.latest_pointer_path is not None:
            _write_latest_pointer(
                paths.latest_pointer_path,
                summary_path=paths.summary_path,
                artifact_dir=paths.artifact_dir,
                run_timestamp=run_timestamp,
            )
        return exit_code, summary

    paper_command = build_paper_run_command(
        python_executable=python_path,
        paper_command_used=routing.paper_command_used,
        config_path=config_path,
        state_path=state_path,
        artifact_dir=paths.artifact_dir,
    )
    paper_result = _run_command(paper_command)
    paper_exit_status = int(paper_result.returncode)

    if paper_exit_status != 0:
        warnings.append("paper run failed; daily system report was not executed")
        summary = _build_summary_payload(
            run_timestamp=run_timestamp,
            config_path=config_path,
            state_path=state_path,
            artifact_dir=paths.artifact_dir,
            report_json_path=paths.report_json_path,
            report_md_path=paths.report_md_path,
            paper_run_exit_status=paper_exit_status,
            report_exit_status=None,
            config_type=routing.config_type,
            paper_command_used=routing.paper_command_used,
            execution_mode=routing.execution_mode,
            warnings=warnings,
            overall_status="paper_run_failed",
            alerting=alerting,
        )
        _write_json(paths.summary_path, summary)
        summary, exit_code = _evaluate_alerting(
            summary=summary,
            summary_path=paths.summary_path,
            daily_report_path=None,
            send_alerts=send_alerts,
            alert_dry_run=alert_dry_run,
            alert_no_send=alert_no_send,
            alert_registry_path=alert_registry_path,
            alert_decision_output_path=decision_output_path,
            alert_subject_prefix=alert_subject_prefix,
            current_exit_code=1,
        )
        _write_json(paths.summary_path, summary)
        if paths.latest_pointer_path is not None:
            _write_latest_pointer(
                paths.latest_pointer_path,
                summary_path=paths.summary_path,
                artifact_dir=paths.artifact_dir,
                run_timestamp=run_timestamp,
            )
        return exit_code, summary

    report_command = build_daily_report_command(
        python_executable=python_path,
        artifact_dir=paths.artifact_dir,
        report_json=paths.report_json_path,
        report_md=paths.report_md_path,
        strict=strict_report,
    )
    report_result = _run_command(report_command)
    report_exit_status = int(report_result.returncode)

    overall_status = "success"
    exit_code = 0
    if report_exit_status != 0:
        if strict_report:
            warnings.append("daily system report failed in strict mode")
            overall_status = "report_failed"
            exit_code = 1
        else:
            warnings.append("daily system report failed in non-strict mode")
            overall_status = "success_with_warnings"
    elif not paths.report_json_path.exists():
        warnings.append("daily system report completed without writing the requested JSON artifact")
        if strict_report:
            overall_status = "report_failed"
            exit_code = 1
        else:
            overall_status = "success_with_warnings"

    summary = _build_summary_payload(
        run_timestamp=run_timestamp,
        config_path=config_path,
        state_path=state_path,
        artifact_dir=paths.artifact_dir,
        report_json_path=paths.report_json_path,
        report_md_path=paths.report_md_path,
        paper_run_exit_status=paper_exit_status,
        report_exit_status=report_exit_status,
        config_type=routing.config_type,
        paper_command_used=routing.paper_command_used,
        execution_mode=routing.execution_mode,
        warnings=warnings,
        overall_status=overall_status,
        alerting=alerting,
    )
    _write_json(paths.summary_path, summary)
    summary, exit_code = _evaluate_alerting(
        summary=summary,
        summary_path=paths.summary_path,
        daily_report_path=paths.report_json_path if paths.report_json_path.exists() else None,
        send_alerts=send_alerts,
        alert_dry_run=alert_dry_run,
        alert_no_send=alert_no_send,
        alert_registry_path=alert_registry_path,
        alert_decision_output_path=decision_output_path,
        alert_subject_prefix=alert_subject_prefix,
        current_exit_code=exit_code,
    )
    _write_json(paths.summary_path, summary)
    if paths.latest_pointer_path is not None:
        _write_latest_pointer(
            paths.latest_pointer_path,
            summary_path=paths.summary_path,
            artifact_dir=paths.artifact_dir,
            run_timestamp=run_timestamp,
    )
    return exit_code, summary


def _evaluate_alerting(
    *,
    summary: dict[str, object],
    summary_path: Path,
    daily_report_path: Path | None,
    send_alerts: bool,
    alert_dry_run: bool,
    alert_no_send: bool,
    alert_registry_path: str | Path | None,
    alert_decision_output_path: Path | None,
    alert_subject_prefix: str | None,
    current_exit_code: int,
) -> tuple[dict[str, object], int]:
    if not send_alerts:
        return summary, current_exit_code
    try:
        alert_exit_code, alert_artifact, _ = send_validation_alerts(
            daily_run_summary_path=summary_path,
            daily_report_path=daily_report_path,
            registry_path=alert_registry_path or Path("artifacts/validation_alert_registry.json"),
            decision_output_path=alert_decision_output_path,
            dry_run=alert_dry_run,
            no_send=alert_no_send,
            subject_prefix=alert_subject_prefix,
        )
        summary["alerting"] = _build_alerting_payload(
            enabled=True,
            evaluated=True,
            triggered=bool(alert_artifact.get("alert_triggered")),
            alert_types=list(alert_artifact.get("alert_types") or []),
            sent=bool(alert_artifact.get("sent")),
            mode=str(alert_artifact.get("mode")) if alert_artifact.get("mode") is not None else None,
            decision_output_path=alert_decision_output_path if alert_decision_output_path is not None else None,
            error=None,
            timestamp=str(alert_artifact.get("timestamp") or summary.get("run_timestamp")),
        )
        if current_exit_code != 0:
            return summary, current_exit_code
        if alert_exit_code != 0:
            return summary, ALERT_FAILURE_EXIT_CODE
        return summary, current_exit_code
    except Exception as exc:
        summary["alerting"] = _build_alerting_payload(
            enabled=True,
            evaluated=True,
            triggered=False,
            alert_types=[],
            sent=False,
            mode="error",
            decision_output_path=alert_decision_output_path if alert_decision_output_path is not None else None,
            error=str(exc),
            timestamp=str(summary.get("run_timestamp")),
        )
        if current_exit_code != 0:
            return summary, current_exit_code
        return summary, ALERT_FAILURE_EXIT_CODE


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one paper-trading validation cycle and immediately generate the Phase 1.5 daily system report."
    )
    parser.add_argument("--config", required=True, help="Paper-run config path.")
    parser.add_argument("--state-path", required=True, help="Persistent paper state path.")
    parser.add_argument("--output-dir", required=True, help="Directory or stable run root for paper artifacts.")
    parser.add_argument("--report-json", required=True, help="JSON output path for the daily system report.")
    parser.add_argument("--report-md", help="Optional markdown output path for the daily system report.")
    parser.add_argument("--python-executable", default=None, help="Optional Python interpreter used for subprocess execution.")
    parser.add_argument("--strict-report", action="store_true", help="Fail the overall run if daily report generation fails.")
    parser.add_argument(
        "--timestamp-run-dir",
        action="store_true",
        help="Write each run into a timestamped child directory under --output-dir and update a latest-run pointer.",
    )
    parser.add_argument("--send-alerts", action="store_true", help="Evaluate/send validation alerts for this exact run after report generation.")
    parser.add_argument("--alert-dry-run", action="store_true", help="Evaluate alerts and compose the email without sending.")
    parser.add_argument("--alert-no-send", action="store_true", help="Evaluate alert decisions without attempting SMTP.")
    parser.add_argument("--alert-registry-path", default=None, help="Optional alert dedupe registry path.")
    parser.add_argument("--alert-decision-output", default=None, help="Optional path for validation alert decision JSON.")
    parser.add_argument("--alert-subject-prefix", default=None, help="Optional alert email subject prefix.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    exit_code, _summary = run_daily_validation(
        config_path=args.config,
        state_path=args.state_path,
        output_dir=args.output_dir,
        report_json=args.report_json,
        report_md=args.report_md,
        python_executable=args.python_executable,
        strict_report=bool(args.strict_report),
        timestamp_run_dir=bool(args.timestamp_run_dir),
        send_alerts=bool(args.send_alerts),
        alert_dry_run=bool(args.alert_dry_run),
        alert_no_send=bool(args.alert_no_send),
        alert_registry_path=args.alert_registry_path,
        alert_decision_output=args.alert_decision_output,
        alert_subject_prefix=args.alert_subject_prefix,
    )
    return exit_code
