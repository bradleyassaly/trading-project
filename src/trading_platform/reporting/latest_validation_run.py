from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


SUMMARY_FILENAME = "daily_validation_run_summary.json"
REPORT_FILENAME = "daily_system_report.json"
RUN_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class ResolvedValidationRun:
    run_dir: Path
    run_timestamp: datetime
    summary_path: Path
    report_path: Path
    summary: dict[str, Any]
    report: dict[str, Any]
    validation_status: str
    decision_reason: str


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_run_dir_timestamp(path: Path) -> datetime:
    return datetime.strptime(path.name, "%Y-%m-%dT%H-%M-%S")


def _is_non_failure_status(status: str) -> bool:
    return status in {"success", "success_with_warnings"}


def _candidate_run_dirs(validation_root: Path) -> list[Path]:
    if not validation_root.exists():
        raise FileNotFoundError(f"Validation root not found: {validation_root}")
    if not validation_root.is_dir():
        raise NotADirectoryError(f"Validation root is not a directory: {validation_root}")
    return sorted(
        [path for path in validation_root.iterdir() if path.is_dir() and RUN_DIR_PATTERN.match(path.name)],
        key=lambda path: path.name,
    )


def resolve_latest_validation_run(
    validation_root: str | Path,
    *,
    require_success: bool = True,
) -> ResolvedValidationRun:
    root = Path(validation_root)
    candidates = _candidate_run_dirs(root)
    if not candidates:
        raise FileNotFoundError(f"No timestamped validation run directories found under {root}")

    malformed_reasons: list[str] = []
    failed_reasons: list[str] = []
    valid_runs: list[ResolvedValidationRun] = []
    for run_dir in candidates:
        summary_path = run_dir / SUMMARY_FILENAME
        report_path = run_dir / REPORT_FILENAME
        if not summary_path.exists():
            malformed_reasons.append(f"{run_dir}: missing {SUMMARY_FILENAME}")
            continue
        if not report_path.exists():
            malformed_reasons.append(f"{run_dir}: missing {REPORT_FILENAME}")
            continue
        try:
            summary = _read_json(summary_path)
            report = _read_json(report_path)
        except Exception as exc:
            malformed_reasons.append(f"{run_dir}: invalid json ({exc})")
            continue
        if any(field not in summary for field in ("paper_run_exit_status", "report_exit_status", "overall_status", "run_timestamp")):
            malformed_reasons.append(f"{run_dir}: missing required summary fields")
            continue
        overall_status = str(summary.get("overall_status") or "")
        paper_exit = summary.get("paper_run_exit_status")
        report_exit = summary.get("report_exit_status")
        if require_success and (
            paper_exit not in (None, 0)
            or report_exit not in (None, 0)
            or not _is_non_failure_status(overall_status)
        ):
            failed_reasons.append(
                f"{run_dir}: overall_status={overall_status} paper_run_exit_status={paper_exit} report_exit_status={report_exit}"
            )
            continue
        try:
            run_timestamp = datetime.fromisoformat(str(summary["run_timestamp"]))
        except Exception:
            run_timestamp = _parse_run_dir_timestamp(run_dir)
        valid_runs.append(
            ResolvedValidationRun(
                run_dir=run_dir,
                run_timestamp=run_timestamp,
                summary_path=summary_path,
                report_path=report_path,
                summary=summary,
                report=report,
                validation_status=overall_status or "unknown",
                decision_reason="latest_successful_run" if require_success else "latest_valid_run",
            )
        )

    if valid_runs:
        return sorted(valid_runs, key=lambda item: item.run_timestamp)[-1]
    if require_success and failed_reasons:
        detail = "; ".join(failed_reasons[-3:])
        raise ValueError(f"No successful validation runs found under {root}. Latest failures: {detail}")
    detail = "; ".join((malformed_reasons or failed_reasons)[-3:])
    raise ValueError(f"No valid validation runs found under {root}. Details: {detail}")
