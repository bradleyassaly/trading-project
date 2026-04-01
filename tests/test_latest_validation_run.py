from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_platform.reporting.latest_validation_run import resolve_latest_validation_run


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _summary(*, timestamp: str, overall_status: str = "success", paper_exit: int = 0, report_exit: int = 0) -> dict:
    return {
        "run_timestamp": timestamp,
        "paper_run_exit_status": paper_exit,
        "report_exit_status": report_exit,
        "overall_status": overall_status,
    }


def _report() -> dict:
    return {"evaluation_flags": {"overall_status": "healthy"}}


def _write_run(root: Path, run_name: str, *, summary: dict | None = None, report: dict | None = None) -> Path:
    run_dir = root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    if summary is not None:
        _write_json(run_dir / "daily_validation_run_summary.json", summary)
    if report is not None:
        _write_json(run_dir / "daily_system_report.json", report)
    return run_dir


def test_resolve_latest_validation_run_selects_newest_valid_run(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    _write_run(root, "2026-04-01T09-30-00", summary=_summary(timestamp="2026-04-01T09:30:00+00:00"), report=_report())
    newest = _write_run(root, "2026-04-02T09-30-00", summary=_summary(timestamp="2026-04-02T09:30:00+00:00"), report=_report())

    resolved = resolve_latest_validation_run(root, require_success=True)

    assert resolved.run_dir == newest
    assert resolved.summary_path == newest / "daily_validation_run_summary.json"
    assert resolved.report_path == newest / "daily_system_report.json"


def test_resolve_latest_validation_run_skips_incomplete_and_failed_runs(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    _write_run(root, "2026-04-01T09-30-00", summary=_summary(timestamp="2026-04-01T09:30:00+00:00", overall_status="paper_run_failed", paper_exit=2), report=_report())
    _write_run(root, "2026-04-02T09-30-00", summary=_summary(timestamp="2026-04-02T09:30:00+00:00"), report=None)
    valid = _write_run(root, "2026-04-03T09-30-00", summary=_summary(timestamp="2026-04-03T09:30:00+00:00"), report=_report())

    resolved = resolve_latest_validation_run(root, require_success=True)

    assert resolved.run_dir == valid


def test_resolve_latest_validation_run_fails_when_no_successful_runs_exist(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    _write_run(root, "2026-04-01T09-30-00", summary=_summary(timestamp="2026-04-01T09:30:00+00:00", overall_status="paper_run_failed", paper_exit=2), report=_report())

    with pytest.raises(ValueError, match="No successful validation runs found"):
        resolve_latest_validation_run(root, require_success=True)

