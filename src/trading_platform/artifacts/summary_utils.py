"""Standard summary fields shared by run-summary artifacts.

Every workflow that persists a ``*_summary.json`` artifact routes its
payload through :func:`add_standard_summary_fields` so downstream
readers (reporting, dashboard payloads) can rely on one field shape.

Health checks are ``pass`` / ``warn`` / ``fail`` rows, either plain
dicts (``{"check_name", "status", "message", ...}``) or dataclasses
with the same attribute names (e.g. ``LivePreviewHealthCheck``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping


def _field(check: Any, name: str) -> str:
    if isinstance(check, Mapping):
        return str(check.get(name, "") or "")
    return str(getattr(check, name, "") or "")


def workflow_status_from_checks(checks: Iterable[Any]) -> str:
    """Roll a list of health checks up to a single workflow status."""

    statuses = {_field(check, "status") for check in checks}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def warnings_and_errors_from_checks(
    checks: Iterable[Any],
) -> tuple[list[str], list[str]]:
    """Split check messages into (warnings, errors) by status."""

    warnings: list[str] = []
    errors: list[str] = []
    for check in checks:
        status = _field(check, "status")
        if status not in ("warn", "fail"):
            continue
        name = _field(check, "check_name")
        message = _field(check, "message")
        text = f"{name}: {message}" if name else message
        if status == "warn":
            warnings.append(text)
        else:
            errors.append(text)
    return warnings, errors


def add_standard_summary_fields(
    payload: Mapping[str, Any] | None,
    *,
    summary_type: str,
    timestamp: Any,
    status: str,
    key_counts: Mapping[str, Any] | None = None,
    key_metrics: Mapping[str, Any] | None = None,
    warnings: Iterable[str] | None = None,
    errors: Iterable[str] | None = None,
    artifact_paths: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return ``payload`` merged with the standard summary envelope."""

    out: dict[str, Any] = dict(payload or {})
    out["summary_type"] = str(summary_type)
    out["timestamp"] = None if timestamp is None else str(timestamp)
    out["status"] = str(status)
    out["key_counts"] = dict(key_counts or {})
    out["key_metrics"] = dict(key_metrics or {})
    out["warnings"] = [str(item) for item in (warnings or [])]
    out["errors"] = [str(item) for item in (errors or [])]
    if artifact_paths is not None:
        out["artifact_paths"] = {
            str(key): str(value) if isinstance(value, Path) else value
            for key, value in dict(artifact_paths).items()
        }
    return out
