from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from trading_platform.paper.models import PaperTradingRunResult


SYSTEM_HEALTH_SCHEMA_VERSION = "system_health_payload_v1"
CHECK_STATUSES = {"pass", "warn", "fail"}


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


def _flat_dict(value: dict[str, Any]) -> str:
    return "|".join(f"{key}={value[key]}" for key in sorted(value) if value[key] not in (None, "", [], {}))


@dataclass(frozen=True)
class SystemHealthCheck:
    as_of: str
    check_name: str
    category: str
    status: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in CHECK_STATUSES:
            raise ValueError(f"Unsupported system health status: {self.status}")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "check_name": self.check_name,
            "category": self.category,
            "status": self.status,
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _flat_dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SystemHealthCheck":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            check_name=str(data["check_name"]),
            category=str(data["category"]),
            status=str(data["status"]),
            message=str(data["message"]),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True)
class SystemHealthPayload:
    as_of: str
    schema_version: str = SYSTEM_HEALTH_SCHEMA_VERSION
    checks: list[SystemHealthCheck] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "checks": [row.to_dict() for row in self.checks],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SystemHealthPayload":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", SYSTEM_HEALTH_SCHEMA_VERSION)),
            checks=[SystemHealthCheck.from_dict(row) for row in data.get("checks", [])],
            summary=dict(data.get("summary") or {}),
        )


def _status_rollup(checks: list[SystemHealthCheck]) -> str:
    statuses = {row.status for row in checks}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def build_system_health_payload(
    *,
    result: "PaperTradingRunResult",
    artifact_paths: dict[str, str | Path] | None = None,
) -> SystemHealthPayload:
    checks: list[SystemHealthCheck] = []
    artifact_paths = artifact_paths or {}
    paper_execution = dict(result.diagnostics.get("paper_execution", {}))
    target_construction = dict(result.diagnostics.get("target_construction", {}))

    stale_symbol_count = int(
        paper_execution.get(
            "stale_symbol_count",
            sum(1 for row in result.price_snapshots if row.latest_data_stale is True),
        )
        or 0
    )
    latest_data_stale = paper_execution.get("latest_data_stale")
    if latest_data_stale is None:
        latest_data_stale = target_construction.get("latest_data_stale")
    latest_bar_age_seconds = paper_execution.get("latest_bar_age_seconds")
    if latest_bar_age_seconds is None:
        latest_bar_age_seconds = target_construction.get("latest_bar_age_seconds")
    snapshot_symbol_count = int(
        paper_execution.get("snapshot_symbol_count", len(result.price_snapshots))
        or len(result.price_snapshots)
    )

    data_freshness_status = "warn" if latest_data_stale else "pass"
    if snapshot_symbol_count <= 0:
        data_freshness_status = "warn"
    checks.append(
        SystemHealthCheck(
            as_of=result.as_of,
            check_name="data_freshness",
            category="data_freshness",
            status=data_freshness_status,
            message=(
                f"latest market data stale for {stale_symbol_count} symbol(s)"
                if latest_data_stale
                else ("no price snapshots available" if snapshot_symbol_count <= 0 else "latest market data fresh")
            ),
            metadata={
                "latest_data_stale": latest_data_stale,
                "latest_bar_age_seconds": latest_bar_age_seconds,
                "stale_symbol_count": stale_symbol_count,
                "snapshot_symbol_count": snapshot_symbol_count,
                "latest_data_source": paper_execution.get("latest_data_source"),
                "latest_data_fallback_used": bool(paper_execution.get("latest_data_fallback_used", False)),
            },
        )
    )

    stale_signal_count = stale_symbol_count
    stale_signals_status = "warn" if stale_signal_count > 0 else "pass"
    if not result.latest_scores and result.latest_target_weights:
        stale_signals_status = "fail"
        stale_signal_count = max(stale_signal_count, len(result.latest_target_weights))
    checks.append(
        SystemHealthCheck(
            as_of=result.as_of,
            check_name="stale_signals",
            category="stale_signals",
            status=stale_signals_status,
            message=(
                f"{stale_signal_count} stale signal(s) detected"
                if stale_signals_status != "pass"
                else "signal inputs fresh"
            ),
            metadata={
                "stale_signal_count": stale_signal_count,
                "latest_score_count": len(result.latest_scores),
                "target_symbol_count": len(result.latest_target_weights),
            },
        )
    )

    required_artifact_keys = [
        "summary_path",
        "portfolio_performance_summary_path",
        "execution_summary_json_path",
        "kpi_payload_json_path",
        "trade_explorer_payload_json_path",
        "strategy_health_payload_json_path",
        "realtime_kpi_monitoring_json_path",
    ]
    missing_artifacts: list[str] = []
    for key in required_artifact_keys:
        path = artifact_paths.get(key)
        if path is None or not Path(path).exists():
            missing_artifacts.append(key)
    checks.append(
        SystemHealthCheck(
            as_of=result.as_of,
            check_name="artifact_presence",
            category="missing_artifacts",
            status="fail" if missing_artifacts else "pass",
            message=(
                f"missing required artifacts: {', '.join(missing_artifacts)}"
                if missing_artifacts
                else "required reporting artifacts present"
            ),
            metadata={
                "required_artifact_count": len(required_artifact_keys),
                "missing_artifact_count": len(missing_artifacts),
                "missing_artifact_keys": missing_artifacts,
            },
        )
    )

    pipeline_failures: list[str] = []
    if not result.trade_decision_contracts:
        pipeline_failures.append("trade_decision_contracts_missing")
    if not result.order_lifecycle_records:
        pipeline_failures.append("order_lifecycle_records_missing")
    if result.reconciliation_result is None:
        pipeline_failures.append("reconciliation_result_missing")
    if not result.decision_bundle:
        pipeline_failures.append("decision_bundle_missing")
    checks.append(
        SystemHealthCheck(
            as_of=result.as_of,
            check_name="pipeline_integrity",
            category="pipeline_failures",
            status="fail" if pipeline_failures else "pass",
            message=(
                f"pipeline failures detected: {', '.join(pipeline_failures)}"
                if pipeline_failures
                else "core paper-trading pipeline artifacts available"
            ),
            metadata={
                "pipeline_failure_count": len(pipeline_failures),
                "pipeline_failure_codes": pipeline_failures,
                "decision_count": len(result.trade_decision_contracts),
                "lifecycle_record_count": len(result.order_lifecycle_records),
                "fill_count": len(result.fills),
                "requested_order_count": len(result.requested_orders or result.orders),
            },
        )
    )

    return SystemHealthPayload(
        as_of=result.as_of,
        checks=checks,
        summary={
            "overall_status": _status_rollup(checks),
            "check_count": len(checks),
            "warning_count": sum(1 for row in checks if row.status == "warn"),
            "failure_count": sum(1 for row in checks if row.status == "fail"),
            "missing_artifact_count": len(missing_artifacts),
            "pipeline_failure_count": len(pipeline_failures),
            "stale_signal_count": stale_signal_count,
        },
    )


def write_system_health_artifacts(
    *,
    output_dir: str | Path,
    payload: SystemHealthPayload,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "system_health_payload.json"
    csv_path = output_path / "system_health_checks.csv"
    json_path.write_text(json.dumps(payload.to_dict(), indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in payload.checks]).to_csv(csv_path, index=False)
    return {"system_health_payload_json_path": json_path, "system_health_checks_csv_path": csv_path}
