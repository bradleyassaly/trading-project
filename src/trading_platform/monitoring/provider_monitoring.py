from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.research.dataset_registry import ResearchDatasetRegistryEntry, list_dataset_registry_entries


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    candidate = Path(path)
    if not candidate.exists():
        return {}
    try:
        return dict(json.loads(candidate.read_text(encoding="utf-8")) or {})
    except json.JSONDecodeError:
        return {}


def read_monitoring_artifact(path: str | Path | None) -> dict[str, Any]:
    return _read_json(path)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text.count("-") == 2:
        text = f"{text}T00:00:00+00:00"
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _severity_rank(status: str) -> int:
    ranks = {
        "healthy": 0,
        "pass": 0,
        "completed": 0,
        "warning": 1,
        "warn": 1,
        "completed_with_failures": 1,
        "critical": 2,
        "fail": 2,
        "failed": 2,
        "error": 2,
        "missing": 2,
        "unknown": 1,
    }
    return ranks.get(str(status).lower(), 1)


def _normalize_status(status: str | None) -> str:
    value = str(status or "unknown").lower()
    if value in {"healthy", "pass", "completed"}:
        return "healthy"
    if value in {"warning", "warn", "completed_with_failures"}:
        return "warning"
    if value in {"critical", "fail", "failed", "error", "missing"}:
        return "critical"
    return "unknown"


def _record_from_entry(entry: ResearchDatasetRegistryEntry, *, staleness_threshold_hours: int) -> dict[str, Any]:
    now = _now_utc()
    latest_event_time = entry.latest_event_time
    latest_materialized_at = entry.latest_materialized_at
    latest_sync_outcome: str | None = None
    status = "unknown"
    warnings: list[str] = []

    health_refs = dict(entry.health_references)
    manifest_refs = dict(entry.manifest_references)
    if entry.provider == "binance":
        health_summary = _read_json(health_refs.get("health_summary_path"))
        alerts_summary = _read_json(health_refs.get("alerts_summary_path"))
        latest_manifest = _read_json(manifest_refs.get("latest_sync_manifest_path"))
        status = _normalize_status(health_summary.get("status") or alerts_summary.get("status"))
        latest_sync_outcome = latest_manifest.get("status")
    elif entry.provider == "kalshi":
        validation_summary = _read_json(health_refs.get("validation_summary_path"))
        latest_status = _read_json(health_refs.get("latest_status_artifact_path"))
        latest_run_summary = _read_json(health_refs.get("latest_run_summary_path"))
        status = _normalize_status(validation_summary.get("status"))
        latest_sync_outcome = latest_status.get("overall_status") or latest_run_summary.get("overall_status")
        if not validation_summary:
            warnings.append("missing_validation_summary")
    elif entry.provider == "polymarket":
        ingest_manifest = _read_json(manifest_refs.get("ingest_manifest_path"))
        status = "healthy" if ingest_manifest else "critical"
        latest_sync_outcome = "completed" if ingest_manifest else "missing_manifest"
        if not ingest_manifest:
            warnings.append("missing_ingest_manifest")

    freshness_dt = _parse_dt(latest_event_time) or _parse_dt(latest_materialized_at)
    freshness_age_seconds = round((now - freshness_dt).total_seconds(), 6) if freshness_dt is not None else None
    stale = bool(freshness_age_seconds is not None and freshness_age_seconds > staleness_threshold_hours * 3600)
    if stale and status == "healthy":
        status = "warning"

    return {
        "provider": entry.provider,
        "asset_class": entry.asset_class,
        "dataset_key": entry.dataset_key,
        "dataset_name": entry.dataset_name,
        "status": status,
        "latest_sync_outcome": latest_sync_outcome,
        "symbols": list(entry.symbols),
        "intervals": list(entry.intervals),
        "latest_event_time": latest_event_time,
        "latest_materialized_at": latest_materialized_at,
        "freshness_age_seconds": freshness_age_seconds,
        "staleness_threshold_hours": staleness_threshold_hours,
        "stale": stale,
        "summary_path": entry.summary_path,
        "manifest_references": manifest_refs,
        "health_references": health_refs,
        "warnings": warnings,
    }


@dataclass(frozen=True)
class CrossProviderMonitoringResult:
    monitoring_summary_path: str
    health_summary_path: str
    record_count: int
    provider_count: int
    highest_status: str


def read_latest_registry_summary(*, summary_path: str | Path) -> dict[str, Any]:
    return _read_json(summary_path)


def read_latest_monitoring_summary(*, output_root: str | Path) -> dict[str, Any]:
    return _read_json(Path(output_root) / "latest_monitoring_summary.json")


def read_latest_provider_health_summary(*, output_root: str | Path) -> dict[str, Any]:
    return _read_json(Path(output_root) / "cross_provider_health_summary.json")


def build_cross_provider_monitoring_summary(
    *,
    registry_path: str | Path,
    output_root: str | Path,
    providers: list[str] | tuple[str, ...] | None = None,
    asset_class: str | None = None,
    staleness_threshold_hours: int = 48,
) -> CrossProviderMonitoringResult:
    entries = list_dataset_registry_entries(registry_path=registry_path, asset_class=asset_class)
    provider_filter = {provider.lower() for provider in (providers or [])}
    if provider_filter:
        entries = [entry for entry in entries if entry.provider.lower() in provider_filter]

    records = [_record_from_entry(entry, staleness_threshold_hours=staleness_threshold_hours) for entry in entries]
    records = sorted(records, key=lambda record: (record["provider"], record["dataset_name"], record["dataset_key"]))

    provider_summaries: list[dict[str, Any]] = []
    highest_status = "healthy"
    for provider in sorted({record["provider"] for record in records}):
        provider_records = [record for record in records if record["provider"] == provider]
        provider_status = max((record["status"] for record in provider_records), key=_severity_rank, default="unknown")
        if _severity_rank(provider_status) > _severity_rank(highest_status):
            highest_status = provider_status
        provider_summaries.append(
            {
                "provider": provider,
                "dataset_count": len(provider_records),
                "stale_dataset_count": sum(1 for record in provider_records if record["stale"]),
                "status": provider_status,
                "latest_sync_outcomes": sorted(
                    {
                        str(record["latest_sync_outcome"])
                        for record in provider_records
                        if record.get("latest_sync_outcome") is not None
                    }
                ),
            }
        )

    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    monitoring_summary_path = output_dir / "latest_monitoring_summary.json"
    health_summary_path = output_dir / "cross_provider_health_summary.json"

    monitoring_payload = {
        "generated_at": _now_utc().isoformat(),
        "registry_path": str(registry_path),
        "record_count": len(records),
        "provider_count": len(provider_summaries),
        "staleness_threshold_hours": staleness_threshold_hours,
        "records": records,
    }
    health_payload = {
        "generated_at": _now_utc().isoformat(),
        "registry_path": str(registry_path),
        "overall_status": highest_status if records else "unknown",
        "provider_summaries": provider_summaries,
        "record_count": len(records),
    }
    monitoring_summary_path.write_text(json.dumps(monitoring_payload, indent=2), encoding="utf-8")
    health_summary_path.write_text(json.dumps(health_payload, indent=2), encoding="utf-8")
    return CrossProviderMonitoringResult(
        monitoring_summary_path=str(monitoring_summary_path),
        health_summary_path=str(health_summary_path),
        record_count=len(records),
        provider_count=len(provider_summaries),
        highest_status=health_payload["overall_status"],
    )
