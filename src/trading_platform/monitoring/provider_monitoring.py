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


def read_monitoring_history(*, output_root: str | Path) -> list[dict[str, Any]]:
    path = Path(output_root) / "monitoring_history.jsonl"
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            entries.append(dict(json.loads(stripped) or {}))
        except json.JSONDecodeError:
            continue
    return entries


def read_recent_transitions(*, output_root: str | Path) -> dict[str, Any]:
    return _read_json(Path(output_root) / "latest_transition_summary.json")


def _append_monitoring_history(*, output_root: Path, monitoring_payload: dict[str, Any]) -> None:
    history_path = output_root / "monitoring_history.jsonl"
    snapshot = {
        "generated_at": monitoring_payload.get("generated_at"),
        "record_count": monitoring_payload.get("record_count"),
        "records": monitoring_payload.get("records", []),
    }
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot) + "\n")


def _build_transition_summary(*, history: list[dict[str, Any]], latest_records: list[dict[str, Any]]) -> dict[str, Any]:
    previous_records: dict[str, dict[str, Any]] = {}
    if len(history) >= 2:
        for record in history[-2].get("records", []) or []:
            if isinstance(record, dict) and record.get("dataset_key"):
                previous_records[str(record["dataset_key"])] = record
    transitions: list[dict[str, Any]] = []
    for record in latest_records:
        dataset_key = str(record.get("dataset_key") or "")
        previous = previous_records.get(dataset_key, {})
        previous_status = previous.get("status")
        current_status = record.get("status")
        if previous_status is None:
            continue
        if previous_status != current_status or bool(previous.get("stale")) != bool(record.get("stale")):
            transitions.append(
                {
                    "dataset_key": dataset_key,
                    "provider": record.get("provider"),
                    "previous_status": previous_status,
                    "current_status": current_status,
                    "previous_stale": bool(previous.get("stale")),
                    "current_stale": bool(record.get("stale")),
                    "latest_event_time": record.get("latest_event_time"),
                    "latest_materialized_at": record.get("latest_materialized_at"),
                }
            )
    return {
        "generated_at": _now_utc().isoformat(),
        "transition_count": len(transitions),
        "transitions": transitions,
    }


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
    _append_monitoring_history(output_root=output_dir, monitoring_payload=monitoring_payload)
    history = read_monitoring_history(output_root=output_dir)
    transition_summary = _build_transition_summary(history=history, latest_records=records)
    (output_dir / "latest_transition_summary.json").write_text(
        json.dumps(transition_summary, indent=2),
        encoding="utf-8",
    )
    return CrossProviderMonitoringResult(
        monitoring_summary_path=str(monitoring_summary_path),
        health_summary_path=str(health_summary_path),
        record_count=len(records),
        provider_count=len(provider_summaries),
        highest_status=health_payload["overall_status"],
    )
