from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from trading_platform.monitoring.provider_monitoring import (
    read_monitoring_history,
    read_latest_monitoring_summary,
    read_latest_provider_health_summary,
    read_latest_registry_summary,
    read_recent_transitions,
)
from trading_platform.research.dataset_reader import (
    ResearchDatasetDescriptor,
    list_research_datasets,
    resolve_research_dataset,
)


@dataclass(frozen=True)
class ProviderDrilldownResult:
    provider: str
    registry_summary: dict[str, Any]
    health_summary: dict[str, Any]
    monitoring_records: list[dict[str, Any]]
    datasets: list[ResearchDatasetDescriptor]

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "registry_summary": dict(self.registry_summary),
            "health_summary": dict(self.health_summary),
            "monitoring_records": list(self.monitoring_records),
            "datasets": [asdict(dataset) for dataset in self.datasets],
        }


@dataclass(frozen=True)
class DatasetDrilldownResult:
    dataset: ResearchDatasetDescriptor
    monitoring_record: dict[str, Any]
    provider_health_summary: dict[str, Any]
    registry_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": asdict(self.dataset),
            "monitoring_record": dict(self.monitoring_record),
            "provider_health_summary": dict(self.provider_health_summary),
            "registry_summary": dict(self.registry_summary),
        }


@dataclass(frozen=True)
class MonitoringTimelineResult:
    scope_type: str
    scope_value: str
    history: list[dict[str, Any]]
    transitions: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_type": self.scope_type,
            "scope_value": self.scope_value,
            "history": list(self.history),
            "transitions": list(self.transitions),
        }


def load_provider_drilldown(
    *,
    registry_path: str | Path,
    monitoring_output_root: str | Path,
    provider: str,
) -> ProviderDrilldownResult:
    registry_summary = read_latest_registry_summary(
        summary_path=Path(monitoring_output_root) / "latest_registry_summary.json"
    )
    health_summary = read_latest_provider_health_summary(output_root=monitoring_output_root)
    monitoring_summary = read_latest_monitoring_summary(output_root=monitoring_output_root)
    datasets = list_research_datasets(registry_path=registry_path, provider=provider)
    provider_health = next(
        (summary for summary in (health_summary.get("provider_summaries") or []) if summary.get("provider") == provider),
        {},
    )
    monitoring_records = [
        record
        for record in (monitoring_summary.get("records") or [])
        if record.get("provider") == provider
    ]
    return ProviderDrilldownResult(
        provider=provider,
        registry_summary=registry_summary,
        health_summary=provider_health,
        monitoring_records=monitoring_records,
        datasets=datasets,
    )


def load_dataset_drilldown(
    *,
    registry_path: str | Path,
    monitoring_output_root: str | Path,
    dataset_key: str,
) -> DatasetDrilldownResult:
    dataset = resolve_research_dataset(registry_path=registry_path, dataset_key=dataset_key)
    registry_summary = read_latest_registry_summary(
        summary_path=Path(monitoring_output_root) / "latest_registry_summary.json"
    )
    monitoring_summary = read_latest_monitoring_summary(output_root=monitoring_output_root)
    health_summary = read_latest_provider_health_summary(output_root=monitoring_output_root)
    monitoring_record = next(
        (record for record in (monitoring_summary.get("records") or []) if record.get("dataset_key") == dataset_key),
        {},
    )
    provider_health = next(
        (
            summary
            for summary in (health_summary.get("provider_summaries") or [])
            if summary.get("provider") == dataset.provider
        ),
        {},
    )
    return DatasetDrilldownResult(
        dataset=dataset,
        monitoring_record=monitoring_record,
        provider_health_summary=provider_health,
        registry_summary=registry_summary,
    )


def load_provider_timeline(
    *,
    monitoring_output_root: str | Path,
    provider: str,
) -> MonitoringTimelineResult:
    history = read_monitoring_history(output_root=monitoring_output_root)
    transitions_payload = read_recent_transitions(output_root=monitoring_output_root)
    provider_history: list[dict[str, Any]] = []
    for snapshot in history:
        records = [
            record
            for record in (snapshot.get("records") or [])
            if record.get("provider") == provider
        ]
        provider_history.append(
            {
                "generated_at": snapshot.get("generated_at"),
                "record_count": len(records),
                "records": records,
                "status": max(
                    (str(record.get("status", "unknown")) for record in records),
                    default="unknown",
                ),
            }
        )
    transitions = [
        transition
        for transition in (transitions_payload.get("transitions") or [])
        if transition.get("provider") == provider
    ]
    return MonitoringTimelineResult(
        scope_type="provider",
        scope_value=provider,
        history=provider_history,
        transitions=transitions,
    )


def load_dataset_timeline(
    *,
    monitoring_output_root: str | Path,
    dataset_key: str,
) -> MonitoringTimelineResult:
    history = read_monitoring_history(output_root=monitoring_output_root)
    transitions_payload = read_recent_transitions(output_root=monitoring_output_root)
    dataset_history: list[dict[str, Any]] = []
    for snapshot in history:
        record = next(
            (
                item
                for item in (snapshot.get("records") or [])
                if item.get("dataset_key") == dataset_key
            ),
            None,
        )
        if record is None:
            continue
        dataset_history.append(
            {
                "generated_at": snapshot.get("generated_at"),
                "record": record,
            }
        )
    transitions = [
        transition
        for transition in (transitions_payload.get("transitions") or [])
        if transition.get("dataset_key") == dataset_key
    ]
    return MonitoringTimelineResult(
        scope_type="dataset",
        scope_value=dataset_key,
        history=dataset_history,
        transitions=transitions,
    )
