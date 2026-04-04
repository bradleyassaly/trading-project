from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from trading_platform.monitoring.provider_monitoring import (
    read_latest_monitoring_summary,
    read_latest_provider_health_summary,
    read_latest_registry_summary,
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
