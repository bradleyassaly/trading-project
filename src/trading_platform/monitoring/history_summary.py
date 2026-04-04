from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trading_platform.monitoring.provider_monitoring import read_monitoring_history, read_recent_transitions


@dataclass(frozen=True)
class MonitoringHistorySummary:
    scope_type: str
    scope_value: str
    snapshot_count: int
    healthy_count: int
    warning_count: int
    critical_count: int
    stale_count: int
    transition_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope_type": self.scope_type,
            "scope_value": self.scope_value,
            "snapshot_count": self.snapshot_count,
            "healthy_count": self.healthy_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "stale_count": self.stale_count,
            "transition_count": self.transition_count,
        }


def summarize_provider_history(*, output_root: str | Path, provider: str) -> MonitoringHistorySummary:
    history = read_monitoring_history(output_root=output_root)
    transitions = read_recent_transitions(output_root=output_root)
    statuses: list[str] = []
    stale_count = 0
    snapshot_count = 0
    for snapshot in history:
        records = [record for record in (snapshot.get("records") or []) if record.get("provider") == provider]
        if not records:
            continue
        snapshot_count += 1
        statuses.extend(str(record.get("status", "unknown")) for record in records)
        stale_count += sum(1 for record in records if record.get("stale"))
    provider_transitions = [item for item in (transitions.get("transitions") or []) if item.get("provider") == provider]
    return MonitoringHistorySummary(
        scope_type="provider",
        scope_value=provider,
        snapshot_count=snapshot_count,
        healthy_count=sum(1 for status in statuses if status == "healthy"),
        warning_count=sum(1 for status in statuses if status == "warning"),
        critical_count=sum(1 for status in statuses if status == "critical"),
        stale_count=stale_count,
        transition_count=len(provider_transitions),
    )


def summarize_dataset_history(*, output_root: str | Path, dataset_key: str) -> MonitoringHistorySummary:
    history = read_monitoring_history(output_root=output_root)
    transitions = read_recent_transitions(output_root=output_root)
    statuses: list[str] = []
    stale_count = 0
    snapshot_count = 0
    for snapshot in history:
        record = next((item for item in (snapshot.get("records") or []) if item.get("dataset_key") == dataset_key), None)
        if record is None:
            continue
        snapshot_count += 1
        statuses.append(str(record.get("status", "unknown")))
        stale_count += 1 if record.get("stale") else 0
    dataset_transitions = [item for item in (transitions.get("transitions") or []) if item.get("dataset_key") == dataset_key]
    return MonitoringHistorySummary(
        scope_type="dataset",
        scope_value=dataset_key,
        snapshot_count=snapshot_count,
        healthy_count=sum(1 for status in statuses if status == "healthy"),
        warning_count=sum(1 for status in statuses if status == "warning"),
        critical_count=sum(1 for status in statuses if status == "critical"),
        stale_count=stale_count,
        transition_count=len(dataset_transitions),
    )
