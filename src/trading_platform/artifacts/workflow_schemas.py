from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from trading_platform.artifacts.summary_utils import add_standard_summary_fields


@dataclass(frozen=True)
class ArtifactHealthCheck:
    check_name: str
    status: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkflowArtifactSummary:
    summary_type: str
    workflow_stage: str
    timestamp: str
    status: str
    name: str
    strategy: str | None = None
    universe: str | None = None
    preset_name: str | None = None
    key_counts: dict[str, Any] = field(default_factory=dict)
    key_metrics: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "workflow_stage": self.workflow_stage,
            "name": self.name,
            "strategy": self.strategy,
            "universe": self.universe,
            "preset_name": self.preset_name,
            "details": self.details,
        }
        return add_standard_summary_fields(
            payload,
            summary_type=self.summary_type,
            timestamp=self.timestamp,
            status=self.status,
            key_counts=self.key_counts,
            key_metrics=self.key_metrics,
            warnings=self.warnings,
            errors=self.errors,
            artifact_paths=self.artifact_paths,
        )
