from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _csv_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return "|".join(str(item) for item in value if item not in (None, ""))
    if isinstance(value, dict):
        return "|".join(f"{key}={value[key]}" for key in sorted(value))
    return str(value)


@dataclass(frozen=True)
class BaseUniverseDefinition:
    universe_id: str
    source_type: str
    symbols: list[str]
    rule_description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SubUniverseDefinition:
    sub_universe_id: str
    base_universe_id: str | None = None
    rule_description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseFilterDefinition:
    filter_name: str
    filter_type: str
    stage_name: str = "screening"
    enabled: bool = True
    threshold: float | str | None = None
    rule_description: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseFilterResult:
    universe_id: str | None
    base_universe_id: str | None
    sub_universe_id: str | None
    symbol: str
    as_of: str
    stage_name: str
    filter_name: str
    filter_type: str
    status: str
    passed: bool | None = None
    threshold: float | str | None = None
    observed_value: float | str | None = None
    inclusion_reason: str | None = None
    exclusion_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _csv_scalar(payload["metadata"])
        return payload


@dataclass(frozen=True)
class UniverseMembershipRecord:
    universe_id: str | None
    base_universe_id: str | None
    sub_universe_id: str | None
    symbol: str
    as_of: str
    inclusion_status: str
    inclusion_reason: str | None = None
    exclusion_reason: str | None = None
    stage_name: str = "final_eligibility"
    filter_failures: list[str] = field(default_factory=list)
    filter_passes: list[str] = field(default_factory=list)
    group_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["filter_failures"] = _csv_scalar(payload["filter_failures"])
        payload["filter_passes"] = _csv_scalar(payload["filter_passes"])
        payload["metadata"] = _csv_scalar(payload["metadata"])
        return payload


@dataclass(frozen=True)
class UniverseBuildSummary:
    as_of: str
    universe_id: str | None
    base_universe_id: str | None
    sub_universe_id: str | None
    base_symbol_count: int
    eligible_symbol_count: int
    excluded_symbol_count: int
    unavailable_filter_count: int
    active_filter_count: int
    active_filters: list[str] = field(default_factory=list)
    failure_counts_by_filter: dict[str, int] = field(default_factory=dict)
    status_counts: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class UniverseBuildBundle:
    base_definition: BaseUniverseDefinition
    sub_universe_definition: SubUniverseDefinition | None = None
    filter_definitions: list[UniverseFilterDefinition] = field(default_factory=list)
    filter_results: list[UniverseFilterResult] = field(default_factory=list)
    membership_records: list[UniverseMembershipRecord] = field(default_factory=list)
    summary: UniverseBuildSummary | None = None

    @property
    def eligible_symbols(self) -> list[str]:
        return [
            row.symbol
            for row in self.membership_records
            if row.inclusion_status == "included"
        ]

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_definition": self.base_definition.to_dict(),
            "sub_universe_definition": self.sub_universe_definition.to_dict()
            if self.sub_universe_definition is not None
            else None,
            "filter_definitions": [row.to_dict() for row in self.filter_definitions],
            "filter_results": [row.to_dict() for row in self.filter_results],
            "membership_records": [row.to_dict() for row in self.membership_records],
            "summary": self.summary.to_dict() if self.summary is not None else None,
        }
