from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class UniverseMembershipHistoryRecord:
    symbol: str
    universe_id: str
    effective_start_date: str | None = None
    effective_end_date: str | None = None
    source: str | None = None
    source_version: str | None = None
    resolution_status: str = "confirmed"
    coverage_status: str = "confirmed"
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseMembershipSnapshot:
    symbol: str
    universe_id: str
    as_of_date: str
    membership_status: str
    source: str | None = None
    source_version: str | None = None
    resolution_status: str = "confirmed"
    coverage_status: str = "confirmed"
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TaxonomySnapshotRecord:
    symbol: str
    as_of_date: str | None = None
    effective_start_date: str | None = None
    effective_end_date: str | None = None
    sector: str | None = None
    industry: str | None = None
    group: str | None = None
    source: str | None = None
    source_version: str | None = None
    resolution_status: str = "confirmed"
    coverage_status: str = "confirmed"
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkMappingSnapshot:
    symbol: str
    as_of_date: str | None = None
    effective_start_date: str | None = None
    effective_end_date: str | None = None
    benchmark_id: str | None = None
    benchmark_symbol: str | None = None
    source: str | None = None
    source_version: str | None = None
    resolution_status: str = "confirmed"
    coverage_status: str = "confirmed"
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReferenceDataCoverageSummary:
    as_of_date: str
    universe_id: str | None = None
    confirmed_membership_count: int = 0
    fallback_membership_count: int = 0
    unavailable_membership_count: int = 0
    confirmed_taxonomy_count: int = 0
    fallback_taxonomy_count: int = 0
    unavailable_taxonomy_count: int = 0
    confirmed_benchmark_mapping_count: int = 0
    fallback_benchmark_mapping_count: int = 0
    unavailable_benchmark_mapping_count: int = 0
    source_versions: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReferenceDataVersionManifest:
    version: str
    generated_at: str | None = None
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)
    notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
