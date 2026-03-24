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
class PointInTimeUniverseMembership:
    symbol: str
    as_of: str
    base_universe_id: str | None
    membership_status: str
    membership_source: str
    membership_resolution_status: str
    membership_confidence: float | None = None
    effective_start: str | None = None
    effective_end: str | None = None
    unavailable_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["metadata"] = _csv_scalar(payload["metadata"])
        return payload


@dataclass(frozen=True)
class TaxonomySnapshot:
    symbol: str
    as_of: str
    sector: str | None = None
    industry: str | None = None
    group: str | None = None
    taxonomy_source: str | None = None
    taxonomy_resolution_status: str | None = None
    unavailable_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkContextSnapshot:
    symbol: str
    as_of: str
    benchmark_id: str | None = None
    benchmark_symbol: str | None = None
    relative_strength_20: float | None = None
    benchmark_return_20: float | None = None
    asset_return_20: float | None = None
    benchmark_source: str | None = None
    benchmark_resolution_status: str | None = None
    unavailable_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SymbolMetadataSnapshot:
    symbol: str
    as_of: str
    latest_price: float | None = None
    avg_dollar_volume_20: float | None = None
    volatility_20: float | None = None
    feature_history_bars: int | None = None
    feature_availability_status: str | None = None
    regime_label: str | None = None
    regime_source: str | None = None
    regime_resolution_status: str | None = None
    missing_fields: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseEnrichmentRecord:
    symbol: str
    as_of: str
    base_universe_id: str | None
    sub_universe_id: str | None
    membership: PointInTimeUniverseMembership
    taxonomy: TaxonomySnapshot
    benchmark_context: BenchmarkContextSnapshot
    metadata_snapshot: SymbolMetadataSnapshot
    metadata_coverage_status: str
    unavailable_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        return payload

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        flat: dict[str, Any] = {
            "symbol": payload["symbol"],
            "as_of": payload["as_of"],
            "base_universe_id": payload["base_universe_id"],
            "sub_universe_id": payload["sub_universe_id"],
            "membership_status": payload["membership"]["membership_status"],
            "membership_source": payload["membership"]["membership_source"],
            "membership_resolution_status": payload["membership"]["membership_resolution_status"],
            "sector": payload["taxonomy"]["sector"],
            "industry": payload["taxonomy"]["industry"],
            "group": payload["taxonomy"]["group"],
            "taxonomy_resolution_status": payload["taxonomy"]["taxonomy_resolution_status"],
            "benchmark_id": payload["benchmark_context"]["benchmark_id"],
            "relative_strength_20": payload["benchmark_context"]["relative_strength_20"],
            "benchmark_resolution_status": payload["benchmark_context"]["benchmark_resolution_status"],
            "latest_price": payload["metadata_snapshot"]["latest_price"],
            "avg_dollar_volume_20": payload["metadata_snapshot"]["avg_dollar_volume_20"],
            "volatility_20": payload["metadata_snapshot"]["volatility_20"],
            "feature_history_bars": payload["metadata_snapshot"]["feature_history_bars"],
            "feature_availability_status": payload["metadata_snapshot"]["feature_availability_status"],
            "regime_label": payload["metadata_snapshot"]["regime_label"],
            "regime_resolution_status": payload["metadata_snapshot"]["regime_resolution_status"],
            "metadata_coverage_status": payload["metadata_coverage_status"],
            "unavailable_reason": payload["unavailable_reason"],
            "missing_fields": _csv_scalar(payload["metadata_snapshot"]["missing_fields"]),
            "metadata": _csv_scalar(payload["metadata"]),
        }
        return flat


@dataclass(frozen=True)
class UniverseEnrichmentSummary:
    as_of: str
    base_universe_id: str | None
    sub_universe_id: str | None
    confirmed_membership_count: int
    inferred_membership_count: int
    static_fallback_membership_count: int
    unavailable_membership_count: int
    taxonomy_coverage_count: int
    benchmark_context_coverage_count: int
    regime_coverage_count: int
    metadata_coverage_counts: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

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
    membership_source: str | None = None
    membership_resolution_status: str | None = None
    membership_confidence: float | None = None
    inclusion_reason: str | None = None
    exclusion_reason: str | None = None
    stage_name: str = "final_eligibility"
    filter_failures: list[str] = field(default_factory=list)
    filter_passes: list[str] = field(default_factory=list)
    group_label: str | None = None
    sector: str | None = None
    industry: str | None = None
    benchmark_id: str | None = None
    benchmark_symbol: str | None = None
    regime_label: str | None = None
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
    point_in_time_membership: list[PointInTimeUniverseMembership] = field(default_factory=list)
    enrichment_records: list[UniverseEnrichmentRecord] = field(default_factory=list)
    membership_records: list[UniverseMembershipRecord] = field(default_factory=list)
    summary: UniverseBuildSummary | None = None
    enrichment_summary: UniverseEnrichmentSummary | None = None

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
            "point_in_time_membership": [row.to_dict() for row in self.point_in_time_membership],
            "enrichment_records": [row.to_dict() for row in self.enrichment_records],
            "membership_records": [row.to_dict() for row in self.membership_records],
            "summary": self.summary.to_dict() if self.summary is not None else None,
            "enrichment_summary": self.enrichment_summary.to_dict() if self.enrichment_summary is not None else None,
        }
