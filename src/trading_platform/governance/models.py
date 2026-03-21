from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


STRATEGY_REGISTRY_SCHEMA_VERSION = 1
STRATEGY_STATUSES = {
    "research",
    "candidate",
    "paper",
    "approved",
    "live_disabled",
    "retired",
}
DEPLOYMENT_STAGES = {
    "research",
    "candidate",
    "paper",
    "approved",
    "live_disabled",
    "retired",
}
STATUS_TRANSITIONS: dict[str, set[str]] = {
    "research": {"candidate", "retired"},
    "candidate": {"research", "paper", "retired"},
    "paper": {"candidate", "approved", "live_disabled", "retired"},
    "approved": {"paper", "live_disabled", "retired"},
    "live_disabled": {"paper", "approved", "retired"},
    "retired": set(),
}


def _validate_optional_nonnegative(value: float | int | None, field_name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be >= 0")


@dataclass(frozen=True)
class StrategyRegistryEntry:
    strategy_id: str
    strategy_name: str
    family: str
    version: str
    preset_name: str
    research_artifact_paths: list[str]
    created_at: str
    status: str
    owner: str
    source: str
    current_deployment_stage: str
    notes: str | None = None
    tags: list[str] = field(default_factory=list)
    universe: str | None = None
    signal_type: str | None = None
    rebalance_frequency: str | None = None
    benchmark: str | None = None
    risk_profile: str | None = None
    paper_artifact_path: str | None = None
    live_artifact_path: str | None = None
    latest_promotion_decision_path: str | None = None
    latest_degradation_report_path: str | None = None
    latest_metrics_snapshot_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in [
            "strategy_id",
            "strategy_name",
            "family",
            "version",
            "preset_name",
            "created_at",
            "owner",
            "source",
        ]:
            if not getattr(self, field_name) or not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if not self.research_artifact_paths:
            raise ValueError("research_artifact_paths must contain at least one path")
        if self.status not in STRATEGY_STATUSES:
            raise ValueError(f"Unsupported status: {self.status}")
        if self.current_deployment_stage not in DEPLOYMENT_STAGES:
            raise ValueError(
                f"Unsupported current_deployment_stage: {self.current_deployment_stage}"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyRegistryAuditEvent:
    timestamp: str
    strategy_id: str
    action: str
    from_status: str | None = None
    to_status: str | None = None
    note: str | None = None

    def __post_init__(self) -> None:
        if not self.timestamp or not self.timestamp.strip():
            raise ValueError("timestamp must be a non-empty string")
        if not self.strategy_id or not self.strategy_id.strip():
            raise ValueError("strategy_id must be a non-empty string")
        if not self.action or not self.action.strip():
            raise ValueError("action must be a non-empty string")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyRegistry:
    schema_version: int = STRATEGY_REGISTRY_SCHEMA_VERSION
    updated_at: str = ""
    entries: list[StrategyRegistryEntry] = field(default_factory=list)
    audit_log: list[StrategyRegistryAuditEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != STRATEGY_REGISTRY_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported strategy registry schema_version: {self.schema_version}"
            )
        strategy_ids = [entry.strategy_id for entry in self.entries]
        if len(strategy_ids) != len(set(strategy_ids)):
            raise ValueError("strategy registry contains duplicate strategy_id values")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "updated_at": self.updated_at,
            "entries": [entry.to_dict() for entry in self.entries],
            "audit_log": [event.to_dict() for event in self.audit_log],
        }


@dataclass(frozen=True)
class PromotionCriteria:
    minimum_walk_forward_folds: int = 1
    minimum_mean_test_return: float | None = None
    minimum_sharpe: float | None = None
    maximum_drawdown: float | None = None
    minimum_hit_rate: float | None = None
    minimum_ic_rank_ic: float | None = None
    maximum_turnover: float | None = None
    maximum_redundancy_correlation: float | None = None
    minimum_paper_trading_observation_window: int | None = None
    minimum_trade_count: int | None = None

    def __post_init__(self) -> None:
        if self.minimum_walk_forward_folds < 0:
            raise ValueError("minimum_walk_forward_folds must be >= 0")
        _validate_optional_nonnegative(
            self.minimum_paper_trading_observation_window,
            "minimum_paper_trading_observation_window",
        )
        _validate_optional_nonnegative(self.minimum_trade_count, "minimum_trade_count")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DegradationCriteria:
    maximum_rolling_underperformance_vs_benchmark: float | None = None
    maximum_drawdown: float | None = None
    maximum_turnover: float | None = None
    minimum_signal_stability: float | None = None
    maximum_missing_data_failures: int | None = None
    maximum_live_fail_checks: int | None = None
    maximum_live_warn_checks: int | None = None

    def __post_init__(self) -> None:
        _validate_optional_nonnegative(
            self.maximum_missing_data_failures, "maximum_missing_data_failures"
        )
        _validate_optional_nonnegative(
            self.maximum_live_fail_checks, "maximum_live_fail_checks"
        )
        _validate_optional_nonnegative(
            self.maximum_live_warn_checks, "maximum_live_warn_checks"
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GovernanceCriteriaConfig:
    promotion: PromotionCriteria = field(default_factory=PromotionCriteria)
    degradation: DegradationCriteria = field(default_factory=DegradationCriteria)

    def to_dict(self) -> dict[str, Any]:
        return {
            "promotion": self.promotion.to_dict(),
            "degradation": self.degradation.to_dict(),
        }


@dataclass(frozen=True)
class StrategyMetricsSnapshot:
    strategy_id: str
    strategy_name: str
    family: str
    version: str
    preset_name: str
    timestamp: str
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CriteriaComparison:
    criterion: str
    actual: Any
    threshold: Any
    passed: bool
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GovernanceDecisionReport:
    strategy_id: str
    strategy_name: str
    family: str
    version: str
    timestamp: str
    decision_type: str
    passed: bool
    failed_criteria: list[str]
    summary_metrics: dict[str, Any]
    comparisons: list[CriteriaComparison]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "family": self.family,
            "version": self.version,
            "timestamp": self.timestamp,
            "decision_type": self.decision_type,
            "passed": self.passed,
            "failed_criteria": self.failed_criteria,
            "summary_metrics": self.summary_metrics,
            "comparisons": [item.to_dict() for item in self.comparisons],
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True)
class RegistrySelectionOptions:
    include_statuses: list[str] = field(default_factory=lambda: ["approved"])
    universe: str | None = None
    family: str | None = None
    tag: str | None = None
    deployment_stage: str | None = None
    max_strategies: int | None = None
    weighting_scheme: str = "equal"

    def __post_init__(self) -> None:
        for status in self.include_statuses:
            if status not in STRATEGY_STATUSES:
                raise ValueError(f"Unsupported include status: {status}")
        if self.deployment_stage is not None and self.deployment_stage not in DEPLOYMENT_STAGES:
            raise ValueError(
                f"Unsupported deployment_stage filter: {self.deployment_stage}"
            )
        if self.max_strategies is not None and self.max_strategies <= 0:
            raise ValueError("max_strategies must be > 0")
        if self.weighting_scheme not in {"equal", "score_weighted"}:
            raise ValueError("weighting_scheme must be one of: equal, score_weighted")
