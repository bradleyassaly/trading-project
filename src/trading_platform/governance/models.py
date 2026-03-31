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


def _normalize_string_list(value: list[Any] | tuple[Any, ...] | str | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _normalize_metadata(value: dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return {str(key): value[key] for key in sorted(value)}


def _normalize_promotion_gate_results(
    value: list[Any] | tuple[Any, ...] | None,
) -> list["PromotionGateResult"]:
    if value is None:
        return []
    return [
        item if isinstance(item, PromotionGateResult) else PromotionGateResult.from_dict(item)
        for item in value
    ]


@dataclass(frozen=True)
class PromotionGateResult:
    gate_name: str
    passed: bool
    reason_code: str
    actual: float | int | None = None
    threshold: float | int | None = None
    comparator: str | None = None
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.gate_name or "").strip():
            raise ValueError("gate_name must be a non-empty string")
        if not str(self.reason_code or "").strip():
            raise ValueError("reason_code must be a non-empty string")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PromotionGateResult":
        data = dict(payload or {})
        data.setdefault("actual", None)
        data.setdefault("threshold", None)
        data.setdefault("comparator", None)
        data.setdefault("message", None)
        data.setdefault("metadata", {})
        return cls(
            gate_name=str(data["gate_name"]),
            passed=bool(data["passed"]),
            reason_code=str(data["reason_code"]),
            actual=data.get("actual"),
            threshold=data.get("threshold"),
            comparator=str(data["comparator"]) if data.get("comparator") is not None else None,
            message=str(data["message"]) if data.get("message") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class PromotionGateEvaluation:
    candidate_id: str
    passed: bool
    gate_results: list[PromotionGateResult]
    rejection_reasons: list[str] = field(default_factory=list)
    passed_gate_names: list[str] = field(default_factory=list)
    failed_gate_names: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id must be a non-empty string")
        object.__setattr__(self, "rejection_reasons", _normalize_string_list(self.rejection_reasons))
        object.__setattr__(self, "passed_gate_names", _normalize_string_list(self.passed_gate_names))
        object.__setattr__(self, "failed_gate_names", _normalize_string_list(self.failed_gate_names))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PromotionGateEvaluation":
        data = dict(payload or {})
        data.setdefault("rejection_reasons", [])
        data.setdefault("passed_gate_names", [])
        data.setdefault("failed_gate_names", [])
        data.setdefault("metadata", {})
        gate_rows = [PromotionGateResult.from_dict(row) for row in data.get("gate_results", [])]
        return cls(
            candidate_id=str(data["candidate_id"]),
            passed=bool(data["passed"]),
            gate_results=gate_rows,
            rejection_reasons=_normalize_string_list(data.get("rejection_reasons")),
            passed_gate_names=_normalize_string_list(data.get("passed_gate_names")),
            failed_gate_names=_normalize_string_list(data.get("failed_gate_names")),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "passed": self.passed,
            "gate_results": [row.to_dict() for row in self.gate_results],
            "rejection_reasons": list(self.rejection_reasons),
            "passed_gate_names": list(self.passed_gate_names),
            "failed_gate_names": list(self.failed_gate_names),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class PromotionDecision:
    candidate_id: str
    final_status: str
    gate_results: list[PromotionGateResult] = field(default_factory=list)
    passed_gate_names: list[str] = field(default_factory=list)
    failed_gate_names: list[str] = field(default_factory=list)
    rejection_reasons: list[str] = field(default_factory=list)
    summary_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id must be a non-empty string")
        if not str(self.final_status or "").strip():
            raise ValueError("final_status must be a non-empty string")
        object.__setattr__(self, "gate_results", _normalize_promotion_gate_results(self.gate_results))
        object.__setattr__(self, "passed_gate_names", _normalize_string_list(self.passed_gate_names))
        object.__setattr__(self, "failed_gate_names", _normalize_string_list(self.failed_gate_names))
        object.__setattr__(self, "rejection_reasons", _normalize_string_list(self.rejection_reasons))
        object.__setattr__(self, "summary_metadata", _normalize_metadata(self.summary_metadata))

    @classmethod
    def from_gate_evaluation(
        cls,
        evaluation: "PromotionGateEvaluation",
        *,
        final_status: str,
        summary_metadata: dict[str, Any] | None = None,
    ) -> "PromotionDecision":
        metadata = {
            "gate_count": len(evaluation.gate_results),
            "passed_gate_count": len(evaluation.passed_gate_names),
            "failed_gate_count": len(evaluation.failed_gate_names),
            "candidate_id": evaluation.candidate_id,
            **evaluation.metadata,
        }
        if summary_metadata:
            metadata.update(summary_metadata)
        return cls(
            candidate_id=evaluation.candidate_id,
            final_status=final_status,
            gate_results=list(evaluation.gate_results),
            passed_gate_names=list(evaluation.passed_gate_names),
            failed_gate_names=list(evaluation.failed_gate_names),
            rejection_reasons=list(evaluation.rejection_reasons),
            summary_metadata=metadata,
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PromotionDecision":
        data = dict(payload or {})
        data.setdefault("gate_results", [])
        data.setdefault("passed_gate_names", [])
        data.setdefault("failed_gate_names", [])
        data.setdefault("rejection_reasons", [])
        data.setdefault("summary_metadata", {})
        return cls(
            candidate_id=str(data["candidate_id"]),
            final_status=str(data["final_status"]),
            gate_results=[PromotionGateResult.from_dict(row) for row in data.get("gate_results", [])],
            passed_gate_names=_normalize_string_list(data.get("passed_gate_names")),
            failed_gate_names=_normalize_string_list(data.get("failed_gate_names")),
            rejection_reasons=_normalize_string_list(data.get("rejection_reasons")),
            summary_metadata=_normalize_metadata(data.get("summary_metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "final_status": self.final_status,
            "gate_results": [row.to_dict() for row in self.gate_results],
            "passed_gate_names": list(self.passed_gate_names),
            "failed_gate_names": list(self.failed_gate_names),
            "rejection_reasons": list(self.rejection_reasons),
            "summary_metadata": dict(self.summary_metadata),
        }


@dataclass(frozen=True)
class LiveReadinessCheckResult:
    check_name: str
    passed: bool
    reason_code: str
    status: str = "missing"
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.check_name or "").strip():
            raise ValueError("check_name must be a non-empty string")
        if not str(self.reason_code or "").strip():
            raise ValueError("reason_code must be a non-empty string")
        if self.status not in {"ready", "missing", "blocked", "unknown"}:
            raise ValueError("status must be one of: ready, missing, blocked, unknown")
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LiveReadinessCheckResult":
        data = dict(payload or {})
        data.setdefault("status", "missing")
        data.setdefault("message", None)
        data.setdefault("metadata", {})
        return cls(
            check_name=str(data["check_name"]),
            passed=bool(data["passed"]),
            reason_code=str(data["reason_code"]),
            status=str(data["status"]),
            message=str(data["message"]) if data.get("message") is not None else None,
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "reason_code": self.reason_code,
            "status": self.status,
            "message": self.message,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class LiveReadinessDecision:
    strategy_id: str
    ready_for_live: bool
    live_trading_enabled: bool = False
    final_status: str = "not_ready"
    check_results: list[LiveReadinessCheckResult] = field(default_factory=list)
    passed_check_names: list[str] = field(default_factory=list)
    failed_check_names: list[str] = field(default_factory=list)
    blocking_reasons: list[str] = field(default_factory=list)
    summary_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.strategy_id or "").strip():
            raise ValueError("strategy_id must be a non-empty string")
        if self.final_status not in {"not_ready", "shadow_only", "ready_candidate"}:
            raise ValueError("final_status must be one of: not_ready, shadow_only, ready_candidate")
        normalized_checks = [
            item if isinstance(item, LiveReadinessCheckResult) else LiveReadinessCheckResult.from_dict(item)
            for item in self.check_results
        ]
        object.__setattr__(self, "check_results", normalized_checks)
        object.__setattr__(self, "passed_check_names", _normalize_string_list(self.passed_check_names))
        object.__setattr__(self, "failed_check_names", _normalize_string_list(self.failed_check_names))
        object.__setattr__(self, "blocking_reasons", _normalize_string_list(self.blocking_reasons))
        object.__setattr__(self, "summary_metadata", _normalize_metadata(self.summary_metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LiveReadinessDecision":
        data = dict(payload or {})
        data.setdefault("live_trading_enabled", False)
        data.setdefault("final_status", "not_ready")
        data.setdefault("check_results", [])
        data.setdefault("passed_check_names", [])
        data.setdefault("failed_check_names", [])
        data.setdefault("blocking_reasons", [])
        data.setdefault("summary_metadata", {})
        return cls(
            strategy_id=str(data["strategy_id"]),
            ready_for_live=bool(data["ready_for_live"]),
            live_trading_enabled=bool(data["live_trading_enabled"]),
            final_status=str(data["final_status"]),
            check_results=[LiveReadinessCheckResult.from_dict(row) for row in data.get("check_results", [])],
            passed_check_names=_normalize_string_list(data.get("passed_check_names")),
            failed_check_names=_normalize_string_list(data.get("failed_check_names")),
            blocking_reasons=_normalize_string_list(data.get("blocking_reasons")),
            summary_metadata=_normalize_metadata(data.get("summary_metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "ready_for_live": self.ready_for_live,
            "live_trading_enabled": self.live_trading_enabled,
            "final_status": self.final_status,
            "check_results": [row.to_dict() for row in self.check_results],
            "passed_check_names": list(self.passed_check_names),
            "failed_check_names": list(self.failed_check_names),
            "blocking_reasons": list(self.blocking_reasons),
            "summary_metadata": dict(self.summary_metadata),
        }


def build_live_readiness_skeleton(
    strategy_id: str,
    *,
    summary_metadata: dict[str, Any] | None = None,
) -> LiveReadinessDecision:
    checks = [
        LiveReadinessCheckResult(
            check_name="monitoring_coverage",
            passed=False,
            reason_code="monitoring_not_validated",
            status="missing",
            message="Monitoring readiness has not been validated for live trading.",
        ),
        LiveReadinessCheckResult(
            check_name="reconciliation_coverage",
            passed=False,
            reason_code="reconciliation_not_validated",
            status="missing",
            message="Reconciliation readiness has not been validated for live trading.",
        ),
        LiveReadinessCheckResult(
            check_name="execution_support",
            passed=False,
            reason_code="execution_support_not_validated",
            status="missing",
            message="Execution support readiness has not been validated for live trading.",
        ),
        LiveReadinessCheckResult(
            check_name="capital_controls",
            passed=False,
            reason_code="capital_controls_not_validated",
            status="missing",
            message="Capital controls have not been validated for live trading.",
        ),
        LiveReadinessCheckResult(
            check_name="risk_controls",
            passed=False,
            reason_code="risk_controls_not_validated",
            status="missing",
            message="Risk controls have not been validated for live trading.",
        ),
        LiveReadinessCheckResult(
            check_name="operator_approval",
            passed=False,
            reason_code="operator_approval_missing",
            status="blocked",
            message="Operator approval is required before any live path can be enabled.",
        ),
    ]
    metadata = {
        "governance_scaffolding_only": True,
        "live_path_enabled_by_default": False,
        "required_dimensions": [
            "monitoring_coverage",
            "reconciliation_coverage",
            "execution_support",
            "capital_controls",
            "risk_controls",
            "operator_approval",
        ],
    }
    if summary_metadata:
        metadata.update(summary_metadata)
    return LiveReadinessDecision(
        strategy_id=strategy_id,
        ready_for_live=False,
        live_trading_enabled=False,
        final_status="not_ready",
        check_results=checks,
        passed_check_names=[],
        failed_check_names=[check.check_name for check in checks],
        blocking_reasons=[check.reason_code for check in checks if not check.passed],
        summary_metadata=metadata,
    )


@dataclass(frozen=True)
class StrategyScorecard:
    candidate_id: str
    strategy_family: str
    training_period: str | None = None
    validation_period: str | None = None
    prediction_count: int | None = None
    realized_return: float | None = None
    expected_return: float | None = None
    turnover: float | None = None
    slippage_estimate: float | None = None
    drawdown: float | None = None
    calibration_score: float | None = None
    stability_score: float | None = None
    regime_robustness_score: float | None = None
    readiness_flags: list[str] = field(default_factory=list)
    rejection_reasons: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.candidate_id or "").strip():
            raise ValueError("candidate_id must be a non-empty string")
        if not str(self.strategy_family or "").strip():
            raise ValueError("strategy_family must be a non-empty string")
        if self.prediction_count is not None and int(self.prediction_count) < 0:
            raise ValueError("prediction_count must be >= 0")
        for field_name in (
            "turnover",
            "slippage_estimate",
            "drawdown",
            "calibration_score",
            "stability_score",
            "regime_robustness_score",
        ):
            _validate_optional_nonnegative(getattr(self, field_name), field_name)
        object.__setattr__(self, "readiness_flags", _normalize_string_list(self.readiness_flags))
        object.__setattr__(self, "rejection_reasons", _normalize_string_list(self.rejection_reasons))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyScorecard":
        data = dict(payload or {})
        data.setdefault("training_period", None)
        data.setdefault("validation_period", None)
        data.setdefault("prediction_count", None)
        data.setdefault("realized_return", None)
        data.setdefault("expected_return", None)
        data.setdefault("turnover", None)
        data.setdefault("slippage_estimate", None)
        data.setdefault("drawdown", None)
        data.setdefault("calibration_score", None)
        data.setdefault("stability_score", None)
        data.setdefault("regime_robustness_score", None)
        data.setdefault("readiness_flags", [])
        data.setdefault("rejection_reasons", [])
        data.setdefault("metadata", {})
        return cls(
            candidate_id=str(data["candidate_id"]),
            strategy_family=str(data["strategy_family"]),
            training_period=str(data["training_period"]) if data.get("training_period") is not None else None,
            validation_period=str(data["validation_period"]) if data.get("validation_period") is not None else None,
            prediction_count=int(data["prediction_count"]) if data.get("prediction_count") is not None else None,
            realized_return=float(data["realized_return"]) if data.get("realized_return") is not None else None,
            expected_return=float(data["expected_return"]) if data.get("expected_return") is not None else None,
            turnover=float(data["turnover"]) if data.get("turnover") is not None else None,
            slippage_estimate=(
                float(data["slippage_estimate"]) if data.get("slippage_estimate") is not None else None
            ),
            drawdown=float(data["drawdown"]) if data.get("drawdown") is not None else None,
            calibration_score=(
                float(data["calibration_score"]) if data.get("calibration_score") is not None else None
            ),
            stability_score=float(data["stability_score"]) if data.get("stability_score") is not None else None,
            regime_robustness_score=(
                float(data["regime_robustness_score"])
                if data.get("regime_robustness_score") is not None
                else None
            ),
            readiness_flags=_normalize_string_list(data.get("readiness_flags")),
            rejection_reasons=_normalize_string_list(data.get("rejection_reasons")),
            metadata=_normalize_metadata(data.get("metadata")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["readiness_flags"] = list(self.readiness_flags)
        payload["rejection_reasons"] = list(self.rejection_reasons)
        payload["metadata"] = dict(self.metadata)
        return payload


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
