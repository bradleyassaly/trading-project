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


def _flat_dict(payload: dict[str, Any], *, skip_keys: set[str] | None = None) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in payload.items():
        if skip_keys and key in skip_keys:
            continue
        flat[key] = _csv_scalar(value)
    return flat


@dataclass(frozen=True)
class ScreenCheckResult:
    check_name: str
    status: str
    passed: bool | None = None
    value: float | str | None = None
    threshold: float | str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SignalBreakdown:
    signal_name: str
    final_score: float | None = None
    confidence: float | None = None
    raw_components: dict[str, float | str | None] = field(default_factory=dict)
    transformed_components: dict[str, float | str | None] = field(default_factory=dict)
    reason_labels: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateEvaluation:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    side: str | None
    strategy_id: str | None
    universe_id: str | None
    candidate_status: str
    final_signal_score: float | None = None
    rank: int | None = None
    rank_percentile: float | None = None
    rejection_reason: str | None = None
    selected_feature_values: dict[str, float | str | None] = field(default_factory=dict)
    signal_breakdown: SignalBreakdown | None = None
    screening_checks: list[ScreenCheckResult] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["signal_breakdown"] = self.signal_breakdown.to_dict() if self.signal_breakdown is not None else None
        payload["screening_checks"] = [check.to_dict() for check in self.screening_checks]
        return payload

    def flat_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        flat = _flat_dict(payload, skip_keys={"signal_breakdown", "screening_checks", "selected_feature_values", "metadata"})
        flat["feature_snapshot"] = _csv_scalar(self.selected_feature_values)
        flat["screen_checks"] = _csv_scalar([f"{row.check_name}:{row.status}" for row in self.screening_checks])
        flat["signal_components"] = _csv_scalar(self.signal_breakdown.raw_components if self.signal_breakdown else {})
        flat["reason_labels"] = _csv_scalar(self.signal_breakdown.reason_labels if self.signal_breakdown else [])
        flat["metadata"] = _csv_scalar(self.metadata)
        return flat


@dataclass(frozen=True)
class PortfolioSelectionDecision:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    strategy_id: str | None
    selection_status: str
    selected: bool
    final_signal_score: float | None = None
    rank: int | None = None
    rank_percentile: float | None = None
    candidate_count: int | None = None
    selected_count: int | None = None
    target_weight_pre_constraint: float | None = None
    target_weight_post_constraint: float | None = None
    rejection_reason: str | None = None
    rationale_summary: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        return _flat_dict(self.to_dict())


@dataclass(frozen=True)
class SizingDecision:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    strategy_id: str | None
    side: str | None
    target_weight_pre_constraint: float | None = None
    target_weight_post_constraint: float | None = None
    target_quantity: int | None = None
    current_quantity: int | None = None
    portfolio_equity: float | None = None
    investable_equity: float | None = None
    reserve_cash_pct: float | None = None
    sizing_inputs: dict[str, Any] = field(default_factory=dict)
    rationale_summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = _flat_dict(self.to_dict(), skip_keys={"sizing_inputs"})
        payload["sizing_inputs"] = _csv_scalar(self.sizing_inputs)
        return payload


@dataclass(frozen=True)
class TradeDecisionRecord:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    side: str | None
    strategy_id: str | None
    universe_id: str | None
    candidate_status: str
    entry_reason_summary: str | None = None
    rejection_reason: str | None = None
    final_signal_score: float | None = None
    target_weight_pre_constraint: float | None = None
    target_weight_post_constraint: float | None = None
    target_quantity: int | None = None
    current_quantity: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        return _flat_dict(self.to_dict())


@dataclass(frozen=True)
class ExecutionDecisionRecord:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    side: str | None
    strategy_id: str | None
    order_status: str
    requested_shares: int | None = None
    adjusted_shares: int | None = None
    requested_notional: float | None = None
    adjusted_notional: float | None = None
    target_weight: float | None = None
    current_weight: float | None = None
    estimated_fill_price: float | None = None
    commission: float | None = None
    slippage_bps: float | None = None
    rejection_reason: str | None = None
    rationale_summary: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        return _flat_dict(self.to_dict())


@dataclass(frozen=True)
class ExitDecisionRecord:
    decision_id: str
    timestamp: str
    run_id: str | None
    cycle_id: str | None
    symbol: str
    side: str | None
    strategy_id: str | None
    exit_trigger_type: str
    exit_reason_summary: str | None = None
    supporting_values: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = _flat_dict(self.to_dict(), skip_keys={"supporting_values"})
        payload["supporting_values"] = _csv_scalar(self.supporting_values)
        return payload


@dataclass(frozen=True)
class TradeLifecycleRecord:
    trade_id: str
    decision_id: str
    timestamp: str
    symbol: str
    strategy_id: str | None
    stage: str
    status: str
    summary: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def flat_dict(self) -> dict[str, Any]:
        payload = _flat_dict(self.to_dict(), skip_keys={"details"})
        payload["details"] = _csv_scalar(self.details)
        return payload


@dataclass
class DecisionJournalBundle:
    candidate_evaluations: list[CandidateEvaluation] = field(default_factory=list)
    selection_decisions: list[PortfolioSelectionDecision] = field(default_factory=list)
    sizing_decisions: list[SizingDecision] = field(default_factory=list)
    trade_decisions: list[TradeDecisionRecord] = field(default_factory=list)
    execution_decisions: list[ExecutionDecisionRecord] = field(default_factory=list)
    exit_decisions: list[ExitDecisionRecord] = field(default_factory=list)
    lifecycle_records: list[TradeLifecycleRecord] = field(default_factory=list)
    provenance_by_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_evaluations": [row.to_dict() for row in self.candidate_evaluations],
            "selection_decisions": [row.to_dict() for row in self.selection_decisions],
            "sizing_decisions": [row.to_dict() for row in self.sizing_decisions],
            "trade_decisions": [row.to_dict() for row in self.trade_decisions],
            "execution_decisions": [row.to_dict() for row in self.execution_decisions],
            "exit_decisions": [row.to_dict() for row in self.exit_decisions],
            "lifecycle_records": [row.to_dict() for row in self.lifecycle_records],
            "provenance_by_symbol": dict(self.provenance_by_symbol),
        }
