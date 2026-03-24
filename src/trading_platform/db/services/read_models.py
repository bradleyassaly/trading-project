from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class RunQueryFilters:
    status: str | None = None
    run_kind: str | None = None
    run_type: str | None = None
    mode: str | None = None
    strategy: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 20
    offset: int = 0
    sort_desc: bool = True


@dataclass(frozen=True)
class DecisionQueryFilters:
    symbol: str | None = None
    strategy: str | None = None
    decision_status: str | None = None
    run_id: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 100
    offset: int = 0
    sort_desc: bool = True


@dataclass(frozen=True)
class StrategyHistoryFilters:
    strategy: str | None = None
    decision: str | None = None
    active: bool | None = None
    status: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 20
    offset: int = 0
    sort_desc: bool = True


@dataclass(frozen=True)
class OpsActivityFilters:
    status: str | None = None
    activity_type: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 20
    offset: int = 0


@dataclass(frozen=True)
class PagedResultReadModel:
    items: list[Any]
    total_count: int
    limit: int
    offset: int
    source: str = "db"

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.items) < self.total_count

    def to_dict(self) -> dict[str, Any]:
        serialized: list[Any] = []
        for item in self.items:
            to_dict = getattr(item, "to_dict", None)
            serialized.append(to_dict() if callable(to_dict) else item)
        return {
            "items": serialized,
            "total_count": self.total_count,
            "limit": self.limit,
            "offset": self.offset,
            "has_more": self.has_more,
            "source": self.source,
        }


@dataclass(frozen=True)
class ArtifactReadModel:
    artifact_id: str
    artifact_type: str
    path: str
    format: str | None = None
    content_hash: str | None = None
    schema_version: str | None = None
    row_count: int | None = None
    role: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunSummaryReadModel:
    run_id: str
    run_kind: str
    run_name: str
    status: str
    started_at: str | None = None
    completed_at: str | None = None
    mode: str | None = None
    run_type: str | None = None
    config_hash: str | None = None
    git_commit: str | None = None
    notes: str | None = None
    artifact_count: int = 0
    artifact_dir: str | None = None
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunDetailReadModel:
    summary: RunSummaryReadModel
    config_json: dict[str, Any] = field(default_factory=dict)
    artifacts: list[ArtifactReadModel] = field(default_factory=list)
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["summary"] = self.summary.to_dict()
        payload["artifacts"] = [artifact.to_dict() for artifact in self.artifacts]
        return payload


@dataclass(frozen=True)
class TradeDecisionReadModel:
    trade_id: str
    portfolio_run_id: str
    timestamp: str | None
    symbol: str
    side: str | None
    quantity: int | None
    target_weight: float | None
    strategy_id: str | None
    signal_score: float | None
    rank_score: float | None
    expected_edge: float | None
    order_status: str | None
    status: str
    entry_reason_summary: str | None = None
    rejection_reason: str | None = None
    base_universe_id: str | None = None
    sub_universe_id: str | None = None
    run_name: str | None = None
    mode: str | None = None
    explanation: dict[str, Any] = field(default_factory=dict)
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateEvaluationReadModel:
    evaluation_id: str
    portfolio_run_id: str
    symbol: str
    base_universe_id: str | None
    sub_universe_id: str | None
    score: float | None
    rank: int | None
    candidate_status: str
    rejection_reason: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSummaryReadModel:
    order_id: str
    symbol: str
    side: str | None
    status: str
    submitted_at: str | None = None
    updated_at: str | None = None
    quantity: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    broker: str | None = None
    broker_order_id: str | None = None
    event_count: int = 0
    fill_count: int = 0
    fills: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PromotionReadModel:
    promotion_decision_id: str
    strategy_name: str
    strategy_version: str
    decision: str
    reason: str | None
    created_at: str | None
    source_research_run_name: str | None = None
    promoted_status: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpsHealthReadModel:
    latest_run_name: str | None
    latest_run_status: str | None
    latest_run_kind: str | None
    latest_run_started_at: str | None
    latest_run_completed_at: str | None
    recent_failure_count: int
    research_run_count: int
    portfolio_run_count: int
    recent_promotion_count: int
    recent_execution_event_count: int
    source: str = "db"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
