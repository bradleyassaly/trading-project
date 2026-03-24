from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.execution import Order
from trading_platform.db.models.portfolio import DecisionSignalContribution, PortfolioDecision
from trading_platform.db.models.provenance import CandidateEvaluation, UniverseFilterResult
from trading_platform.db.models.reference import Symbol
from trading_platform.db.models.runs import PortfolioRun
from trading_platform.db.services.artifact_query_service import ArtifactQueryService
from trading_platform.db.services.execution_query_service import ExecutionQueryService
from trading_platform.db.services.read_models import CandidateEvaluationReadModel, DecisionQueryFilters, PagedResultReadModel, TradeDecisionReadModel


def _iso(value: object) -> str | None:
    if value is None:
        return None
    text = getattr(value, "isoformat", None)
    return text() if callable(text) else str(value)


def _safe_dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _uuid_value(value: object) -> object:
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return value
    return value


class DecisionQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory
        self.execution_queries = ExecutionQueryService(session_factory)
        self.artifact_queries = ArtifactQueryService(session_factory)

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def default_filters(self, **overrides: Any) -> DecisionQueryFilters:
        return DecisionQueryFilters(**overrides)

    def list_trade_decisions(self, filters: DecisionQueryFilters | None = None) -> PagedResultReadModel:
        filters = filters or DecisionQueryFilters()
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=filters.limit, offset=filters.offset, source="db")
        with self.session_factory() as session:
            statement = (
                select(PortfolioDecision, Symbol, PortfolioRun)
                .join(Symbol, Symbol.id == PortfolioDecision.symbol_id)
                .join(PortfolioRun, PortfolioRun.id == PortfolioDecision.portfolio_run_id)
            )
            count_statement = select(func.count(PortfolioDecision.id)).select_from(PortfolioDecision)
            if filters.symbol:
                statement = statement.where(Symbol.symbol == filters.symbol.upper())
                count_statement = count_statement.join(Symbol, Symbol.id == PortfolioDecision.symbol_id).where(Symbol.symbol == filters.symbol.upper())
            if filters.strategy:
                statement = statement.where(PortfolioDecision.explanation_json["strategy_id"].as_string() == filters.strategy)
                count_statement = count_statement.where(PortfolioDecision.explanation_json["strategy_id"].as_string() == filters.strategy)
            if filters.decision_status:
                statement = statement.where(PortfolioDecision.decision_status == filters.decision_status)
                count_statement = count_statement.where(PortfolioDecision.decision_status == filters.decision_status)
            if filters.run_id:
                statement = statement.where(PortfolioDecision.portfolio_run_id == _uuid_value(filters.run_id))
                count_statement = count_statement.where(PortfolioDecision.portfolio_run_id == _uuid_value(filters.run_id))
            date_from = _parse_dt(filters.date_from)
            if date_from is not None:
                statement = statement.where(PortfolioDecision.created_at >= date_from)
                count_statement = count_statement.where(PortfolioDecision.created_at >= date_from)
            date_to = _parse_dt(filters.date_to)
            if date_to is not None:
                statement = statement.where(PortfolioDecision.created_at <= date_to)
                count_statement = count_statement.where(PortfolioDecision.created_at <= date_to)
            order_column = PortfolioDecision.created_at.desc() if filters.sort_desc else PortfolioDecision.created_at.asc()
            rows = session.execute(statement.order_by(order_column).offset(filters.offset).limit(filters.limit)).all()
            total_count = int(session.scalar(count_statement) or 0)
            decision_ids = [decision.id for decision, _symbol, _run in rows]
            order_rows = session.execute(
                select(Order.portfolio_decision_id, Order.status)
                .where(Order.portfolio_decision_id.in_(decision_ids))
                .order_by(Order.updated_at_event.desc(), Order.created_at.desc())
            ).all() if decision_ids else []

        latest_order_status: dict[object, str] = {}
        for decision_id, status in order_rows:
            latest_order_status.setdefault(decision_id, status)

        items = []
        for decision, symbol, run in rows:
            explanation = _safe_dict(decision.explanation_json)
            items.append(
                TradeDecisionReadModel(
                    trade_id=str(decision.id),
                    portfolio_run_id=str(run.id),
                    timestamp=explanation.get("timestamp") or _iso(decision.created_at),
                    symbol=symbol.symbol,
                    side=decision.side,
                    quantity=decision.target_shares,
                    target_weight=decision.target_weight,
                    strategy_id=explanation.get("strategy_id"),
                    signal_score=explanation.get("final_signal_score"),
                    rank_score=decision.rank_score,
                    expected_edge=explanation.get("expected_edge") or decision.rank_score,
                    order_status=latest_order_status.get(decision.id),
                    status=decision.decision_status,
                    entry_reason_summary=explanation.get("entry_reason_summary"),
                    rejection_reason=explanation.get("rejection_reason"),
                    base_universe_id=explanation.get("base_universe_id"),
                    sub_universe_id=explanation.get("sub_universe_id"),
                    run_name=run.run_key,
                    mode=run.mode,
                    explanation=explanation,
                )
            )
        return PagedResultReadModel(items=items, total_count=total_count, limit=filters.limit, offset=filters.offset, source="db")

    def list_recent_trade_decisions(
        self,
        *,
        limit: int = 100,
        statuses: Iterable[str] | None = None,
    ) -> list[TradeDecisionReadModel]:
        status = None
        if statuses:
            status = list(statuses)[0]
        return list(self.list_trade_decisions(DecisionQueryFilters(limit=limit, decision_status=status)).items)

    def list_candidate_evaluations_for_run(
        self,
        portfolio_run_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> PagedResultReadModel:
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=limit, offset=offset, source="db")
        with self.session_factory() as session:
            statement = (
                select(CandidateEvaluation, Symbol)
                .join(Symbol, Symbol.id == CandidateEvaluation.symbol_id)
                .where(CandidateEvaluation.portfolio_run_id == _uuid_value(portfolio_run_id))
                .order_by(CandidateEvaluation.rank.asc().nulls_last(), CandidateEvaluation.created_at.desc())
                .offset(offset)
                .limit(limit)
            )
            rows = session.execute(statement).all()
            total_count = int(
                session.scalar(
                    select(func.count(CandidateEvaluation.id)).where(CandidateEvaluation.portfolio_run_id == _uuid_value(portfolio_run_id))
                )
                or 0
            )
        return PagedResultReadModel(
            items=[
                CandidateEvaluationReadModel(
                    evaluation_id=str(evaluation.id),
                    portfolio_run_id=str(evaluation.portfolio_run_id),
                    symbol=symbol.symbol,
                    base_universe_id=evaluation.base_universe_id,
                    sub_universe_id=evaluation.sub_universe_id,
                    score=evaluation.score,
                    rank=evaluation.rank,
                    candidate_status=evaluation.candidate_status,
                    rejection_reason=evaluation.rejection_reason,
                    metadata=dict(evaluation.metadata_json or {}),
                )
                for evaluation, symbol in rows
            ],
            total_count=total_count,
            limit=limit,
            offset=offset,
            source="db",
        )

    def get_trade_decision_detail(self, trade_id: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        with self.session_factory() as session:
            row = session.execute(
                select(PortfolioDecision, Symbol, PortfolioRun)
                .join(Symbol, Symbol.id == PortfolioDecision.symbol_id)
                .join(PortfolioRun, PortfolioRun.id == PortfolioDecision.portfolio_run_id)
                .where(PortfolioDecision.id == _uuid_value(trade_id))
            ).first()
            if row is None:
                return None
            decision, symbol, run = row
            explanation = _safe_dict(decision.explanation_json)
            candidates = session.execute(
                select(CandidateEvaluation)
                .where(
                    CandidateEvaluation.portfolio_run_id == decision.portfolio_run_id,
                    CandidateEvaluation.symbol_id == decision.symbol_id,
                )
                .order_by(CandidateEvaluation.created_at.desc())
            ).scalars().all()
            filter_results = session.execute(
                select(UniverseFilterResult)
                .where(
                    UniverseFilterResult.portfolio_run_id == decision.portfolio_run_id,
                    UniverseFilterResult.symbol_id == decision.symbol_id,
                )
                .order_by(UniverseFilterResult.created_at.asc())
            ).scalars().all()
            contributions = session.execute(
                select(DecisionSignalContribution)
                .where(DecisionSignalContribution.portfolio_decision_id == decision.id)
                .order_by(DecisionSignalContribution.contribution_rank.asc().nulls_last())
            ).scalars().all()

        execution = self.execution_queries.list_execution_events_for_decision(str(decision.id))
        artifacts = self.artifact_queries.list_run_artifacts(portfolio_run_id=str(run.id))
        return {
            "decision": TradeDecisionReadModel(
                trade_id=str(decision.id),
                portfolio_run_id=str(run.id),
                timestamp=explanation.get("timestamp") or _iso(decision.created_at),
                symbol=symbol.symbol,
                side=decision.side,
                quantity=decision.target_shares,
                target_weight=decision.target_weight,
                strategy_id=explanation.get("strategy_id"),
                signal_score=explanation.get("final_signal_score"),
                rank_score=decision.rank_score,
                expected_edge=explanation.get("expected_edge") or decision.rank_score,
                order_status=execution[0].status if execution else None,
                status=decision.decision_status,
                entry_reason_summary=explanation.get("entry_reason_summary"),
                rejection_reason=explanation.get("rejection_reason"),
                base_universe_id=explanation.get("base_universe_id"),
                sub_universe_id=explanation.get("sub_universe_id"),
                run_name=run.run_key,
                mode=run.mode,
                explanation=explanation,
            ).to_dict(),
            "run": {
                "run_id": str(run.id),
                "run_name": run.run_key,
                "mode": run.mode,
                "status": run.status,
                "started_at": _iso(run.started_at),
                "completed_at": _iso(run.completed_at),
                "config_hash": run.config_hash,
                "git_commit": run.git_commit,
                "notes": run.notes,
            },
            "candidate_evaluations": [
                {
                    "evaluation_id": str(candidate.id),
                    "symbol": symbol.symbol,
                    "base_universe_id": candidate.base_universe_id,
                    "sub_universe_id": candidate.sub_universe_id,
                    "score": candidate.score,
                    "rank": candidate.rank,
                    "candidate_status": candidate.candidate_status,
                    "rejection_reason": candidate.rejection_reason,
                    "metadata": dict(candidate.metadata_json or {}),
                }
                for candidate in candidates
            ],
            "filter_results": [
                {
                    "filter_result_id": str(result.id),
                    "filter_name": result.filter_name,
                    "pass_fail": result.pass_fail,
                    "observed_value": result.observed_value,
                    "reason": result.reason,
                    "metadata": dict(result.metadata_json or {}),
                }
                for result in filter_results
            ],
            "signal_contributions": [
                {
                    "contribution_id": str(contribution.id),
                    "signal_name": contribution.signal_name,
                    "contribution_value": contribution.contribution_value,
                    "contribution_rank": contribution.contribution_rank,
                    "metadata": dict(contribution.metadata_json or {}),
                }
                for contribution in contributions
            ],
            "execution": [row.to_dict() for row in execution],
            "artifacts": [artifact.to_dict() for artifact in artifacts],
            "source": "db",
        }
