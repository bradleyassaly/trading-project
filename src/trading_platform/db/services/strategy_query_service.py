from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.runs import ResearchRun
from trading_platform.db.models.strategy import PromotionDecision, PromotedStrategy, StrategyDefinition
from trading_platform.db.services.read_models import PagedResultReadModel, PromotionReadModel, StrategyHistoryFilters


def _iso(value: object) -> str | None:
    if value is None:
        return None
    text = getattr(value, "isoformat", None)
    return text() if callable(text) else str(value)


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


class StrategyQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_promotions(self, filters: StrategyHistoryFilters | None = None) -> PagedResultReadModel:
        filters = filters or StrategyHistoryFilters()
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=filters.limit, offset=filters.offset, source="db")
        with self.session_factory() as session:
            statement = (
                select(PromotionDecision, StrategyDefinition, ResearchRun, PromotedStrategy)
                .join(StrategyDefinition, StrategyDefinition.id == PromotionDecision.strategy_definition_id)
                .outerjoin(ResearchRun, ResearchRun.id == PromotionDecision.source_research_run_id)
                .outerjoin(PromotedStrategy, PromotedStrategy.promotion_decision_id == PromotionDecision.id)
            )
            count_statement = select(func.count(PromotionDecision.id)).select_from(PromotionDecision).join(
                StrategyDefinition,
                StrategyDefinition.id == PromotionDecision.strategy_definition_id,
            )
            if filters.strategy:
                statement = statement.where(StrategyDefinition.name == filters.strategy)
                count_statement = count_statement.where(StrategyDefinition.name == filters.strategy)
            if filters.decision:
                statement = statement.where(PromotionDecision.decision == filters.decision)
                count_statement = count_statement.where(PromotionDecision.decision == filters.decision)
            if filters.status:
                statement = statement.where(PromotedStrategy.status == filters.status)
                count_statement = count_statement.outerjoin(
                    PromotedStrategy,
                    PromotedStrategy.promotion_decision_id == PromotionDecision.id,
                ).where(PromotedStrategy.status == filters.status)
            if filters.active is True:
                statement = statement.where(PromotedStrategy.active_to.is_(None))
                count_statement = count_statement.outerjoin(
                    PromotedStrategy,
                    PromotedStrategy.promotion_decision_id == PromotionDecision.id,
                ).where(PromotedStrategy.active_to.is_(None))
            date_from = _parse_dt(filters.date_from)
            if date_from is not None:
                statement = statement.where(PromotionDecision.created_at >= date_from)
                count_statement = count_statement.where(PromotionDecision.created_at >= date_from)
            date_to = _parse_dt(filters.date_to)
            if date_to is not None:
                statement = statement.where(PromotionDecision.created_at <= date_to)
                count_statement = count_statement.where(PromotionDecision.created_at <= date_to)
            order_column = PromotionDecision.created_at.desc() if filters.sort_desc else PromotionDecision.created_at.asc()
            rows = session.execute(statement.order_by(order_column).offset(filters.offset).limit(filters.limit)).all()
            total_count = int(session.scalar(count_statement) or 0)
        return PagedResultReadModel(
            items=[
                PromotionReadModel(
                    promotion_decision_id=str(decision.id),
                    strategy_name=strategy.name,
                    strategy_version=strategy.version,
                    decision=decision.decision,
                    reason=decision.reason,
                    created_at=_iso(decision.created_at),
                    source_research_run_name=research_run.run_key if research_run is not None else None,
                    promoted_status=promoted.status if promoted is not None else None,
                    metrics=dict(decision.metrics_json or {}),
                )
                for decision, strategy, research_run, promoted in rows
            ],
            total_count=total_count,
            limit=filters.limit,
            offset=filters.offset,
            source="db",
        )

    def list_recent_promotions(self, limit: int = 20) -> list[PromotionReadModel]:
        return list(self.list_promotions(StrategyHistoryFilters(limit=limit)).items)

    def list_promotions_for_research_run(self, research_run_id: str, *, limit: int = 20, offset: int = 0) -> PagedResultReadModel:
        filters = StrategyHistoryFilters(limit=limit, offset=offset)
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=limit, offset=offset, source="db")
        with self.session_factory() as session:
            statement = (
                select(PromotionDecision, StrategyDefinition, ResearchRun, PromotedStrategy)
                .join(StrategyDefinition, StrategyDefinition.id == PromotionDecision.strategy_definition_id)
                .outerjoin(ResearchRun, ResearchRun.id == PromotionDecision.source_research_run_id)
                .outerjoin(PromotedStrategy, PromotedStrategy.promotion_decision_id == PromotionDecision.id)
                .where(PromotionDecision.source_research_run_id == _uuid_value(research_run_id))
                .order_by(PromotionDecision.created_at.desc())
                .offset(offset)
                .limit(limit)
            )
            count_statement = select(func.count(PromotionDecision.id)).where(PromotionDecision.source_research_run_id == _uuid_value(research_run_id))
            rows = session.execute(statement).all()
            total_count = int(session.scalar(count_statement) or 0)
        return PagedResultReadModel(
            items=[
                PromotionReadModel(
                    promotion_decision_id=str(decision.id),
                    strategy_name=strategy.name,
                    strategy_version=strategy.version,
                    decision=decision.decision,
                    reason=decision.reason,
                    created_at=_iso(decision.created_at),
                    source_research_run_name=research_run.run_key if research_run is not None else None,
                    promoted_status=promoted.status if promoted is not None else None,
                    metrics=dict(decision.metrics_json or {}),
                )
                for decision, strategy, research_run, promoted in rows
            ],
            total_count=total_count,
            limit=filters.limit,
            offset=filters.offset,
            source="db",
        )
