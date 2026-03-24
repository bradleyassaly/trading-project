from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.runs import ResearchRun
from trading_platform.db.models.strategy import PromotionDecision, PromotedStrategy, StrategyDefinition
from trading_platform.db.services.read_models import PromotionReadModel


def _iso(value: object) -> str | None:
    if value is None:
        return None
    text = getattr(value, "isoformat", None)
    return text() if callable(text) else str(value)


class StrategyQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_recent_promotions(self, limit: int = 20) -> list[PromotionReadModel]:
        if not self.enabled:
            return []
        with self.session_factory() as session:
            rows = session.execute(
                select(PromotionDecision, StrategyDefinition, ResearchRun, PromotedStrategy)
                .join(StrategyDefinition, StrategyDefinition.id == PromotionDecision.strategy_definition_id)
                .outerjoin(ResearchRun, ResearchRun.id == PromotionDecision.source_research_run_id)
                .outerjoin(PromotedStrategy, PromotedStrategy.promotion_decision_id == PromotionDecision.id)
                .order_by(PromotionDecision.created_at.desc())
                .limit(limit)
            ).all()
        return [
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
        ]
