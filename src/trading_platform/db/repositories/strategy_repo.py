from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from trading_platform.db.models.strategy import PromotionDecision, PromotedStrategy, StrategyDefinition


class StrategyRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_strategy_definition(self, *, name: str, version: str, config_json: dict[str, Any] | None = None, code_hash: str | None = None, is_active: bool = True) -> StrategyDefinition:
        row = self.session.scalar(select(StrategyDefinition).where(StrategyDefinition.name == name, StrategyDefinition.version == version))
        if row is None:
            row = StrategyDefinition(name=name, version=version, config_json=dict(config_json or {}), code_hash=code_hash, is_active=is_active)
            self.session.add(row)
        else:
            row.config_json = dict(config_json or row.config_json)
            row.code_hash = code_hash or row.code_hash
            row.is_active = is_active
        self.session.flush()
        return row

    def record_promotion_decision(self, *, strategy_definition_id, source_research_run_id=None, decision: str, reason: str | None = None, metrics_json: dict[str, Any] | None = None) -> PromotionDecision:
        row = PromotionDecision(strategy_definition_id=strategy_definition_id, source_research_run_id=source_research_run_id, decision=decision, reason=reason, metrics_json=dict(metrics_json or {}))
        self.session.add(row)
        self.session.flush()
        return row

    def record_promoted_strategy(self, *, strategy_definition_id, promotion_decision_id=None, active_from: datetime | None = None, active_to: datetime | None = None, status: str) -> PromotedStrategy:
        row = PromotedStrategy(strategy_definition_id=strategy_definition_id, promotion_decision_id=promotion_decision_id, active_from=active_from, active_to=active_to, status=status)
        self.session.add(row)
        self.session.flush()
        return row
