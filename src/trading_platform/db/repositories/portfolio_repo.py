from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from trading_platform.db.models.portfolio import DecisionSignalContribution, PortfolioDecision, PositionSnapshot
from trading_platform.db.repositories.reference_repo import ReferenceRepository


class PortfolioRepository:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.reference_repo = ReferenceRepository(session)

    def record_portfolio_decision(self, *, portfolio_run_id, symbol: str, side: str | None, target_weight: float | None, target_shares: int | None, rank_score: float | None, decision_status: str, explanation_json: dict[str, Any] | None = None) -> PortfolioDecision:
        symbol_row = self.reference_repo.upsert_symbol(symbol=symbol)
        row = PortfolioDecision(portfolio_run_id=portfolio_run_id, symbol_id=symbol_row.id, side=side, target_weight=target_weight, target_shares=target_shares, rank_score=rank_score, decision_status=decision_status, explanation_json=dict(explanation_json or {}))
        self.session.add(row)
        self.session.flush()
        return row

    def record_signal_contribution(self, *, portfolio_decision_id, signal_name: str | None, contribution_value: float | None, contribution_rank: int | None, metadata_json: dict[str, Any] | None = None) -> DecisionSignalContribution:
        row = DecisionSignalContribution(portfolio_decision_id=portfolio_decision_id, signal_name=signal_name, contribution_value=contribution_value, contribution_rank=contribution_rank, metadata_json=dict(metadata_json or {}))
        self.session.add(row)
        self.session.flush()
        return row

    def update_position_snapshot(self, *, symbol: str, account: str | None, quantity: float, avg_price: float | None, market_value: float | None, as_of_ts: datetime, source: str | None, metadata_json: dict[str, Any] | None = None) -> PositionSnapshot:
        symbol_row = self.reference_repo.upsert_symbol(symbol=symbol)
        row = PositionSnapshot(symbol_id=symbol_row.id, account=account, quantity=quantity, avg_price=avg_price, market_value=market_value, as_of_ts=as_of_ts, source=source, metadata_json=dict(metadata_json or {}))
        self.session.add(row)
        self.session.flush()
        return row
