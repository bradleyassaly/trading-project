from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from trading_platform.db.models.execution import Fill, Order, OrderEvent
from trading_platform.db.repositories.reference_repo import ReferenceRepository


class ExecutionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.reference_repo = ReferenceRepository(session)

    def record_order(self, *, symbol: str, status: str, broker: str | None = None, broker_order_id: str | None = None, side: str | None = None, order_type: str | None = None, tif: str | None = None, quantity: float | None = None, limit_price: float | None = None, stop_price: float | None = None, submitted_at: datetime | None = None, updated_at: datetime | None = None, portfolio_decision_id=None, notes: str | None = None) -> Order:
        symbol_row = self.reference_repo.upsert_symbol(symbol=symbol)
        row = Order(portfolio_decision_id=portfolio_decision_id, broker=broker, broker_order_id=broker_order_id, symbol_id=symbol_row.id, side=side, order_type=order_type, tif=tif, quantity=quantity, limit_price=limit_price, stop_price=stop_price, status=status, submitted_at=submitted_at, updated_at_event=updated_at, notes=notes)
        self.session.add(row)
        self.session.flush()
        return row

    def record_order_event(self, *, order_id, event_type: str, event_ts: datetime, payload_json: dict[str, Any] | None = None) -> OrderEvent:
        row = OrderEvent(order_id=order_id, event_type=event_type, event_ts=event_ts, payload_json=dict(payload_json or {}))
        self.session.add(row)
        self.session.flush()
        return row

    def record_fill(self, *, order_id, quantity: float, price: float | None, fill_ts: datetime | None, fees: float | None = None, liquidity_flag: str | None = None, payload_json: dict[str, Any] | None = None) -> Fill:
        row = Fill(order_id=order_id, quantity=quantity, price=price, fill_ts=fill_ts, fees=fees, liquidity_flag=liquidity_flag, payload_json=dict(payload_json or {}))
        self.session.add(row)
        self.session.flush()
        return row
