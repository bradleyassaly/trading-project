from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.execution import Fill, Order, OrderEvent
from trading_platform.db.models.reference import Symbol
from trading_platform.db.services.read_models import ExecutionSummaryReadModel


def _iso(value: object) -> str | None:
    if value is None:
        return None
    text = getattr(value, "isoformat", None)
    return text() if callable(text) else str(value)


def _uuid_value(value: object) -> object:
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return value
    return value


class ExecutionQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_execution_events_for_decision(self, portfolio_decision_id: str) -> list[ExecutionSummaryReadModel]:
        if not self.enabled:
            return []
        with self.session_factory() as session:
            orders = session.execute(
                select(Order, Symbol)
                .join(Symbol, Symbol.id == Order.symbol_id)
                .where(Order.portfolio_decision_id == _uuid_value(portfolio_decision_id))
                .order_by(Order.submitted_at.desc(), Order.created_at.desc())
            ).all()
            order_ids = [order.id for order, _symbol in orders]
            events = session.execute(
                select(OrderEvent).where(OrderEvent.order_id.in_(order_ids)).order_by(OrderEvent.event_ts.asc())
            ).scalars().all() if order_ids else []
            fills = session.execute(
                select(Fill).where(Fill.order_id.in_(order_ids)).order_by(Fill.fill_ts.asc(), Fill.id.asc())
            ).scalars().all() if order_ids else []

        events_by_order: dict[object, list[OrderEvent]] = {}
        for event in events:
            events_by_order.setdefault(event.order_id, []).append(event)
        fills_by_order: dict[object, list[Fill]] = {}
        for fill in fills:
            fills_by_order.setdefault(fill.order_id, []).append(fill)

        return [
            ExecutionSummaryReadModel(
                order_id=str(order.id),
                symbol=symbol.symbol,
                side=order.side,
                status=order.status,
                submitted_at=_iso(order.submitted_at),
                updated_at=_iso(order.updated_at_event),
                quantity=order.quantity,
                limit_price=order.limit_price,
                stop_price=order.stop_price,
                broker=order.broker,
                broker_order_id=order.broker_order_id,
                event_count=len(events_by_order.get(order.id, [])),
                fill_count=len(fills_by_order.get(order.id, [])),
                fills=[
                    {
                        "fill_id": str(fill.id),
                        "fill_ts": _iso(fill.fill_ts),
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fees": fill.fees,
                        "liquidity_flag": fill.liquidity_flag,
                        "payload": dict(fill.payload_json or {}),
                    }
                    for fill in fills_by_order.get(order.id, [])
                ],
                events=[
                    {
                        "event_id": str(event.id),
                        "event_type": event.event_type,
                        "event_ts": _iso(event.event_ts),
                        "payload": dict(event.payload_json or {}),
                    }
                    for event in events_by_order.get(order.id, [])
                ],
            )
            for order, symbol in orders
        ]

    def list_recent_execution_activity(self, limit: int = 20) -> list[ExecutionSummaryReadModel]:
        if not self.enabled:
            return []
        with self.session_factory() as session:
            rows = session.execute(
                select(Order, Symbol)
                .join(Symbol, Symbol.id == Order.symbol_id)
                .order_by(Order.submitted_at.desc(), Order.created_at.desc())
                .limit(limit)
            ).all()
        results: list[ExecutionSummaryReadModel] = []
        for order, symbol in rows:
            details = self.list_execution_events_for_decision(str(order.portfolio_decision_id)) if order.portfolio_decision_id else []
            matched = next((row for row in details if row.order_id == str(order.id)), None)
            if matched is not None:
                results.append(matched)
                continue
            results.append(
                ExecutionSummaryReadModel(
                    order_id=str(order.id),
                    symbol=symbol.symbol,
                    side=order.side,
                    status=order.status,
                    submitted_at=_iso(order.submitted_at),
                    updated_at=_iso(order.updated_at_event),
                    quantity=order.quantity,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    broker=order.broker,
                    broker_order_id=order.broker_order_id,
                )
            )
        return results
