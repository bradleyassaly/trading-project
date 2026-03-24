from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from trading_platform.db.repositories import ExecutionRepository
from trading_platform.db.session import session_scope
from trading_platform.db.services.lineage_service import DatabaseLineageService


def _parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def log_paper_orders_and_fills(*, db_service: DatabaseLineageService, orders: list[Any], fills: list[Any], as_of: str, broker: str) -> None:
    if not db_service.enabled:
        return
    fill_map: dict[str, list[Any]] = {}
    for fill in fills:
        fill_map.setdefault(str(getattr(fill, "symbol", "")), []).append(fill)
    with session_scope(db_service.session_factory) as session:
        repo = ExecutionRepository(session)
        for order in orders:
            row = repo.record_order(
                symbol=order.symbol,
                status="paper_generated",
                broker=broker,
                side=order.side,
                order_type="market",
                tif="day",
                quantity=float(order.quantity),
                submitted_at=_parse_dt(as_of),
                updated_at=_parse_dt(as_of),
                notes=str(order.reason),
            )
            repo.record_order_event(
                order_id=row.id,
                event_type="generated",
                event_ts=_parse_dt(as_of),
                payload_json={"target_weight": order.target_weight, "reference_price": order.reference_price, "target_quantity": order.target_quantity},
            )
            for fill in fill_map.get(order.symbol, []):
                repo.record_fill(
                    order_id=row.id,
                    quantity=float(getattr(fill, "quantity", 0.0)),
                    price=float(getattr(fill, "price", 0.0)) if getattr(fill, "price", None) is not None else None,
                    fill_ts=_parse_dt(getattr(fill, "timestamp", None) or as_of),
                    fees=float(getattr(fill, "fees", 0.0)) if getattr(fill, "fees", None) is not None else None,
                    liquidity_flag=str(getattr(fill, "liquidity_flag", "")) or None,
                    payload_json={},
                )


def log_live_preview_orders(*, db_service: DatabaseLineageService, adjusted_orders: list[Any], execution_result: Any | None, as_of: str, broker: str) -> None:
    if not db_service.enabled:
        return
    with session_scope(db_service.session_factory) as session:
        repo = ExecutionRepository(session)
        orders_by_symbol: dict[str, Any] = {}
        for order in adjusted_orders:
            row = repo.record_order(
                symbol=order.symbol,
                status="preview_adjusted",
                broker=broker,
                side=order.side,
                order_type=getattr(order, "order_type", None),
                tif=getattr(order, "time_in_force", None),
                quantity=float(getattr(order, "quantity", 0.0)),
                limit_price=float(getattr(order, "limit_price", 0.0)) if getattr(order, "limit_price", None) is not None else None,
                stop_price=float(getattr(order, "stop_price", 0.0)) if getattr(order, "stop_price", None) is not None else None,
                submitted_at=_parse_dt(as_of),
                updated_at=_parse_dt(as_of),
            )
            orders_by_symbol[order.symbol] = row
            repo.record_order_event(order_id=row.id, event_type="preview_adjusted", event_ts=_parse_dt(as_of), payload_json={})
        if execution_result is None:
            return
        for executable in list(getattr(execution_result, "executable_orders", [])):
            row = orders_by_symbol.get(executable.symbol)
            if row is None:
                row = repo.record_order(symbol=executable.symbol, status="preview_executable", broker=broker, side=executable.side, order_type="market", tif="day", quantity=float(executable.final_shares), submitted_at=_parse_dt(as_of), updated_at=_parse_dt(as_of))
            repo.record_order_event(order_id=row.id, event_type="executable", event_ts=_parse_dt(as_of), payload_json={"final_shares": executable.final_shares, "price": executable.price})
        for rejected in list(getattr(execution_result, "rejected_orders", [])):
            row = orders_by_symbol.get(rejected.symbol)
            if row is None:
                row = repo.record_order(symbol=rejected.symbol, status="preview_rejected", broker=broker, side=rejected.side, order_type="market", tif="day", quantity=float(rejected.requested_shares), submitted_at=_parse_dt(as_of), updated_at=_parse_dt(as_of), notes=getattr(rejected, "reason", None))
            repo.record_order_event(order_id=row.id, event_type="rejected", event_ts=_parse_dt(as_of), payload_json={"reason": getattr(rejected, "reason", None)})
