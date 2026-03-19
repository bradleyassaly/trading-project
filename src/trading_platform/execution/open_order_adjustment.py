from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trading_platform.broker.live_models import (
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
)


@dataclass(frozen=True)
class OpenOrderAdjustmentResult:
    adjusted_orders: list[LiveBrokerOrderRequest]
    pending_deltas: dict[str, int]
    diagnostics: dict[str, Any]


def _signed_open_order_quantity(order: LiveBrokerOrderStatus) -> int:
    remaining = int(order.remaining_quantity)
    if remaining <= 0:
        return 0
    if order.side.upper() == "BUY":
        return remaining
    if order.side.upper() == "SELL":
        return -remaining
    return 0


def adjust_orders_for_open_orders(
    *,
    proposed_orders: list[LiveBrokerOrderRequest],
    open_orders: list[LiveBrokerOrderStatus],
) -> OpenOrderAdjustmentResult:
    pending_deltas: dict[str, int] = {}

    for order in open_orders:
        status = order.status.lower()
        if status in {"canceled", "cancelled", "expired", "filled", "rejected"}:
            continue
        pending_deltas[order.symbol] = pending_deltas.get(order.symbol, 0) + _signed_open_order_quantity(order)

    adjusted_orders: list[LiveBrokerOrderRequest] = []
    dropped_count = 0
    reduced_count = 0

    for order in proposed_orders:
        proposed_signed = order.quantity if order.side.upper() == "BUY" else -order.quantity
        pending_signed = pending_deltas.get(order.symbol, 0)
        net_needed = proposed_signed - pending_signed

        if net_needed == 0:
            dropped_count += 1
            continue

        if proposed_signed > 0 and net_needed <= 0:
            dropped_count += 1
            continue

        if proposed_signed < 0 and net_needed >= 0:
            dropped_count += 1
            continue

        adjusted_quantity = abs(net_needed)
        if adjusted_quantity != order.quantity:
            reduced_count += 1

        adjusted_side = "BUY" if net_needed > 0 else "SELL"

        adjusted_orders.append(
            LiveBrokerOrderRequest(
                symbol=order.symbol,
                side=adjusted_side,
                quantity=adjusted_quantity,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                limit_price=order.limit_price,
                client_order_id=order.client_order_id,
                reason=order.reason,
            )
        )

    diagnostics = {
        "open_order_count": len(open_orders),
        "symbols_with_pending_orders": len(pending_deltas),
        "dropped_orders": dropped_count,
        "reduced_orders": reduced_count,
    }

    return OpenOrderAdjustmentResult(
        adjusted_orders=adjusted_orders,
        pending_deltas=pending_deltas,
        diagnostics=diagnostics,
    )