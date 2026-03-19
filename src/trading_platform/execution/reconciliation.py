from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerOrderRequest,
    LiveBrokerPosition,
)


@dataclass(frozen=True)
class ReconciliationResult:
    orders: list[LiveBrokerOrderRequest]
    target_quantities: dict[str, int]
    current_quantities: dict[str, int]
    diagnostics: dict[str, Any]


def build_rebalance_orders_from_broker_state(
    *,
    account: BrokerAccount,
    positions: dict[str, LiveBrokerPosition],
    latest_target_weights: dict[str, float],
    latest_prices: dict[str, float],
    reserve_cash_pct: float = 0.0,
    min_trade_dollars: float = 25.0,
    lot_size: int = 1,
    order_type: str = "market",
    time_in_force: str = "day",
) -> ReconciliationResult:
    if lot_size <= 0:
        raise ValueError("lot_size must be > 0")
    if min_trade_dollars < 0:
        raise ValueError("min_trade_dollars must be >= 0")
    if not 0.0 <= reserve_cash_pct < 1.0:
        raise ValueError("reserve_cash_pct must be in [0, 1)")

    investable_equity = float(account.equity) * (1.0 - reserve_cash_pct)

    all_symbols = sorted(set(positions.keys()) | set(latest_target_weights.keys()))
    orders: list[LiveBrokerOrderRequest] = []
    target_quantities: dict[str, int] = {}
    current_quantities: dict[str, int] = {}

    for symbol in all_symbols:
        price = float(latest_prices.get(symbol, 0.0))
        if price <= 0:
            continue

        current_quantity = int(positions[symbol].quantity) if symbol in positions else 0
        current_quantities[symbol] = current_quantity

        target_weight = float(latest_target_weights.get(symbol, 0.0))
        target_notional = investable_equity * target_weight
        raw_target_quantity = int(target_notional / price)
        target_quantity = (raw_target_quantity // lot_size) * lot_size
        target_quantities[symbol] = target_quantity

        delta_quantity = target_quantity - current_quantity
        if delta_quantity == 0:
            continue

        notional = abs(delta_quantity) * price
        if notional < min_trade_dollars:
            continue

        side = "BUY" if delta_quantity > 0 else "SELL"
        orders.append(
            LiveBrokerOrderRequest(
                symbol=symbol,
                side=side,
                quantity=abs(delta_quantity),
                order_type=order_type,
                time_in_force=time_in_force,
                reason="rebalance_to_target",
            )
        )

    diagnostics = {
        "account_equity": float(account.equity),
        "investable_equity": investable_equity,
        "reserve_cash_pct": float(reserve_cash_pct),
        "order_count": len(orders),
        "target_weight_sum": float(sum(latest_target_weights.values())),
        "symbols_considered": len(all_symbols),
    }

    return ReconciliationResult(
        orders=orders,
        target_quantities=target_quantities,
        current_quantities=current_quantities,
        diagnostics=diagnostics,
    )