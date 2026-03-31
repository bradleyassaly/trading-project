from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerOrderRequest,
    LiveBrokerPosition,
)
from trading_platform.execution.order_lifecycle import OrderLifecycleRecord
from trading_platform.paper.models import PaperPortfolioState


@dataclass(frozen=True)
class ReconciliationResult:
    orders: list[LiveBrokerOrderRequest]
    target_quantities: dict[str, int]
    current_quantities: dict[str, int]
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class ReconciliationMismatch:
    mismatch_type: str
    symbol: str
    expected: Any
    actual: Any
    reason_code: str
    severity: str = "warn"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mismatch_type": self.mismatch_type,
            "symbol": self.symbol,
            "expected": self.expected,
            "actual": self.actual,
            "reason_code": self.reason_code,
            "severity": self.severity,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OrderLifecycleReconciliationResult:
    as_of: str
    intended_target_weights: dict[str, float]
    realized_positions: dict[str, int]
    order_lifecycle_records: list[OrderLifecycleRecord]
    mismatches: list[ReconciliationMismatch]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "intended_target_weights": dict(self.intended_target_weights),
            "realized_positions": dict(self.realized_positions),
            "order_lifecycle_records": [row.to_dict() for row in self.order_lifecycle_records],
            "mismatches": [row.to_dict() for row in self.mismatches],
            "diagnostics": dict(self.diagnostics),
        }


def build_order_lifecycle_reconciliation_skeleton(
    *,
    as_of: str,
    intended_target_weights: dict[str, float],
    lifecycle_records: list[OrderLifecycleRecord],
    realized_state: PaperPortfolioState,
) -> OrderLifecycleReconciliationResult:
    realized_positions = {
        symbol: int(position.quantity)
        for symbol, position in sorted(realized_state.positions.items())
    }
    lifecycle_by_symbol = {record.intent.symbol: record for record in lifecycle_records}
    mismatches: list[ReconciliationMismatch] = []

    for symbol, target_weight in sorted(intended_target_weights.items()):
        lifecycle = lifecycle_by_symbol.get(symbol)
        realized_quantity = realized_positions.get(symbol, 0)
        if abs(float(target_weight)) > 0.0 and lifecycle is None and realized_quantity == 0:
            mismatches.append(
                ReconciliationMismatch(
                    mismatch_type="missing_lifecycle_record",
                    symbol=symbol,
                    expected=float(target_weight),
                    actual=realized_quantity,
                    reason_code="missing_order_lifecycle",
                    severity="warn",
                )
            )
        elif lifecycle is not None and lifecycle.final_status == "intent" and realized_quantity == 0:
            mismatches.append(
                ReconciliationMismatch(
                    mismatch_type="unfilled_intent",
                    symbol=symbol,
                    expected=float(target_weight),
                    actual=realized_quantity,
                    reason_code="intent_not_submitted_or_filled",
                    severity="warn",
                    metadata={"final_status": lifecycle.final_status},
                )
            )

    for symbol, quantity in sorted(realized_positions.items()):
        if symbol not in intended_target_weights and quantity != 0:
            mismatches.append(
                ReconciliationMismatch(
                    mismatch_type="unexpected_realized_position",
                    symbol=symbol,
                    expected=0.0,
                    actual=quantity,
                    reason_code="realized_position_without_intent",
                    severity="warn",
                )
            )

    diagnostics = {
        "intended_symbol_count": len(intended_target_weights),
        "realized_symbol_count": len(realized_positions),
        "lifecycle_record_count": len(lifecycle_records),
        "mismatch_count": len(mismatches),
        "reconciled": len(mismatches) == 0,
    }
    return OrderLifecycleReconciliationResult(
        as_of=as_of,
        intended_target_weights={str(symbol): float(weight) for symbol, weight in sorted(intended_target_weights.items())},
        realized_positions=realized_positions,
        order_lifecycle_records=lifecycle_records,
        mismatches=mismatches,
        diagnostics=diagnostics,
    )


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
