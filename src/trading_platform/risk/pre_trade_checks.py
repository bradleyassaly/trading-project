from __future__ import annotations

from dataclasses import dataclass

from trading_platform.broker.base import BrokerOrder


@dataclass(frozen=True)
class PreTradeCheckResult:
    passed: bool
    violations: list[str]


def validate_orders(
    *,
    orders: list[BrokerOrder],
    equity: float,
    max_single_order_notional: float | None = None,
    max_gross_order_notional_pct: float | None = None,
) -> PreTradeCheckResult:
    violations: list[str] = []

    total_notional = sum(abs(order.quantity * order.reference_price) for order in orders)

    if max_single_order_notional is not None:
        for order in orders:
            notional = abs(order.quantity * order.reference_price)
            if notional > max_single_order_notional:
                violations.append(
                    f"{order.symbol} order notional {notional:.2f} exceeds max {max_single_order_notional:.2f}"
                )

    if max_gross_order_notional_pct is not None and equity > 0:
        gross_pct = total_notional / equity
        if gross_pct > max_gross_order_notional_pct:
            violations.append(
                f"gross order notional pct {gross_pct:.4f} exceeds max {max_gross_order_notional_pct:.4f}"
            )

    return PreTradeCheckResult(
        passed=not violations,
        violations=violations,
    )