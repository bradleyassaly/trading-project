from __future__ import annotations

from trading_platform.broker.live_models import (
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
)
from trading_platform.execution.open_order_adjustment import adjust_orders_for_open_orders


def test_adjust_orders_for_open_orders_reduces_and_drops_orders() -> None:
    proposed_orders = [
        LiveBrokerOrderRequest(symbol="AAPL", side="BUY", quantity=10),
        LiveBrokerOrderRequest(symbol="MSFT", side="SELL", quantity=8),
        LiveBrokerOrderRequest(symbol="NVDA", side="BUY", quantity=5),
    ]
    open_orders = [
        LiveBrokerOrderStatus(
            broker_order_id="1",
            client_order_id=None,
            symbol="AAPL",
            side="BUY",
            quantity=6,
            filled_quantity=0,
            order_type="market",
            time_in_force="day",
            status="new",
        ),
        LiveBrokerOrderStatus(
            broker_order_id="2",
            client_order_id=None,
            symbol="MSFT",
            side="SELL",
            quantity=8,
            filled_quantity=0,
            order_type="market",
            time_in_force="day",
            status="new",
        ),
    ]

    result = adjust_orders_for_open_orders(
        proposed_orders=proposed_orders,
        open_orders=open_orders,
    )

    assert len(result.adjusted_orders) == 2
    aapl = next(order for order in result.adjusted_orders if order.symbol == "AAPL")
    nvda = next(order for order in result.adjusted_orders if order.symbol == "NVDA")

    assert aapl.side == "BUY"
    assert aapl.quantity == 4
    assert nvda.quantity == 5