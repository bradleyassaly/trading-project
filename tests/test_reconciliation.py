from __future__ import annotations

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerPosition
from trading_platform.execution.reconciliation import (
    build_rebalance_orders_from_broker_state,
)


def test_build_rebalance_orders_from_broker_state_generates_expected_deltas() -> None:
    account = BrokerAccount(
        account_id="acct-1",
        cash=5_000.0,
        equity=10_000.0,
        buying_power=10_000.0,
    )
    positions = {
        "AAPL": LiveBrokerPosition(
            symbol="AAPL",
            quantity=10,
            avg_price=100.0,
            market_price=100.0,
            market_value=1_000.0,
        ),
        "MSFT": LiveBrokerPosition(
            symbol="MSFT",
            quantity=5,
            avg_price=200.0,
            market_price=200.0,
            market_value=1_000.0,
        ),
    }

    result = build_rebalance_orders_from_broker_state(
        account=account,
        positions=positions,
        latest_target_weights={"AAPL": 0.10, "NVDA": 0.30},
        latest_prices={"AAPL": 100.0, "MSFT": 200.0, "NVDA": 500.0},
        reserve_cash_pct=0.0,
        min_trade_dollars=1.0,
        lot_size=1,
    )

    assert {order.symbol for order in result.orders} == {"MSFT", "NVDA"}
    assert {order.side for order in result.orders if order.symbol == "MSFT"} == {"SELL"}
    assert {order.side for order in result.orders if order.symbol == "NVDA"} == {"BUY"}