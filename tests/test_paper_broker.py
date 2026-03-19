
from __future__ import annotations

from trading_platform.broker.base import BrokerOrder
from trading_platform.broker.paper_broker import PaperBroker, PaperBrokerConfig
from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperPosition,
    PaperTradingConfig,
    PaperOrder,
    PaperSignalSnapshot,
    OrderGenerationResult,
)
from trading_platform.paper.service import (
    JsonPaperStateStore,
    apply_filled_orders,
    generate_rebalance_orders,
    run_paper_trading_cycle,
)
from trading_platform.risk.pre_trade_checks import validate_orders



def test_paper_broker_buy_new_position_updates_cash_and_positions() -> None:
    state = PaperPortfolioState(cash=10_000.0, positions={})
    broker = PaperBroker(
        state=state,
        config=PaperBrokerConfig(commission_per_order=1.0, slippage_bps=10.0),
    )

    fills = broker.submit_orders(
        [
            BrokerOrder(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                reference_price=100.0,
                reason="rebalance_to_target",
            )
        ]
    )

    assert len(fills) == 1
    fill = fills[0]
    assert fill.symbol == "AAPL"
    assert fill.side == "BUY"
    assert round(fill.fill_price, 2) == 100.10
    assert round(state.cash, 2) == 8_998.00

    position = state.positions["AAPL"]
    assert position.quantity == 10
    assert round(position.avg_price, 2) == 100.10
    assert round(position.last_price, 2) == 100.10


def test_paper_broker_add_then_close_position() -> None:
    state = PaperPortfolioState(
        cash=5_000.0,
        positions={
            "AAPL": PaperPosition(
                symbol="AAPL",
                quantity=10,
                avg_price=100.0,
                last_price=100.0,
            )
        },
    )
    broker = PaperBroker(
        state=state,
        config=PaperBrokerConfig(commission_per_order=0.0, slippage_bps=0.0),
    )

    broker.submit_orders(
        [
            BrokerOrder(
                symbol="AAPL",
                side="BUY",
                quantity=5,
                reference_price=120.0,
                reason="rebalance_to_target",
            )
        ]
    )

    position = state.positions["AAPL"]
    assert position.quantity == 15
    assert round(position.avg_price, 2) == round((10 * 100.0 + 5 * 120.0) / 15, 2)

    broker.submit_orders(
        [
            BrokerOrder(
                symbol="AAPL",
                side="SELL",
                quantity=15,
                reference_price=110.0,
                reason="rebalance_to_target",
            )
        ]
    )

    assert "AAPL" not in state.positions
    assert round(state.cash, 2) == 6_050.00


def test_validate_orders_blocks_oversized_orders() -> None:
    orders = [
        BrokerOrder(
            symbol="NVDA",
            side="BUY",
            quantity=100,
            reference_price=200.0,
            reason="rebalance_to_target",
        )
    ]

    result = validate_orders(
        orders=orders,
        equity=10_000.0,
        max_single_order_notional=5_000.0,
        max_gross_order_notional_pct=0.25,
    )

    assert result.passed is False
    assert len(result.violations) == 2
    assert "exceeds max" in result.violations[0]
