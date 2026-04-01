"""Tests for KalshiBroker — all Kalshi API calls are mocked."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trading_platform.broker.live_models import LiveBrokerOrderRequest
from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.broker import KalshiBroker, _parse_side_action
from trading_platform.kalshi.models import KalshiOrderStatus, KalshiPosition


DUMMY_PEM = "-----BEGIN RSA PRIVATE KEY-----\ndummy\n-----END RSA PRIVATE KEY-----"


@pytest.fixture()
def config():
    return KalshiConfig(api_key_id="test", private_key_pem=DUMMY_PEM, demo=True)


@pytest.fixture()
def mock_client():
    return MagicMock()


@pytest.fixture()
def broker(config, mock_client):
    b = KalshiBroker(config, max_drawdown_pct=0.20)
    b._client = mock_client
    return b


# ── _parse_side_action ────────────────────────────────────────────────────────

@pytest.mark.parametrize("input_side,expected", [
    ("BUY_YES", ("yes", "buy")),
    ("SELL_YES", ("yes", "sell")),
    ("BUY_NO", ("no", "buy")),
    ("SELL_NO", ("no", "sell")),
    ("buy_yes", ("yes", "buy")),
])
def test_parse_side_action(input_side, expected):
    assert _parse_side_action(input_side) == expected


# ── get_account ───────────────────────────────────────────────────────────────

def test_get_account(broker, mock_client):
    mock_client.get_balance.return_value = {"balance": 10000, "portfolio_value": 12000}
    account = broker.get_account()
    assert account.cash == pytest.approx(100.0)   # 10000 cents / 100
    assert account.equity == pytest.approx(120.0)


# ── get_positions ─────────────────────────────────────────────────────────────

def test_get_positions_filters_zero(broker, mock_client):
    mock_client.get_positions.return_value = [
        KalshiPosition(ticker="A", market_exposure=5, position=5, resting_orders_count=0,
                       total_traded=10, fees_paid="0.50", realized_pnl=None, unrealized_pnl=None),
        KalshiPosition(ticker="B", market_exposure=0, position=0, resting_orders_count=0,
                       total_traded=0, fees_paid="0", realized_pnl=None, unrealized_pnl=None),
    ]
    positions = broker.get_positions()
    assert "A" in positions
    assert "B" not in positions


# ── submit_orders ─────────────────────────────────────────────────────────────

def test_submit_orders_success(broker, mock_client):
    mock_client.get_balance.return_value = {"balance": 50000, "portfolio_value": 50000}
    mock_client.create_order.return_value = KalshiOrderStatus(
        order_id="ord-1", client_order_id="cli-1", ticker="FOO-24",
        side="yes", action="buy", status="resting", order_type="limit",
        yes_price="0.6000", no_price=None, count=10, remaining_count=10,
        amend_count=0, created_time="2024-01-01T00:00:00Z", close_time=None, fees=None,
    )

    orders = [LiveBrokerOrderRequest(
        symbol="FOO-24", side="BUY_YES", quantity=10, order_type="limit", limit_price=0.60
    )]
    results = broker.submit_orders(orders)

    assert len(results) == 1
    assert results[0].broker_order_id == "ord-1"
    assert results[0].status == "resting"


def test_submit_orders_blocked_by_kill_switch(broker):
    broker._killed = True
    with pytest.raises(RuntimeError, match="Kill switch"):
        broker.submit_orders([
            LiveBrokerOrderRequest(symbol="FOO", side="BUY_YES", quantity=1, order_type="limit", limit_price=0.5)
        ])


# ── kill switch ───────────────────────────────────────────────────────────────

def test_kill_switch_cancels_orders(broker, mock_client):
    broker.activate_kill_switch()
    mock_client.cancel_all_orders.assert_called_once()
    assert broker._killed is True


def test_reset_kill_switch(broker):
    broker._killed = True
    broker.reset_kill_switch()
    assert broker._killed is False


# ── max drawdown ──────────────────────────────────────────────────────────────

def test_max_drawdown_triggers_kill_switch(broker, mock_client):
    # Seed starting equity at $100
    mock_client.get_balance.return_value = {"balance": 10000, "portfolio_value": 10000}
    broker._starting_equity = 100.0

    # Simulate 25% drawdown (exceeds 20% limit)
    mock_client.get_balance.return_value = {"balance": 7500, "portfolio_value": 7500}
    mock_client.create_order.return_value = KalshiOrderStatus(
        order_id="x", client_order_id=None, ticker="T", side="yes", action="buy",
        status="resting", order_type="limit", yes_price="0.5", no_price=None,
        count=1, remaining_count=1, amend_count=0, created_time=None, close_time=None, fees=None,
    )

    with pytest.raises(RuntimeError, match="drawdown"):
        broker.submit_orders([
            LiveBrokerOrderRequest(symbol="T", side="BUY_YES", quantity=1, order_type="limit", limit_price=0.5)
        ])

    assert broker._killed is True
