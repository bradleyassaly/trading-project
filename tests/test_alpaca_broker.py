from __future__ import annotations

from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig


class DummyAccount:
    id = "acct-123"
    cash = "10000"
    equity = "10500"
    buying_power = "15000"
    currency = "USD"


class DummyPosition:
    def __init__(self, symbol, qty, avg_entry_price, current_price, market_value):
        self.symbol = symbol
        self.qty = qty
        self.avg_entry_price = avg_entry_price
        self.current_price = current_price
        self.market_value = market_value


class DummyTradingClient:
    def __init__(self, api_key, secret_key, paper):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper

    def get_account(self):
        return DummyAccount()

    def get_all_positions(self):
        return [
            DummyPosition("AAPL", "10", "100", "110", "1100"),
            DummyPosition("MSFT", "5", "200", "210", "1050"),
        ]

    def submit_order(self, order_data):
        return type(
            "Order",
            (),
            {
                "id": "order-123",
                "client_order_id": getattr(order_data, "client_order_id", "client-123"),
                "symbol": getattr(order_data, "symbol", "AAPL"),
                "side": getattr(order_data, "side", "buy"),
                "qty": getattr(order_data, "qty", 1),
                "filled_qty": 0,
                "order_type": getattr(order_data, "type", getattr(order_data, "order_type", "market")),
                "time_in_force": getattr(order_data, "time_in_force", "day"),
                "status": "accepted",
                "submitted_at": "2026-03-24T00:00:00+00:00",
            },
        )()


def test_alpaca_broker_get_account_and_positions(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.TradingClient",
        DummyTradingClient,
    )

    broker = AlpacaBroker(
        AlpacaBrokerConfig(
            api_key="key",
            secret_key="secret",
            paper=True,
        )
    )

    account = broker.get_account()
    positions = broker.get_positions()

    assert account.account_id == "acct-123"
    assert account.cash == 10000.0
    assert account.equity == 10500.0
    assert "AAPL" in positions
    assert positions["AAPL"].quantity == 10
    assert positions["AAPL"].market_value == 1100.0


def test_alpaca_broker_submit_orders(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.TradingClient",
        DummyTradingClient,
    )
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.MarketOrderRequest",
        lambda **kwargs: type("MarketOrderRequest", (), kwargs)(),
    )
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.LimitOrderRequest",
        lambda **kwargs: type("LimitOrderRequest", (), kwargs)(),
    )
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.OrderSide",
        type("OrderSide", (), {"BUY": "buy", "SELL": "sell"}),
    )
    monkeypatch.setattr(
        "trading_platform.broker.alpaca_broker.TimeInForce",
        type("TimeInForce", (), {"DAY": "day", "GTC": "gtc"}),
    )

    broker = AlpacaBroker(
        AlpacaBrokerConfig(
            api_key="key",
            secret_key="secret",
            paper=True,
        )
    )

    results = broker.submit_orders(
        [
            type(
                "Req",
                (),
                {
                    "symbol": "AAPL",
                    "side": "BUY",
                    "quantity": 10,
                    "order_type": "market",
                    "time_in_force": "day",
                    "limit_price": None,
                    "client_order_id": "client-123",
                },
            )()
        ]
    )

    assert len(results) == 1
    assert results[0].broker_order_id == "order-123"
    assert results[0].status == "accepted"
