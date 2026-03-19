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