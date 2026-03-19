from __future__ import annotations

import os
from dataclasses import dataclass

from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerFill,
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
    LiveBrokerPosition,
)

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.enums import QueryOrderStatus
except ImportError:  # pragma: no cover
    TradingClient = None
    GetOrdersRequest = None
    QueryOrderStatus = None

try:
    from alpaca.trading.client import TradingClient
except ImportError:  # pragma: no cover
    TradingClient = None


@dataclass(frozen=True)
class AlpacaBrokerConfig:
    api_key: str
    secret_key: str
    paper: bool = True
    base_url: str | None = None

    @classmethod
    def from_env(cls) -> "AlpacaBrokerConfig":
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError(
                "Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY."
            )

        paper_value = os.getenv("ALPACA_PAPER", "true").strip().lower()
        paper = paper_value in {"1", "true", "yes", "y"}
        base_url = os.getenv("ALPACA_BASE_URL")

        return cls(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
            base_url=base_url,
        )


class AlpacaBroker:
    def __init__(self, config: AlpacaBrokerConfig) -> None:
        self.config = config
        self._client = self._build_client()

    def _build_client(self):
        if TradingClient is None:
            raise ImportError(
                "alpaca-py is not installed. Install it with: pip install alpaca-py"
            )

        # base_url is intentionally ignored for now because TradingClient's
        # standard paper/live selection is controlled via the `paper` flag.
        # If you later need custom endpoints, you can extend this safely.
        return TradingClient(
            api_key=self.config.api_key,
            secret_key=self.config.secret_key,
            paper=self.config.paper,
        )

    def get_account(self) -> BrokerAccount:
        account = self._client.get_account()

        return BrokerAccount(
            account_id=str(getattr(account, "id", None)),
            cash=float(getattr(account, "cash", 0.0) or 0.0),
            equity=float(getattr(account, "equity", 0.0) or 0.0),
            buying_power=float(getattr(account, "buying_power", 0.0) or 0.0),
            currency=str(getattr(account, "currency", "USD") or "USD"),
        )

    def get_positions(self) -> dict[str, LiveBrokerPosition]:
        raw_positions = self._client.get_all_positions()
        positions: dict[str, LiveBrokerPosition] = {}

        for raw in raw_positions:
            symbol = str(getattr(raw, "symbol", ""))
            if not symbol:
                continue

            qty_raw = getattr(raw, "qty", 0)
            avg_price_raw = getattr(raw, "avg_entry_price", 0.0)
            market_price_raw = getattr(raw, "current_price", 0.0)
            market_value_raw = getattr(raw, "market_value", None)

            quantity = int(float(qty_raw or 0))
            avg_price = float(avg_price_raw or 0.0)
            market_price = float(market_price_raw or 0.0)

            if market_value_raw is None:
                market_value = quantity * market_price
            else:
                market_value = float(market_value_raw or 0.0)

            positions[symbol] = LiveBrokerPosition(
                symbol=symbol,
                quantity=quantity,
                avg_price=avg_price,
                market_price=market_price,
                market_value=market_value,
            )

        return positions

    def list_open_orders(self) -> list[LiveBrokerOrderStatus]:
        if GetOrdersRequest is None or QueryOrderStatus is None:
            raise ImportError(
                "alpaca-py is not installed. Install it with: pip install alpaca-py"
            )

        request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        raw_orders = self._client.get_orders(filter=request)

        orders: list[LiveBrokerOrderStatus] = []
        for raw in raw_orders:
            qty_raw = getattr(raw, "qty", 0)
            filled_qty_raw = getattr(raw, "filled_qty", 0)

            orders.append(
                LiveBrokerOrderStatus(
                    broker_order_id=str(getattr(raw, "id", None)),
                    client_order_id=str(getattr(raw, "client_order_id", None)),
                    symbol=str(getattr(raw, "symbol", "")),
                    side=str(getattr(raw, "side", "")).upper(),
                    quantity=int(float(qty_raw or 0)),
                    filled_quantity=int(float(filled_qty_raw or 0)),
                    order_type=str(getattr(raw, "order_type", "")),
                    time_in_force=str(getattr(raw, "time_in_force", "")),
                    status=str(getattr(raw, "status", "")),
                    submitted_at=(
                        str(getattr(raw, "submitted_at", None))
                        if getattr(raw, "submitted_at", None) is not None
                        else None
                    ),
                )
            )

        return orders

    def cancel_open_orders(self) -> None:
        raise NotImplementedError(
            "AlpacaBroker.cancel_open_orders is not implemented yet."
        )

    def submit_orders(
        self,
        orders: list[LiveBrokerOrderRequest],
    ) -> list[LiveBrokerOrderStatus]:
        raise NotImplementedError(
            "AlpacaBroker.submit_orders is not implemented yet."
        )

    def list_recent_fills(self) -> list[LiveBrokerFill]:
        raise NotImplementedError(
            "AlpacaBroker.list_recent_fills is not implemented yet."
        )