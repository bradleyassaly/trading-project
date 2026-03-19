from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class BrokerAccount:
    account_id: str | None
    cash: float
    equity: float
    buying_power: float
    currency: str = "USD"


@dataclass(frozen=True)
class LiveBrokerPosition:
    symbol: str
    quantity: int
    avg_price: float
    market_price: float
    market_value: float


@dataclass(frozen=True)
class LiveBrokerOrderRequest:
    symbol: str
    side: str  # BUY | SELL
    quantity: int
    order_type: str = "market"
    time_in_force: str = "day"
    limit_price: float | None = None
    client_order_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class LiveBrokerOrderStatus:
    broker_order_id: str | None
    client_order_id: str | None
    symbol: str
    side: str
    quantity: int
    filled_quantity: int
    order_type: str
    time_in_force: str
    status: str
    submitted_at: str | None = None

    @property
    def remaining_quantity(self) -> int:
        remaining = int(self.quantity) - int(self.filled_quantity)
        return max(remaining, 0)


@dataclass(frozen=True)
class LiveBrokerFill:
    broker_order_id: str | None
    symbol: str
    side: str
    quantity: int
    fill_price: float
    notional: float
    commission: float = 0.0
    metadata: dict[str, str] = field(default_factory=dict)