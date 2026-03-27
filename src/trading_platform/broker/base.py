from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from typing import Protocol


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    quantity: int
    avg_price: float
    last_price: float


@dataclass(frozen=True)
class BrokerOrder:
    symbol: str
    side: str  # BUY | SELL
    quantity: int
    reference_price: float
    reason: str = "rebalance_to_target"


@dataclass(frozen=True)
class BrokerFill:
    symbol: str
    side: str
    quantity: int
    fill_price: float
    notional: float
    commission: float = 0.0
    slippage_bps: float = 0.0
    realized_pnl: float = 0.0
    trade_id: str | None = None
    strategy_id: str | None = None
    signal_source: str | None = None
    provenance: dict[str, Any] = field(default_factory=dict)


class Broker(Protocol):
    def get_cash(self) -> float: ...

    def get_positions(self) -> dict[str, BrokerPosition]: ...

    def submit_orders(self, orders: list[BrokerOrder]) -> list[BrokerFill]: ...
