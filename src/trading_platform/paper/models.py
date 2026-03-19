from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from trading_platform.broker.base import BrokerFill

import pandas as pd


@dataclass(frozen=True)
class PaperTradingConfig:
    symbols: list[str]
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    top_n: int = 10
    weighting_scheme: str = "equal"
    vol_window: int = 20
    min_score: float | None = None
    max_weight: float | None = None
    max_names_per_group: int | None = None
    max_group_weight: float | None = None
    group_map_path: str | None = None
    rebalance_frequency: str = "daily"
    timing: str = "next_bar"
    initial_cash: float = 100_000.0
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0


@dataclass
class PaperPosition:
    symbol: str
    quantity: int
    avg_price: float = 0.0
    last_price: float = 0.0

    @property
    def market_value(self) -> float:
        return float(self.quantity) * float(self.last_price)


@dataclass
class PaperOrder:
    symbol: str
    side: str
    quantity: int
    reference_price: float
    target_weight: float
    current_quantity: int
    target_quantity: int
    notional: float
    reason: str


@dataclass
class PaperPortfolioState:
    as_of: str | None = None
    cash: float = 0.0
    positions: dict[str, PaperPosition] = field(default_factory=dict)
    last_targets: dict[str, float] = field(default_factory=dict)

    @property
    def gross_market_value(self) -> float:
        return float(sum(p.market_value for p in self.positions.values()))

    @property
    def equity(self) -> float:
        return float(self.cash + self.gross_market_value)



@dataclass
class PaperTradingRunResult:
    as_of: str
    state: PaperPortfolioState
    latest_prices: dict[str, float]
    latest_scores: dict[str, float]
    latest_target_weights: dict[str, float]
    scheduled_target_weights: dict[str, float]
    orders: list[PaperOrder]
    fills: list[BrokerFill] = field(default_factory=list)
    skipped_symbols: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

@dataclass
class PaperSignalSnapshot:
    asset_returns: pd.DataFrame
    scores: pd.DataFrame
    closes: pd.DataFrame
    skipped_symbols: list[str]


@dataclass
class OrderGenerationResult:
    orders: list[PaperOrder]
    target_weights: dict[str, float]
    diagnostics: dict[str, Any]