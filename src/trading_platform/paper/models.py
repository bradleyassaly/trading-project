from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from trading_platform.broker.base import BrokerFill

import pandas as pd

from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.universe_provenance.models import UniverseBuildBundle


@dataclass(frozen=True)
class PaperTradingConfig:
    symbols: list[str]
    preset_name: str | None = None
    universe_name: str | None = None
    signal_source: str = "legacy"
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    lookback_bars: int | None = None
    skip_bars: int = 0
    top_n: int = 10
    weighting_scheme: str = "equal"
    vol_window: int = 20
    rebalance_bars: int | None = None
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_score: float | None = None
    max_weight: float | None = None
    max_names_per_group: int | None = None
    max_group_weight: float | None = None
    group_map_path: str | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    benchmark: str | None = None
    rebalance_frequency: str = "daily"
    timing: str = "next_bar"
    initial_cash: float = 100_000.0
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0
    approved_model_state_path: str | None = None
    composite_artifact_dir: str | None = None
    composite_horizon: int = 1
    composite_weighting_scheme: str = "equal"
    composite_portfolio_mode: str = "long_only_top_n"
    composite_long_quantile: float = 0.2
    composite_short_quantile: float = 0.2
    min_price: float | None = None
    min_volume: float | None = None
    min_avg_dollar_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    use_alpaca_latest_data: bool = False
    latest_data_max_age_seconds: int = 86_400
    slippage_model: str = "none"
    slippage_buy_bps: float = 0.0
    slippage_sell_bps: float = 0.0
    ensemble_enabled: bool = False
    ensemble_mode: str = "disabled"
    ensemble_weight_method: str = "equal"
    ensemble_normalize_scores: str = "rank_pct"
    ensemble_max_members: int = 5
    ensemble_require_promoted_only: bool = True
    ensemble_max_members_per_family: int | None = None
    ensemble_minimum_member_observations: int = 0
    ensemble_minimum_member_metric: float | None = None
    sub_universe_id: str | None = None
    universe_filters: list[dict[str, Any]] = field(default_factory=list)
    universe_membership_path: str | None = None
    market_regime_path: str | None = None
    data_sources: dict[str, Any] = field(default_factory=dict)


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
    expected_fill_price: float | None = None
    expected_fees: float = 0.0
    expected_slippage_bps: float = 0.0


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
class PaperExecutionPriceSnapshot:
    symbol: str
    decision_timestamp: str | None
    historical_price: float | None
    latest_price: float | None
    final_price_used: float | None
    price_source_used: str
    fallback_used: bool
    latest_bar_timestamp: str | None
    latest_bar_age_seconds: float | None
    latest_data_stale: bool | None
    latest_data_source: str



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
    price_snapshots: list[PaperExecutionPriceSnapshot] = field(default_factory=list)
    decision_bundle: DecisionJournalBundle | None = None
    universe_bundle: UniverseBuildBundle | None = None

@dataclass
class PaperSignalSnapshot:
    asset_returns: pd.DataFrame
    scores: pd.DataFrame
    closes: pd.DataFrame
    skipped_symbols: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderGenerationResult:
    orders: list[PaperOrder]
    target_weights: dict[str, float]
    diagnostics: dict[str, Any]
