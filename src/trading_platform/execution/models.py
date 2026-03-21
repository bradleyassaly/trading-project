from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


PRICE_SOURCE_ASSUMPTIONS = {"close", "next_open", "vwap_proxy"}
COMMISSION_MODEL_TYPES = {"per_share", "bps", "flat"}
SLIPPAGE_MODEL_TYPES = {"fixed_bps", "spread_plus_bps", "liquidity_scaled"}
PARTIAL_FILL_BEHAVIORS = {"reject", "clip", "allow_partial"}
MISSING_LIQUIDITY_BEHAVIORS = {"reject", "warn_and_clip"}
STALE_MARKET_DATA_BEHAVIORS = {"reject", "warn"}
ORDER_STATUSES = {"executable", "clipped", "rejected"}


def _validate_nonnegative(value: float | int | None, field_name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be >= 0")


@dataclass(frozen=True)
class ExecutionConfig:
    enabled: bool = True
    price_source_assumption: str = "close"
    commission_model_type: str = "bps"
    commission_per_share: float = 0.0
    commission_bps: float = 0.0
    flat_commission_per_order: float = 0.0
    slippage_model_type: str = "liquidity_scaled"
    fixed_slippage_bps: float = 0.0
    half_spread_bps: float = 0.0
    liquidity_slippage_bps: float = 0.0
    max_participation_of_adv: float | None = None
    min_average_dollar_volume: float | None = None
    min_price: float | None = None
    min_trade_notional: float = 25.0
    lot_size: int = 1
    max_turnover_per_rebalance: float | None = None
    max_position_notional_change: float | None = None
    allow_shorts: bool = True
    enforce_short_borrow_proxy: bool = False
    max_short_gross_exposure: float | None = None
    short_borrow_blocklist: list[str] = field(default_factory=list)
    partial_fill_behavior: str = "clip"
    missing_liquidity_behavior: str = "reject"
    stale_market_data_behavior: str = "reject"
    cash_buffer_bps: float = 0.0
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.price_source_assumption not in PRICE_SOURCE_ASSUMPTIONS:
            raise ValueError(f"Unsupported price_source_assumption: {self.price_source_assumption}")
        if self.commission_model_type not in COMMISSION_MODEL_TYPES:
            raise ValueError(f"Unsupported commission_model_type: {self.commission_model_type}")
        if self.slippage_model_type not in SLIPPAGE_MODEL_TYPES:
            raise ValueError(f"Unsupported slippage_model_type: {self.slippage_model_type}")
        if self.partial_fill_behavior not in PARTIAL_FILL_BEHAVIORS:
            raise ValueError(f"Unsupported partial_fill_behavior: {self.partial_fill_behavior}")
        if self.missing_liquidity_behavior not in MISSING_LIQUIDITY_BEHAVIORS:
            raise ValueError(f"Unsupported missing_liquidity_behavior: {self.missing_liquidity_behavior}")
        if self.stale_market_data_behavior not in STALE_MARKET_DATA_BEHAVIORS:
            raise ValueError(f"Unsupported stale_market_data_behavior: {self.stale_market_data_behavior}")
        _validate_nonnegative(self.commission_per_share, "commission_per_share")
        _validate_nonnegative(self.commission_bps, "commission_bps")
        _validate_nonnegative(self.flat_commission_per_order, "flat_commission_per_order")
        _validate_nonnegative(self.fixed_slippage_bps, "fixed_slippage_bps")
        _validate_nonnegative(self.half_spread_bps, "half_spread_bps")
        _validate_nonnegative(self.liquidity_slippage_bps, "liquidity_slippage_bps")
        _validate_nonnegative(self.max_participation_of_adv, "max_participation_of_adv")
        _validate_nonnegative(self.min_average_dollar_volume, "min_average_dollar_volume")
        _validate_nonnegative(self.min_price, "min_price")
        _validate_nonnegative(self.min_trade_notional, "min_trade_notional")
        _validate_nonnegative(self.max_turnover_per_rebalance, "max_turnover_per_rebalance")
        _validate_nonnegative(self.max_position_notional_change, "max_position_notional_change")
        _validate_nonnegative(self.max_short_gross_exposure, "max_short_gross_exposure")
        _validate_nonnegative(self.cash_buffer_bps, "cash_buffer_bps")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be > 0")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketDataInput:
    symbol: str
    price: float
    average_daily_volume_shares: float | None = None
    average_daily_dollar_volume: float | None = None
    spread_bps: float | None = None
    borrow_available: bool | None = None
    stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionRequest:
    symbol: str
    side: str
    requested_shares: int
    requested_notional: float
    current_shares: int
    target_shares: int
    current_weight: float = 0.0
    target_weight: float = 0.0
    price: float = 0.0
    average_daily_volume_shares: float | None = None
    average_daily_dollar_volume: float | None = None
    spread_bps: float | None = None
    borrow_available: bool | None = None
    stale_market_data: bool = False
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutableOrder:
    symbol: str
    side: str
    requested_shares: int
    requested_notional: float
    adjusted_shares: int
    adjusted_notional: float
    estimated_fill_price: float
    slippage_bps: float
    commission: float
    participation_pct_adv: float | None
    filled_fraction: float
    status: str
    clipping_reason: str | None = None
    provenance: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in ORDER_STATUSES:
            raise ValueError(f"Unsupported order status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RejectedOrder:
    symbol: str
    side: str
    requested_shares: int
    requested_notional: float
    adjusted_shares: int
    adjusted_notional: float
    estimated_fill_price: float
    slippage_bps: float
    commission: float
    participation_pct_adv: float | None
    filled_fraction: float
    status: str
    rejection_reason: str
    provenance: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status != "rejected":
            raise ValueError("RejectedOrder status must be rejected")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LiquidityDiagnostic:
    symbol: str
    tradeable: bool
    reason: str
    price: float
    average_daily_volume_shares: float | None
    average_daily_dollar_volume: float | None
    spread_bps: float | None
    borrow_available: bool | None
    stale_market_data: bool
    requested_shares: int
    adjusted_shares: int
    participation_pct_adv: float | None
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSummary:
    requested_order_count: int
    executable_order_count: int
    rejected_order_count: int
    clipped_order_count: int
    requested_notional: float
    executed_notional: float
    expected_commission_total: float
    expected_slippage_cost_total: float
    expected_total_cost: float
    turnover_before_constraints: float
    turnover_after_constraints: float
    rejected_order_ratio: float
    clipped_order_ratio: float
    liquidity_failure_count: int
    short_borrow_failure_count: int
    zero_executable_orders: bool
    max_participation_pct_adv: float
    estimated_cost_bps_on_executed_notional: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSimulationResult:
    requested_orders: list[ExecutionRequest]
    executable_orders: list[ExecutableOrder]
    rejected_orders: list[RejectedOrder]
    summary: ExecutionSummary
    liquidity_diagnostics: list[LiquidityDiagnostic]
    turnover_rows: list[dict[str, Any]]
    symbol_tradeability_rows: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_orders": [order.to_dict() for order in self.requested_orders],
            "executable_orders": [order.to_dict() for order in self.executable_orders],
            "rejected_orders": [order.to_dict() for order in self.rejected_orders],
            "summary": self.summary.to_dict(),
            "liquidity_diagnostics": [row.to_dict() for row in self.liquidity_diagnostics],
            "turnover_rows": self.turnover_rows,
            "symbol_tradeability_rows": self.symbol_tradeability_rows,
        }
