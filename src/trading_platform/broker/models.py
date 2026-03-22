from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


BROKER_ORDER_SIDES = {"BUY", "SELL"}
BROKER_ORDER_TYPES = {"market", "limit"}
BROKER_TIME_IN_FORCE = {"day", "gtc"}
BROKER_ORDER_RESULT_STATUSES = {
    "accepted",
    "submitted",
    "rejected",
    "skipped",
    "failed",
    "cancelled",
    "validation_only",
}


def _validate_nonnegative_optional(value: float | int | None, field_name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be >= 0")


@dataclass(frozen=True)
class BrokerConfig:
    broker_name: str = "mock"
    live_trading_enabled: bool = False
    require_manual_enable_flag: bool = True
    manual_enable_flag_path: str | None = None
    global_kill_switch_path: str | None = None
    expected_account_id: str | None = None
    max_orders_per_run: int | None = None
    max_total_notional_per_run: float | None = None
    max_symbol_notional_per_order: float | None = None
    max_gross_exposure: float | None = None
    max_net_exposure: float | None = None
    max_position_weight: float | None = None
    max_position_change_notional: float | None = None
    allowed_order_types: list[str] = field(default_factory=lambda: ["market"])
    default_order_type: str = "market"
    allow_shorts_live: bool = False
    cancel_existing_open_orders_before_submit: bool = False
    skip_submission_if_existing_open_orders: bool = True
    require_fresh_market_data: bool = False
    max_market_data_age_seconds: int | None = None
    require_clean_monitoring_status: bool = False
    allowed_monitoring_statuses: list[str] = field(default_factory=lambda: ["healthy"])
    monitoring_status_path: str | None = None
    mock_equity: float = 100_000.0
    mock_cash: float = 100_000.0
    mock_positions_path: str | None = None
    mock_open_orders_path: str | None = None
    mock_reject_symbols: list[str] = field(default_factory=list)
    alpaca_api_key_env_var: str = "ALPACA_API_KEY"
    alpaca_secret_key_env_var: str = "ALPACA_SECRET_KEY"
    alpaca_paper: bool = True
    alpaca_base_url: str | None = None
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.broker_name or not self.broker_name.strip():
            raise ValueError("broker_name must be a non-empty string")
        if self.default_order_type not in BROKER_ORDER_TYPES:
            raise ValueError(f"default_order_type must be one of: {sorted(BROKER_ORDER_TYPES)}")
        if not self.allowed_order_types:
            raise ValueError("allowed_order_types must contain at least one order type")
        invalid_order_types = [value for value in self.allowed_order_types if value not in BROKER_ORDER_TYPES]
        if invalid_order_types:
            raise ValueError(f"Unsupported allowed_order_types: {invalid_order_types}")
        if self.default_order_type not in self.allowed_order_types:
            raise ValueError("default_order_type must be included in allowed_order_types")
        if self.max_orders_per_run is not None and self.max_orders_per_run <= 0:
            raise ValueError("max_orders_per_run must be > 0")
        _validate_nonnegative_optional(self.max_total_notional_per_run, "max_total_notional_per_run")
        _validate_nonnegative_optional(self.max_symbol_notional_per_order, "max_symbol_notional_per_order")
        _validate_nonnegative_optional(self.max_gross_exposure, "max_gross_exposure")
        _validate_nonnegative_optional(self.max_net_exposure, "max_net_exposure")
        _validate_nonnegative_optional(self.max_position_weight, "max_position_weight")
        _validate_nonnegative_optional(self.max_position_change_notional, "max_position_change_notional")
        _validate_nonnegative_optional(self.max_market_data_age_seconds, "max_market_data_age_seconds")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerAccountSnapshot:
    account_id: str | None
    cash: float
    equity: float
    buying_power: float
    currency: str = "USD"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    quantity: int
    avg_price: float
    market_price: float
    market_value: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerOpenOrder:
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

    def __post_init__(self) -> None:
        if self.side not in BROKER_ORDER_SIDES:
            raise ValueError(f"Unsupported broker order side: {self.side}")

    @property
    def remaining_quantity(self) -> int:
        return max(int(self.quantity) - int(self.filled_quantity), 0)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerOrderRequest:
    symbol: str
    side: str
    quantity: int
    order_type: str = "market"
    time_in_force: str = "day"
    limit_price: float | None = None
    client_order_id: str | None = None
    requested_notional: float = 0.0
    price_reference: float = 0.0
    reason: str | None = None
    provenance: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.side not in BROKER_ORDER_SIDES:
            raise ValueError(f"Unsupported broker order side: {self.side}")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")
        if self.order_type not in BROKER_ORDER_TYPES:
            raise ValueError(f"Unsupported order_type: {self.order_type}")
        if self.time_in_force not in BROKER_TIME_IN_FORCE:
            raise ValueError(f"Unsupported time_in_force: {self.time_in_force}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerOrderResult:
    symbol: str
    side: str
    quantity: int
    order_type: str
    time_in_force: str
    status: str
    submitted: bool
    client_order_id: str | None = None
    broker_order_id: str | None = None
    filled_quantity: int = 0
    submitted_at: str | None = None
    message: str | None = None

    def __post_init__(self) -> None:
        if self.status not in BROKER_ORDER_RESULT_STATUSES:
            raise ValueError(f"Unsupported broker order result status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BrokerExecutionSummary:
    timestamp: str
    broker_name: str
    account_id: str | None
    validate_only: bool
    risk_passed: bool
    submitted: bool
    requested_order_count: int
    submitted_order_count: int
    skipped_order_count: int
    rejected_order_count: int
    duplicate_order_skip_count: int
    cancel_all_invoked: bool
    total_requested_notional: float
    total_submitted_notional: float
    projected_gross_exposure: float
    projected_net_exposure: float
    projected_max_position_weight: float
    hard_block_count: int
    warning_count: int
    monitoring_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LiveRiskCheckResult:
    check_name: str
    passed: bool
    hard_block: bool
    message: str
    severity: str
    metric_value: Any = None
    threshold_value: Any = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
