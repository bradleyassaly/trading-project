from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any
from trading_platform.broker.base import BrokerFill

import pandas as pd

from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.universe_provenance.models import UniverseBuildBundle

if TYPE_CHECKING:
    from trading_platform.decision_journal.models import TradeDecision
    from trading_platform.execution.costs import TransactionCostReport
    from trading_platform.execution.order_lifecycle import OrderLifecycleRecord
    from trading_platform.execution.reconciliation import OrderLifecycleReconciliationResult


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
    enable_cost_model: bool = False
    commission_bps: float = 0.0
    minimum_commission: float = 0.0
    spread_bps: float = 0.0
    min_weight_change_to_trade: float = 0.0
    entry_score_threshold: float | None = None
    exit_score_threshold: float | None = None
    hold_score_band: bool = True
    use_percentile_thresholds: bool = False
    entry_score_percentile: float | None = None
    exit_score_percentile: float | None = None
    apply_bands_to_new_entries: bool = True
    apply_bands_to_reductions: bool = True
    apply_bands_to_full_exits: bool = True
    ev_gate_enabled: bool = False
    ev_gate_model_type: str = "bucketed_mean"
    ev_gate_horizon_days: int = 5
    ev_gate_target_type: str = "market_proxy"
    ev_gate_hybrid_alpha: float = 0.8
    ev_gate_mode: str = "hard"
    ev_gate_weight_multiplier: bool = False
    ev_gate_weight_scale: float = 1.0
    ev_gate_extreme_negative_threshold: float | None = None
    ev_gate_score_clip_min: float | None = None
    ev_gate_score_clip_max: float | None = None
    ev_gate_normalize_scores: bool = False
    ev_gate_normalization_method: str = "zscore"
    ev_gate_normalize_within: str = "all_candidates"
    ev_gate_use_normalized_score_for_weighting: bool = True
    ev_gate_weight_multiplier_min: float | None = None
    ev_gate_weight_multiplier_max: float | None = None
    ev_gate_use_confidence_weighting: bool = False
    ev_gate_confidence_method: str = "residual_std"
    ev_gate_confidence_scale: float = 1.0
    ev_gate_confidence_clip_min: float = 0.5
    ev_gate_confidence_clip_max: float = 1.5
    ev_gate_confidence_min_samples_per_bucket: int = 20
    ev_gate_confidence_shrinkage_enabled: bool = True
    ev_gate_confidence_component_residual_std_weight: float = 1.0
    ev_gate_confidence_component_magnitude_weight: float = 0.0
    ev_gate_confidence_component_model_performance_weight: float = 0.0
    ev_gate_use_confidence_filter: bool = False
    ev_gate_confidence_threshold: float = 0.0
    ev_gate_use_reliability_weighting: bool = False
    ev_gate_reliability_model_type: str = "logistic"
    ev_gate_reliability_calibration_method: str = "none"
    ev_gate_use_reliability_filter: bool = False
    ev_gate_reliability_threshold: float = 0.5
    ev_gate_reliability_bootstrap_min_training_samples: int | None = None
    ev_gate_reliability_min_training_samples: int = 20
    ev_gate_reliability_enabled_after_min_history_rows: int = 0
    ev_gate_reliability_enabled_after_min_fit_days: int = 0
    ev_gate_reliability_cold_start_behavior: str = "disabled_passthrough"
    ev_gate_reliability_recent_window: int = 20
    ev_gate_reliability_target_type: str = "sign_success"
    ev_gate_reliability_top_percentile: float = 0.8
    ev_gate_reliability_top_bucket_pct: float | None = None
    ev_gate_reliability_hurdle: float = 0.0
    ev_gate_reliability_usage_mode: str = "weighting_only"
    ev_gate_reliability_weight_multiplier_min: float = 0.75
    ev_gate_reliability_weight_multiplier_max: float = 1.25
    ev_gate_reliability_neutral_band: float = 0.05
    ev_gate_reliability_max_promoted_trades_per_day: int | None = None
    ev_gate_min_expected_net_return: float = 0.0
    ev_gate_min_probability_positive: float | None = None
    ev_gate_risk_penalty_lambda: float = 0.0
    ev_gate_fallback_to_score_bands: bool = True
    ev_gate_training_root: str | None = None
    ev_gate_training_source: str = "executed_trades"
    ev_gate_min_training_samples: int = 20
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
    reference_data_root: str | None = None
    universe_membership_path: str | None = None
    taxonomy_snapshot_path: str | None = None
    benchmark_mapping_path: str | None = None
    market_regime_path: str | None = None
    data_sources: dict[str, Any] = field(default_factory=dict)
    replay_as_of_date: str | None = None


@dataclass
class PaperPosition:
    symbol: str
    quantity: int
    avg_price: float = 0.0
    last_price: float = 0.0

    @property
    def market_value(self) -> float:
        return float(self.quantity) * float(self.last_price)

    @property
    def cost_basis(self) -> float:
        return float(self.quantity) * float(self.avg_price)

    @property
    def unrealized_pnl(self) -> float:
        return float(self.market_value - self.cost_basis)


@dataclass
class PaperTradeLot:
    trade_id: str
    symbol: str
    strategy_id: str
    signal_source: str | None
    signal_family: str | None
    side: str
    entry_as_of: str
    entry_reference_price: float
    entry_price: float
    quantity: int
    remaining_quantity: int
    entry_slippage_cost: float = 0.0
    entry_spread_cost: float = 0.0
    entry_commission_cost: float = 0.0
    entry_total_execution_cost: float = 0.0
    cost_model: str = "disabled"
    attribution_method: str = "target_weight_proportional"
    metadata: dict[str, Any] = field(default_factory=dict)


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
    expected_spread_bps: float = 0.0
    expected_slippage_cost: float = 0.0
    expected_spread_cost: float = 0.0
    expected_commission_cost: float = 0.0
    expected_total_execution_cost: float = 0.0
    expected_gross_notional: float = 0.0
    cost_model: str = "disabled"
    provenance: dict[str, Any] = field(default_factory=dict)


@dataclass
class PaperPortfolioState:
    as_of: str | None = None
    cash: float = 0.0
    positions: dict[str, PaperPosition] = field(default_factory=dict)
    last_targets: dict[str, float] = field(default_factory=dict)
    initial_cash_basis: float = 0.0
    cumulative_realized_pnl: float = 0.0
    cumulative_gross_realized_pnl: float = 0.0
    cumulative_fees: float = 0.0
    cumulative_slippage_cost: float = 0.0
    cumulative_spread_cost: float = 0.0
    cumulative_execution_cost: float = 0.0
    open_lots: dict[str, list[PaperTradeLot]] = field(default_factory=dict)
    next_trade_id: int = 1

    @property
    def gross_market_value(self) -> float:
        return float(sum(p.market_value for p in self.positions.values()))

    @property
    def cost_basis(self) -> float:
        return float(sum(p.cost_basis for p in self.positions.values()))

    @property
    def unrealized_pnl(self) -> float:
        return float(sum(p.unrealized_pnl for p in self.positions.values()))

    @property
    def equity(self) -> float:
        return float(self.cash + self.gross_market_value)

    @property
    def total_pnl(self) -> float:
        baseline = float(self.initial_cash_basis or 0.0)
        return float(self.equity - baseline)


PERSISTENT_PAPER_STATE_SCHEMA_VERSION = 1
PAPER_EXECUTION_SIMULATION_SCHEMA_VERSION = "paper_execution_simulation_v1"


def _normalize_float_map(value: dict[str, Any] | None) -> dict[str, float]:
    if value is None:
        return {}
    return {str(key): float(value[key] or 0.0) for key in sorted(value)}


@dataclass(frozen=True)
class PersistentPaperState:
    schema_version: int = PERSISTENT_PAPER_STATE_SCHEMA_VERSION
    as_of: str | None = None
    cash: float = 0.0
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_targets: dict[str, float] = field(default_factory=dict)
    initial_cash_basis: float = 0.0
    cumulative_realized_pnl: float = 0.0
    cumulative_gross_realized_pnl: float = 0.0
    cumulative_fees: float = 0.0
    cumulative_slippage_cost: float = 0.0
    cumulative_spread_cost: float = 0.0
    cumulative_execution_cost: float = 0.0
    open_lots: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    next_trade_id: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != PERSISTENT_PAPER_STATE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported persistent paper state schema_version: {self.schema_version}")

    @classmethod
    def from_portfolio_state(cls, state: PaperPortfolioState) -> "PersistentPaperState":
        return cls(
            as_of=state.as_of,
            cash=float(state.cash),
            positions={symbol: asdict(position) for symbol, position in sorted(state.positions.items())},
            last_targets=_normalize_float_map(state.last_targets),
            initial_cash_basis=float(state.initial_cash_basis),
            cumulative_realized_pnl=float(state.cumulative_realized_pnl),
            cumulative_gross_realized_pnl=float(state.cumulative_gross_realized_pnl),
            cumulative_fees=float(state.cumulative_fees),
            cumulative_slippage_cost=float(state.cumulative_slippage_cost),
            cumulative_spread_cost=float(state.cumulative_spread_cost),
            cumulative_execution_cost=float(state.cumulative_execution_cost),
            open_lots={symbol: [asdict(lot) for lot in lots] for symbol, lots in sorted(state.open_lots.items())},
            next_trade_id=int(state.next_trade_id),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "PersistentPaperState":
        data = dict(payload or {})
        data.setdefault("schema_version", PERSISTENT_PAPER_STATE_SCHEMA_VERSION)
        data.setdefault("as_of", None)
        data.setdefault("cash", 0.0)
        data.setdefault("positions", {})
        data.setdefault("last_targets", {})
        data.setdefault("initial_cash_basis", 0.0)
        data.setdefault("cumulative_realized_pnl", 0.0)
        data.setdefault("cumulative_gross_realized_pnl", 0.0)
        data.setdefault("cumulative_fees", 0.0)
        data.setdefault("cumulative_slippage_cost", 0.0)
        data.setdefault("cumulative_spread_cost", 0.0)
        data.setdefault("cumulative_execution_cost", 0.0)
        data.setdefault("open_lots", {})
        data.setdefault("next_trade_id", 1)
        positions: dict[str, dict[str, Any]] = {}
        for symbol, row in dict(data.get("positions", {})).items():
            try:
                positions[str(symbol)] = {
                    "symbol": str(row.get("symbol") or symbol),
                    "quantity": int(row.get("quantity", 0) or 0),
                    "avg_price": float(row.get("avg_price", 0.0) or 0.0),
                    "last_price": float(row.get("last_price", 0.0) or 0.0),
                }
            except (AttributeError, TypeError, ValueError):
                continue
        normalized_lots: dict[str, list[dict[str, Any]]] = {}
        for symbol, rows in dict(data.get("open_lots", {})).items():
            clean_rows: list[dict[str, Any]] = []
            for row in rows or []:
                try:
                    clean_rows.append(
                        {
                            "trade_id": str(row.get("trade_id") or ""),
                            "symbol": str(row.get("symbol") or symbol),
                            "strategy_id": str(row.get("strategy_id") or "unknown_strategy"),
                            "signal_source": row.get("signal_source"),
                            "signal_family": row.get("signal_family"),
                            "side": str(row.get("side") or "BUY"),
                            "entry_as_of": str(row.get("entry_as_of") or ""),
                            "entry_reference_price": float(row.get("entry_reference_price", 0.0) or 0.0),
                            "entry_price": float(row.get("entry_price", 0.0) or 0.0),
                            "quantity": int(row.get("quantity", 0) or 0),
                            "remaining_quantity": int(row.get("remaining_quantity", row.get("quantity", 0)) or 0),
                            "entry_slippage_cost": float(row.get("entry_slippage_cost", 0.0) or 0.0),
                            "entry_spread_cost": float(row.get("entry_spread_cost", 0.0) or 0.0),
                            "entry_commission_cost": float(row.get("entry_commission_cost", 0.0) or 0.0),
                            "entry_total_execution_cost": float(row.get("entry_total_execution_cost", 0.0) or 0.0),
                            "cost_model": str(row.get("cost_model") or "disabled"),
                            "attribution_method": str(row.get("attribution_method") or "target_weight_proportional"),
                            "metadata": dict(row.get("metadata") or {}),
                        }
                    )
                except (AttributeError, TypeError, ValueError):
                    continue
            normalized_lots[str(symbol)] = clean_rows
        return cls(
            schema_version=int(data["schema_version"]),
            as_of=str(data["as_of"]) if data.get("as_of") is not None else None,
            cash=float(data.get("cash", 0.0) or 0.0),
            positions=positions,
            last_targets=_normalize_float_map(data.get("last_targets")),
            initial_cash_basis=float(data.get("initial_cash_basis", 0.0) or 0.0),
            cumulative_realized_pnl=float(data.get("cumulative_realized_pnl", 0.0) or 0.0),
            cumulative_gross_realized_pnl=float(data.get("cumulative_gross_realized_pnl", 0.0) or 0.0),
            cumulative_fees=float(data.get("cumulative_fees", 0.0) or 0.0),
            cumulative_slippage_cost=float(data.get("cumulative_slippage_cost", 0.0) or 0.0),
            cumulative_spread_cost=float(data.get("cumulative_spread_cost", 0.0) or 0.0),
            cumulative_execution_cost=float(data.get("cumulative_execution_cost", 0.0) or 0.0),
            open_lots=normalized_lots,
            next_trade_id=int(data.get("next_trade_id", 1) or 1),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "as_of": self.as_of,
            "cash": float(self.cash),
            "positions": {symbol: dict(row) for symbol, row in sorted(self.positions.items())},
            "last_targets": dict(self.last_targets),
            "initial_cash_basis": float(self.initial_cash_basis),
            "cumulative_realized_pnl": float(self.cumulative_realized_pnl),
            "cumulative_gross_realized_pnl": float(self.cumulative_gross_realized_pnl),
            "cumulative_fees": float(self.cumulative_fees),
            "cumulative_slippage_cost": float(self.cumulative_slippage_cost),
            "cumulative_spread_cost": float(self.cumulative_spread_cost),
            "cumulative_execution_cost": float(self.cumulative_execution_cost),
            "open_lots": {symbol: list(rows) for symbol, rows in sorted(self.open_lots.items())},
            "next_trade_id": int(self.next_trade_id),
        }

    def to_portfolio_state(self) -> PaperPortfolioState:
        positions = {
            symbol: PaperPosition(**row)
            for symbol, row in sorted(self.positions.items())
        }
        state = PaperPortfolioState(
            as_of=self.as_of,
            cash=float(self.cash),
            positions=positions,
            last_targets=dict(self.last_targets),
            initial_cash_basis=float(self.initial_cash_basis),
            cumulative_realized_pnl=float(self.cumulative_realized_pnl),
            cumulative_gross_realized_pnl=float(self.cumulative_gross_realized_pnl),
            cumulative_fees=float(self.cumulative_fees),
            cumulative_slippage_cost=float(self.cumulative_slippage_cost),
            cumulative_spread_cost=float(self.cumulative_spread_cost),
            cumulative_execution_cost=float(self.cumulative_execution_cost),
            open_lots={
                symbol: [PaperTradeLot(**row) for row in rows]
                for symbol, rows in sorted(self.open_lots.items())
            },
            next_trade_id=int(self.next_trade_id),
        )
        if state.initial_cash_basis <= 0.0:
            state.initial_cash_basis = float(state.cash + sum(position.cost_basis for position in positions.values()))
        return state


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
    requested_orders: list[PaperOrder] = field(default_factory=list)
    fills: list[BrokerFill] = field(default_factory=list)
    skipped_symbols: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    price_snapshots: list[PaperExecutionPriceSnapshot] = field(default_factory=list)
    decision_bundle: DecisionJournalBundle | None = None
    trade_decision_contracts: list["TradeDecision"] = field(default_factory=list)
    order_lifecycle_records: list["OrderLifecycleRecord"] = field(default_factory=list)
    reconciliation_result: "OrderLifecycleReconciliationResult | None" = None
    universe_bundle: UniverseBuildBundle | None = None
    attribution: dict[str, Any] = field(default_factory=dict)
    execution_simulation_report: "PaperExecutionSimulationReport | None" = None
    transaction_cost_report: "TransactionCostReport | None" = None


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


@dataclass(frozen=True)
class PaperExecutionSimulationOrder:
    symbol: str
    side: str
    requested_quantity: int
    executable_quantity: int
    requested_notional: float
    executable_notional: float
    reference_price: float
    estimated_fill_price: float
    filled_fraction: float
    status: str
    slippage_bps: float = 0.0
    spread_bps: float | None = None
    commission: float = 0.0
    submission_delay_seconds: float = 0.0
    fill_delay_seconds: float = 0.0
    clipping_reason: str | None = None
    rejection_reason: str | None = None
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "requested_quantity": int(self.requested_quantity),
            "executable_quantity": int(self.executable_quantity),
            "requested_notional": float(self.requested_notional),
            "executable_notional": float(self.executable_notional),
            "reference_price": float(self.reference_price),
            "estimated_fill_price": float(self.estimated_fill_price),
            "filled_fraction": float(self.filled_fraction),
            "status": self.status,
            "slippage_bps": float(self.slippage_bps),
            "spread_bps": float(self.spread_bps) if self.spread_bps is not None else None,
            "commission": float(self.commission),
            "submission_delay_seconds": float(self.submission_delay_seconds),
            "fill_delay_seconds": float(self.fill_delay_seconds),
            "clipping_reason": self.clipping_reason,
            "rejection_reason": self.rejection_reason,
            "provenance": dict(self.provenance),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PaperExecutionSimulationOrder":
        data = dict(payload or {})
        return cls(
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            requested_quantity=int(data.get("requested_quantity", 0) or 0),
            executable_quantity=int(data.get("executable_quantity", 0) or 0),
            requested_notional=float(data.get("requested_notional", 0.0) or 0.0),
            executable_notional=float(data.get("executable_notional", 0.0) or 0.0),
            reference_price=float(data.get("reference_price", 0.0) or 0.0),
            estimated_fill_price=float(data.get("estimated_fill_price", 0.0) or 0.0),
            filled_fraction=float(data.get("filled_fraction", 0.0) or 0.0),
            status=str(data.get("status", "rejected") or "rejected"),
            slippage_bps=float(data.get("slippage_bps", 0.0) or 0.0),
            spread_bps=float(data["spread_bps"]) if data.get("spread_bps") is not None else None,
            commission=float(data.get("commission", 0.0) or 0.0),
            submission_delay_seconds=float(data.get("submission_delay_seconds", 0.0) or 0.0),
            fill_delay_seconds=float(data.get("fill_delay_seconds", 0.0) or 0.0),
            clipping_reason=str(data["clipping_reason"]) if data.get("clipping_reason") is not None else None,
            rejection_reason=str(data["rejection_reason"]) if data.get("rejection_reason") is not None else None,
            provenance=dict(data.get("provenance") or {}),
        )


@dataclass(frozen=True)
class PaperExecutionSimulationReport:
    as_of: str
    schema_version: str = PAPER_EXECUTION_SIMULATION_SCHEMA_VERSION
    config: dict[str, Any] = field(default_factory=dict)
    orders: list[PaperExecutionSimulationOrder] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "schema_version": self.schema_version,
            "config": dict(self.config),
            "orders": [row.to_dict() for row in self.orders],
            "summary": dict(self.summary),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PaperExecutionSimulationReport":
        data = dict(payload or {})
        return cls(
            as_of=str(data["as_of"]),
            schema_version=str(data.get("schema_version", PAPER_EXECUTION_SIMULATION_SCHEMA_VERSION)),
            config=dict(data.get("config") or {}),
            orders=[PaperExecutionSimulationOrder.from_dict(row) for row in data.get("orders", [])],
            summary=dict(data.get("summary") or {}),
        )
