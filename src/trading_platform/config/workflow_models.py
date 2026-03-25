from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _validate_symbol_selection(
    *,
    symbols: list[str] | None,
    universe: str | None,
    preset: str | None,
) -> None:
    selected = sum(bool(value) for value in (symbols, universe, preset))
    if selected != 1:
        raise ValueError("exactly one of symbols, universe, or preset must be provided")


@dataclass(frozen=True)
class ResearchRunWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    preset: str | None = None
    strategy: str = "sma_cross"
    engine: str = "vectorized"
    start: str | None = None
    end: str | None = None
    output_dir: str = "artifacts/research"
    cash: float = 10_000.0
    commission: float = 0.001
    rebalance_frequency: str = "daily"
    fast: int = 20
    slow: int = 100
    lookback: int = 20
    entry_lookback: int = 55
    exit_lookback: int = 20
    momentum_lookback: int | None = None
    lookback_bars: int = 126
    skip_bars: int = 0
    top_n: int = 3
    rebalance_bars: int = 21
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_avg_dollar_volume: float | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    weighting_scheme: str = "equal"
    vol_lookback_bars: int = 20
    benchmark: str = "equal_weight"
    cost_bps: float | None = None
    enable_conditional_evaluation: bool = False
    conditional_condition_types: list[str] = field(default_factory=list)
    conditional_min_sample_size: int = 20
    conditional_compare_to_baseline: bool = True
    conditional_allow_variants: bool = False
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None

    def __post_init__(self) -> None:
        _validate_symbol_selection(
            symbols=self.symbols,
            universe=self.universe,
            preset=self.preset,
        )

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WalkForwardWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    preset: str | None = None
    strategy: str = "sma_cross"
    engine: str = "vectorized"
    start: str | None = None
    end: str | None = None
    output: str = "artifacts/walkforward/walkforward_results.csv"
    select_by: str = "Sharpe Ratio"
    min_train_rows: int = 126
    min_test_rows: int = 21
    train_bars: int | None = None
    test_bars: int | None = None
    step_bars: int | None = None
    train_years: int = 3
    test_years: int = 1
    fast_values: list[int] = field(default_factory=list)
    slow_values: list[int] = field(default_factory=list)
    lookback_values: list[int] = field(default_factory=list)
    entry_lookback_values: list[int] = field(default_factory=list)
    exit_lookback_values: list[int] = field(default_factory=list)
    momentum_lookback_values: list[int] = field(default_factory=list)
    lookback_bars_values: list[int] = field(default_factory=list)
    skip_bars_values: list[int] = field(default_factory=list)
    top_n_values: list[int] = field(default_factory=list)
    rebalance_bars_values: list[int] = field(default_factory=list)
    cash: float = 10_000.0
    commission: float = 0.001
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_avg_dollar_volume: float | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    weighting_scheme: str = "equal"
    vol_lookback_bars: int = 20
    benchmark: str = "equal_weight"
    cost_bps: float | None = None

    def __post_init__(self) -> None:
        _validate_symbol_selection(
            symbols=self.symbols,
            universe=self.universe,
            preset=self.preset,
        )

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperRunWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    preset: str | None = None
    strategy: str = "sma_cross"
    signal_source: str = "legacy"
    state_path: str = "artifacts/paper/paper_state.json"
    output_dir: str = "artifacts/paper"
    execution_config: str | None = None
    initial_cash: float = 100_000.0
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0
    top_n: int = 10
    weighting_scheme: str = "equal"
    vol_lookback_bars: int = 20
    lookback_bars: int = 126
    skip_bars: int = 0
    rebalance_bars: int = 21
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_avg_dollar_volume: float | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    benchmark: str | None = None
    rebalance_frequency: str = "daily"
    timing: str = "next_bar"
    approved_model_state: str | None = None
    composite_artifact_dir: str | None = None
    composite_horizon: int = 1
    composite_weighting_scheme: str = "equal"
    composite_portfolio_mode: str = "long_only_top_n"
    composite_long_quantile: float = 0.2
    composite_short_quantile: float = 0.2
    min_price: float | None = None
    min_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    auto_apply_fills: bool = False
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
    reference_data_root: str | None = None
    universe_membership_path: str | None = None
    taxonomy_snapshot_path: str | None = None
    benchmark_mapping_path: str | None = None
    market_regime_path: str | None = None
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None
    data_sources: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_symbol_selection(
            symbols=self.symbols,
            universe=self.universe,
            preset=self.preset,
        )

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LiveDryRunWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    preset: str | None = None
    strategy: str = "sma_cross"
    output_dir: str = "artifacts/live_dry_run"
    execution_config: str | None = None
    broker: str = "mock"
    order_type: str = "market"
    time_in_force: str = "day"
    initial_cash: float = 100_000.0
    mock_equity: float = 100_000.0
    mock_cash: float = 100_000.0
    mock_positions_path: str | None = None
    min_trade_dollars: float = 25.0
    lot_size: int = 1
    reserve_cash_pct: float = 0.0
    top_n: int = 1
    weighting_scheme: str = "equal"
    vol_lookback_bars: int = 20
    lookback_bars: int = 126
    skip_bars: int = 0
    rebalance_bars: int = 21
    portfolio_construction_mode: str = "pure_topn"
    max_position_weight: float | None = None
    min_avg_dollar_volume: float | None = None
    max_names_per_sector: int | None = None
    turnover_buffer_bps: float = 0.0
    max_turnover_per_rebalance: float | None = None
    benchmark: str | None = None
    sub_universe_id: str | None = None
    universe_filters: list[dict[str, Any]] = field(default_factory=list)
    reference_data_root: str | None = None
    universe_membership_path: str | None = None
    taxonomy_snapshot_path: str | None = None
    benchmark_mapping_path: str | None = None
    market_regime_path: str | None = None
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None

    def __post_init__(self) -> None:
        _validate_symbol_selection(
            symbols=self.symbols,
            universe=self.universe,
            preset=self.preset,
        )

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchInputRefreshWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    feature_groups: list[str] | None = None
    sub_universe_id: str | None = None
    feature_dir: str = "data/features"
    metadata_dir: str = "data/metadata"
    normalized_dir: str = "data/normalized"
    reference_data_root: str | None = None
    universe_membership_path: str | None = None
    taxonomy_snapshot_path: str | None = None
    benchmark_mapping_path: str | None = None
    market_regime_path: str | None = None
    group_map_path: str | None = None
    benchmark: str | None = None
    failure_policy: str = "partial_success"

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")
        if self.failure_policy not in {"partial_success", "fail"}:
            raise ValueError("failure_policy must be one of: partial_success, fail")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)
