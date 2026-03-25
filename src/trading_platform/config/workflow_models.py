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


@dataclass(frozen=True)
class AlphaResearchWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    feature_dir: str = "data/features"
    signal_family: str = "momentum"
    candidate_grid_preset: str = "standard"
    max_variants_per_family: int | None = None
    lookbacks: list[int] = field(default_factory=lambda: [5, 10, 20, 60])
    horizons: list[int] = field(default_factory=lambda: [1, 5, 20])
    min_rows: int = 126
    equity_context_enabled: bool = False
    equity_context_include_volume: bool = False
    enable_ensemble: bool = False
    ensemble_mode: str = "disabled"
    ensemble_weight_method: str = "equal"
    ensemble_normalize_scores: str = "rank_pct"
    ensemble_max_members: int = 5
    ensemble_max_members_per_family: int | None = None
    ensemble_minimum_member_observations: int = 0
    ensemble_minimum_member_metric: float | None = None
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    output_dir: str = "artifacts/alpha_research"
    train_size: int = 756
    test_size: int = 63
    step_size: int | None = None
    min_train_size: int | None = None
    portfolio_top_n: int = 10
    portfolio_long_quantile: float = 0.2
    portfolio_short_quantile: float = 0.2
    commission: float = 0.0
    min_price: float | None = None
    min_volume: float | None = None
    min_avg_dollar_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    slippage_bps_per_turnover: float = 0.0
    slippage_bps_per_adv: float = 10.0
    dynamic_recent_quality_window: int = 20
    dynamic_min_history: int = 5
    dynamic_downweight_mean_rank_ic: float = 0.01
    dynamic_deactivate_mean_rank_ic: float = -0.02
    regime_aware_enabled: bool = False
    regime_min_history: int = 5
    regime_underweight_mean_rank_ic: float = 0.01
    regime_exclude_mean_rank_ic: float = -0.01
    experiment_tracker_dir: str | None = None

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")
        if self.candidate_grid_preset not in {"standard", "broad_v1"}:
            raise ValueError("candidate_grid_preset must be one of: standard, broad_v1")
        if self.max_variants_per_family is not None and self.max_variants_per_family <= 0:
            raise ValueError("max_variants_per_family must be > 0 when provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CanonicalBundleExperimentVariantConfig:
    name: str
    promotion_policy_overrides: dict[str, Any] = field(default_factory=dict)
    strategy_portfolio_policy_overrides: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.name or "").strip():
            raise ValueError("variant name is required")


@dataclass(frozen=True)
class CanonicalBundleExperimentWorkflowConfig:
    bundle_dir: str
    promoted_dir: str
    output_dir: str
    artifacts_root: str | None = None
    lifecycle: str | None = None
    base_promotion_policy_config: str | None = None
    base_strategy_portfolio_policy_config: str | None = None
    baseline_variant_name: str = "baseline"
    preset_set: str | None = None
    variants: list[CanonicalBundleExperimentVariantConfig] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not str(self.bundle_dir or "").strip():
            raise ValueError("bundle_dir is required")
        if not str(self.promoted_dir or "").strip():
            raise ValueError("promoted_dir is required")
        if not str(self.output_dir or "").strip():
            raise ValueError("output_dir is required")
        variant_names = [variant.name for variant in self.variants]
        if not variant_names and not self.preset_set:
            raise ValueError("at least one experiment variant or preset_set is required")
        if not variant_names:
            if self.preset_set and self.baseline_variant_name != "baseline":
                raise ValueError("baseline_variant_name must be `baseline` when using preset_set variants")
            return
        if len(set(variant_names)) != len(variant_names):
            raise ValueError("experiment variant names must be unique")
        if self.baseline_variant_name not in set(variant_names):
            raise ValueError("baseline_variant_name must match one of the configured variants")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CanonicalBundleExperimentMatrixCaseConfig:
    case_id: str
    bundle_dir: str
    promoted_dir: str
    label: str | None = None
    artifacts_root: str | None = None
    lifecycle: str | None = None

    def __post_init__(self) -> None:
        normalized_case_id = str(self.case_id or "").strip()
        if not normalized_case_id:
            raise ValueError("case_id is required")
        object.__setattr__(self, "case_id", normalized_case_id)
        if self.label is not None:
            object.__setattr__(self, "label", str(self.label))
        if not str(self.bundle_dir or "").strip():
            raise ValueError("bundle_dir is required")
        if not str(self.promoted_dir or "").strip():
            raise ValueError("promoted_dir is required")


@dataclass(frozen=True)
class CanonicalBundleExperimentMatrixWorkflowConfig:
    experiment_name: str
    output_dir: str
    baseline_variant_name: str = "baseline"
    preset_set: str = "policy_sensitivity_v1"
    base_promotion_policy_config: str | None = None
    base_strategy_portfolio_policy_config: str | None = None
    cases: list[CanonicalBundleExperimentMatrixCaseConfig] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not str(self.experiment_name or "").strip():
            raise ValueError("experiment_name is required")
        if not str(self.output_dir or "").strip():
            raise ValueError("output_dir is required")
        if self.baseline_variant_name != "baseline":
            raise ValueError("baseline_variant_name must be `baseline` for the canonical policy stability matrix")
        if self.preset_set != "policy_sensitivity_v1":
            raise ValueError("preset_set must be `policy_sensitivity_v1`")
        if not self.cases:
            raise ValueError("at least one matrix case is required")
        case_ids = [case.case_id for case in self.cases]
        if len(set(case_ids)) != len(case_ids):
            raise ValueError("matrix case_ids must be unique")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)
