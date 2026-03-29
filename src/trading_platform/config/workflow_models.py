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
    enable_cost_model: bool = False
    commission_bps: float = 0.0
    minimum_commission: float = 0.0
    spread_bps: float = 0.0
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
    fundamentals_enabled: bool = False
    fundamentals_artifact_root: str | None = None
    fundamentals_providers: list[str] | None = None
    fundamentals_sec_companyfacts_root: str | None = None
    fundamentals_sec_submissions_root: str | None = None
    fundamentals_vendor_file_path: str | None = None
    fundamentals_vendor_api_key: str | None = None
    fundamentals_vendor_cache_enabled: bool = True
    fundamentals_vendor_cache_root: str | None = None
    fundamentals_vendor_cache_ttl_hours: float = 24.0
    fundamentals_vendor_force_refresh: bool = False
    fundamentals_vendor_request_delay_seconds: float = 0.5
    fundamentals_vendor_max_retries: int = 4
    fundamentals_vendor_max_symbols_per_run: int | None = None
    fundamentals_vendor_max_requests_per_run: int | None = None

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")
        if self.failure_policy not in {"partial_success", "fail"}:
            raise ValueError("failure_policy must be one of: partial_success, fail")
        if self.fundamentals_vendor_cache_ttl_hours < 0:
            raise ValueError("fundamentals_vendor_cache_ttl_hours must be >= 0")
        if self.fundamentals_vendor_request_delay_seconds < 0:
            raise ValueError("fundamentals_vendor_request_delay_seconds must be >= 0")
        if self.fundamentals_vendor_max_retries < 0:
            raise ValueError("fundamentals_vendor_max_retries must be >= 0")
        if (
            self.fundamentals_vendor_max_symbols_per_run is not None
            and self.fundamentals_vendor_max_symbols_per_run <= 0
        ):
            raise ValueError("fundamentals_vendor_max_symbols_per_run must be > 0 when provided")
        if (
            self.fundamentals_vendor_max_requests_per_run is not None
            and self.fundamentals_vendor_max_requests_per_run <= 0
        ):
            raise ValueError("fundamentals_vendor_max_requests_per_run must be > 0 when provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FundamentalsSnapshotWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    artifact_root: str = "data/fundamentals"
    raw_sec_cache_root: str | None = None
    symbol_cik_map_path: str | None = None
    sec_user_agent: str | None = None
    sec_request_delay_seconds: float = 0.2
    sec_max_retries: int = 4
    cache_enabled: bool = True
    cache_ttl_days: float = 30.0
    force_refresh: bool = False
    max_symbols_per_run: int | None = None
    max_requests_per_run: int | None = None
    build_daily_features: bool = True
    calendar_dir: str | None = "data/features"
    offline: bool = False

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")
        if self.sec_request_delay_seconds < 0:
            raise ValueError("sec_request_delay_seconds must be >= 0")
        if self.sec_max_retries < 0:
            raise ValueError("sec_max_retries must be >= 0")
        if self.cache_ttl_days < 0:
            raise ValueError("cache_ttl_days must be >= 0")
        if self.max_symbols_per_run is not None and self.max_symbols_per_run <= 0:
            raise ValueError("max_symbols_per_run must be > 0 when provided")
        if self.max_requests_per_run is not None and self.max_requests_per_run <= 0:
            raise ValueError("max_requests_per_run must be > 0 when provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClassificationBuildWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    output_dir: str = "artifacts/reference/classifications"
    as_of_date: str | None = None

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioOptimizerWorkflowConfig:
    returns_path: str
    output_dir: str
    optimizer_name: str = "equal_weight"
    fallback_optimizer_name: str = "equal_weight"
    risk_free_rate: float = 0.0
    min_history_rows: int = 20

    def __post_init__(self) -> None:
        if not str(self.returns_path or "").strip():
            raise ValueError("returns_path is required")
        if not str(self.output_dir or "").strip():
            raise ValueError("output_dir is required")
        if self.min_history_rows <= 1:
            raise ValueError("min_history_rows must be > 1")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BacktesterValidationWorkflowConfig:
    output_dir: str = "artifacts/validation/vectorbt"

    def __post_init__(self) -> None:
        if not str(self.output_dir or "").strip():
            raise ValueError("output_dir is required")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlphaResearchWorkflowConfig:
    symbols: list[str] | None = None
    universe: str | None = None
    feature_dir: str = "data/features"
    signal_family: str | None = "momentum"
    signal_families: list[str] | None = None
    candidate_grid_preset: str = "standard"
    signal_composition_preset: str = "standard"
    max_variants_per_family: int | None = None
    lookbacks: list[int] = field(default_factory=lambda: [5, 10, 20, 60])
    horizons: list[int] = field(default_factory=lambda: [1, 5, 20])
    min_rows: int = 126
    equity_context_enabled: bool = False
    equity_context_include_volume: bool = False
    fundamentals_enabled: bool = False
    fundamentals_daily_features_path: str | None = None
    enable_context_confirmations: bool | None = None
    enable_relative_features: bool | None = None
    enable_flow_confirmations: bool | None = None
    enable_ensemble: bool = False
    ensemble_mode: str = "disabled"
    ensemble_weight_method: str = "equal"
    ensemble_normalize_scores: str = "rank_pct"
    ensemble_max_members: int = 5
    ensemble_max_members_per_family: int | None = None
    ensemble_minimum_member_observations: int = 0
    ensemble_minimum_member_metric: float | None = None
    require_runtime_computability_for_approval: bool = False
    min_runtime_computable_symbols_for_approval: int = 5
    allow_research_only_noncomputable_candidates: bool = True
    runtime_computability_penalty_on_ranking: float = 0.02
    runtime_computability_check_mode: str = "strict"
    require_composite_runtime_computability_for_approval: bool = False
    min_composite_runtime_computable_symbols_for_approval: int = 5
    allow_research_only_noncomputable_composites: bool = True
    composite_runtime_computability_check_mode: str = "strict"
    composite_runtime_computability_penalty_on_ranking: float = 0.02
    fast_refresh_mode: bool = False
    skip_heavy_diagnostics: bool = True
    reuse_existing_fold_results: bool = True
    restrict_to_existing_candidates: bool = True
    max_families_for_refresh: int | None = None
    max_candidates_for_refresh: int | None = None
    diagnostics_alphalens_enabled: bool = False
    diagnostics_alphalens_groupby_field: str | None = None
    diagnostics_classification_path: str | None = None
    diagnostics_output_dir: str | None = None
    reporting_quantstats_enabled: bool = False
    reporting_quantstats_output_dir: str | None = None
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
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None
    tracking_write_candidates: bool = True
    tracking_write_metrics: bool = True
    tracking_write_promotions: bool = True

    def __post_init__(self) -> None:
        selected = sum(bool(value) for value in (self.symbols, self.universe))
        if selected != 1:
            raise ValueError("exactly one of symbols or universe must be provided")
        normalized_families = list(
            dict.fromkeys(
                str(value).strip()
                for value in (self.signal_families or ([self.signal_family] if self.signal_family else []))
                if str(value).strip()
            )
        )
        if not normalized_families:
            raise ValueError("at least one signal family must be provided")
        object.__setattr__(self, "signal_families", normalized_families)
        object.__setattr__(self, "signal_family", normalized_families[0])
        if self.candidate_grid_preset not in {"standard", "broad_v1"}:
            raise ValueError("candidate_grid_preset must be one of: standard, broad_v1")
        if self.signal_composition_preset not in {"standard", "composite_v1", "research_rich_v1"}:
            raise ValueError("signal_composition_preset must be one of: standard, composite_v1, research_rich_v1")
        if self.max_variants_per_family is not None and self.max_variants_per_family <= 0:
            raise ValueError("max_variants_per_family must be > 0 when provided")
        if self.min_runtime_computable_symbols_for_approval < 0:
            raise ValueError("min_runtime_computable_symbols_for_approval must be >= 0")
        if self.runtime_computability_penalty_on_ranking < 0:
            raise ValueError("runtime_computability_penalty_on_ranking must be >= 0")
        if self.runtime_computability_check_mode not in {"strict", "penalize", "diagnostic_only"}:
            raise ValueError("runtime_computability_check_mode must be one of: strict, penalize, diagnostic_only")
        if self.min_composite_runtime_computable_symbols_for_approval < 0:
            raise ValueError("min_composite_runtime_computable_symbols_for_approval must be >= 0")
        if self.composite_runtime_computability_penalty_on_ranking < 0:
            raise ValueError("composite_runtime_computability_penalty_on_ranking must be >= 0")
        if self.composite_runtime_computability_check_mode not in {"strict", "penalize", "diagnostic_only"}:
            raise ValueError(
                "composite_runtime_computability_check_mode must be one of: strict, penalize, diagnostic_only"
            )
        if self.max_families_for_refresh is not None and self.max_families_for_refresh <= 0:
            raise ValueError("max_families_for_refresh must be > 0 when provided")
        if self.max_candidates_for_refresh is not None and self.max_candidates_for_refresh <= 0:
            raise ValueError("max_candidates_for_refresh must be > 0 when provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlphaCycleStageToggles:
    refresh: bool = True
    research: bool = True
    promotion: bool = True
    portfolio: bool = True
    export_bundle: bool = True
    report: bool = True

    def enabled_stage_names(self) -> list[str]:
        return [
            stage_name
            for stage_name in ("refresh", "research", "promotion", "portfolio", "export_bundle", "report")
            if bool(getattr(self, stage_name))
        ]


@dataclass(frozen=True)
class AlphaCycleWorkflowConfig:
    refresh_config: str | None = None
    research_config: str | None = None
    promotion_policy_config: str | None = None
    strategy_portfolio_policy_config: str | None = None
    output_root: str = "artifacts/alpha_cycle"
    research_output_dir: str | None = None
    registry_dir: str | None = None
    promoted_dir: str | None = None
    portfolio_dir: str | None = None
    export_dir: str | None = None
    run_name: str = "alpha_cycle"
    run_id: str | None = None
    strict_mode: bool = True
    best_effort_mode: bool = False
    validation_path: str | None = None
    promotion_top_n: int | None = None
    allow_overwrite: bool = False
    inactive: bool = False
    override_validation: bool = False
    lifecycle_path: str | None = None
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None
    tracking_write_candidates: bool = True
    tracking_write_metrics: bool = True
    tracking_write_promotions: bool = True
    stages: AlphaCycleStageToggles = field(default_factory=AlphaCycleStageToggles)

    def __post_init__(self) -> None:
        if not str(self.run_name or "").strip():
            raise ValueError("run_name must be a non-empty string")
        if self.strict_mode and self.best_effort_mode:
            raise ValueError("strict_mode and best_effort_mode cannot both be true")
        if self.promotion_top_n is not None and self.promotion_top_n <= 0:
            raise ValueError("promotion_top_n must be > 0 when provided")
        if self.stages.refresh and not self.refresh_config:
            raise ValueError("refresh_config is required when refresh stage is enabled")
        if self.stages.research and not self.research_config:
            raise ValueError("research_config is required when research stage is enabled")
        if self.stages.promotion and not self.promotion_policy_config:
            raise ValueError("promotion_policy_config is required when promotion stage is enabled")
        if self.stages.portfolio and not self.strategy_portfolio_policy_config:
            raise ValueError("strategy_portfolio_policy_config is required when portfolio stage is enabled")

    def to_cli_defaults(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = asdict(self.stages)
        return payload


@dataclass(frozen=True)
class DailyTradingStageToggles:
    refresh_inputs: bool = False
    research: bool = False
    promote: bool = True
    build_portfolio: bool = True
    activate_portfolio: bool = True
    export_bundle: bool = True
    paper_run: bool = True
    report: bool = True

    def enabled_stage_names(self) -> list[str]:
        return [
            stage_name
            for stage_name in (
                "refresh_inputs",
                "research",
                "promote",
                "build_portfolio",
                "activate_portfolio",
                "export_bundle",
                "paper_run",
                "report",
            )
            if bool(getattr(self, stage_name))
        ]


@dataclass(frozen=True)
class DailyTradingWorkflowConfig:
    refresh_config: str | None = None
    research_config: str | None = None
    promotion_policy_config: str | None = None
    strategy_portfolio_policy_config: str | None = None
    execution_config: str | None = None
    output_root: str = "artifacts/daily_trading"
    research_output_dir: str = "artifacts/alpha_research/run_configured"
    registry_dir: str = "artifacts/alpha_research/run_configured/research_registry"
    promoted_dir: str = "artifacts/promoted/run_current"
    portfolio_dir: str = "artifacts/strategy_portfolio/run_current"
    activated_dir: str = "artifacts/strategy_portfolio/run_current/activated"
    export_dir: str = "artifacts/strategy_portfolio/run_current/run_bundle_activated"
    paper_output_dir: str = "artifacts/daily_trading/run_current/paper"
    paper_state_path: str = "artifacts/daily_trading/run_current/paper_state.json"
    strategy_weighting_metrics_path: str | None = None
    report_dir: str | None = None
    dashboard_output_dir: str | None = None
    run_name: str = "daily_trading"
    run_id: str | None = None
    research_mode: str = "skip"
    strict_mode: bool = True
    best_effort_mode: bool = False
    promotion_top_n: int | None = None
    allow_overwrite: bool = True
    inactive: bool = False
    override_validation: bool = False
    lifecycle_path: str | None = None
    use_activated_portfolio_for_paper: bool = True
    fail_if_no_active_strategies: bool = False
    include_inactive_conditionals_in_reports: bool = True
    auto_apply_fills: bool = True
    fail_if_zero_targets_after_validation: bool = False
    enable_cost_model: bool = False
    slippage_model: str = "none"
    slippage_buy_bps: float = 0.0
    slippage_sell_bps: float = 0.0
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
    ev_gate_reliability_min_training_samples: int = 20
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
    enable_strategy_diagnostics: bool = True
    refresh_dashboard_static_data: bool = False
    evaluate_conditional_activation: bool | None = None
    activation_context_sources: list[str] | None = None
    include_inactive_conditionals_in_output: bool | None = None
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None
    tracking_write_candidates: bool = True
    tracking_write_metrics: bool = True
    tracking_write_promotions: bool = True
    stages: DailyTradingStageToggles = field(default_factory=DailyTradingStageToggles)

    def __post_init__(self) -> None:
        if not str(self.run_name or "").strip():
            raise ValueError("run_name must be a non-empty string")
        if self.research_mode not in {"full", "fast_refresh", "skip"}:
            raise ValueError("research_mode must be one of: full, fast_refresh, skip")
        if self.strict_mode and self.best_effort_mode:
            raise ValueError("strict_mode and best_effort_mode cannot both be true")
        if self.promotion_top_n is not None and self.promotion_top_n <= 0:
            raise ValueError("promotion_top_n must be > 0 when provided")
        if self.stages.refresh_inputs and not self.refresh_config:
            raise ValueError("refresh_config is required when refresh_inputs stage is enabled")
        if self.stages.research and self.research_mode in {"full", "fast_refresh"} and not self.research_config:
            raise ValueError(
                "research_config is required when research stage is enabled with full or fast_refresh mode"
            )
        if self.stages.promote and not self.promotion_policy_config:
            raise ValueError("promotion_policy_config is required when promote stage is enabled")
        if self.stages.build_portfolio and not self.strategy_portfolio_policy_config:
            raise ValueError("strategy_portfolio_policy_config is required when build_portfolio stage is enabled")

    def to_cli_defaults(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = asdict(self.stages)
        return payload


@dataclass(frozen=True)
class DailyReplayTuningConfig:
    relax_thresholds_for_testing: bool = False
    min_expected_trade_days: int | None = None
    min_expected_total_trades: int | None = None
    warn_if_all_days_no_op: bool = True
    warn_if_turnover_too_low: float | None = None
    override_max_weight_per_strategy: float | None = None
    override_min_signal_strength: float | None = None
    profile_timings: bool = False

    def __post_init__(self) -> None:
        if self.min_expected_trade_days is not None and self.min_expected_trade_days < 0:
            raise ValueError("min_expected_trade_days must be >= 0 when provided")
        if self.min_expected_total_trades is not None and self.min_expected_total_trades < 0:
            raise ValueError("min_expected_total_trades must be >= 0 when provided")
        if self.warn_if_turnover_too_low is not None and self.warn_if_turnover_too_low < 0:
            raise ValueError("warn_if_turnover_too_low must be >= 0 when provided")
        if self.override_max_weight_per_strategy is not None and self.override_max_weight_per_strategy < 0:
            raise ValueError("override_max_weight_per_strategy must be >= 0 when provided")


@dataclass(frozen=True)
class DailyReplayWorkflowConfig:
    daily_trading: DailyTradingWorkflowConfig
    output_dir: str = "artifacts/daily_replay/run_current"
    start_date: str | None = None
    end_date: str | None = None
    dates_file: str | None = None
    initial_state_path: str | None = None
    stop_on_error: bool = True
    continue_on_error: bool = False
    max_days: int | None = None
    replay: DailyReplayTuningConfig = field(default_factory=DailyReplayTuningConfig)

    def __post_init__(self) -> None:
        if not str(self.output_dir or "").strip():
            raise ValueError("output_dir is required")
        if self.stop_on_error and self.continue_on_error:
            raise ValueError("stop_on_error and continue_on_error cannot both be true")
        if self.max_days is not None and self.max_days < 0:
            raise ValueError("max_days must be >= 0 when provided")

    def to_cli_defaults(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["daily_trading"] = self.daily_trading.to_cli_defaults()
        payload["replay"] = asdict(self.replay)
        return payload


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
