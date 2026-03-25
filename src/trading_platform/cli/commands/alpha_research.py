from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.cli.common import resolve_symbols
from trading_platform.config.loader import load_alpha_research_workflow_config
from trading_platform.research.experiment_tracking import (
    build_alpha_experiment_record,
    register_experiment,
)
from trading_platform.research.alpha_lab.runner import run_alpha_research


@dataclass(frozen=True)
class AlphaResearchRequest:
    symbols: list[str]
    feature_dir: Path
    output_dir: Path
    signal_family: str
    signal_families: list[str]
    candidate_grid_preset: str
    signal_composition_preset: str
    max_variants_per_family: int | None
    lookbacks: list[int]
    horizons: list[int]
    min_rows: int
    top_quantile: float
    bottom_quantile: float
    train_size: int
    test_size: int
    step_size: int | None
    min_train_size: int | None
    portfolio_top_n: int
    portfolio_long_quantile: float
    portfolio_short_quantile: float
    commission: float
    min_price: float | None
    min_volume: float | None
    min_avg_dollar_volume: float | None
    max_adv_participation: float
    max_position_pct_of_adv: float
    max_notional_per_name: float | None
    slippage_bps_per_turnover: float
    slippage_bps_per_adv: float
    dynamic_recent_quality_window: int
    dynamic_min_history: int
    dynamic_downweight_mean_rank_ic: float
    dynamic_deactivate_mean_rank_ic: float
    regime_aware_enabled: bool
    regime_min_history: int
    regime_underweight_mean_rank_ic: float
    regime_exclude_mean_rank_ic: float
    equity_context_enabled: bool
    equity_context_include_volume: bool
    fundamentals_enabled: bool
    fundamentals_daily_features_path: Path | None
    enable_context_confirmations: bool | None
    enable_relative_features: bool | None
    enable_flow_confirmations: bool | None
    ensemble_enabled: bool
    ensemble_mode: str
    ensemble_weight_method: str
    ensemble_normalize_scores: str
    ensemble_max_members: int
    ensemble_max_members_per_family: int | None
    ensemble_minimum_member_observations: int
    ensemble_minimum_member_metric: float | None
    experiment_tracker_dir: Path


def _build_alpha_research_request(args) -> AlphaResearchRequest:
    output_dir = Path(args.output_dir)
    tracker_dir_arg = getattr(args, "experiment_tracker_dir", None)
    tracker_dir = Path(tracker_dir_arg) if tracker_dir_arg else output_dir.parent / "experiment_tracking"
    fundamentals_enabled = bool(getattr(args, "fundamentals_enabled", False))
    fundamentals_daily_features_arg = getattr(args, "fundamentals_daily_features_path", None)
    return AlphaResearchRequest(
        symbols=resolve_symbols(args),
        feature_dir=Path(args.feature_dir),
        output_dir=output_dir,
        signal_family=args.signal_family,
        signal_families=list(getattr(args, "signal_families", None) or [args.signal_family]),
        candidate_grid_preset=getattr(args, "candidate_grid_preset", "standard"),
        signal_composition_preset=getattr(args, "signal_composition_preset", "standard"),
        max_variants_per_family=getattr(args, "max_variants_per_family", None),
        lookbacks=list(args.lookbacks),
        horizons=list(args.horizons),
        min_rows=args.min_rows,
        top_quantile=args.top_quantile,
        bottom_quantile=args.bottom_quantile,
        train_size=args.train_size,
        test_size=args.test_size,
        step_size=args.step_size,
        min_train_size=args.min_train_size,
        portfolio_top_n=args.portfolio_top_n,
        portfolio_long_quantile=args.portfolio_long_quantile,
        portfolio_short_quantile=args.portfolio_short_quantile,
        commission=args.commission,
        min_price=args.min_price,
        min_volume=args.min_volume,
        min_avg_dollar_volume=args.min_avg_dollar_volume,
        max_adv_participation=args.max_adv_participation,
        max_position_pct_of_adv=args.max_position_pct_of_adv,
        max_notional_per_name=args.max_notional_per_name,
        slippage_bps_per_turnover=args.slippage_bps_per_turnover,
        slippage_bps_per_adv=args.slippage_bps_per_adv,
        dynamic_recent_quality_window=args.dynamic_recent_quality_window,
        dynamic_min_history=args.dynamic_min_history,
        dynamic_downweight_mean_rank_ic=args.dynamic_downweight_mean_rank_ic,
        dynamic_deactivate_mean_rank_ic=args.dynamic_deactivate_mean_rank_ic,
        regime_aware_enabled=args.regime_aware_enabled,
        regime_min_history=args.regime_min_history,
        regime_underweight_mean_rank_ic=args.regime_underweight_mean_rank_ic,
        regime_exclude_mean_rank_ic=args.regime_exclude_mean_rank_ic,
        equity_context_enabled=getattr(args, "equity_context_enabled", False),
        equity_context_include_volume=getattr(args, "equity_context_include_volume", False),
        fundamentals_enabled=fundamentals_enabled,
        fundamentals_daily_features_path=(
            Path(fundamentals_daily_features_arg)
            if fundamentals_daily_features_arg
            else (Path("data/fundamentals/daily_fundamental_features.parquet") if fundamentals_enabled else None)
        ),
        enable_context_confirmations=getattr(args, "enable_context_confirmations", None),
        enable_relative_features=getattr(args, "enable_relative_features", None),
        enable_flow_confirmations=getattr(args, "enable_flow_confirmations", None),
        ensemble_enabled=getattr(args, "enable_ensemble", False),
        ensemble_mode=getattr(args, "ensemble_mode", "disabled"),
        ensemble_weight_method=getattr(args, "ensemble_weight_method", "equal"),
        ensemble_normalize_scores=getattr(args, "ensemble_normalize_scores", "rank_pct"),
        ensemble_max_members=getattr(args, "ensemble_max_members", 5),
        ensemble_max_members_per_family=getattr(args, "ensemble_max_members_per_family", None),
        ensemble_minimum_member_observations=getattr(args, "ensemble_minimum_member_observations", 0),
        ensemble_minimum_member_metric=getattr(args, "ensemble_minimum_member_metric", None),
        experiment_tracker_dir=tracker_dir,
    )


def cmd_alpha_research(args) -> None:
    load_and_apply_workflow_config(
        args,
        loader=load_alpha_research_workflow_config,
    )
    request = _build_alpha_research_request(args)
    result = run_alpha_research(
        symbols=request.symbols,
        universe=None,
        feature_dir=request.feature_dir,
        signal_family=request.signal_family,
        signal_families=request.signal_families,
        lookbacks=request.lookbacks,
        horizons=request.horizons,
        min_rows=request.min_rows,
        top_quantile=request.top_quantile,
        bottom_quantile=request.bottom_quantile,
        candidate_grid_preset=request.candidate_grid_preset,
        signal_composition_preset=request.signal_composition_preset,
        max_variants_per_family=request.max_variants_per_family,
        output_dir=request.output_dir,
        train_size=request.train_size,
        test_size=request.test_size,
        step_size=request.step_size,
        min_train_size=request.min_train_size,
        portfolio_top_n=request.portfolio_top_n,
        portfolio_long_quantile=request.portfolio_long_quantile,
        portfolio_short_quantile=request.portfolio_short_quantile,
        commission=request.commission,
        min_price=request.min_price,
        min_volume=request.min_volume,
        min_avg_dollar_volume=request.min_avg_dollar_volume,
        max_adv_participation=request.max_adv_participation,
        max_position_pct_of_adv=request.max_position_pct_of_adv,
        max_notional_per_name=request.max_notional_per_name,
        slippage_bps_per_turnover=request.slippage_bps_per_turnover,
        slippage_bps_per_adv=request.slippage_bps_per_adv,
        dynamic_recent_quality_window=request.dynamic_recent_quality_window,
        dynamic_min_history=request.dynamic_min_history,
        dynamic_downweight_mean_rank_ic=request.dynamic_downweight_mean_rank_ic,
        dynamic_deactivate_mean_rank_ic=request.dynamic_deactivate_mean_rank_ic,
        regime_aware_enabled=request.regime_aware_enabled,
        regime_min_history=request.regime_min_history,
        regime_underweight_mean_rank_ic=request.regime_underweight_mean_rank_ic,
        regime_exclude_mean_rank_ic=request.regime_exclude_mean_rank_ic,
        equity_context_enabled=request.equity_context_enabled,
        equity_context_include_volume=request.equity_context_include_volume,
        fundamentals_enabled=request.fundamentals_enabled,
        fundamentals_daily_features_path=request.fundamentals_daily_features_path,
        enable_context_confirmations=request.enable_context_confirmations,
        enable_relative_features=request.enable_relative_features,
        enable_flow_confirmations=request.enable_flow_confirmations,
        ensemble_enabled=request.ensemble_enabled,
        ensemble_mode=request.ensemble_mode,
        ensemble_weight_method=request.ensemble_weight_method,
        ensemble_normalize_scores=request.ensemble_normalize_scores,
        ensemble_max_members=request.ensemble_max_members,
        ensemble_require_promoted_only=True,
        ensemble_max_members_per_family=request.ensemble_max_members_per_family,
        ensemble_minimum_member_observations=request.ensemble_minimum_member_observations,
        ensemble_minimum_member_metric=request.ensemble_minimum_member_metric,
    )
    registry_paths = register_experiment(
        build_alpha_experiment_record(request.output_dir),
        tracker_dir=request.experiment_tracker_dir,
    )

    print("Alpha research complete.")
    print(f"Leaderboard: {result['leaderboard_path']}")
    print(f"Detailed results: {result['fold_results_path']}")
    print(f"Composite portfolio returns: {result['portfolio_returns_path']}")
    print(f"Ensemble member summary: {result['ensemble_member_summary_path']}")
    print(f"Implementability report: {result['implementability_report_path']}")
    print(f"Sub-universe slicing: {result['signal_performance_by_sub_universe_path']}")
    print(f"Benchmark-context slicing: {result['signal_performance_by_benchmark_context_path']}")
    print(f"Research manifest: {result['research_manifest_path']}")
    print(f"Experiment registry: {registry_paths['experiment_registry_path']}")
