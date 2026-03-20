from __future__ import annotations

from pathlib import Path

from trading_platform.research.multi_universe import (
    MultiUniverseResearchConfig,
    run_multi_universe_alpha_research,
)


def cmd_multi_universe_alpha_research(args) -> None:
    result = run_multi_universe_alpha_research(
        config=MultiUniverseResearchConfig(
            universes=tuple(args.universes),
            feature_dir=Path(args.feature_dir),
            output_dir=Path(args.output_dir),
            signal_family=args.signal_family,
            lookbacks=tuple(args.lookbacks),
            horizons=tuple(args.horizons),
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
            experiment_tracker_dir=(
                Path(args.experiment_tracker_dir)
                if args.experiment_tracker_dir
                else None
            ),
        )
    )
    print("Multi-universe alpha research complete.")
    for key, value in sorted(result.items()):
        print(f"{key}: {value}")
