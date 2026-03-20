from __future__ import annotations

from pathlib import Path

from trading_platform.research.alpha_lab.runner import run_alpha_research


def cmd_alpha_research(args) -> None:
    result = run_alpha_research(
        symbols=args.symbols,
        universe=args.universe,
        feature_dir=Path(args.feature_dir),
        signal_family=args.signal_family,
        lookbacks=args.lookbacks,
        horizons=args.horizons,
        min_rows=args.min_rows,
        top_quantile=args.top_quantile,
        bottom_quantile=args.bottom_quantile,
        output_dir=Path(args.output_dir),
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
    )

    print("Alpha research complete.")
    print(f"Leaderboard: {result['leaderboard_path']}")
    print(f"Detailed results: {result['fold_results_path']}")
    print(f"Composite portfolio returns: {result['portfolio_returns_path']}")
    print(f"Implementability report: {result['implementability_report_path']}")
