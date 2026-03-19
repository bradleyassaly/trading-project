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
    )

    print("Alpha research complete.")
    print(f"Leaderboard: {result['leaderboard_path']}")
    print(f"Detailed results: {result['fold_results_path']}")