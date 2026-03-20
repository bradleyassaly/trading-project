from __future__ import annotations

from pathlib import Path

from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    SignalSearchSpace,
    run_automated_alpha_research_loop,
)


def cmd_alpha_research_loop(args) -> None:
    search_spaces = tuple(
        SignalSearchSpace(
            signal_family=signal_family,
            lookbacks=tuple(args.lookbacks),
            horizons=tuple(args.horizons),
        )
        for signal_family in args.signal_families
    )
    config = AutomatedAlphaResearchConfig(
        symbols=args.symbols,
        universe=args.universe,
        feature_dir=Path(args.feature_dir),
        output_dir=Path(args.output_dir),
        search_spaces=search_spaces,
        min_rows=args.min_rows,
        top_quantile=args.top_quantile,
        bottom_quantile=args.bottom_quantile,
        train_size=args.train_size,
        test_size=args.test_size,
        step_size=args.step_size,
        min_train_size=args.min_train_size,
        schedule_frequency=args.schedule_frequency,
        force=args.force,
    )
    result = run_automated_alpha_research_loop(config=config)
    print(f"Automated alpha research loop: {result['status']}")
    for key, value in sorted(result.items()):
        if key == "status":
            continue
        print(f"{key}: {value}")
