from __future__ import annotations

from pathlib import Path

from trading_platform.cli.common import resolve_symbols
from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    run_automated_alpha_research_loop,
)
from trading_platform.research.alpha_lab.generation import SignalGenerationConfig


def cmd_alpha_research_loop(args) -> None:
    symbols = resolve_symbols(args)
    config = AutomatedAlphaResearchConfig(
        symbols=symbols,
        universe=None,
        feature_dir=Path(args.feature_dir),
        output_dir=Path(args.output_dir),
        generation_config=SignalGenerationConfig(
            signal_families=tuple(args.signal_families),
            lookbacks=tuple(args.lookbacks),
            vol_windows=tuple(args.vol_windows),
            combo_thresholds=tuple(args.combo_thresholds),
            horizons=tuple(args.horizons),
        ),
        min_rows=args.min_rows,
        top_quantile=args.top_quantile,
        bottom_quantile=args.bottom_quantile,
        train_size=args.train_size,
        test_size=args.test_size,
        step_size=args.step_size,
        min_train_size=args.min_train_size,
        schedule_frequency=args.schedule_frequency,
        force=args.force,
        max_iterations=args.max_iterations,
    )
    result = run_automated_alpha_research_loop(config=config)
    print(f"Automated alpha research loop: {result['status']}")
    for key, value in sorted(result.items()):
        if key == "status":
            continue
        print(f"{key}: {value}")
