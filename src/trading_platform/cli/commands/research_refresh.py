from __future__ import annotations

from pathlib import Path

from trading_platform.research.alpha_lab.automation import AutomatedAlphaResearchConfig
from trading_platform.research.alpha_lab.generation import SignalGenerationConfig
from trading_platform.research.refresh_monitoring import (
    ScheduledResearchRefreshConfig,
    run_scheduled_research_refresh,
)


def cmd_research_refresh(args) -> None:
    alpha_config = AutomatedAlphaResearchConfig(
        symbols=args.symbols,
        universe=args.universe,
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
        stale_after_days=args.stale_after_days,
    )
    result = run_scheduled_research_refresh(
        config=ScheduledResearchRefreshConfig(
            alpha_config=alpha_config,
            tracker_dir=Path(args.tracker_dir) if args.tracker_dir else None,
        )
    )
    print(f"Scheduled refresh: {result['status']}")
    for key, value in sorted(result.items()):
        if key == "status":
            continue
        print(f"{key}: {value}")
