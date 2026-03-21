from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_multi_strategy_portfolio_config
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)


def cmd_portfolio_allocate_multi_strategy(args) -> None:
    config = load_multi_strategy_portfolio_config(args.config)
    result = allocate_multi_strategy_portfolio(config)
    artifact_paths = write_multi_strategy_artifacts(result, Path(args.output_dir))

    print(f"As of: {result.as_of}")
    print(f"Enabled sleeves: {result.summary['enabled_sleeve_count']}")
    print(f"Gross before constraints: {result.summary['gross_exposure_before_constraints']:.6f}")
    print(f"Gross after constraints: {result.summary['gross_exposure_after_constraints']:.6f}")
    print(f"Net after constraints: {result.summary['net_exposure_after_constraints']:.6f}")
    print(f"Turnover estimate: {result.summary['turnover_estimate']:.6f}")
    print("Artifacts:")
    for name, path in sorted(artifact_paths.items()):
        print(f"  {name}: {path}")
