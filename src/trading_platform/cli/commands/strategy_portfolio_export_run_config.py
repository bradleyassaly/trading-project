from __future__ import annotations

from pathlib import Path

from trading_platform.portfolio.strategy_portfolio import export_strategy_portfolio_run_config


def cmd_strategy_portfolio_export_run_config(args) -> None:
    result = export_strategy_portfolio_run_config(
        strategy_portfolio_path=Path(args.portfolio),
        output_dir=Path(args.output_dir),
    )
    print(f"Multi-strategy config: {result['multi_strategy_config_path']}")
    print(f"Pipeline config: {result['pipeline_config_path']}")
    print(f"Run bundle: {result['run_bundle_path']}")
