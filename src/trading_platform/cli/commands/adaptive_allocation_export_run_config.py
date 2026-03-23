from __future__ import annotations

from pathlib import Path

from trading_platform.portfolio.adaptive_allocation import export_adaptive_allocation_run_config


def cmd_adaptive_allocation_export_run_config(args) -> None:
    result = export_adaptive_allocation_run_config(
        adaptive_allocation_path=Path(args.allocation),
        output_dir=Path(args.output_dir),
    )
    print(f"Multi-strategy config: {result['multi_strategy_config_path']}")
    print(f"Pipeline config: {result['pipeline_config_path']}")
    print(f"Run bundle: {result['run_bundle_path']}")
