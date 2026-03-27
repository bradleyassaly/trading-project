from __future__ import annotations

from pathlib import Path

from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.config.loader import load_backtester_validation_workflow_config
from trading_platform.research.backtester_validation import (
    run_vectorbt_validation_harness,
    write_vectorbt_validation_artifacts,
)


def cmd_research_validate_backtester(args) -> None:
    load_and_apply_workflow_config(
        args,
        loader=load_backtester_validation_workflow_config,
    )
    result = run_vectorbt_validation_harness()
    paths = write_vectorbt_validation_artifacts(
        result=result,
        output_dir=Path(args.output_dir),
    )
    print("Backtester validation complete.")
    print(f"Summary: {paths['vectorbt_validation_summary_path']}")
    print(f"Metrics: {paths['vectorbt_validation_metrics_path']}")
    print(f"Trade comparison: {paths['vectorbt_validation_trade_comparison_path']}")
