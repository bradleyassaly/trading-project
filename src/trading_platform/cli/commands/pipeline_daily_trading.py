from __future__ import annotations

from trading_platform.config.loader import load_daily_trading_workflow_config
from trading_platform.orchestration.daily_trading import run_daily_trading_pipeline


def cmd_pipeline_daily_trading(args) -> None:
    config = load_daily_trading_workflow_config(args.config)
    result = run_daily_trading_pipeline(config)
    print(f"Run name: {result.run_name}")
    print(f"Status: {result.status}")
    print(f"Summary JSON: {result.summary_json_path}")
    print(f"Summary Markdown: {result.summary_md_path}")
    for record in result.stage_records:
        duration = f"{record.duration_seconds:.3f}" if record.duration_seconds is not None else "n/a"
        print(f"{record.stage_name}: {record.status} ({duration}s)")
        if record.error_message:
            print(f"  error: {record.error_message}")
    if result.status in {"failed", "partial_failed"}:
        raise SystemExit(1)
