from __future__ import annotations

from trading_platform.config.loader import load_alpha_cycle_workflow_config
from trading_platform.orchestration.alpha_cycle import run_alpha_cycle


def cmd_pipeline_alpha_cycle(args) -> None:
    config = load_alpha_cycle_workflow_config(args.config)
    result = run_alpha_cycle(config)
    print(f"Run name: {result.run_name}")
    print(f"Status: {result.status}")
    print(f"Summary JSON: {result.summary_json_path}")
    print(f"Summary Markdown: {result.summary_md_path}")
    for record in result.stage_records:
        duration = f"{record.duration_seconds:.3f}" if record.duration_seconds is not None else "n/a"
        print(f"{record.stage_name}: {record.status} ({duration}s)")
        if record.error_message:
            print(f"  error: {record.error_message}")
    if result.status != "succeeded":
        raise SystemExit(1)
