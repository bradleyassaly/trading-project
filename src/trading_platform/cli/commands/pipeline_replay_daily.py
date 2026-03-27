from __future__ import annotations

from dataclasses import replace

from trading_platform.config.loader import load_daily_replay_workflow_config
from trading_platform.orchestration.daily_replay import run_daily_replay


def cmd_pipeline_replay_daily(args) -> None:
    config = load_daily_replay_workflow_config(args.config)
    if getattr(args, "start_date", None) is not None:
        config = replace(config, start_date=args.start_date)
    if getattr(args, "end_date", None) is not None:
        config = replace(config, end_date=args.end_date)
    if getattr(args, "dates_file", None) is not None:
        config = replace(config, dates_file=args.dates_file)
    if getattr(args, "initial_state_path", None) is not None:
        config = replace(config, initial_state_path=args.initial_state_path)
    if getattr(args, "output_dir", None) is not None:
        config = replace(config, output_dir=args.output_dir)
    if getattr(args, "max_days", None) is not None:
        config = replace(config, max_days=args.max_days)
    if getattr(args, "stop_on_error", False):
        config = replace(config, stop_on_error=True, continue_on_error=False)
    if getattr(args, "continue_on_error", False):
        config = replace(config, stop_on_error=False, continue_on_error=True)

    result = run_daily_replay(config)
    print(f"Output dir: {result.output_dir}")
    print(f"Replay status: {result.status}")
    print(f"Requested dates: {len(result.requested_dates)}")
    print(f"Processed dates: {len(result.processed_dates)}")
    print(f"Replay summary JSON: {result.summary_json_path}")
    print(f"Replay summary Markdown: {result.summary_md_path}")
    print(f"State path: {result.state_path}")
    if result.status in {"failed", "partial_failed"}:
        raise SystemExit(1)
