from __future__ import annotations

from pathlib import Path

from trading_platform.research.experiment_tracking import build_experiment_summary_report


def cmd_experiments_dashboard(args) -> None:
    result = build_experiment_summary_report(
        tracker_dir=Path(args.tracker_dir),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        top_metric=args.top_metric,
        limit=args.limit,
    )
    print(f"Experiment summary report: {result['experiment_summary_report_path']}")
    print(f"Latest model state: {result['latest_model_state_path']}")
