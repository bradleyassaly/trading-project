from __future__ import annotations

from trading_platform.monitoring.service import build_dashboard_data


def cmd_monitor_build_dashboard_data(args) -> None:
    paths = build_dashboard_data(
        pipeline_root=args.pipeline_root,
        output_dir=args.output_dir,
    )
    print(f"Dashboard CSV: {paths['dashboard_runs_csv_path']}")
    print(f"Dashboard JSON: {paths['dashboard_runs_json_path']}")
