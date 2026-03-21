from __future__ import annotations

from trading_platform.config.loader import load_monitoring_config
from trading_platform.monitoring.service import evaluate_run_health, find_latest_pipeline_run_dir


def cmd_monitor_latest(args) -> None:
    config = load_monitoring_config(args.config)
    run_dir = find_latest_pipeline_run_dir(args.pipeline_root)
    report, paths = evaluate_run_health(
        run_dir=run_dir,
        config=config,
        output_dir=args.output_dir,
    )
    print(f"Latest run: {run_dir}")
    print(f"Run health status: {report.status}")
    print(f"Alerts: info={report.alert_counts['info']} warning={report.alert_counts['warning']} critical={report.alert_counts['critical']}")
    print(f"Run health artifact: {paths['run_health_json_path']}")
