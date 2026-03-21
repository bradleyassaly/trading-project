from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_monitoring_config
from trading_platform.monitoring.service import evaluate_run_health


def cmd_monitor_run_health(args) -> None:
    config = load_monitoring_config(args.config)
    report, paths = evaluate_run_health(
        run_dir=args.run_dir,
        config=config,
        output_dir=Path(args.run_dir) / "monitoring",
    )
    print(f"Run health status: {report.status}")
    print(f"Alerts: info={report.alert_counts['info']} warning={report.alert_counts['warning']} critical={report.alert_counts['critical']}")
    print(f"Run health artifact: {paths['run_health_json_path']}")
