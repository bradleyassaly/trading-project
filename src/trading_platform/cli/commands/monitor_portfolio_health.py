from __future__ import annotations

from trading_platform.config.loader import load_monitoring_config
from trading_platform.monitoring.service import evaluate_portfolio_health


def cmd_monitor_portfolio_health(args) -> None:
    config = load_monitoring_config(args.config)
    report, paths = evaluate_portfolio_health(
        allocation_dir=args.allocation_dir,
        config=config,
        output_dir=args.output_dir,
    )
    print(f"Portfolio health status: {report.status}")
    print(f"Alerts: info={report.alert_counts['info']} warning={report.alert_counts['warning']} critical={report.alert_counts['critical']}")
    print(f"Portfolio health artifact: {paths['portfolio_health_json_path']}")
