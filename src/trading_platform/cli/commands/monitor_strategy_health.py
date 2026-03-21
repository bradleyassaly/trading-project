from __future__ import annotations

from trading_platform.config.loader import load_monitoring_config
from trading_platform.monitoring.service import evaluate_strategy_health


def cmd_monitor_strategy_health(args) -> None:
    config = load_monitoring_config(args.config)
    report, paths = evaluate_strategy_health(
        registry_path=args.registry,
        artifacts_root=args.artifacts_root,
        config=config,
        output_dir=args.output_dir,
    )
    print(f"Strategy health status: {report.status}")
    print(f"Alerts: info={report.alert_counts['info']} warning={report.alert_counts['warning']} critical={report.alert_counts['critical']}")
    print(f"Strategy health artifact: {paths['strategy_health_json_path']}")
