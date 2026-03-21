from __future__ import annotations

from trading_platform.config.loader import load_notification_config
from trading_platform.monitoring.notification_service import load_alerts, send_notifications


def cmd_monitor_notify(args) -> None:
    alerts = load_alerts(args.alerts)
    config = load_notification_config(args.config)
    result = send_notifications(alerts=alerts, config=config)
    print(f"Notification sent: {result['sent']}")
    print(f"Filtered alerts: {result['filtered_alert_count']}")
