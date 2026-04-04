from __future__ import annotations

import json
from pathlib import Path

from trading_platform.binance.models import BinanceAlertsConfig, BinanceHealthCheckConfig, BinanceNotifyConfig
from trading_platform.binance.notify import run_binance_monitor_notifications


class FakeSMTP:
    sent_messages = []

    def __init__(self, host, port, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def starttls(self):
        return None

    def login(self, username, password):
        return None

    def send_message(self, message):
        self.__class__.sent_messages.append(message)


def _notification_config(path: Path) -> None:
    path.write_text(
        """
smtp_host: smtp.example.com
smtp_port: 587
from_address: alerts@example.com
channels:
  - channel_type: email
    recipients:
      - ops@example.com
min_severity: warning
subject_prefix: Trading Platform
""",
        encoding="utf-8",
    )


def _write_unhealthy_inputs(root: Path) -> tuple[Path, Path]:
    latest_manifest = root / "sync" / "latest_sync_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text(
        json.dumps(
            {
                "sync_id": "sync-1",
                "status": "failed",
                "completed_at": "2024-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    status_summary = root / "status" / "binance_status.json"
    status_summary.parent.mkdir(parents=True, exist_ok=True)
    status_summary.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "dataset_name": "crypto_market_features",
                        "dataset_family": "feature",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": True,
                        "freshness_age_seconds": 1000,
                        "staleness_threshold_seconds": 900,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return latest_manifest, status_summary


def _write_healthy_inputs(root: Path) -> tuple[Path, Path]:
    latest_manifest = root / "sync" / "latest_sync_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text(
        json.dumps(
            {
                "sync_id": "sync-2",
                "status": "completed",
                "completed_at": "2999-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    status_summary = root / "status" / "binance_status.json"
    status_summary.parent.mkdir(parents=True, exist_ok=True)
    status_summary.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "dataset_name": "crypto_ohlcv_bars",
                        "dataset_family": "projection",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": False,
                    },
                    {
                        "dataset_name": "crypto_market_features",
                        "dataset_family": "feature",
                        "symbol": "BTCUSDT",
                        "interval": "1m",
                        "stale": False,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return latest_manifest, status_summary


def test_binance_notify_sends_on_initial_critical_transition(tmp_path: Path) -> None:
    FakeSMTP.sent_messages = []
    latest_manifest, status_summary = _write_unhealthy_inputs(tmp_path)
    notification_cfg = tmp_path / "notifications.yaml"
    _notification_config(notification_cfg)

    result = run_binance_monitor_notifications(
        BinanceNotifyConfig(
            alerts=BinanceAlertsConfig(
                latest_sync_manifest_path=str(latest_manifest),
                status_summary_path=str(status_summary),
                output_root=str(tmp_path / "alerts"),
                summary_path=str(tmp_path / "alerts" / "summary.json"),
                symbols=("BTCUSDT",),
                intervals=("1m",),
                latest_sync_max_age_sec=60,
            ),
            health=BinanceHealthCheckConfig(
                latest_sync_manifest_path=str(latest_manifest),
                status_summary_path=str(status_summary),
                output_root=str(tmp_path / "health"),
                summary_path=str(tmp_path / "health" / "summary.json"),
                symbols=("BTCUSDT",),
                intervals=("1m",),
                latest_sync_max_age_sec=60,
            ),
            output_root=str(tmp_path / "notify"),
            summary_path=str(tmp_path / "notify" / "summary.json"),
            state_path=str(tmp_path / "notify" / "state.json"),
            notification_config_path=str(notification_cfg),
            enabled=True,
        ),
        smtp_client_factory=FakeSMTP,
    )

    assert result.status == "critical"
    assert result.transition == "initial->critical"
    assert result.notified is True
    assert len(FakeSMTP.sent_messages) == 1


def test_binance_notify_suppresses_duplicate_warning_within_window(tmp_path: Path) -> None:
    FakeSMTP.sent_messages = []
    latest_manifest, status_summary = _write_unhealthy_inputs(tmp_path)
    notification_cfg = tmp_path / "notifications.yaml"
    _notification_config(notification_cfg)
    config = BinanceNotifyConfig(
        alerts=BinanceAlertsConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "alerts"),
            summary_path=str(tmp_path / "alerts" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            latest_sync_max_age_sec=60,
        ),
        health=BinanceHealthCheckConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "health"),
            summary_path=str(tmp_path / "health" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            latest_sync_max_age_sec=60,
        ),
        output_root=str(tmp_path / "notify"),
        summary_path=str(tmp_path / "notify" / "summary.json"),
        state_path=str(tmp_path / "notify" / "state.json"),
        notification_config_path=str(notification_cfg),
        enabled=True,
        duplicate_suppression_window_sec=86400,
    )

    first = run_binance_monitor_notifications(config, smtp_client_factory=FakeSMTP)
    second = run_binance_monitor_notifications(config, smtp_client_factory=FakeSMTP)

    assert first.notified is True
    assert second.should_notify is False
    assert second.suppressed is True
    assert second.notified is False
    assert len(FakeSMTP.sent_messages) == 1


def test_binance_notify_emits_recovery_notification(tmp_path: Path) -> None:
    FakeSMTP.sent_messages = []
    notification_cfg = tmp_path / "notifications.yaml"
    _notification_config(notification_cfg)
    latest_manifest, status_summary = _write_unhealthy_inputs(tmp_path)
    config = BinanceNotifyConfig(
        alerts=BinanceAlertsConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "alerts"),
            summary_path=str(tmp_path / "alerts" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            latest_sync_max_age_sec=60,
        ),
        health=BinanceHealthCheckConfig(
            latest_sync_manifest_path=str(latest_manifest),
            status_summary_path=str(status_summary),
            output_root=str(tmp_path / "health"),
            summary_path=str(tmp_path / "health" / "summary.json"),
            symbols=("BTCUSDT",),
            intervals=("1m",),
            latest_sync_max_age_sec=60,
        ),
        output_root=str(tmp_path / "notify"),
        summary_path=str(tmp_path / "notify" / "summary.json"),
        state_path=str(tmp_path / "notify" / "state.json"),
        notification_config_path=str(notification_cfg),
        enabled=True,
        notify_on_recovery=True,
    )
    run_binance_monitor_notifications(config, smtp_client_factory=FakeSMTP)
    healthy_manifest, healthy_status = _write_healthy_inputs(tmp_path)
    recovered = run_binance_monitor_notifications(
        BinanceNotifyConfig(
            **{
                **config.__dict__,
                "alerts": BinanceAlertsConfig(
                    **{
                        **config.alerts.__dict__,
                        "latest_sync_manifest_path": str(healthy_manifest),
                        "status_summary_path": str(healthy_status),
                    }
                ),
                "health": BinanceHealthCheckConfig(
                    **{
                        **config.health.__dict__,
                        "latest_sync_manifest_path": str(healthy_manifest),
                        "status_summary_path": str(healthy_status),
                    }
                ),
            }
        ),
        smtp_client_factory=FakeSMTP,
    )

    assert recovered.status == "healthy"
    assert recovered.transition == "critical->healthy"
    assert recovered.notified is True
    assert len(FakeSMTP.sent_messages) == 2
