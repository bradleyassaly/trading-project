from __future__ import annotations

import json
from pathlib import Path

from trading_platform.system.doctor import run_system_doctor


def test_system_doctor_with_valid_configs(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    dashboard_config = tmp_path / "dashboard.yaml"
    dashboard_config.write_text("artifacts_root: artifacts\nhost: 127.0.0.1\nport: 8000\n", encoding="utf-8")
    monitoring_config = tmp_path / "monitoring.yaml"
    monitoring_config.write_text("maximum_failed_stages: 0\nminimum_generated_position_count: 1\n", encoding="utf-8")
    notification_config = tmp_path / "notifications.yaml"
    notification_config.write_text(
        "smtp_host: smtp.example.com\nsmtp_port: 587\nfrom_address: alerts@example.com\nchannels:\n  - channel_type: email\n    recipients: [ops@example.com]\n",
        encoding="utf-8",
    )
    execution_config = tmp_path / "execution.yaml"
    execution_config.write_text("commission_model_type: bps\ncommission_bps: 1.0\n", encoding="utf-8")
    broker_config = tmp_path / "broker.yaml"
    broker_config.write_text(
        "broker_name: mock\nlive_trading_enabled: false\nrequire_manual_enable_flag: false\nallowed_order_types: [market]\ndefault_order_type: market\n",
        encoding="utf-8",
    )

    report, paths = run_system_doctor(
        artifacts_root=artifacts_root,
        output_dir=tmp_path / "doctor",
        monitoring_config=str(monitoring_config),
        notification_config=str(notification_config),
        execution_config=str(execution_config),
        broker_config=str(broker_config),
        dashboard_config=str(dashboard_config),
    )

    assert report["status"] in {"succeeded", "warning"}
    assert paths["doctor_report_json_path"].exists()
    payload = json.loads(paths["doctor_report_json_path"].read_text(encoding="utf-8"))
    assert payload["check_count"] >= 5


def test_system_doctor_flags_bad_config(tmp_path: Path) -> None:
    bad_broker = tmp_path / "broker_bad.yaml"
    bad_broker.write_text("broker_name: mock\nmax_orders_per_run: -1\n", encoding="utf-8")

    report, _paths = run_system_doctor(
        artifacts_root=tmp_path / "missing_artifacts",
        output_dir=tmp_path / "doctor",
        broker_config=str(bad_broker),
    )

    assert report["status"] == "failed"
    assert any(check["check_name"] == "broker_config" and check["status"] == "fail" for check in report["checks"])
