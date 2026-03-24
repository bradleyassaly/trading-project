from __future__ import annotations

from trading_platform.config.loader import load_daily_alerts_config


def test_load_daily_alerts_config_from_yaml(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_PLATFORM_SMTP_PASSWORD", "secret")
    path = tmp_path / "alerts.yaml"
    path.write_text(
        """
email_enabled: true
sms_enabled: true
smtp_host: smtp.example.com
smtp_port: 587
smtp_username: alerts@example.com
smtp_password_env_var: TRADING_PLATFORM_SMTP_PASSWORD
email_from: alerts@example.com
email_to:
  - ops@example.com
email_min_severity: info
sms_provider: stub
sms_target:
  - "+15555550123"
sms_min_severity: critical
send_daily_success_summary: true
send_on_failure: true
send_on_zero_promotions: true
send_on_monitoring_warnings: true
send_on_kill_switch_recommendations: true
monitoring_warning_threshold: 2
""".strip(),
        encoding="utf-8",
    )

    config = load_daily_alerts_config(path)

    assert config.email_enabled is True
    assert config.sms_enabled is True
    assert config.smtp_password == "secret"
    assert config.email_to == ["ops@example.com"]
    assert config.sms_target == ["+15555550123"]
    assert config.monitoring_warning_threshold == 2
