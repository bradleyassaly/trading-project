from __future__ import annotations

from dataclasses import asdict, dataclass, field
from os import getenv
from typing import Any


ALERT_SEVERITIES = {"info", "warning", "critical"}
HEALTH_STATUSES = {"healthy", "warning", "critical"}
NOTIFICATION_CHANNELS = {"email", "sms"}
SMS_PROVIDERS = {"stub", "email_gateway"}


def _validate_nonnegative_optional(value: float | int | None, field_name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be >= 0")


@dataclass(frozen=True)
class Alert:
    code: str
    severity: str
    message: str
    timestamp: str
    entity_type: str
    entity_id: str
    metric_value: Any = None
    threshold_value: Any = None
    artifact_path: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.code or not self.code.strip():
            raise ValueError("code must be a non-empty string")
        if self.severity not in ALERT_SEVERITIES:
            raise ValueError(f"Unsupported alert severity: {self.severity}")
        if not self.message or not self.message.strip():
            raise ValueError("message must be a non-empty string")
        if not self.entity_type or not self.entity_type.strip():
            raise ValueError("entity_type must be a non-empty string")
        if not self.entity_id or not self.entity_id.strip():
            raise ValueError("entity_id must be a non-empty string")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MonitoringConfig:
    maximum_failed_stages: int = 0
    stale_artifact_max_age_hours: float | None = None
    minimum_approved_strategy_count: int | None = None
    minimum_generated_position_count: int = 1
    maximum_gross_exposure: float | None = None
    maximum_net_exposure: float | None = None
    maximum_symbol_concentration: float | None = None
    maximum_turnover: float | None = None
    maximum_drawdown: float | None = None
    minimum_rolling_sharpe: float | None = None
    maximum_benchmark_underperformance: float | None = None
    maximum_missing_data_incidents: int | None = None
    maximum_zero_weight_runs: int | None = None
    max_drift_between_sleeve_target_and_final_combined_weight: float | None = None
    unusual_order_count_change_multiple: float | None = None
    maximum_rejected_order_count: int | None = None
    maximum_liquidity_breaches: int | None = None
    maximum_short_availability_failures: int | None = None
    maximum_rejected_order_ratio: float | None = None
    maximum_clipped_order_ratio: float | None = None
    maximum_turnover_after_execution: float | None = None
    maximum_execution_cost: float | None = None
    maximum_zero_executable_order_runs: int | None = None
    maximum_live_risk_check_failures: int | None = None
    maximum_live_submission_failures: int | None = None
    maximum_duplicate_order_skip_events: int | None = None

    def __post_init__(self) -> None:
        if self.maximum_failed_stages < 0:
            raise ValueError("maximum_failed_stages must be >= 0")
        if self.minimum_generated_position_count < 0:
            raise ValueError("minimum_generated_position_count must be >= 0")
        _validate_nonnegative_optional(self.stale_artifact_max_age_hours, "stale_artifact_max_age_hours")
        _validate_nonnegative_optional(self.minimum_approved_strategy_count, "minimum_approved_strategy_count")
        _validate_nonnegative_optional(self.maximum_gross_exposure, "maximum_gross_exposure")
        _validate_nonnegative_optional(self.maximum_net_exposure, "maximum_net_exposure")
        _validate_nonnegative_optional(self.maximum_symbol_concentration, "maximum_symbol_concentration")
        _validate_nonnegative_optional(self.maximum_turnover, "maximum_turnover")
        _validate_nonnegative_optional(self.maximum_drawdown, "maximum_drawdown")
        _validate_nonnegative_optional(self.maximum_benchmark_underperformance, "maximum_benchmark_underperformance")
        _validate_nonnegative_optional(self.maximum_missing_data_incidents, "maximum_missing_data_incidents")
        _validate_nonnegative_optional(self.maximum_zero_weight_runs, "maximum_zero_weight_runs")
        _validate_nonnegative_optional(
            self.max_drift_between_sleeve_target_and_final_combined_weight,
            "max_drift_between_sleeve_target_and_final_combined_weight",
        )
        _validate_nonnegative_optional(self.unusual_order_count_change_multiple, "unusual_order_count_change_multiple")
        _validate_nonnegative_optional(self.maximum_rejected_order_count, "maximum_rejected_order_count")
        _validate_nonnegative_optional(self.maximum_liquidity_breaches, "maximum_liquidity_breaches")
        _validate_nonnegative_optional(self.maximum_short_availability_failures, "maximum_short_availability_failures")
        _validate_nonnegative_optional(self.maximum_rejected_order_ratio, "maximum_rejected_order_ratio")
        _validate_nonnegative_optional(self.maximum_clipped_order_ratio, "maximum_clipped_order_ratio")
        _validate_nonnegative_optional(self.maximum_turnover_after_execution, "maximum_turnover_after_execution")
        _validate_nonnegative_optional(self.maximum_execution_cost, "maximum_execution_cost")
        _validate_nonnegative_optional(self.maximum_zero_executable_order_runs, "maximum_zero_executable_order_runs")
        _validate_nonnegative_optional(self.maximum_live_risk_check_failures, "maximum_live_risk_check_failures")
        _validate_nonnegative_optional(self.maximum_live_submission_failures, "maximum_live_submission_failures")
        _validate_nonnegative_optional(self.maximum_duplicate_order_skip_events, "maximum_duplicate_order_skip_events")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunHealthReport:
    run_dir: str
    run_name: str
    evaluated_at: str
    status: str
    metrics: dict[str, Any]
    alert_counts: dict[str, int]
    alerts: list[Alert]

    def __post_init__(self) -> None:
        if self.status not in HEALTH_STATUSES:
            raise ValueError(f"Unsupported health status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_dir": self.run_dir,
            "run_name": self.run_name,
            "evaluated_at": self.evaluated_at,
            "status": self.status,
            "metrics": self.metrics,
            "alert_counts": self.alert_counts,
            "alerts": [alert.to_dict() for alert in self.alerts],
        }


@dataclass(frozen=True)
class StrategyHealthReport:
    evaluated_at: str
    status: str
    strategy_rows: list[dict[str, Any]]
    alert_counts: dict[str, int]
    alerts: list[Alert]

    def __post_init__(self) -> None:
        if self.status not in HEALTH_STATUSES:
            raise ValueError(f"Unsupported health status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluated_at": self.evaluated_at,
            "status": self.status,
            "strategy_rows": self.strategy_rows,
            "alert_counts": self.alert_counts,
            "alerts": [alert.to_dict() for alert in self.alerts],
        }


@dataclass(frozen=True)
class PortfolioHealthReport:
    allocation_dir: str
    evaluated_at: str
    status: str
    metrics: dict[str, Any]
    alert_counts: dict[str, int]
    alerts: list[Alert]

    def __post_init__(self) -> None:
        if self.status not in HEALTH_STATUSES:
            raise ValueError(f"Unsupported health status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "allocation_dir": self.allocation_dir,
            "evaluated_at": self.evaluated_at,
            "status": self.status,
            "metrics": self.metrics,
            "alert_counts": self.alert_counts,
            "alerts": [alert.to_dict() for alert in self.alerts],
        }


@dataclass(frozen=True)
class NotificationChannel:
    channel_type: str
    recipients: list[str]

    def __post_init__(self) -> None:
        if self.channel_type not in NOTIFICATION_CHANNELS:
            raise ValueError(f"Unsupported notification channel: {self.channel_type}")
        if not self.recipients:
            raise ValueError("recipients must contain at least one value")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NotificationConfig:
    smtp_host: str
    smtp_port: int
    from_address: str
    channels: list[NotificationChannel]
    min_severity: str = "warning"
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True
    subject_prefix: str = "Trading Platform"

    def __post_init__(self) -> None:
        if not self.smtp_host or not self.smtp_host.strip():
            raise ValueError("smtp_host must be a non-empty string")
        if self.smtp_port <= 0:
            raise ValueError("smtp_port must be > 0")
        if not self.from_address or not self.from_address.strip():
            raise ValueError("from_address must be a non-empty string")
        if not self.channels:
            raise ValueError("channels must contain at least one notification channel")
        if self.min_severity not in ALERT_SEVERITIES:
            raise ValueError(f"Unsupported min_severity: {self.min_severity}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port,
            "from_address": self.from_address,
            "channels": [channel.to_dict() for channel in self.channels],
            "min_severity": self.min_severity,
            "smtp_username": self.smtp_username,
            "smtp_password": self.smtp_password,
            "smtp_use_tls": self.smtp_use_tls,
            "subject_prefix": self.subject_prefix,
        }


@dataclass(frozen=True)
class DailyAlertsConfig:
    email_enabled: bool
    sms_enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password_env_var: str | None = None
    email_from: str = ""
    email_to: list[str] = field(default_factory=list)
    smtp_use_tls: bool = True
    subject_prefix: str = "Trading Platform"
    email_min_severity: str = "warning"
    sms_provider: str = "stub"
    sms_target: list[str] = field(default_factory=list)
    sms_min_severity: str = "critical"
    send_daily_success_summary: bool = True
    send_on_failure: bool = True
    send_on_zero_promotions: bool = True
    send_on_monitoring_warnings: bool = True
    send_on_kill_switch_recommendations: bool = True
    monitoring_warning_threshold: int = 1

    def __post_init__(self) -> None:
        if self.email_min_severity not in ALERT_SEVERITIES:
            raise ValueError(f"Unsupported email_min_severity: {self.email_min_severity}")
        if self.sms_min_severity not in ALERT_SEVERITIES:
            raise ValueError(f"Unsupported sms_min_severity: {self.sms_min_severity}")
        if self.sms_provider not in SMS_PROVIDERS:
            raise ValueError(f"Unsupported sms_provider: {self.sms_provider}")
        if self.monitoring_warning_threshold < 0:
            raise ValueError("monitoring_warning_threshold must be >= 0")
        if self.email_enabled:
            if not self.smtp_host or not self.smtp_host.strip():
                raise ValueError("smtp_host must be a non-empty string when email is enabled")
            if self.smtp_port <= 0:
                raise ValueError("smtp_port must be > 0 when email is enabled")
            if not self.email_from or not self.email_from.strip():
                raise ValueError("email_from must be a non-empty string when email is enabled")
            if not self.email_to:
                raise ValueError("email_to must contain at least one recipient when email is enabled")
        if self.sms_enabled and not self.sms_target:
            raise ValueError("sms_target must contain at least one recipient when sms is enabled")

    @property
    def smtp_password(self) -> str | None:
        if not self.smtp_password_env_var:
            return None
        return getenv(self.smtp_password_env_var)

    def to_email_notification_config(self) -> NotificationConfig | None:
        if not self.email_enabled:
            return None
        return NotificationConfig(
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
            from_address=self.email_from,
            channels=[NotificationChannel(channel_type="email", recipients=self.email_to)],
            min_severity=self.email_min_severity,
            smtp_username=self.smtp_username,
            smtp_password=self.smtp_password,
            smtp_use_tls=self.smtp_use_tls,
            subject_prefix=self.subject_prefix,
        )

    def to_sms_notification_config(self) -> NotificationConfig | None:
        if not self.sms_enabled:
            return None
        return NotificationConfig(
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
            from_address=self.email_from,
            channels=[NotificationChannel(channel_type="sms", recipients=self.sms_target)],
            min_severity=self.sms_min_severity,
            smtp_username=self.smtp_username,
            smtp_password=self.smtp_password,
            smtp_use_tls=self.smtp_use_tls,
            subject_prefix=self.subject_prefix,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "email_enabled": self.email_enabled,
            "sms_enabled": self.sms_enabled,
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port,
            "smtp_username": self.smtp_username,
            "smtp_password_env_var": self.smtp_password_env_var,
            "email_from": self.email_from,
            "email_to": self.email_to,
            "smtp_use_tls": self.smtp_use_tls,
            "subject_prefix": self.subject_prefix,
            "email_min_severity": self.email_min_severity,
            "sms_provider": self.sms_provider,
            "sms_target": self.sms_target,
            "sms_min_severity": self.sms_min_severity,
            "send_daily_success_summary": self.send_daily_success_summary,
            "send_on_failure": self.send_on_failure,
            "send_on_zero_promotions": self.send_on_zero_promotions,
            "send_on_monitoring_warnings": self.send_on_monitoring_warnings,
            "send_on_kill_switch_recommendations": self.send_on_kill_switch_recommendations,
            "monitoring_warning_threshold": self.monitoring_warning_threshold,
        }
