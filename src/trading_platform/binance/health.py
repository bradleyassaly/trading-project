from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.binance.models import (
    BinanceAlertsConfig,
    BinanceAlertsResult,
    BinanceHealthCheckConfig,
    BinanceHealthCheckResult,
)
from trading_platform.monitoring.models import Alert


def _read_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return dict(json.loads(file_path.read_text(encoding="utf-8")) or {})
    except json.JSONDecodeError:
        return {}


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _status_from_alerts(alerts: list[Alert]) -> str:
    if any(alert.severity == "critical" for alert in alerts):
        return "critical"
    if any(alert.severity == "warning" for alert in alerts):
        return "warning"
    return "healthy"


def _alert_counts(alerts: list[Alert]) -> dict[str, int]:
    return {
        "info": sum(1 for alert in alerts if alert.severity == "info"),
        "warning": sum(1 for alert in alerts if alert.severity == "warning"),
        "critical": sum(1 for alert in alerts if alert.severity == "critical"),
    }


def _write_alert_artifacts(output_dir: Path, alerts: list[Alert]) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    alerts_json = output_dir / "alerts.json"
    alerts_csv = output_dir / "alerts.csv"
    rows = [alert.to_dict() for alert in alerts]
    alerts_json.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(rows).to_csv(alerts_csv, index=False)
    return {"alerts_json_path": alerts_json, "alerts_csv_path": alerts_csv}


def _required_projection_scopes(symbols: tuple[str, ...], intervals: tuple[str, ...]) -> set[tuple[str, str]]:
    return {(str(symbol).upper(), str(interval)) for symbol in symbols for interval in intervals}


def _required_feature_scopes(symbols: tuple[str, ...], intervals: tuple[str, ...]) -> set[tuple[str, str]]:
    return {(str(symbol).upper(), str(interval)) for symbol in symbols for interval in intervals}


def evaluate_binance_alerts(config: BinanceAlertsConfig) -> BinanceAlertsResult:
    latest_manifest = _read_json(config.latest_sync_manifest_path)
    status_payload = _read_json(config.status_summary_path)
    alerts: list[Alert] = []
    now = datetime.now(UTC)

    if not latest_manifest:
        alerts.append(
            Alert(
                code="missing_latest_sync_manifest",
                severity="critical",
                message="Latest Binance sync manifest is missing or unreadable",
                timestamp=_now_utc(),
                entity_type="binance_sync",
                entity_id="latest_manifest",
                artifact_path=str(config.latest_sync_manifest_path),
            )
        )
    else:
        completed_at = _iso_to_dt(str(latest_manifest.get("completed_at")) if latest_manifest.get("completed_at") is not None else None)
        if config.require_sync_status_completed and str(latest_manifest.get("status")) != "completed":
            alerts.append(
                Alert(
                    code="unhealthy_latest_sync_status",
                    severity="critical",
                    message=f"Latest Binance sync status is {latest_manifest.get('status')}",
                    timestamp=_now_utc(),
                    entity_type="binance_sync",
                    entity_id=str(latest_manifest.get("sync_id") or "latest"),
                    metric_value=latest_manifest.get("status"),
                    threshold_value="completed",
                    artifact_path=str(config.latest_sync_manifest_path),
                )
            )
        if completed_at is not None:
            age_seconds = (now - completed_at).total_seconds()
            if age_seconds > config.latest_sync_max_age_sec:
                alerts.append(
                    Alert(
                        code="stale_latest_sync_manifest",
                        severity="warning",
                        message=f"Latest Binance sync completed {age_seconds:.1f} seconds ago",
                        timestamp=_now_utc(),
                        entity_type="binance_sync",
                        entity_id=str(latest_manifest.get("sync_id") or "latest"),
                        metric_value=round(age_seconds, 6),
                        threshold_value=config.latest_sync_max_age_sec,
                        artifact_path=str(config.latest_sync_manifest_path),
                    )
                )

    if not status_payload:
        alerts.append(
            Alert(
                code="missing_binance_status",
                severity="critical",
                message="Binance status artifact is missing or unreadable",
                timestamp=_now_utc(),
                entity_type="binance_status",
                entity_id="latest",
                artifact_path=str(config.status_summary_path),
            )
        )
    else:
        records = list(status_payload.get("records") or [])
        for record in records:
            if record.get("stale"):
                alerts.append(
                    Alert(
                        code="stale_dataset",
                        severity="warning" if record.get("dataset_family") == "projection" else "critical",
                        message=f"{record.get('dataset_name')} is stale for {record.get('symbol')} {record.get('interval') or ''}".strip(),
                        timestamp=_now_utc(),
                        entity_type=str(record.get("dataset_family") or "binance_dataset"),
                        entity_id=f"{record.get('dataset_name')}:{record.get('symbol')}:{record.get('interval') or 'na'}",
                        metric_value=record.get("freshness_age_seconds"),
                        threshold_value=record.get("staleness_threshold_seconds"),
                        artifact_path=str(config.status_summary_path),
                        context=dict(record),
                    )
                )
        required_scopes = _required_feature_scopes(config.symbols, config.intervals)
        present_feature_scopes = {
            (str(record.get("symbol")).upper(), str(record.get("interval")))
            for record in records
            if record.get("dataset_name") == "crypto_market_features" and record.get("interval") is not None
        }
        missing_feature_scopes = sorted(required_scopes - present_feature_scopes)
        for symbol, interval in missing_feature_scopes:
            alerts.append(
                Alert(
                    code="missing_required_feature_scope",
                    severity=config.missing_scope_severity,
                    message=f"Missing Binance feature scope {symbol} {interval}",
                    timestamp=_now_utc(),
                    entity_type="binance_feature_scope",
                    entity_id=f"{symbol}:{interval}",
                    artifact_path=str(config.status_summary_path),
                )
            )

    output_root = Path(config.output_root)
    paths = _write_alert_artifacts(output_root, alerts)
    summary = {
        "evaluated_at": _now_utc(),
        "status": _status_from_alerts(alerts),
        "alert_counts": _alert_counts(alerts),
        "alert_count": len(alerts),
        "latest_sync_manifest_path": str(config.latest_sync_manifest_path),
        "status_summary_path": str(config.status_summary_path),
        "alerts_json_path": str(paths["alerts_json_path"]),
        "alerts_csv_path": str(paths["alerts_csv_path"]),
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return BinanceAlertsResult(
        summary_path=str(summary_path),
        alerts_json_path=str(paths["alerts_json_path"]),
        alerts_csv_path=str(paths["alerts_csv_path"]),
        status=summary["status"],
        alert_counts=summary["alert_counts"],
        alert_count=len(alerts),
    )


def evaluate_binance_health_check(config: BinanceHealthCheckConfig) -> BinanceHealthCheckResult:
    latest_manifest = _read_json(config.latest_sync_manifest_path)
    status_payload = _read_json(config.status_summary_path)
    alerts: list[Alert] = []
    checks: list[dict[str, Any]] = []
    now = datetime.now(UTC)

    if not latest_manifest:
        alerts.append(
            Alert(
                code="missing_latest_sync_manifest",
                severity="critical",
                message="Latest Binance sync manifest is missing or unreadable",
                timestamp=_now_utc(),
                entity_type="binance_sync",
                entity_id="latest_manifest",
                artifact_path=str(config.latest_sync_manifest_path),
            )
        )
        checks.append({"check": "latest_manifest_readable", "status": "fail"})
    else:
        checks.append({"check": "latest_manifest_readable", "status": "pass"})
        completed_at = _iso_to_dt(str(latest_manifest.get("completed_at")) if latest_manifest.get("completed_at") is not None else None)
        if str(latest_manifest.get("status")) != "completed":
            alerts.append(
                Alert(
                    code="latest_sync_not_completed",
                    severity="critical",
                    message=f"Latest Binance sync status is {latest_manifest.get('status')}",
                    timestamp=_now_utc(),
                    entity_type="binance_sync",
                    entity_id=str(latest_manifest.get("sync_id") or "latest"),
                    artifact_path=str(config.latest_sync_manifest_path),
                )
            )
            checks.append({"check": "latest_sync_completed", "status": "fail"})
        else:
            checks.append({"check": "latest_sync_completed", "status": "pass"})
        if completed_at is not None and (now - completed_at).total_seconds() > config.latest_sync_max_age_sec:
            alerts.append(
                Alert(
                    code="latest_sync_too_old",
                    severity="warning",
                    message="Latest Binance sync is older than the allowed threshold",
                    timestamp=_now_utc(),
                    entity_type="binance_sync",
                    entity_id=str(latest_manifest.get("sync_id") or "latest"),
                    metric_value=round((now - completed_at).total_seconds(), 6),
                    threshold_value=config.latest_sync_max_age_sec,
                    artifact_path=str(config.latest_sync_manifest_path),
                )
            )
            checks.append({"check": "latest_sync_recent_enough", "status": "warn"})
        else:
            checks.append({"check": "latest_sync_recent_enough", "status": "pass"})

    if not status_payload:
        alerts.append(
            Alert(
                code="missing_status_summary",
                severity="critical",
                message="Binance status summary is missing or unreadable",
                timestamp=_now_utc(),
                entity_type="binance_status",
                entity_id="latest",
                artifact_path=str(config.status_summary_path),
            )
        )
        checks.append({"check": "status_summary_readable", "status": "fail"})
    else:
        checks.append({"check": "status_summary_readable", "status": "pass"})
        records = list(status_payload.get("records") or [])
        stale_records = [record for record in records if record.get("stale")]
        if stale_records:
            severity = "critical" if any(record.get("dataset_family") == "feature" for record in stale_records) else "warning"
            alerts.append(
                Alert(
                    code="stale_binance_datasets",
                    severity=severity,
                    message=f"{len(stale_records)} Binance datasets are stale",
                    timestamp=_now_utc(),
                    entity_type="binance_status",
                    entity_id="freshness",
                    metric_value=len(stale_records),
                    threshold_value=0,
                    artifact_path=str(config.status_summary_path),
                )
            )
            checks.append({"check": "freshness_thresholds", "status": "fail" if severity == "critical" else "warn"})
        else:
            checks.append({"check": "freshness_thresholds", "status": "pass"})

        required_projection_scopes = _required_projection_scopes(config.symbols, config.intervals)
        required_feature_scopes = _required_feature_scopes(config.symbols, config.intervals)
        projection_scopes = {
            (str(record.get("symbol")).upper(), str(record.get("interval")))
            for record in records
            if record.get("dataset_name") == "crypto_ohlcv_bars" and record.get("interval") is not None
        }
        feature_scopes = {
            (str(record.get("symbol")).upper(), str(record.get("interval")))
            for record in records
            if record.get("dataset_name") == "crypto_market_features" and record.get("interval") is not None
        }
        missing_projection = sorted(required_projection_scopes - projection_scopes)
        missing_features = sorted(required_feature_scopes - feature_scopes)
        if config.require_projection_scopes and missing_projection:
            alerts.append(
                Alert(
                    code="missing_projection_scopes",
                    severity="critical",
                    message=f"Missing required Binance projection scopes: {missing_projection}",
                    timestamp=_now_utc(),
                    entity_type="binance_projection_scope",
                    entity_id="required",
                    artifact_path=str(config.status_summary_path),
                )
            )
            checks.append({"check": "required_projection_scopes_present", "status": "fail"})
        else:
            checks.append({"check": "required_projection_scopes_present", "status": "pass"})
        if config.require_feature_scopes and missing_features:
            alerts.append(
                Alert(
                    code="missing_feature_scopes",
                    severity="critical",
                    message=f"Missing required Binance feature scopes: {missing_features}",
                    timestamp=_now_utc(),
                    entity_type="binance_feature_scope",
                    entity_id="required",
                    artifact_path=str(config.status_summary_path),
                )
            )
            checks.append({"check": "required_feature_scopes_present", "status": "fail"})
        else:
            checks.append({"check": "required_feature_scopes_present", "status": "pass"})

    status = _status_from_alerts(alerts)
    payload = {
        "evaluated_at": _now_utc(),
        "status": status,
        "alert_counts": _alert_counts(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
        "checks": checks,
        "latest_sync_manifest_path": str(config.latest_sync_manifest_path),
        "status_summary_path": str(config.status_summary_path),
    }
    summary_path = Path(config.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return BinanceHealthCheckResult(
        summary_path=str(summary_path),
        status=status,
        alert_counts=payload["alert_counts"],
        check_count=len(checks),
    )
