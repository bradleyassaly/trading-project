from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.governance.persistence import load_strategy_registry
from trading_platform.governance.service import extract_strategy_metrics
from trading_platform.monitoring.models import (
    Alert,
    MonitoringConfig,
    PortfolioHealthReport,
    RunHealthReport,
    StrategyHealthReport,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


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


def _append_history_csv(path: str | Path, rows: list[dict[str, Any]], columns: list[str]) -> Path:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows, columns=columns)
    existing_df = _safe_read_csv(file_path)
    combined = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
    combined.to_csv(file_path, index=False)
    return file_path


def _write_alert_artifacts(output_dir: Path, alerts: list[Alert]) -> dict[str, Path]:
    alerts_json = output_dir / "alerts.json"
    alerts_csv = output_dir / "alerts.csv"
    rows = [alert.to_dict() for alert in alerts]
    alerts_json.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(rows).to_csv(alerts_csv, index=False)
    return {"alerts_json_path": alerts_json, "alerts_csv_path": alerts_csv}


def _resolve_entry_paths(entry, artifacts_root: str | Path | None):
    if artifacts_root is None:
        return entry
    root = Path(artifacts_root)

    def _resolve(path: str | None) -> str | None:
        if path is None:
            return None
        file_path = Path(path)
        return str(file_path if file_path.is_absolute() else root / file_path)

    return replace(
        entry,
        research_artifact_paths=[_resolve(path) or "" for path in entry.research_artifact_paths],
        paper_artifact_path=_resolve(entry.paper_artifact_path),
        live_artifact_path=_resolve(entry.live_artifact_path),
    )


def _render_run_health_markdown(report: RunHealthReport) -> str:
    lines = [
        f"# Run Health: {report.run_name}",
        "",
        f"- Status: `{report.status}`",
        f"- Evaluated at: `{report.evaluated_at}`",
        f"- Failed stages: `{report.metrics.get('failed_stage_count')}`",
        f"- Generated positions: `{report.metrics.get('generated_position_count')}`",
        f"- Gross exposure: `{report.metrics.get('gross_exposure')}`",
        f"- Net exposure: `{report.metrics.get('net_exposure')}`",
        "",
        "## Alert Counts",
        f"- info: `{report.alert_counts['info']}`",
        f"- warning: `{report.alert_counts['warning']}`",
        f"- critical: `{report.alert_counts['critical']}`",
    ]
    if report.alerts:
        lines.extend(["", "## Alerts"])
        for alert in report.alerts:
            lines.append(f"- `{alert.severity}` `{alert.code}`: {alert.message}")
    return "\n".join(lines) + "\n"


def _render_strategy_alerts_markdown(report: StrategyHealthReport) -> str:
    lines = [
        "# Strategy Alerts",
        "",
        f"- Status: `{report.status}`",
        f"- Evaluated at: `{report.evaluated_at}`",
        "",
        "## Alert Counts",
        f"- info: `{report.alert_counts['info']}`",
        f"- warning: `{report.alert_counts['warning']}`",
        f"- critical: `{report.alert_counts['critical']}`",
    ]
    if report.alerts:
        lines.extend(["", "## Alerts"])
        for alert in report.alerts:
            lines.append(f"- `{alert.entity_id}` `{alert.severity}` `{alert.code}`: {alert.message}")
    return "\n".join(lines) + "\n"


def _render_portfolio_health_markdown(report: PortfolioHealthReport) -> str:
    lines = [
        "# Portfolio Health",
        "",
        f"- Status: `{report.status}`",
        f"- Evaluated at: `{report.evaluated_at}`",
        f"- Position count: `{report.metrics.get('position_count')}`",
        f"- Gross exposure: `{report.metrics.get('gross_exposure')}`",
        f"- Net exposure: `{report.metrics.get('net_exposure')}`",
        f"- Max symbol concentration: `{report.metrics.get('max_symbol_concentration')}`",
        f"- Turnover: `{report.metrics.get('turnover_estimate')}`",
    ]
    if report.alerts:
        lines.extend(["", "## Alerts"])
        for alert in report.alerts:
            lines.append(f"- `{alert.severity}` `{alert.code}`: {alert.message}")
    return "\n".join(lines) + "\n"


def _required_run_artifacts(stage_names: set[str], run_dir: Path, *, include_core_artifacts: bool) -> list[Path]:
    required: list[Path] = []
    if include_core_artifacts:
        required.extend(
            [
                run_dir / "run_summary.json",
                run_dir / "run_summary.md",
                run_dir / "stage_status.csv",
                run_dir / "pipeline_config_snapshot.json",
            ]
        )
    if "portfolio_allocation" in stage_names:
        required.extend(
            [
                run_dir / "portfolio_allocation" / "allocation_summary.json",
                run_dir / "portfolio_allocation" / "combined_target_weights.csv",
            ]
        )
    if "paper_trading" in stage_names:
        required.append(run_dir / "paper_trading" / "paper_run_summary_latest.json")
    if "live_dry_run" in stage_names:
        required.append(run_dir / "live_dry_run" / "live_dry_run_summary.json")
    return required


def _rolling_sharpe_from_equity_curve(equity_curve: pd.DataFrame) -> float | None:
    if equity_curve.empty or "equity" not in equity_curve.columns:
        return None
    returns = equity_curve["equity"].pct_change().dropna()
    if len(returns) < 2 or returns.std() == 0:
        return None
    return float((returns.mean() / returns.std()) * (252**0.5))


def _drawdown_from_equity_curve(equity_curve: pd.DataFrame) -> float | None:
    if equity_curve.empty or "equity" not in equity_curve.columns:
        return None
    running_max = equity_curve["equity"].cummax()
    drawdown = (equity_curve["equity"] / running_max) - 1.0
    return float(abs(drawdown.min()))


def _rolling_return_from_equity_curve(equity_curve: pd.DataFrame) -> float | None:
    if equity_curve.empty or "equity" not in equity_curve.columns:
        return None
    first = float(equity_curve["equity"].iloc[0])
    last = float(equity_curve["equity"].iloc[-1])
    if first <= 0:
        return None
    return float((last / first) - 1.0)


def _selection_instability(paper_summary_df: pd.DataFrame) -> float | None:
    if paper_summary_df.empty or "target_names" not in paper_summary_df.columns or len(paper_summary_df) < 2:
        return None
    prev = {name for name in str(paper_summary_df.iloc[-2]["target_names"]).split(",") if name}
    current = {name for name in str(paper_summary_df.iloc[-1]["target_names"]).split(",") if name}
    union = prev | current
    if not union:
        return 0.0
    return float(len(prev ^ current) / len(union))


def _evaluate_run_health_payload(
    *,
    run_dir: Path,
    run_payload: dict[str, Any],
    config: MonitoringConfig,
    output_dir: Path,
    include_core_artifacts: bool,
) -> tuple[RunHealthReport, dict[str, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluated_at = _now_utc()
    alerts: list[Alert] = []
    stage_records = run_payload.get("stage_records", [])
    outputs = run_payload.get("outputs", {})
    failed_stage_count = sum(1 for record in stage_records if record.get("status") == "failed")
    succeeded_stage_names = {
        str(record.get("stage_name"))
        for record in stage_records
        if record.get("status") == "succeeded"
    }

    if failed_stage_count > config.maximum_failed_stages:
        alerts.append(
            Alert(
                code="failed_stages",
                severity="critical",
                message=f"failed_stage_count={failed_stage_count} exceeds maximum_failed_stages={config.maximum_failed_stages}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=failed_stage_count,
                threshold_value=config.maximum_failed_stages,
            )
        )

    required_artifacts = _required_run_artifacts(
        succeeded_stage_names,
        run_dir,
        include_core_artifacts=include_core_artifacts,
    )
    missing_artifacts = [str(path) for path in required_artifacts if not path.exists()]
    for artifact in missing_artifacts:
        alerts.append(
            Alert(
                code="missing_artifact",
                severity="critical",
                message=f"required artifact missing: {artifact}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                artifact_path=artifact,
            )
        )

    if config.stale_artifact_max_age_hours is not None:
        now_dt = _iso_to_dt(evaluated_at) or datetime.now(UTC)
        for path in required_artifacts:
            if not path.exists():
                continue
            age_hours = (now_dt - datetime.fromtimestamp(path.stat().st_mtime, UTC)).total_seconds() / 3600.0
            if age_hours > config.stale_artifact_max_age_hours:
                alerts.append(
                    Alert(
                        code="stale_artifact",
                        severity="warning",
                        message=f"{path} is stale at {age_hours:.2f} hours",
                        timestamp=evaluated_at,
                        entity_type="run",
                        entity_id=run_payload.get("run_name", run_dir.name),
                        metric_value=round(age_hours, 6),
                        threshold_value=config.stale_artifact_max_age_hours,
                        artifact_path=str(path),
                    )
                )

    generated_position_count = 0
    gross_exposure = None
    net_exposure = None
    max_symbol_concentration = 0.0
    turnover_estimate = None
    max_drift = 0.0
    zero_weight_rows = 0
    approved_strategy_count = len(outputs.get("multi_strategy_selected_strategies", []))
    live_order_count = None

    allocation_summary = _safe_read_json(run_dir / "portfolio_allocation" / "allocation_summary.json")
    combined_targets = _safe_read_csv(run_dir / "portfolio_allocation" / "combined_target_weights.csv")
    sleeve_targets = _safe_read_csv(run_dir / "portfolio_allocation" / "sleeve_target_weights.csv")
    if not combined_targets.empty:
        generated_position_count = int(len(combined_targets))
        max_symbol_concentration = float(combined_targets["target_weight"].abs().max())
        zero_weight_rows = int((combined_targets["target_weight"].abs() <= 1e-12).sum())
    if allocation_summary:
        summary = allocation_summary.get("summary", allocation_summary)
        gross_exposure = summary.get("gross_exposure_after_constraints")
        net_exposure = abs(float(summary.get("net_exposure_after_constraints", 0.0)))
        turnover_estimate = summary.get("turnover_estimate")
    if not sleeve_targets.empty and not combined_targets.empty:
        sleeve_sum = sleeve_targets.groupby("symbol", dropna=False)["scaled_target_weight"].sum().rename("sleeve_weight").reset_index()
        merged = combined_targets.merge(sleeve_sum, on="symbol", how="left").fillna({"sleeve_weight": 0.0})
        max_drift = float((merged["target_weight"] - merged["sleeve_weight"]).abs().max())

    portfolio_stage_available = (
        "portfolio_allocation" in succeeded_stage_names
        or bool(allocation_summary)
        or not combined_targets.empty
    )

    if portfolio_stage_available and generated_position_count < config.minimum_generated_position_count:
        alerts.append(
            Alert(
                code="empty_or_small_portfolio",
                severity="critical",
                message=f"generated_position_count={generated_position_count} is below minimum_generated_position_count={config.minimum_generated_position_count}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=generated_position_count,
                threshold_value=config.minimum_generated_position_count,
            )
        )
    if config.minimum_approved_strategy_count is not None and approved_strategy_count < config.minimum_approved_strategy_count:
        alerts.append(
            Alert(
                code="approved_strategy_count",
                severity="critical",
                message=f"approved_strategy_count={approved_strategy_count} is below minimum_approved_strategy_count={config.minimum_approved_strategy_count}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=approved_strategy_count,
                threshold_value=config.minimum_approved_strategy_count,
            )
        )
    if portfolio_stage_available and config.maximum_gross_exposure is not None and gross_exposure is not None and float(gross_exposure) > config.maximum_gross_exposure:
        alerts.append(
            Alert(
                code="gross_exposure",
                severity="critical",
                message=f"gross_exposure={gross_exposure} exceeds maximum_gross_exposure={config.maximum_gross_exposure}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=gross_exposure,
                threshold_value=config.maximum_gross_exposure,
            )
        )
    if portfolio_stage_available and config.maximum_net_exposure is not None and net_exposure is not None and float(net_exposure) > config.maximum_net_exposure:
        alerts.append(
            Alert(
                code="net_exposure",
                severity="critical",
                message=f"net_exposure={net_exposure} exceeds maximum_net_exposure={config.maximum_net_exposure}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=net_exposure,
                threshold_value=config.maximum_net_exposure,
            )
        )
    if portfolio_stage_available and config.maximum_symbol_concentration is not None and max_symbol_concentration > config.maximum_symbol_concentration:
        alerts.append(
            Alert(
                code="symbol_concentration",
                severity="critical",
                message=f"max_symbol_concentration={max_symbol_concentration} exceeds maximum_symbol_concentration={config.maximum_symbol_concentration}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=max_symbol_concentration,
                threshold_value=config.maximum_symbol_concentration,
            )
        )
    if portfolio_stage_available and config.maximum_turnover is not None and turnover_estimate is not None and float(turnover_estimate) > config.maximum_turnover:
        alerts.append(
            Alert(
                code="turnover",
                severity="warning",
                message=f"turnover_estimate={turnover_estimate} exceeds maximum_turnover={config.maximum_turnover}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=turnover_estimate,
                threshold_value=config.maximum_turnover,
            )
        )
    if portfolio_stage_available and config.maximum_zero_weight_runs is not None and zero_weight_rows > config.maximum_zero_weight_runs:
        alerts.append(
            Alert(
                code="zero_weight_rows",
                severity="warning",
                message=f"zero_weight_rows={zero_weight_rows} exceeds maximum_zero_weight_runs={config.maximum_zero_weight_runs}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=zero_weight_rows,
                threshold_value=config.maximum_zero_weight_runs,
            )
        )
    if (
        portfolio_stage_available
        and config.max_drift_between_sleeve_target_and_final_combined_weight is not None
        and max_drift > config.max_drift_between_sleeve_target_and_final_combined_weight
    ):
        alerts.append(
            Alert(
                code="sleeve_to_combined_drift",
                severity="warning",
                message=f"max_drift_between_sleeve_target_and_final_combined_weight={max_drift} exceeds threshold={config.max_drift_between_sleeve_target_and_final_combined_weight}",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
                metric_value=max_drift,
                threshold_value=config.max_drift_between_sleeve_target_and_final_combined_weight,
            )
        )

    paper_summary = _safe_read_json(run_dir / "paper_trading" / "paper_run_summary_latest.json")
    if "paper_trading" in succeeded_stage_names and not paper_summary:
        alerts.append(
            Alert(
                code="empty_paper_output",
                severity="critical",
                message="paper_trading stage succeeded but paper_run_summary_latest.json is empty or unreadable",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
            )
        )
    live_summary = _safe_read_json(run_dir / "live_dry_run" / "live_dry_run_summary.json")
    if "live_dry_run" in succeeded_stage_names and not live_summary:
        alerts.append(
            Alert(
                code="empty_live_output",
                severity="critical",
                message="live_dry_run stage succeeded but live_dry_run_summary.json is empty or unreadable",
                timestamp=evaluated_at,
                entity_type="run",
                entity_id=run_payload.get("run_name", run_dir.name),
            )
        )
    if live_summary:
        live_order_count = int(live_summary.get("adjusted_order_count", 0) or 0)

    metrics = {
        "failed_stage_count": failed_stage_count,
        "missing_artifact_count": len(missing_artifacts),
        "approved_strategy_count": approved_strategy_count,
        "generated_position_count": generated_position_count,
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
        "max_symbol_concentration": max_symbol_concentration,
        "turnover_estimate": turnover_estimate,
        "max_drift_between_sleeve_target_and_final_combined_weight": max_drift,
        "zero_weight_rows": zero_weight_rows,
        "live_order_count": live_order_count,
    }
    report = RunHealthReport(
        run_dir=str(run_dir),
        run_name=run_payload.get("run_name", run_dir.name),
        evaluated_at=evaluated_at,
        status=_status_from_alerts(alerts),
        metrics=metrics,
        alert_counts=_alert_counts(alerts),
        alerts=alerts,
    )

    run_health_json = output_dir / "run_health.json"
    run_health_md = output_dir / "run_health.md"
    run_health_json.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    run_health_md.write_text(_render_run_health_markdown(report), encoding="utf-8")
    paths = {
        "run_health_json_path": run_health_json,
        "run_health_md_path": run_health_md,
    }
    paths.update(_write_alert_artifacts(output_dir, alerts))
    paths["run_history_path"] = _append_history_csv(
        output_dir / "run_history.csv",
        [
            {
                "evaluated_at": evaluated_at,
                "run_name": report.run_name,
                "run_dir": report.run_dir,
                "status": report.status,
                "failed_stage_count": failed_stage_count,
                "generated_position_count": generated_position_count,
                "gross_exposure": gross_exposure,
                "net_exposure": net_exposure,
                "critical_alert_count": report.alert_counts["critical"],
                "warning_alert_count": report.alert_counts["warning"],
            }
        ],
        [
            "evaluated_at",
            "run_name",
            "run_dir",
            "status",
            "failed_stage_count",
            "generated_position_count",
            "gross_exposure",
            "net_exposure",
            "critical_alert_count",
            "warning_alert_count",
        ],
    )
    return report, paths


def evaluate_run_health(
    *,
    run_dir: str | Path,
    config: MonitoringConfig,
    output_dir: str | Path | None = None,
) -> tuple[RunHealthReport, dict[str, Path]]:
    run_path = Path(run_dir)
    payload = _safe_read_json(run_path / "run_summary.json")
    if not payload:
        raise FileNotFoundError(f"run_summary.json not found or unreadable under {run_path}")
    return _evaluate_run_health_payload(
        run_dir=run_path,
        run_payload=payload,
        config=config,
        output_dir=Path(output_dir) if output_dir is not None else run_path / "monitoring",
        include_core_artifacts=True,
    )


def evaluate_run_health_snapshot(
    *,
    run_dir: str | Path,
    run_payload: dict[str, Any],
    config: MonitoringConfig,
    output_dir: str | Path,
) -> tuple[RunHealthReport, dict[str, Path]]:
    return _evaluate_run_health_payload(
        run_dir=Path(run_dir),
        run_payload=run_payload,
        config=config,
        output_dir=Path(output_dir),
        include_core_artifacts=False,
    )


def evaluate_strategy_health(
    *,
    registry_path: str | Path,
    artifacts_root: str | Path | None,
    config: MonitoringConfig,
    output_dir: str | Path,
) -> tuple[StrategyHealthReport, dict[str, Path]]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    evaluated_at = _now_utc()
    registry = load_strategy_registry(registry_path)
    alerts: list[Alert] = []
    rows: list[dict[str, Any]] = []

    for raw_entry in sorted(registry.entries, key=lambda item: item.strategy_id):
        entry = _resolve_entry_paths(raw_entry, artifacts_root)
        artifact_failure_count = sum(1 for path in entry.research_artifact_paths if not Path(path).exists())
        snapshot = extract_strategy_metrics(entry)
        paper_dir = Path(entry.paper_artifact_path) if entry.paper_artifact_path else None
        paper_equity_curve = _safe_read_csv(paper_dir / "paper_equity_curve.csv") if paper_dir else pd.DataFrame()
        paper_summary_df = _safe_read_csv(paper_dir / "paper_run_summary.csv") if paper_dir else pd.DataFrame()
        paper_health_checks = _safe_read_csv(paper_dir / "paper_health_checks.csv") if paper_dir else pd.DataFrame()

        rolling_return = _rolling_return_from_equity_curve(paper_equity_curve)
        rolling_sharpe = _rolling_sharpe_from_equity_curve(paper_equity_curve) or snapshot.metrics.get("sharpe")
        drawdown = _drawdown_from_equity_curve(paper_equity_curve) or snapshot.metrics.get("max_drawdown")
        turnover_estimate = snapshot.metrics.get("paper_turnover_estimate") or snapshot.metrics.get("turnover")
        benchmark_underperformance = snapshot.metrics.get("rolling_underperformance_vs_benchmark")
        missing_data_incidents = int(snapshot.metrics.get("paper_missing_data_failures") or 0)
        selection_instability = _selection_instability(paper_summary_df)
        latest_turnover = (
            float(paper_summary_df.iloc[-1]["turnover_estimate"])
            if not paper_summary_df.empty and "turnover_estimate" in paper_summary_df.columns
            else turnover_estimate
        )
        zero_weight_runs = int((paper_summary_df["target_selected_count"].fillna(0) <= 0).sum()) if not paper_summary_df.empty and "target_selected_count" in paper_summary_df.columns else 0

        start_alert_count = len(alerts)
        if artifact_failure_count > 0:
            alerts.append(
                Alert(
                    code="missing_strategy_artifacts",
                    severity="critical",
                    message=f"{artifact_failure_count} research artifact path(s) missing",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=artifact_failure_count,
                )
            )
        if config.maximum_drawdown is not None and drawdown is not None and float(drawdown) > config.maximum_drawdown:
            alerts.append(
                Alert(
                    code="strategy_drawdown",
                    severity="critical",
                    message=f"drawdown={drawdown} exceeds maximum_drawdown={config.maximum_drawdown}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=drawdown,
                    threshold_value=config.maximum_drawdown,
                )
            )
        if config.minimum_rolling_sharpe is not None and rolling_sharpe is not None and float(rolling_sharpe) < config.minimum_rolling_sharpe:
            alerts.append(
                Alert(
                    code="rolling_sharpe",
                    severity="warning",
                    message=f"rolling_sharpe={rolling_sharpe} is below minimum_rolling_sharpe={config.minimum_rolling_sharpe}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=rolling_sharpe,
                    threshold_value=config.minimum_rolling_sharpe,
                )
            )
        if config.maximum_benchmark_underperformance is not None and benchmark_underperformance is not None and float(benchmark_underperformance) > config.maximum_benchmark_underperformance:
            alerts.append(
                Alert(
                    code="benchmark_underperformance",
                    severity="warning",
                    message=f"benchmark_underperformance={benchmark_underperformance} exceeds maximum_benchmark_underperformance={config.maximum_benchmark_underperformance}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=benchmark_underperformance,
                    threshold_value=config.maximum_benchmark_underperformance,
                )
            )
        if config.maximum_turnover is not None and latest_turnover is not None and float(latest_turnover) > config.maximum_turnover:
            alerts.append(
                Alert(
                    code="strategy_turnover",
                    severity="warning",
                    message=f"turnover={latest_turnover} exceeds maximum_turnover={config.maximum_turnover}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=latest_turnover,
                    threshold_value=config.maximum_turnover,
                )
            )
        if config.maximum_missing_data_incidents is not None and missing_data_incidents > config.maximum_missing_data_incidents:
            alerts.append(
                Alert(
                    code="missing_data_incidents",
                    severity="critical",
                    message=f"missing_data_incidents={missing_data_incidents} exceeds maximum_missing_data_incidents={config.maximum_missing_data_incidents}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=missing_data_incidents,
                    threshold_value=config.maximum_missing_data_incidents,
                )
            )
        if config.maximum_zero_weight_runs is not None and zero_weight_runs > config.maximum_zero_weight_runs:
            alerts.append(
                Alert(
                    code="zero_weight_runs",
                    severity="warning",
                    message=f"zero_weight_runs={zero_weight_runs} exceeds maximum_zero_weight_runs={config.maximum_zero_weight_runs}",
                    timestamp=evaluated_at,
                    entity_type="strategy",
                    entity_id=entry.strategy_id,
                    metric_value=zero_weight_runs,
                    threshold_value=config.maximum_zero_weight_runs,
                )
            )

        strategy_specific_alerts = [alert for alert in alerts if alert.entity_id == entry.strategy_id]
        health_failures = int((paper_health_checks["status"] == "fail").sum()) if not paper_health_checks.empty and "status" in paper_health_checks.columns else 0
        rows.append(
            {
                "strategy_id": entry.strategy_id,
                "family": entry.family,
                "version": entry.version,
                "status": _status_from_alerts(strategy_specific_alerts),
                "registry_status": entry.status,
                "rolling_return": rolling_return,
                "drawdown": drawdown,
                "rolling_sharpe": rolling_sharpe,
                "benchmark_underperformance": benchmark_underperformance,
                "turnover_estimate": latest_turnover,
                "selection_instability": selection_instability,
                "missing_data_incidents": missing_data_incidents,
                "artifact_failure_count": artifact_failure_count,
                "health_failure_count": health_failures,
                "alert_count": len(alerts) - start_alert_count,
            }
        )

    report = StrategyHealthReport(
        evaluated_at=evaluated_at,
        status=_status_from_alerts(alerts),
        strategy_rows=rows,
        alert_counts=_alert_counts(alerts),
        alerts=alerts,
    )
    csv_path = output_path / "strategy_health.csv"
    json_path = output_path / "strategy_health.json"
    md_path = output_path / "strategy_alerts.md"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_strategy_alerts_markdown(report), encoding="utf-8")
    history_path = _append_history_csv(
        output_path / "strategy_health_history.csv",
        [{"evaluated_at": evaluated_at, **row} for row in rows],
        [
            "evaluated_at",
            "strategy_id",
            "family",
            "version",
            "status",
            "registry_status",
            "rolling_return",
            "drawdown",
            "rolling_sharpe",
            "benchmark_underperformance",
            "turnover_estimate",
            "selection_instability",
            "missing_data_incidents",
            "artifact_failure_count",
            "health_failure_count",
            "alert_count",
        ],
    )
    paths = {
        "strategy_health_csv_path": csv_path,
        "strategy_health_json_path": json_path,
        "strategy_alerts_md_path": md_path,
        "strategy_health_history_path": history_path,
    }
    paths.update(_write_alert_artifacts(output_path, alerts))
    return report, paths


def evaluate_portfolio_health(
    *,
    allocation_dir: str | Path,
    config: MonitoringConfig,
    output_dir: str | Path,
) -> tuple[PortfolioHealthReport, dict[str, Path]]:
    allocation_path = Path(allocation_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    evaluated_at = _now_utc()
    alerts: list[Alert] = []
    summary_payload = _safe_read_json(allocation_path / "allocation_summary.json")
    if not summary_payload:
        raise FileNotFoundError(f"allocation_summary.json not found or unreadable under {allocation_path}")
    summary = summary_payload.get("summary", summary_payload)
    combined = _safe_read_csv(allocation_path / "combined_target_weights.csv")
    sleeve_attr = _safe_read_csv(allocation_path / "sleeve_attribution.csv")
    overlap = _safe_read_csv(allocation_path / "symbol_overlap_report.csv")

    position_count = int(len(combined))
    gross_exposure = float(summary.get("gross_exposure_after_constraints", 0.0))
    net_exposure = abs(float(summary.get("net_exposure_after_constraints", 0.0)))
    max_symbol_concentration = float(combined["target_weight"].abs().max()) if not combined.empty else 0.0
    turnover_estimate = float(summary.get("turnover_estimate", 0.0) or 0.0)
    overlap_concentration = float(summary.get("overlap_concentration", 0.0) or 0.0)
    effective_sleeves = float(summary.get("effective_number_of_sleeves", 0.0) or 0.0)
    effective_positions = float(summary.get("effective_number_of_positions", 0.0) or 0.0)
    top_position_dominance = max_symbol_concentration
    clipped_symbol_count = len(summary.get("symbols_removed_or_clipped", []))
    long_gross = float(combined.loc[combined["target_weight"] > 0, "target_weight"].sum()) if not combined.empty else 0.0
    short_gross = float(abs(combined.loc[combined["target_weight"] < 0, "target_weight"].sum())) if not combined.empty else 0.0
    long_short_balance = abs(long_gross - short_gross)

    if position_count < config.minimum_generated_position_count:
        alerts.append(
            Alert(
                code="portfolio_position_count",
                severity="critical",
                message=f"position_count={position_count} is below minimum_generated_position_count={config.minimum_generated_position_count}",
                timestamp=evaluated_at,
                entity_type="portfolio",
                entity_id=str(allocation_path),
                metric_value=position_count,
                threshold_value=config.minimum_generated_position_count,
            )
        )
    if config.maximum_gross_exposure is not None and gross_exposure > config.maximum_gross_exposure:
        alerts.append(
            Alert(
                code="portfolio_gross_exposure",
                severity="critical",
                message=f"gross_exposure={gross_exposure} exceeds maximum_gross_exposure={config.maximum_gross_exposure}",
                timestamp=evaluated_at,
                entity_type="portfolio",
                entity_id=str(allocation_path),
                metric_value=gross_exposure,
                threshold_value=config.maximum_gross_exposure,
            )
        )
    if config.maximum_net_exposure is not None and net_exposure > config.maximum_net_exposure:
        alerts.append(
            Alert(
                code="portfolio_net_exposure",
                severity="critical",
                message=f"net_exposure={net_exposure} exceeds maximum_net_exposure={config.maximum_net_exposure}",
                timestamp=evaluated_at,
                entity_type="portfolio",
                entity_id=str(allocation_path),
                metric_value=net_exposure,
                threshold_value=config.maximum_net_exposure,
            )
        )
    if config.maximum_symbol_concentration is not None and max_symbol_concentration > config.maximum_symbol_concentration:
        alerts.append(
            Alert(
                code="portfolio_concentration",
                severity="critical",
                message=f"max_symbol_concentration={max_symbol_concentration} exceeds maximum_symbol_concentration={config.maximum_symbol_concentration}",
                timestamp=evaluated_at,
                entity_type="portfolio",
                entity_id=str(allocation_path),
                metric_value=max_symbol_concentration,
                threshold_value=config.maximum_symbol_concentration,
            )
        )
    if config.maximum_turnover is not None and turnover_estimate > config.maximum_turnover:
        alerts.append(
            Alert(
                code="portfolio_turnover",
                severity="warning",
                message=f"turnover_estimate={turnover_estimate} exceeds maximum_turnover={config.maximum_turnover}",
                timestamp=evaluated_at,
                entity_type="portfolio",
                entity_id=str(allocation_path),
                metric_value=turnover_estimate,
                threshold_value=config.maximum_turnover,
            )
        )

    metrics = {
        "position_count": position_count,
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
        "max_symbol_concentration": max_symbol_concentration,
        "overlap_concentration": overlap_concentration,
        "effective_number_of_sleeves": effective_sleeves,
        "effective_number_of_positions": effective_positions,
        "top_position_dominance": top_position_dominance,
        "turnover_estimate": turnover_estimate,
        "long_short_balance": long_short_balance,
        "clipped_symbol_count": clipped_symbol_count,
        "sleeve_count": int(len(sleeve_attr)),
        "overlap_symbol_count": int(len(overlap.loc[overlap["sleeve_count"] > 1])) if not overlap.empty and "sleeve_count" in overlap.columns else 0,
    }
    report = PortfolioHealthReport(
        allocation_dir=str(allocation_path),
        evaluated_at=evaluated_at,
        status=_status_from_alerts(alerts),
        metrics=metrics,
        alert_counts=_alert_counts(alerts),
        alerts=alerts,
    )
    csv_path = output_path / "portfolio_health.csv"
    json_path = output_path / "portfolio_health.json"
    md_path = output_path / "portfolio_health.md"
    pd.DataFrame([metrics | {"status": report.status, "evaluated_at": evaluated_at}]).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_portfolio_health_markdown(report), encoding="utf-8")
    history_path = _append_history_csv(
        output_path / "portfolio_health_history.csv",
        [{"evaluated_at": evaluated_at, "status": report.status, **metrics}],
        [
            "evaluated_at",
            "status",
            "position_count",
            "gross_exposure",
            "net_exposure",
            "max_symbol_concentration",
            "overlap_concentration",
            "effective_number_of_sleeves",
            "effective_number_of_positions",
            "top_position_dominance",
            "turnover_estimate",
            "long_short_balance",
            "clipped_symbol_count",
            "sleeve_count",
            "overlap_symbol_count",
        ],
    )
    paths = {
        "portfolio_health_csv_path": csv_path,
        "portfolio_health_json_path": json_path,
        "portfolio_health_md_path": md_path,
        "portfolio_health_history_path": history_path,
    }
    paths.update(_write_alert_artifacts(output_path, alerts))
    return report, paths


def find_latest_pipeline_run_dir(pipeline_root: str | Path) -> Path:
    root = Path(pipeline_root)
    candidates = list(root.rglob("run_summary.json"))
    if not candidates:
        raise FileNotFoundError(f"No run_summary.json files found under {root}")
    return max(candidates, key=lambda path: path.stat().st_mtime).parent


def build_dashboard_data(
    *,
    pipeline_root: str | Path,
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(pipeline_root)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for run_summary_path in sorted(root.rglob("run_summary.json")):
        payload = _safe_read_json(run_summary_path)
        run_dir = run_summary_path.parent
        run_health = _safe_read_json(run_dir / "monitoring" / "run_health.json")
        rows.append(
            {
                "run_name": payload.get("run_name", run_dir.name),
                "run_dir": str(run_dir),
                "started_at": payload.get("started_at"),
                "status": payload.get("status"),
                "health_status": run_health.get("status"),
                "critical_alert_count": run_health.get("alert_counts", {}).get("critical", 0),
                "warning_alert_count": run_health.get("alert_counts", {}).get("warning", 0),
            }
        )
    dashboard_csv = output_path / "dashboard_runs.csv"
    dashboard_json = output_path / "dashboard_runs.json"
    pd.DataFrame(rows).to_csv(dashboard_csv, index=False)
    dashboard_json.write_text(json.dumps({"runs": rows}, indent=2, default=str), encoding="utf-8")
    return {
        "dashboard_runs_csv_path": dashboard_csv,
        "dashboard_runs_json_path": dashboard_json,
    }
