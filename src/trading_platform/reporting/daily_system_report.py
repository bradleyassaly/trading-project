from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


CORE_ARTIFACTS = {
    "trade_outcome_attribution": "trade_outcome_attribution_report.json",
    "calibration": "calibration_summary_report.json",
    "drift": "drift_detection_report.json",
    "decay": "strategy_decay_report.json",
    "lifecycle": "strategy_lifecycle_report.json",
    "risk": "paper_risk_controls.json",
}

OPTIONAL_ARTIFACTS = {
    "strategy_health": "strategy_health_payload.json",
    "realtime_monitoring": "realtime_kpi_monitoring.json",
    "equity_snapshot": "paper_equity_snapshot.csv",
    "positions": "paper_positions.csv",
}

SEVERITY_ORDER = {"critical": 3, "warning": 2, "watch": 1, "info": 0}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _sum(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean))


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _flag_from_score(score: int) -> str:
    if score >= 2:
        return "concerning"
    if score == 1:
        return "mixed"
    return "healthy"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _artifact_path(artifact_dir: Path, filename: str) -> Path:
    return artifact_dir / filename


def _load_artifacts(artifact_dir: Path, *, strict: bool) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    artifacts: dict[str, Any] = {}
    missing_required: list[str] = []

    for key, filename in CORE_ARTIFACTS.items():
        path = _artifact_path(artifact_dir, filename)
        if path.exists():
            artifacts[key] = _read_json(path)
        else:
            warnings.append(f"missing required artifact: {filename}")
            missing_required.append(filename)

    for key, filename in OPTIONAL_ARTIFACTS.items():
        path = _artifact_path(artifact_dir, filename)
        if not path.exists():
            warnings.append(f"missing optional artifact: {filename}")
            continue
        artifacts[key] = _read_csv(path) if path.suffix.lower() == ".csv" else _read_json(path)

    if strict and missing_required:
        missing_text = ", ".join(sorted(missing_required))
        raise FileNotFoundError(f"Missing required artifacts: {missing_text}")
    return artifacts, warnings


def _report_date(artifacts: dict[str, Any]) -> str | None:
    for key in ("trade_outcome_attribution", "calibration", "drift", "decay", "lifecycle", "risk", "realtime_monitoring"):
        payload = artifacts.get(key)
        if isinstance(payload, dict) and payload.get("as_of") is not None:
            return str(payload["as_of"])
    equity_snapshot = artifacts.get("equity_snapshot")
    if isinstance(equity_snapshot, pd.DataFrame) and not equity_snapshot.empty and "as_of" in equity_snapshot.columns:
        return str(equity_snapshot.iloc[-1]["as_of"])
    return None


def _portfolio_summary(artifacts: dict[str, Any], report_date: str | None) -> dict[str, Any]:
    outcome_payload = dict(artifacts.get("trade_outcome_attribution") or {})
    outcomes = list(outcome_payload.get("outcomes") or [])
    summary = dict(outcome_payload.get("summary") or {})
    trade_count = int(summary.get("closed_trade_count", len(outcomes)) or 0)
    total_expected_value_net = _sum([_safe_float(row.get("predicted_net_return")) for row in outcomes])
    total_realized_net_return = _sum([_safe_float(row.get("realized_net_return")) for row in outcomes])
    expected_vs_realized_gap = None
    if total_expected_value_net is not None and total_realized_net_return is not None:
        expected_vs_realized_gap = float(total_realized_net_return - total_expected_value_net)

    strategy_health = dict(artifacts.get("strategy_health") or {})
    active_strategy_count = _safe_float(dict(strategy_health.get("summary") or {}).get("strategy_count"))
    if active_strategy_count is None:
        lifecycle = dict(artifacts.get("lifecycle") or {})
        states = list(lifecycle.get("states") or [])
        active_strategy_count = float(sum(1 for row in states if bool(row.get("active_for_selection", False))))

    latest_drawdown = None
    exposure_summary: dict[str, Any] = {}
    realtime = dict(artifacts.get("realtime_monitoring") or {})
    realtime_metrics = {str(row.get("metric_name")): row for row in list(realtime.get("metrics") or [])}
    if "drawdown" in realtime_metrics:
        latest_drawdown = _safe_float(realtime_metrics["drawdown"].get("metric_value"))
    if "gross_exposure" in realtime_metrics:
        exposure_summary["gross_exposure"] = _safe_float(realtime_metrics["gross_exposure"].get("metric_value"))
    if "net_exposure" in realtime_metrics:
        exposure_summary["net_exposure"] = _safe_float(realtime_metrics["net_exposure"].get("metric_value"))

    equity_snapshot = artifacts.get("equity_snapshot")
    if isinstance(equity_snapshot, pd.DataFrame) and not equity_snapshot.empty:
        equity_series = equity_snapshot["equity"].astype(float) if "equity" in equity_snapshot.columns else pd.Series(dtype=float)
        if latest_drawdown is None and not equity_series.empty:
            running_peak = equity_series.cummax()
            drawdown = 1.0 - (equity_series / running_peak)
            latest_drawdown = float(drawdown.max()) if not drawdown.empty else None
        if "gross_market_value" in equity_snapshot.columns:
            exposure_summary.setdefault("gross_market_value", _safe_float(equity_snapshot.iloc[-1]["gross_market_value"]))
        if "cash" in equity_snapshot.columns:
            exposure_summary.setdefault("cash", _safe_float(equity_snapshot.iloc[-1]["cash"]))
        if "equity" in equity_snapshot.columns:
            exposure_summary.setdefault("equity", _safe_float(equity_snapshot.iloc[-1]["equity"]))

    return {
        "report_date": report_date,
        "trade_count": trade_count,
        "active_strategy_count": int(active_strategy_count) if active_strategy_count is not None else None,
        "total_expected_value_net": total_expected_value_net,
        "total_realized_net_return": total_realized_net_return,
        "expected_vs_realized_gap": expected_vs_realized_gap,
        "total_realized_pnl": _safe_float(summary.get("total_realized_net_pnl")),
        "max_drawdown_or_latest_drawdown": latest_drawdown,
        "exposure_summary": exposure_summary or None,
    }


def _trade_quality_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    outcome_payload = dict(artifacts.get("trade_outcome_attribution") or {})
    outcomes = list(outcome_payload.get("outcomes") or [])
    summary = dict(outcome_payload.get("summary") or {})
    realized_positive = [1.0 if (_safe_float(row.get("realized_net_return")) or 0.0) > 0.0 else 0.0 for row in outcomes if row.get("realized_net_return") is not None]
    return {
        "average_predicted_return": _mean([_safe_float(row.get("predicted_return")) for row in outcomes]),
        "average_expected_value_net": _mean([_safe_float(row.get("predicted_net_return")) for row in outcomes]),
        "average_realized_net_return": _mean([_safe_float(row.get("realized_net_return")) for row in outcomes]),
        "win_rate": _mean(realized_positive),
        "average_expected_cost": _mean([_safe_float(row.get("predicted_cost")) for row in outcomes]),
        "average_realized_cost": _mean([_safe_float(row.get("realized_cost")) for row in outcomes]),
        "attribution_gap_summary": {
            "mean_forecast_gap": _safe_float(summary.get("mean_forecast_gap")),
            "mean_alpha_error": _safe_float(summary.get("mean_alpha_error")),
            "mean_cost_error": _safe_float(summary.get("mean_cost_error")),
            "mean_execution_error": _safe_float(summary.get("mean_execution_error")),
            "regime_mismatch_count": int(summary.get("regime_mismatch_count", 0) or 0),
        },
    }


def _calibration_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    payload = dict(artifacts.get("calibration") or {})
    records = list(payload.get("records") or [])
    buckets = list(payload.get("buckets") or [])
    summary = dict(payload.get("summary") or {})
    confidence_buckets = [
        {
            "bucket_label": str(row.get("bucket_label")),
            "sample_count": int(row.get("sample_count", 0) or 0),
            "raw_mean": _safe_float(row.get("raw_mean")),
            "realized_mean": _safe_float(row.get("realized_mean")),
            "calibrated_mean": _safe_float(row.get("calibrated_mean")),
        }
        for row in buckets
        if str(row.get("calibration_type")) == "confidence"
    ]
    return {
        "number_of_calibrated_trades": int(summary.get("record_count", len(records)) or 0),
        "confidence_bucket_overview": confidence_buckets,
        "raw_vs_calibrated_confidence": {
            "mean_raw_confidence_error": _safe_float(summary.get("mean_raw_confidence_error")),
            "mean_calibrated_confidence_error": _safe_float(summary.get("mean_calibrated_confidence_error")),
        },
        "raw_vs_calibrated_expected_net": {
            "mean_raw_expected_value_error": _safe_float(summary.get("mean_raw_expected_value_error")),
            "mean_calibrated_expected_value_error": _safe_float(summary.get("mean_calibrated_expected_value_error")),
        },
        "top_level_metrics": {
            "bucket_count": int(summary.get("bucket_count", 0) or 0),
            "adjustment_count": int(summary.get("adjustment_count", 0) or 0),
            "sufficient_scope_count": int(summary.get("sufficient_scope_count", 0) or 0),
            "confidence_noop_count": int(summary.get("confidence_noop_count", 0) or 0),
            "expected_value_noop_count": int(summary.get("expected_value_noop_count", 0) or 0),
        },
    }


def _drift_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    payload = dict(artifacts.get("drift") or {})
    signals = list(payload.get("signals") or [])
    summary = dict(payload.get("summary") or {})
    severity_breakdown = dict(summary.get("severity_counts") or {})
    drift_by_type = dict(summary.get("category_counts") or {})
    top_signals = sorted(
        [
            {
                "category": str(row.get("category")),
                "metric_name": str(row.get("metric_name")),
                "scope": str(row.get("scope")),
                "scope_id": str(row.get("scope_id")),
                "severity": str(row.get("severity")),
                "recommended_action": str(row.get("recommended_action")),
                "delta": _safe_float(row.get("delta")),
                "message": str(row.get("message") or ""),
            }
            for row in signals
        ],
        key=lambda row: (SEVERITY_ORDER.get(row["severity"], -1), abs(row["delta"] or 0.0), row["metric_name"]),
        reverse=True,
    )[:5]
    return {
        "total_drift_signal_count": int(summary.get("signal_count", len(signals)) or 0),
        "severity_breakdown": severity_breakdown,
        "drift_by_type": {
            "performance": int(drift_by_type.get("performance", 0) or 0),
            "decision": int(drift_by_type.get("decision", 0) or 0),
            "execution": int(drift_by_type.get("execution", 0) or 0),
        },
        "most_important_triggered_signals": top_signals,
    }


def _decay_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    payload = dict(artifacts.get("decay") or {})
    records = list(payload.get("records") or [])
    recommendations = list(payload.get("recommendations") or [])
    counts = {
        severity: sum(1 for row in records if str(row.get("severity")) == severity)
        for severity in ("healthy", "watch", "warning", "critical")
    }
    top_decay = sorted(
        [
            {
                "strategy_id": str(row.get("strategy_id")),
                "severity": str(row.get("severity")),
                "decay_score": _safe_float(row.get("decay_score")),
                "recommended_action": str(row.get("recommended_action")),
            }
            for row in records
        ],
        key=lambda row: (SEVERITY_ORDER.get(row["severity"], -1), row["decay_score"] or -1.0, row["strategy_id"]),
        reverse=True,
    )[:5]
    action_counts: dict[str, int] = {}
    for row in recommendations:
        action = str(row.get("recommended_action") or "")
        if not action:
            continue
        action_counts[action] = action_counts.get(action, 0) + 1
    return {
        "severity_counts": counts,
        "top_decaying_strategies": top_decay,
        "recommended_lifecycle_actions": action_counts,
    }


def _lifecycle_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    payload = dict(artifacts.get("lifecycle") or {})
    actions = list(payload.get("actions") or [])
    transitions = list(payload.get("transitions") or [])
    retraining_triggers = list(payload.get("retraining_triggers") or [])
    demotions = list(payload.get("demotion_decisions") or [])
    proposed_actions = [row for row in actions if str(row.get("status")) == "proposed"]
    action_counts = {
        action_type: sum(1 for row in proposed_actions if str(row.get("action_type")) == action_type)
        for action_type in ("watch", "constrain", "demote", "retrain")
    }
    affected = sorted(
        [
            {
                "strategy_id": str(row.get("strategy_id")),
                "action_type": str(row.get("action_type")),
                "severity": str(row.get("severity")),
                "status": str(row.get("status")),
            }
            for row in actions
        ],
        key=lambda row: (row["status"] != "proposed", SEVERITY_ORDER.get(row["severity"], -1), row["strategy_id"]),
    )[:5]
    return {
        "action_type_counts": action_counts,
        "state_transition_count": len(transitions),
        "retraining_trigger_count": len(retraining_triggers),
        "demotion_count": sum(1 for row in demotions if bool(row.get("approved_for_demotion"))),
        "top_affected_strategies": affected,
    }


def _risk_summary(artifacts: dict[str, Any]) -> dict[str, Any]:
    payload = dict(artifacts.get("risk") or {})
    triggers = list(payload.get("triggers") or [])
    actions = list(payload.get("actions") or [])
    events = list(payload.get("events") or [])
    drawdown_breach_count = sum(1 for row in triggers if str(row.get("trigger_type")) in {"drawdown_breach", "drawdown_warning"})
    anomaly_types = {
        trigger_type: sum(1 for row in triggers if str(row.get("trigger_type")) == trigger_type)
        for trigger_type in sorted({str(row.get("trigger_type")) for row in triggers if str(row.get("trigger_type"))})
    }
    return {
        "risk_control_state": str(payload.get("operating_state") or "unknown"),
        "triggered_risk_events_count": len(triggers),
        "risk_recommendation_count": len(actions) + len(events),
        "halted_signal_count": sum(1 for row in triggers if str(row.get("operating_state")) == "halted"),
        "restricted_signal_count": sum(1 for row in triggers if str(row.get("operating_state")) == "restricted"),
        "drawdown_breach_count": drawdown_breach_count,
        "anomaly_counts": anomaly_types,
    }


def _evaluation_flags(report: dict[str, Any]) -> dict[str, Any]:
    portfolio = dict(report.get("portfolio_summary") or {})
    calibration = dict(report.get("calibration_summary") or {})
    drift = dict(report.get("drift_summary") or {})
    decay = dict(report.get("decay_summary") or {})
    lifecycle = dict(report.get("lifecycle_summary") or {})

    ev_gap = abs(_safe_float(portfolio.get("expected_vs_realized_gap")) or 0.0)
    total_expected = abs(_safe_float(portfolio.get("total_expected_value_net")) or 0.0)
    ev_alignment_score = 0
    if ev_gap > max(0.05, total_expected * 1.5):
        ev_alignment_score = 2
    elif ev_gap > max(0.02, total_expected):
        ev_alignment_score = 1

    raw_conf_error = _safe_float(dict(calibration.get("raw_vs_calibrated_confidence") or {}).get("mean_raw_confidence_error"))
    cal_conf_error = _safe_float(dict(calibration.get("raw_vs_calibrated_confidence") or {}).get("mean_calibrated_confidence_error"))
    raw_ev_error = _safe_float(dict(calibration.get("raw_vs_calibrated_expected_net") or {}).get("mean_raw_expected_value_error"))
    cal_ev_error = _safe_float(dict(calibration.get("raw_vs_calibrated_expected_net") or {}).get("mean_calibrated_expected_value_error"))
    calibration_score = 0
    if raw_conf_error is None and raw_ev_error is None:
        calibration_score = 1
    elif ((cal_conf_error or 0.0) > (raw_conf_error or 0.0) + 1e-12) or ((cal_ev_error or 0.0) > (raw_ev_error or 0.0) + 1e-12):
        calibration_score = 2
    elif ((cal_conf_error or 0.0) >= (raw_conf_error or 0.0) * 0.98 and raw_conf_error is not None) or (
        (cal_ev_error or 0.0) >= (raw_ev_error or 0.0) * 0.98 and raw_ev_error is not None
    ):
        calibration_score = 1

    severity_breakdown = dict(drift.get("severity_breakdown") or {})
    warning_plus = int(severity_breakdown.get("warning", 0) or 0) + int(severity_breakdown.get("critical", 0) or 0)
    drift_score = 2 if warning_plus >= 3 else 1 if warning_plus >= 1 else 0

    decay_counts = dict(decay.get("severity_counts") or {})
    decay_score = 2 if int(decay_counts.get("critical", 0) or 0) >= 1 else 1 if int(decay_counts.get("warning", 0) or 0) >= 1 else 0

    lifecycle_actions = dict(lifecycle.get("action_type_counts") or {})
    lifecycle_score = 2 if int(lifecycle_actions.get("demote", 0) or 0) + int(lifecycle_actions.get("retrain", 0) or 0) >= 1 else 1 if int(lifecycle_actions.get("constrain", 0) or 0) >= 1 else 0

    overall_score = max(ev_alignment_score, calibration_score, drift_score, decay_score, lifecycle_score)
    if sum(score >= 1 for score in [ev_alignment_score, calibration_score, drift_score, decay_score, lifecycle_score]) >= 3:
        overall_score = max(overall_score, 2)

    return {
        "ev_alignment_flag": _flag_from_score(ev_alignment_score),
        "calibration_signal_flag": _flag_from_score(calibration_score),
        "drift_noise_flag": _flag_from_score(drift_score),
        "decay_churn_flag": _flag_from_score(decay_score),
        "lifecycle_churn_flag": _flag_from_score(lifecycle_score),
        "overall_status": _flag_from_score(overall_score),
    }


def build_daily_system_report(*, artifact_dir: str | Path, strict: bool = False) -> dict[str, Any]:
    directory = Path(artifact_dir)
    artifacts, warnings = _load_artifacts(directory, strict=strict)
    report_date = _report_date(artifacts)
    report: dict[str, Any] = {
        "artifact_dir": str(directory),
        "report_date": report_date,
        "warnings": warnings,
        "portfolio_summary": _portfolio_summary(artifacts, report_date),
        "trade_quality": _trade_quality_summary(artifacts),
        "calibration_summary": _calibration_summary(artifacts),
        "drift_summary": _drift_summary(artifacts),
        "decay_summary": _decay_summary(artifacts),
        "lifecycle_summary": _lifecycle_summary(artifacts),
        "risk_summary": _risk_summary(artifacts),
    }
    report["evaluation_flags"] = _evaluation_flags(report)
    return report


def render_daily_system_report_markdown(report: dict[str, Any]) -> str:
    portfolio = dict(report.get("portfolio_summary") or {})
    flags = dict(report.get("evaluation_flags") or {})
    lines = [
        "# Daily System Report",
        "",
        f"- Report date: `{report.get('report_date')}`",
        f"- Overall status: `{flags.get('overall_status')}`",
        f"- Trade count: `{portfolio.get('trade_count')}`",
        f"- Active strategy count: `{portfolio.get('active_strategy_count')}`",
        f"- Expected vs realized gap: `{portfolio.get('expected_vs_realized_gap')}`",
        "",
        "## Flags",
        f"- EV alignment: `{flags.get('ev_alignment_flag')}`",
        f"- Calibration signal: `{flags.get('calibration_signal_flag')}`",
        f"- Drift noise: `{flags.get('drift_noise_flag')}`",
        f"- Decay churn: `{flags.get('decay_churn_flag')}`",
        f"- Lifecycle churn: `{flags.get('lifecycle_churn_flag')}`",
        "",
        "## Warnings",
    ]
    warnings = list(report.get("warnings") or [])
    if warnings:
        lines.extend([f"- {warning}" for warning in warnings])
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def write_daily_system_report(
    *,
    report: dict[str, Any],
    output_json: str | Path,
    output_md: str | Path | None = None,
) -> dict[str, Path]:
    output_json_path = Path(output_json)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")
    paths = {"output_json_path": output_json_path}
    if output_md is not None:
        output_md_path = Path(output_md)
        output_md_path.parent.mkdir(parents=True, exist_ok=True)
        output_md_path.write_text(render_daily_system_report_markdown(report), encoding="utf-8")
        paths["output_md_path"] = output_md_path
    return paths


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a compact Phase 1.5 daily validation report from paper-trading artifacts.")
    parser.add_argument("--artifact-dir", required=True, help="Paper-trading artifact directory to summarize.")
    parser.add_argument("--output-json", required=True, help="Path to write the deterministic JSON report.")
    parser.add_argument("--output-md", help="Optional path to write a short markdown summary.")
    parser.add_argument("--strict", action="store_true", help="Fail if required artifacts are missing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    report = build_daily_system_report(artifact_dir=args.artifact_dir, strict=bool(args.strict))
    write_daily_system_report(report=report, output_json=args.output_json, output_md=args.output_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
