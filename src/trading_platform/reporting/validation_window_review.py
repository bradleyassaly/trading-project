from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


SUMMARY_FILENAME = "daily_validation_run_summary.json"
DEFAULT_REPORT_FILENAME = "daily_system_report.json"


@dataclass(frozen=True)
class LoadedValidationRun:
    run_dir: Path
    run_timestamp: datetime
    report: dict[str, Any]
    summary: dict[str, Any]


def _now_local() -> datetime:
    return datetime.now().astimezone()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return float(sum(clean)) if clean else None


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return float(sum(clean) / len(clean)) if clean else None


def _status_from_score(score: int, *, insufficient: bool = False) -> str:
    if insufficient:
        return "insufficient_data"
    if score >= 2:
        return "concerning"
    if score == 1:
        return "mixed"
    return "healthy"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _resolve_report_path(run_dir: Path, summary: dict[str, Any]) -> Path:
    raw = summary.get("report_json_path")
    if raw:
        return Path(str(raw))
    return run_dir / DEFAULT_REPORT_FILENAME


def _candidate_run_dirs(validation_root: Path) -> list[Path]:
    if not validation_root.exists():
        raise FileNotFoundError(f"Validation root not found: {validation_root}")
    if not validation_root.is_dir():
        raise NotADirectoryError(f"Validation root is not a directory: {validation_root}")
    candidates = [path for path in validation_root.iterdir() if path.is_dir()]
    if (validation_root / SUMMARY_FILENAME).exists():
        candidates.append(validation_root)
    return sorted(set(candidates), key=lambda path: path.name)


def _load_validation_runs(
    *,
    validation_root: Path,
    strict: bool,
    days: int | None,
    max_runs: int | None,
) -> tuple[list[LoadedValidationRun], list[str], int]:
    warnings: list[str] = []
    loaded: list[LoadedValidationRun] = []
    found = 0
    cutoff = _now_local() - timedelta(days=days) if days is not None else None

    for run_dir in _candidate_run_dirs(validation_root):
        found += 1
        summary_path = run_dir / SUMMARY_FILENAME
        if not summary_path.exists():
            message = f"skipped run without summary artifact: {run_dir}"
            if strict:
                raise FileNotFoundError(message)
            warnings.append(message)
            continue
        try:
            summary = _read_json(summary_path)
            run_timestamp = _parse_timestamp(str(summary["run_timestamp"]))
            report_path = _resolve_report_path(run_dir, summary)
            if not report_path.exists():
                raise FileNotFoundError(f"missing daily system report: {report_path}")
            report = _read_json(report_path)
        except Exception as exc:
            message = f"skipped invalid validation run {run_dir}: {exc}"
            if strict:
                raise
            warnings.append(message)
            continue
        if cutoff is not None and run_timestamp < cutoff:
            continue
        loaded.append(
            LoadedValidationRun(
                run_dir=run_dir,
                run_timestamp=run_timestamp,
                report=report,
                summary=summary,
            )
        )

    loaded.sort(key=lambda item: item.run_timestamp)
    if max_runs is not None and max_runs > 0:
        loaded = loaded[-max_runs:]
    return loaded, warnings, found


def _collect_strategy_counts(rows: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return [
        {key: strategy_id, "count": count}
        for strategy_id, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]


def _window_summary(
    *,
    validation_root: Path,
    loaded_runs: list[LoadedValidationRun],
    warnings: list[str],
    found_count: int,
) -> dict[str, Any]:
    timestamps = [run.run_timestamp.isoformat() for run in loaded_runs]
    return {
        "validation_root": str(validation_root),
        "run_count_found": found_count,
        "run_count_loaded": len(loaded_runs),
        "run_count_skipped": found_count - len(loaded_runs),
        "window_start": timestamps[0] if timestamps else None,
        "window_end": timestamps[-1] if timestamps else None,
        "latest_run_timestamp": timestamps[-1] if timestamps else None,
        "warnings": warnings,
    }


def _portfolio_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    portfolios = [dict(run.report.get("portfolio_summary") or {}) for run in loaded_runs]
    drawdowns = [_safe_float(item.get("max_drawdown_or_latest_drawdown")) for item in portfolios]
    gross_exposures = [
        _safe_float(dict(item.get("exposure_summary") or {}).get("gross_exposure"))
        or _safe_float(dict(item.get("exposure_summary") or {}).get("gross_market_value"))
        for item in portfolios
    ]
    return {
        "cumulative_expected_value_net": _sum([_safe_float(item.get("total_expected_value_net")) for item in portfolios]),
        "cumulative_realized_net_return": _sum([_safe_float(item.get("total_realized_net_return")) for item in portfolios]),
        "cumulative_expected_vs_realized_gap": _sum([_safe_float(item.get("expected_vs_realized_gap")) for item in portfolios]),
        "average_daily_trade_count": _mean([_safe_float(item.get("trade_count")) for item in portfolios]),
        "average_active_strategy_count": _mean([_safe_float(item.get("active_strategy_count")) for item in portfolios]),
        "drawdown_trend_summary": {
            "mean_drawdown": _mean(drawdowns),
            "max_drawdown": max([value for value in drawdowns if value is not None], default=None),
            "latest_drawdown": drawdowns[-1] if drawdowns else None,
        },
        "exposure_trend_summary": {
            "mean_gross_exposure": _mean(gross_exposures),
            "latest_gross_exposure": gross_exposures[-1] if gross_exposures else None,
        },
    }


def _calibration_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    calibrations = [dict(run.report.get("calibration_summary") or {}) for run in loaded_runs]
    raw_conf = [_safe_float(dict(item.get("raw_vs_calibrated_confidence") or {}).get("mean_raw_confidence_error")) for item in calibrations]
    cal_conf = [_safe_float(dict(item.get("raw_vs_calibrated_confidence") or {}).get("mean_calibrated_confidence_error")) for item in calibrations]
    raw_ev = [_safe_float(dict(item.get("raw_vs_calibrated_expected_net") or {}).get("mean_raw_expected_value_error")) for item in calibrations]
    cal_ev = [_safe_float(dict(item.get("raw_vs_calibrated_expected_net") or {}).get("mean_calibrated_expected_value_error")) for item in calibrations]
    improved_runs = 0
    runs_with_metrics = 0
    for raw_conf_value, cal_conf_value, raw_ev_value, cal_ev_value in zip(raw_conf, cal_conf, raw_ev, cal_ev, strict=False):
        had_metric = raw_conf_value is not None or raw_ev_value is not None
        if not had_metric:
            continue
        runs_with_metrics += 1
        improved = True
        if raw_conf_value is not None and cal_conf_value is not None and cal_conf_value > raw_conf_value + 1e-12:
            improved = False
        if raw_ev_value is not None and cal_ev_value is not None and cal_ev_value > raw_ev_value + 1e-12:
            improved = False
        if improved:
            improved_runs += 1
    usefulness_status = "insufficient_data"
    if runs_with_metrics > 0:
        ratio = improved_runs / runs_with_metrics
        usefulness_status = "healthy" if ratio >= 0.7 else "mixed" if ratio >= 0.4 else "concerning"
    return {
        "run_count_with_calibration_output": sum(1 for item in calibrations if item),
        "mean_raw_confidence_error": _mean(raw_conf),
        "mean_calibrated_confidence_error": _mean(cal_conf),
        "mean_raw_expected_value_error": _mean(raw_ev),
        "mean_calibrated_expected_value_error": _mean(cal_ev),
        "improved_run_count": improved_runs,
        "runs_with_metrics": runs_with_metrics,
        "calibration_discrimination_status": usefulness_status,
    }


def _drift_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    drifts = [dict(run.report.get("drift_summary") or {}) for run in loaded_runs]
    severity_counts = {"info": 0, "watch": 0, "warning": 0, "critical": 0}
    type_counts = {"performance": 0, "decision": 0, "execution": 0}
    total_signals = 0
    warning_plus_runs = 0
    for item in drifts:
        breakdown = dict(item.get("severity_breakdown") or {})
        drift_by_type = dict(item.get("drift_by_type") or {})
        run_total = int(item.get("total_drift_signal_count", 0) or 0)
        total_signals += run_total
        warning_plus = int(breakdown.get("warning", 0) or 0) + int(breakdown.get("critical", 0) or 0)
        if warning_plus > 0:
            warning_plus_runs += 1
        for key in severity_counts:
            severity_counts[key] += int(breakdown.get(key, 0) or 0)
        for key in type_counts:
            type_counts[key] += int(drift_by_type.get(key, 0) or 0)
    avg_signals = total_signals / len(drifts) if drifts else 0.0
    status = "insufficient_data"
    if drifts:
        if avg_signals > 4.0 or warning_plus_runs / len(drifts) > 0.8:
            status = "concerning"
        elif avg_signals > 1.5 or warning_plus_runs > 0:
            status = "mixed"
        else:
            status = "healthy"
    return {
        "total_drift_signal_count": total_signals,
        "drift_severity_counts_over_time": severity_counts,
        "drift_type_counts": type_counts,
        "warning_or_worse_run_count": warning_plus_runs,
        "drift_signal_quality_status": status,
    }


def _decay_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    decays = [dict(run.report.get("decay_summary") or {}) for run in loaded_runs]
    severity_counts = {"healthy": 0, "watch": 0, "warning": 0, "critical": 0}
    top_rows: list[dict[str, Any]] = []
    total_recommendations = 0
    flagged_runs = 0
    for item in decays:
        counts = dict(item.get("severity_counts") or {})
        for key in severity_counts:
            severity_counts[key] += int(counts.get(key, 0) or 0)
        top_rows.extend(list(item.get("top_decaying_strategies") or []))
        total_recommendations += sum(int(value or 0) for value in dict(item.get("recommended_lifecycle_actions") or {}).values())
        if int(counts.get("warning", 0) or 0) + int(counts.get("critical", 0) or 0) > 0:
            flagged_runs += 1
    status = "insufficient_data"
    if decays:
        if severity_counts["critical"] >= max(1, len(decays) // 2) or flagged_runs / len(decays) > 0.8:
            status = "concerning"
        elif severity_counts["warning"] > 0 or flagged_runs / len(decays) > 0.4:
            status = "mixed"
        else:
            status = "healthy"
    return {
        "total_decay_signals_or_recommendations": total_recommendations,
        "severity_counts_over_time": severity_counts,
        "top_repeatedly_flagged_strategies": _collect_strategy_counts(top_rows, key="strategy_id"),
        "decay_signal_quality_status": status,
    }


def _lifecycle_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    lifecycles = [dict(run.report.get("lifecycle_summary") or {}) for run in loaded_runs]
    action_counts = {"watch": 0, "constrain": 0, "demote": 0, "retrain": 0}
    top_rows: list[dict[str, Any]] = []
    transition_count = 0
    for item in lifecycles:
        counts = dict(item.get("action_type_counts") or {})
        for key in action_counts:
            action_counts[key] += int(counts.get(key, 0) or 0)
        transition_count += int(item.get("state_transition_count", 0) or 0)
        top_rows.extend(list(item.get("top_affected_strategies") or []))
    total_actions = sum(action_counts.values())
    runs = len(lifecycles)
    status = "insufficient_data"
    if lifecycles:
        if total_actions > runs * 2 or action_counts["demote"] + action_counts["retrain"] > max(1, runs // 2):
            status = "concerning"
        elif total_actions > runs or action_counts["constrain"] > 0:
            status = "mixed"
        else:
            status = "healthy"
    return {
        "lifecycle_action_counts": action_counts,
        "transition_count": transition_count,
        "top_affected_strategies": _collect_strategy_counts(top_rows, key="strategy_id"),
        "churn_indicator": {"total_actions": total_actions, "actions_per_run": (total_actions / runs) if runs else None},
        "lifecycle_churn_status": status,
    }


def _risk_trend_summary(loaded_runs: list[LoadedValidationRun]) -> dict[str, Any]:
    risks = [dict(run.report.get("risk_summary") or {}) for run in loaded_runs]
    total_events = 0
    halted = 0
    restricted = 0
    drawdown_breaches = 0
    anomaly_counts: dict[str, int] = {}
    for item in risks:
        total_events += int(item.get("triggered_risk_events_count", 0) or 0)
        halted += int(item.get("halted_signal_count", 0) or 0)
        restricted += int(item.get("restricted_signal_count", 0) or 0)
        drawdown_breaches += int(item.get("drawdown_breach_count", 0) or 0)
        for key, value in dict(item.get("anomaly_counts") or {}).items():
            anomaly_counts[str(key)] = anomaly_counts.get(str(key), 0) + int(value or 0)
    status = "insufficient_data"
    if risks:
        if halted > 0 or drawdown_breaches > max(1, len(risks) // 2):
            status = "concerning"
        elif total_events > len(risks) or restricted > 0:
            status = "mixed"
        else:
            status = "healthy"
    return {
        "risk_event_count_across_window": total_events,
        "halted_count": halted,
        "restricted_count": restricted,
        "drawdown_breach_count": drawdown_breaches,
        "anomaly_counts": dict(sorted(anomaly_counts.items())),
        "risk_control_activity_status": status,
    }


def _evaluation_checkpoint(
    *,
    loaded_runs: list[LoadedValidationRun],
    min_valid_runs: int,
    portfolio_trend: dict[str, Any],
    calibration_trend: dict[str, Any],
    drift_trend: dict[str, Any],
    decay_trend: dict[str, Any],
    lifecycle_trend: dict[str, Any],
    risk_trend: dict[str, Any],
) -> dict[str, Any]:
    insufficient = len(loaded_runs) < min_valid_runs
    ev_gap = abs(_safe_float(portfolio_trend.get("cumulative_expected_vs_realized_gap")) or 0.0)
    ev_expected = abs(_safe_float(portfolio_trend.get("cumulative_expected_value_net")) or 0.0)
    ev_score = 0
    if not insufficient:
        if ev_gap > max(0.10, ev_expected * 1.5):
            ev_score = 2
        elif ev_gap > max(0.04, ev_expected):
            ev_score = 1

    calibration_map = {"healthy": 0, "mixed": 1, "concerning": 2, "insufficient_data": 0}
    drift_map = {"healthy": 0, "mixed": 1, "concerning": 2, "insufficient_data": 0}
    decay_map = {"healthy": 0, "mixed": 1, "concerning": 2, "insufficient_data": 0}
    lifecycle_map = {"healthy": 0, "mixed": 1, "concerning": 2, "insufficient_data": 0}
    risk_map = {"healthy": 0, "mixed": 1, "concerning": 2, "insufficient_data": 0}

    calibration_status = calibration_trend.get("calibration_discrimination_status", "insufficient_data")
    drift_status = drift_trend.get("drift_signal_quality_status", "insufficient_data")
    decay_status = decay_trend.get("decay_signal_quality_status", "insufficient_data")
    lifecycle_status = lifecycle_trend.get("lifecycle_churn_status", "insufficient_data")
    risk_status = risk_trend.get("risk_control_activity_status", "insufficient_data")

    scores = [
        ev_score,
        calibration_map.get(str(calibration_status), 0),
        drift_map.get(str(drift_status), 0),
        decay_map.get(str(decay_status), 0),
        lifecycle_map.get(str(lifecycle_status), 0),
        risk_map.get(str(risk_status), 0),
    ]
    overall_score = max(scores) if not insufficient else 0
    if not insufficient and sum(score >= 1 for score in scores) >= 3:
        overall_score = max(overall_score, 2)
    overall_status = _status_from_score(overall_score, insufficient=insufficient)

    if overall_status == "insufficient_data":
        next_step = "continue_validation"
    elif str(risk_status) == "concerning":
        next_step = "inspect_execution"
    elif _status_from_score(ev_score) == "concerning":
        next_step = "refine_alpha_generation"
    elif any(str(status) == "concerning" for status in (drift_status, decay_status, lifecycle_status)):
        next_step = "review_thresholds"
    elif overall_status == "healthy":
        next_step = "prepare_phase_2_review"
    else:
        next_step = "continue_validation"

    return {
        "ev_alignment_status": _status_from_score(ev_score, insufficient=insufficient),
        "calibration_usefulness_status": _status_from_score(
            calibration_map.get(str(calibration_status), 0), insufficient=insufficient
        ),
        "drift_signal_quality_status": _status_from_score(
            drift_map.get(str(drift_status), 0), insufficient=insufficient
        ),
        "decay_signal_quality_status": _status_from_score(
            decay_map.get(str(decay_status), 0), insufficient=insufficient
        ),
        "lifecycle_churn_status": _status_from_score(
            lifecycle_map.get(str(lifecycle_status), 0), insufficient=insufficient
        ),
        "risk_control_status": _status_from_score(
            risk_map.get(str(risk_status), 0), insufficient=insufficient
        ),
        "overall_validation_status": overall_status,
        "recommended_next_step": next_step,
    }


def build_validation_window_review(
    *,
    validation_root: str | Path,
    strict: bool = False,
    max_runs: int | None = None,
    days: int | None = None,
    min_valid_runs: int = 3,
) -> dict[str, Any]:
    root = Path(validation_root)
    loaded_runs, warnings, found_count = _load_validation_runs(
        validation_root=root,
        strict=strict,
        days=days,
        max_runs=max_runs,
    )
    portfolio_trend = _portfolio_trend_summary(loaded_runs)
    calibration_trend = _calibration_trend_summary(loaded_runs)
    drift_trend = _drift_trend_summary(loaded_runs)
    decay_trend = _decay_trend_summary(loaded_runs)
    lifecycle_trend = _lifecycle_trend_summary(loaded_runs)
    risk_trend = _risk_trend_summary(loaded_runs)
    review = {
        "window_summary": _window_summary(
            validation_root=root,
            loaded_runs=loaded_runs,
            warnings=warnings,
            found_count=found_count,
        ),
        "portfolio_trend_summary": portfolio_trend,
        "calibration_trend_summary": calibration_trend,
        "drift_trend_summary": drift_trend,
        "decay_trend_summary": decay_trend,
        "lifecycle_trend_summary": lifecycle_trend,
        "risk_trend_summary": risk_trend,
    }
    review["evaluation_checkpoint"] = _evaluation_checkpoint(
        loaded_runs=loaded_runs,
        min_valid_runs=min_valid_runs,
        portfolio_trend=portfolio_trend,
        calibration_trend=calibration_trend,
        drift_trend=drift_trend,
        decay_trend=decay_trend,
        lifecycle_trend=lifecycle_trend,
        risk_trend=risk_trend,
    )
    return review


def render_validation_window_review_markdown(review: dict[str, Any]) -> str:
    window = dict(review.get("window_summary") or {})
    checkpoint = dict(review.get("evaluation_checkpoint") or {})
    lines = [
        "# Validation Window Review",
        "",
        f"- Validation root: `{window.get('validation_root')}`",
        f"- Runs loaded: `{window.get('run_count_loaded')}` / `{window.get('run_count_found')}`",
        f"- Window end: `{window.get('window_end')}`",
        f"- Overall validation status: `{checkpoint.get('overall_validation_status')}`",
        f"- Recommended next step: `{checkpoint.get('recommended_next_step')}`",
        "",
        "## Checkpoint",
        f"- EV alignment: `{checkpoint.get('ev_alignment_status')}`",
        f"- Calibration usefulness: `{checkpoint.get('calibration_usefulness_status')}`",
        f"- Drift signal quality: `{checkpoint.get('drift_signal_quality_status')}`",
        f"- Decay signal quality: `{checkpoint.get('decay_signal_quality_status')}`",
        f"- Lifecycle churn: `{checkpoint.get('lifecycle_churn_status')}`",
        f"- Risk control status: `{checkpoint.get('risk_control_status')}`",
        "",
        "## Warnings",
    ]
    warnings = list(window.get("warnings") or [])
    lines.extend([f"- {warning}" for warning in warnings] if warnings else ["- none"])
    lines.append("")
    return "\n".join(lines)


def write_validation_window_review(
    *,
    review: dict[str, Any],
    output_json: str | Path,
    output_md: str | Path | None = None,
) -> dict[str, Path]:
    json_path = Path(output_json)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(review, indent=2, sort_keys=False), encoding="utf-8")
    paths = {"output_json_path": json_path}
    if output_md is not None:
        md_path = Path(output_md)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(render_validation_window_review_markdown(review), encoding="utf-8")
        paths["output_md_path"] = md_path
    return paths


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review a Phase 1.5 validation window across multiple daily validation runs."
    )
    parser.add_argument("--validation-root", required=True, help="Root directory containing timestamped validation runs.")
    parser.add_argument("--output-json", required=True, help="Path to write the deterministic JSON review.")
    parser.add_argument("--output-md", help="Optional path to write a short markdown review.")
    parser.add_argument("--strict", action="store_true", help="Fail on malformed or incomplete run directories.")
    parser.add_argument("--max-runs", type=int, default=None, help="Optional maximum number of most-recent runs to include.")
    parser.add_argument("--days", type=int, default=None, help="Optional recent-day window filter.")
    parser.add_argument("--min-valid-runs", type=int, default=3, help="Minimum loaded runs before the checkpoint stops reporting insufficient_data.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    review = build_validation_window_review(
        validation_root=args.validation_root,
        strict=bool(args.strict),
        max_runs=args.max_runs,
        days=args.days,
        min_valid_runs=int(args.min_valid_runs),
    )
    write_validation_window_review(review=review, output_json=args.output_json, output_md=args.output_md)
    return 0
