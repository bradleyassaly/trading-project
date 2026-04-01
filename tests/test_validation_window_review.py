from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_platform.reporting.validation_window_review import (
    build_validation_window_review,
    write_validation_window_review,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _make_daily_report(
    *,
    report_date: str,
    overall_status: str,
    expected_value: float,
    realized_value: float,
    trade_count: int,
    active_strategy_count: int,
    raw_conf_error: float | None,
    cal_conf_error: float | None,
    raw_ev_error: float | None,
    cal_ev_error: float | None,
    drift_severity: dict[str, int],
    drift_types: dict[str, int],
    decay_severity: dict[str, int],
    lifecycle_actions: dict[str, int],
    risk_events: int,
    halted: int,
    restricted: int,
    drawdown_breaches: int,
    top_strategy: str = "alpha",
) -> dict:
    return {
        "report_date": report_date,
        "warnings": [],
        "portfolio_summary": {
            "report_date": report_date,
            "trade_count": trade_count,
            "active_strategy_count": active_strategy_count,
            "total_expected_value_net": expected_value,
            "total_realized_net_return": realized_value,
            "expected_vs_realized_gap": realized_value - expected_value,
            "total_realized_pnl": realized_value * 1000.0,
            "max_drawdown_or_latest_drawdown": 0.03 if overall_status == "healthy" else 0.12,
            "exposure_summary": {"gross_exposure": 0.8 if overall_status == "healthy" else 1.1},
        },
        "trade_quality": {},
        "calibration_summary": {
            "number_of_calibrated_trades": trade_count,
            "raw_vs_calibrated_confidence": {
                "mean_raw_confidence_error": raw_conf_error,
                "mean_calibrated_confidence_error": cal_conf_error,
            },
            "raw_vs_calibrated_expected_net": {
                "mean_raw_expected_value_error": raw_ev_error,
                "mean_calibrated_expected_value_error": cal_ev_error,
            },
        },
        "drift_summary": {
            "total_drift_signal_count": sum(drift_severity.values()),
            "severity_breakdown": drift_severity,
            "drift_by_type": drift_types,
            "most_important_triggered_signals": [],
        },
        "decay_summary": {
            "severity_counts": decay_severity,
            "top_decaying_strategies": (
                [{"strategy_id": top_strategy, "severity": "critical", "decay_score": 0.9, "recommended_action": "demote_candidate"}]
                if decay_severity.get("critical", 0)
                else []
            ),
            "recommended_lifecycle_actions": (
                {"demote_candidate": 1} if decay_severity.get("critical", 0) else {"monitor": 1}
            ),
        },
        "lifecycle_summary": {
            "action_type_counts": lifecycle_actions,
            "state_transition_count": sum(lifecycle_actions.values()),
            "retraining_trigger_count": lifecycle_actions.get("retrain", 0),
            "demotion_count": lifecycle_actions.get("demote", 0),
            "top_affected_strategies": (
                [{"strategy_id": top_strategy, "action_type": "demote", "severity": "critical", "status": "proposed"}]
                if lifecycle_actions.get("demote", 0)
                else []
            ),
        },
        "risk_summary": {
            "risk_control_state": "halted" if halted else "restricted" if restricted else "healthy",
            "triggered_risk_events_count": risk_events,
            "risk_recommendation_count": risk_events,
            "halted_signal_count": halted,
            "restricted_signal_count": restricted,
            "drawdown_breach_count": drawdown_breaches,
            "anomaly_counts": {"execution_anomaly": risk_events} if risk_events else {},
        },
        "evaluation_flags": {"overall_status": overall_status},
    }


def _make_run_summary(run_timestamp: str, run_dir: Path) -> dict:
    return {
        "run_timestamp": run_timestamp,
        "config_path": "config.json",
        "state_path": "state.json",
        "artifact_dir": str(run_dir),
        "report_json_path": str(run_dir / "daily_system_report.json"),
        "report_md_path": None,
        "paper_run_exit_status": 0,
        "report_exit_status": 0,
        "paper_command_used": "run-multi-strategy",
        "config_type": "multi_strategy",
        "execution_mode": "cli_subprocess",
        "warnings": [],
        "overall_status": "success",
    }


def _write_validation_run(root: Path, run_name: str, report: dict, *, run_timestamp: str | None = None) -> Path:
    run_dir = root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    timestamp = run_timestamp or run_name.replace("T", "T") + "+00:00"
    _write_json(run_dir / "daily_system_report.json", report)
    _write_json(run_dir / "daily_validation_run_summary.json", _make_run_summary(timestamp, run_dir))
    return run_dir


def test_build_validation_window_review_aggregates_successfully(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    _write_validation_run(
        root,
        "2026-04-01T09-30-00",
        _make_daily_report(
            report_date="2026-04-01T09:30:00+00:00",
            overall_status="healthy",
            expected_value=0.04,
            realized_value=0.038,
            trade_count=4,
            active_strategy_count=2,
            raw_conf_error=0.10,
            cal_conf_error=0.07,
            raw_ev_error=0.04,
            cal_ev_error=0.02,
            drift_severity={"info": 1, "watch": 1, "warning": 0, "critical": 0},
            drift_types={"performance": 1, "decision": 0, "execution": 1},
            decay_severity={"healthy": 1, "watch": 0, "warning": 0, "critical": 0},
            lifecycle_actions={"watch": 0, "constrain": 0, "demote": 0, "retrain": 0},
            risk_events=0,
            halted=0,
            restricted=0,
            drawdown_breaches=0,
        ),
        run_timestamp="2026-04-01T09:30:00+00:00",
    )
    _write_validation_run(
        root,
        "2026-04-02T09-30-00",
        _make_daily_report(
            report_date="2026-04-02T09:30:00+00:00",
            overall_status="healthy",
            expected_value=0.03,
            realized_value=0.028,
            trade_count=5,
            active_strategy_count=2,
            raw_conf_error=0.09,
            cal_conf_error=0.06,
            raw_ev_error=0.03,
            cal_ev_error=0.02,
            drift_severity={"info": 0, "watch": 1, "warning": 0, "critical": 0},
            drift_types={"performance": 1, "decision": 0, "execution": 0},
            decay_severity={"healthy": 1, "watch": 0, "warning": 0, "critical": 0},
            lifecycle_actions={"watch": 0, "constrain": 0, "demote": 0, "retrain": 0},
            risk_events=0,
            halted=0,
            restricted=0,
            drawdown_breaches=0,
        ),
        run_timestamp="2026-04-02T09:30:00+00:00",
    )
    _write_validation_run(
        root,
        "2026-04-03T09-30-00",
        _make_daily_report(
            report_date="2026-04-03T09:30:00+00:00",
            overall_status="healthy",
            expected_value=0.05,
            realized_value=0.047,
            trade_count=6,
            active_strategy_count=2,
            raw_conf_error=0.08,
            cal_conf_error=0.05,
            raw_ev_error=0.025,
            cal_ev_error=0.015,
            drift_severity={"info": 0, "watch": 1, "warning": 0, "critical": 0},
            drift_types={"performance": 0, "decision": 0, "execution": 1},
            decay_severity={"healthy": 1, "watch": 0, "warning": 0, "critical": 0},
            lifecycle_actions={"watch": 0, "constrain": 0, "demote": 0, "retrain": 0},
            risk_events=0,
            halted=0,
            restricted=0,
            drawdown_breaches=0,
        ),
        run_timestamp="2026-04-03T09:30:00+00:00",
    )

    review = build_validation_window_review(validation_root=root, min_valid_runs=3)

    assert review["window_summary"]["run_count_loaded"] == 3
    assert review["portfolio_trend_summary"]["cumulative_expected_value_net"] == pytest.approx(0.12)
    assert review["portfolio_trend_summary"]["cumulative_realized_net_return"] == pytest.approx(0.113)
    assert review["evaluation_checkpoint"]["overall_validation_status"] == "healthy"
    assert review["evaluation_checkpoint"]["recommended_next_step"] == "prepare_phase_2_review"


def test_build_validation_window_review_skips_incomplete_runs(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    good_dir = root / "2026-04-01T09-30-00"
    good_dir.mkdir(parents=True)
    _write_json(good_dir / "daily_system_report.json", _make_daily_report(
        report_date="2026-04-01T09:30:00+00:00",
        overall_status="healthy",
        expected_value=0.02,
        realized_value=0.019,
        trade_count=3,
        active_strategy_count=1,
        raw_conf_error=0.1,
        cal_conf_error=0.08,
        raw_ev_error=0.04,
        cal_ev_error=0.03,
        drift_severity={"info": 0, "watch": 1, "warning": 0, "critical": 0},
        drift_types={"performance": 1, "decision": 0, "execution": 0},
        decay_severity={"healthy": 1, "watch": 0, "warning": 0, "critical": 0},
        lifecycle_actions={"watch": 0, "constrain": 0, "demote": 0, "retrain": 0},
        risk_events=0,
        halted=0,
        restricted=0,
        drawdown_breaches=0,
    ))
    _write_json(good_dir / "daily_validation_run_summary.json", _make_run_summary("2026-04-01T09:30:00+00:00", good_dir))
    bad_dir = root / "2026-04-02T09-30-00"
    bad_dir.mkdir(parents=True)

    review = build_validation_window_review(validation_root=root, min_valid_runs=1)

    assert review["window_summary"]["run_count_found"] == 2
    assert review["window_summary"]["run_count_loaded"] == 1
    assert review["window_summary"]["run_count_skipped"] == 1
    assert review["window_summary"]["warnings"]


def test_build_validation_window_review_strict_mode_fails_on_incomplete_run(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    (root / "2026-04-01T09-30-00").mkdir(parents=True)

    with pytest.raises(FileNotFoundError):
        build_validation_window_review(validation_root=root, strict=True)


def test_build_validation_window_review_concerning_window(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    for day in range(1, 4):
        _write_validation_run(
            root,
            f"2026-04-0{day}T09-30-00",
            _make_daily_report(
                report_date=f"2026-04-0{day}T09:30:00+00:00",
                overall_status="concerning",
                expected_value=0.05,
                realized_value=-0.03,
                trade_count=6,
                active_strategy_count=2,
                raw_conf_error=0.10,
                cal_conf_error=0.14,
                raw_ev_error=0.03,
                cal_ev_error=0.07,
                drift_severity={"info": 0, "watch": 1, "warning": 2, "critical": 1},
                drift_types={"performance": 2, "decision": 1, "execution": 1},
                decay_severity={"healthy": 0, "watch": 0, "warning": 1, "critical": 1},
                lifecycle_actions={"watch": 1, "constrain": 1, "demote": 1, "retrain": 1},
                risk_events=3,
                halted=1,
                restricted=1,
                drawdown_breaches=1,
                top_strategy="alpha",
            ),
            run_timestamp=f"2026-04-0{day}T09:30:00+00:00",
        )

    review = build_validation_window_review(validation_root=root, min_valid_runs=3)

    assert review["evaluation_checkpoint"]["overall_validation_status"] == "concerning"
    assert review["evaluation_checkpoint"]["risk_control_status"] == "concerning"
    assert review["evaluation_checkpoint"]["recommended_next_step"] == "inspect_execution"


def test_build_validation_window_review_insufficient_data_window(tmp_path: Path) -> None:
    root = tmp_path / "validation"
    _write_validation_run(
        root,
        "2026-04-01T09-30-00",
        _make_daily_report(
            report_date="2026-04-01T09:30:00+00:00",
            overall_status="mixed",
            expected_value=0.02,
            realized_value=0.018,
            trade_count=2,
            active_strategy_count=1,
            raw_conf_error=None,
            cal_conf_error=None,
            raw_ev_error=None,
            cal_ev_error=None,
            drift_severity={"info": 0, "watch": 0, "warning": 0, "critical": 0},
            drift_types={"performance": 0, "decision": 0, "execution": 0},
            decay_severity={"healthy": 1, "watch": 0, "warning": 0, "critical": 0},
            lifecycle_actions={"watch": 0, "constrain": 0, "demote": 0, "retrain": 0},
            risk_events=0,
            halted=0,
            restricted=0,
            drawdown_breaches=0,
        ),
        run_timestamp="2026-04-01T09:30:00+00:00",
    )

    review = build_validation_window_review(validation_root=root, min_valid_runs=3)

    assert review["evaluation_checkpoint"]["overall_validation_status"] == "insufficient_data"
    assert review["evaluation_checkpoint"]["recommended_next_step"] == "continue_validation"


def test_write_validation_window_review_outputs_deterministic_json(tmp_path: Path) -> None:
    review = {
        "window_summary": {"validation_root": "root", "run_count_found": 1, "run_count_loaded": 1, "run_count_skipped": 0, "window_start": None, "window_end": None, "latest_run_timestamp": None, "warnings": []},
        "portfolio_trend_summary": {},
        "calibration_trend_summary": {},
        "drift_trend_summary": {},
        "decay_trend_summary": {},
        "lifecycle_trend_summary": {},
        "risk_trend_summary": {},
        "evaluation_checkpoint": {"overall_validation_status": "healthy", "recommended_next_step": "continue_validation"},
    }

    paths = write_validation_window_review(
        review=review,
        output_json=tmp_path / "validation_window_review.json",
        output_md=tmp_path / "validation_window_review.md",
    )

    assert paths["output_json_path"].exists()
    assert paths["output_md_path"].exists()
    assert json.loads(paths["output_json_path"].read_text(encoding="utf-8")) == review
