from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.reporting.daily_system_report import (
    build_daily_system_report,
    main,
    write_daily_system_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_artifact_bundle(root: Path, *, concerning: bool = False) -> Path:
    artifact_dir = root / "paper_bundle"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    forecast_gap = -0.08 if concerning else -0.005
    raw_conf_error = 0.10
    calibrated_conf_error = 0.14 if concerning else 0.06
    raw_ev_error = 0.03
    calibrated_ev_error = 0.06 if concerning else 0.015
    drift_signals = [
        {
            "as_of": "2026-04-01T00:00:00Z",
            "category": "performance",
            "metric_name": "forecast_gap",
            "scope": "strategy",
            "scope_id": "alpha",
            "severity": "critical" if concerning else "watch",
            "recommended_action": "escalate_to_risk_controls" if concerning else "review",
            "comparator_mode": "rolling_half_split",
            "recent_value": -0.05 if concerning else 0.02,
            "baseline_value": 0.03,
            "delta": -0.08 if concerning else -0.01,
            "relative_delta": -2.6 if concerning else -0.3,
            "threshold": 0.06,
            "recent_window_label": "recent",
            "baseline_window_label": "baseline",
            "message": "performance drift",
            "metadata": {},
        },
        {
            "as_of": "2026-04-01T00:00:00Z",
            "category": "execution",
            "metric_name": "cost_gap",
            "scope": "portfolio",
            "scope_id": "portfolio",
            "severity": "warning" if concerning else "info",
            "recommended_action": "constrain" if concerning else "monitor",
            "comparator_mode": "expected_reference",
            "recent_value": 0.03 if concerning else 0.011,
            "baseline_value": 0.01,
            "delta": 0.02 if concerning else 0.001,
            "relative_delta": 2.0 if concerning else 0.1,
            "threshold": 0.02,
            "recent_window_label": "recent",
            "baseline_window_label": "expected_reference",
            "message": "execution drift",
            "metadata": {},
        },
    ]

    _write_json(
        artifact_dir / "trade_outcome_attribution_report.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "trade_outcome_attribution_v1",
            "outcomes": [
                {
                    "trade_id": "t1",
                    "decision_id": "d1",
                    "strategy_id": "alpha",
                    "instrument": "AAPL",
                    "entry_date": "2026-03-20",
                    "exit_date": "2026-04-01",
                    "side": "long",
                    "quantity": 10,
                    "horizon_days": 5,
                    "holding_period_days": 5,
                    "regime_label": "risk_on",
                    "confidence_bucket": "high",
                    "predicted_return": 0.03,
                    "predicted_cost": 0.01,
                    "predicted_net_return": 0.02,
                    "realized_cost": 0.011,
                    "realized_net_return": -0.06 if concerning else 0.018,
                    "realized_net_pnl": -60.0 if concerning else 18.0,
                    "realized_cost_total": 11.0,
                    "status": "closed",
                    "metadata": {},
                },
                {
                    "trade_id": "t2",
                    "decision_id": "d2",
                    "strategy_id": "beta",
                    "instrument": "MSFT",
                    "entry_date": "2026-03-21",
                    "exit_date": "2026-04-01",
                    "side": "long",
                    "quantity": 8,
                    "horizon_days": 5,
                    "holding_period_days": 5,
                    "regime_label": "risk_off",
                    "confidence_bucket": "medium",
                    "predicted_return": 0.025,
                    "predicted_cost": 0.009,
                    "predicted_net_return": 0.016,
                    "realized_cost": 0.010,
                    "realized_net_return": -0.03 if concerning else 0.012,
                    "realized_net_pnl": -24.0 if concerning else 9.6,
                    "realized_cost_total": 8.0,
                    "status": "closed",
                    "metadata": {},
                },
            ],
            "attributions": [
                {
                    "trade_id": "t1",
                    "decision_id": "d1",
                    "strategy_id": "alpha",
                    "instrument": "AAPL",
                    "as_of": "2026-04-01T00:00:00Z",
                    "forecast_gap": forecast_gap,
                    "alpha_error": forecast_gap,
                    "cost_error": -0.001,
                    "timing_error": 0.0,
                    "execution_error": 0.01,
                    "sizing_error": 0.0,
                    "regime_mismatch": False,
                    "regime_mismatch_score": 0.0,
                    "metadata": {},
                },
                {
                    "trade_id": "t2",
                    "decision_id": "d2",
                    "strategy_id": "beta",
                    "instrument": "MSFT",
                    "as_of": "2026-04-01T00:00:00Z",
                    "forecast_gap": forecast_gap,
                    "alpha_error": forecast_gap,
                    "cost_error": -0.001,
                    "timing_error": 0.0,
                    "execution_error": 0.008,
                    "sizing_error": 0.0,
                    "regime_mismatch": False,
                    "regime_mismatch_score": 0.0,
                    "metadata": {},
                },
            ],
            "aggregates": [],
            "summary": {
                "closed_trade_count": 2,
                "mean_forecast_gap": forecast_gap,
                "mean_alpha_error": forecast_gap,
                "mean_cost_error": -0.001,
                "mean_execution_error": 0.009,
                "total_realized_net_pnl": -84.0 if concerning else 27.6,
            },
        },
    )

    _write_json(
        artifact_dir / "calibration_summary_report.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "calibration_pipeline_v1",
            "records": [
                {"trade_id": "t1", "confidence_bucket": "high"},
                {"trade_id": "t2", "confidence_bucket": "medium"},
            ],
            "buckets": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "calibration_type": "confidence",
                    "scope": "portfolio",
                    "scope_id": "portfolio",
                    "bucket_label": "0.60_to_0.80",
                    "lower_bound": 0.6,
                    "upper_bound": 0.8,
                    "sample_count": 2,
                    "raw_mean": 0.70,
                    "realized_mean": 0.50 if concerning else 0.68,
                    "correction_delta": -0.20 if concerning else -0.02,
                    "shrinkage_weight": 0.25,
                    "calibrated_mean": 0.50 if concerning else 0.68,
                    "sufficient_samples": True,
                    "metadata": {},
                }
            ],
            "adjustments": [],
            "scope_summaries": [],
            "summary": {
                "record_count": 2,
                "bucket_count": 1,
                "adjustment_count": 1,
                "sufficient_scope_count": 1,
                "mean_raw_confidence_error": raw_conf_error,
                "mean_calibrated_confidence_error": calibrated_conf_error,
                "mean_raw_expected_value_error": raw_ev_error,
                "mean_calibrated_expected_value_error": calibrated_ev_error,
                "confidence_noop_count": 0,
                "expected_value_noop_count": 0,
            },
        },
    )

    _write_json(
        artifact_dir / "drift_detection_report.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "drift_detection_v1",
            "metric_snapshots": [],
            "signals": drift_signals,
            "summary": {
                "signal_count": len(drift_signals),
                "severity_counts": {
                    "info": 0 if concerning else 1,
                    "watch": 0 if concerning else 1,
                    "warning": 1 if concerning else 0,
                    "critical": 1 if concerning else 0,
                },
                "category_counts": {"performance": 1, "decision": 0, "execution": 1},
            },
        },
    )

    _write_json(
        artifact_dir / "strategy_decay_report.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "strategy_decay_v1",
            "records": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "evaluation_window_start": "2026-03-01",
                    "evaluation_window_end": "2026-04-01",
                    "trade_count": 6,
                    "sufficient_samples": True,
                    "mean_predicted_net_return": 0.02,
                    "mean_realized_net_return": -0.05 if concerning else 0.015,
                    "mean_forecast_gap": -0.08 if concerning else -0.005,
                    "mean_cost_error": 0.0,
                    "mean_execution_error": 0.01,
                    "drift_signal_count": 2,
                    "drift_warning_or_worse_count": 2 if concerning else 0,
                    "calibration_confidence_error": 0.04,
                    "calibration_expected_value_error": 0.07 if concerning else 0.015,
                    "risk_trigger_count": 1 if concerning else 0,
                    "risk_halted_or_restricted_count": 1 if concerning else 0,
                    "realized_drawdown_proxy": 0.12 if concerning else 0.02,
                    "decay_score": 0.88 if concerning else 0.18,
                    "severity": "critical" if concerning else "healthy",
                    "recommended_action": "demote_candidate" if concerning else "monitor",
                    "metadata": {},
                }
            ],
            "signals": [],
            "recommendations": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "severity": "critical" if concerning else "healthy",
                    "recommended_action": "demote_candidate" if concerning else "monitor",
                    "decay_score": 0.88 if concerning else 0.18,
                    "sufficient_samples": True,
                    "rationale": [],
                    "metadata": {},
                }
            ],
            "summary": {
                "strategy_count": 1,
                "signal_count": 0,
                "critical_count": 1 if concerning else 0,
                "warning_count": 0,
                "watch_count": 0,
                "healthy_count": 0 if concerning else 1,
                "portfolio_decay_score": 0.88 if concerning else 0.18,
            },
        },
    )

    _write_json(
        artifact_dir / "strategy_lifecycle_report.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "strategy_lifecycle_v1",
            "states": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "state": "demoted" if concerning else "active",
                    "active_for_selection": False if concerning else True,
                    "constrained": False,
                    "monitoring_level": "elevated" if concerning else "standard",
                    "retraining_requested": concerning,
                    "last_action": "demote" if concerning else None,
                    "last_action_at": "2026-04-01T00:00:00Z" if concerning else None,
                    "metadata": {},
                }
            ],
            "actions": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "action_type": "demote" if concerning else "none",
                    "status": "proposed" if concerning else "no_action",
                    "severity": "critical" if concerning else "healthy",
                    "previous_state": "active",
                    "proposed_state": "demoted" if concerning else "active",
                    "final_state": "demoted" if concerning else "active",
                    "evidence_sources": ["strategy_decay"],
                    "reason_codes": ["decay_demote_candidate"] if concerning else [],
                    "cooldown_applied": False,
                    "deduplicated": False,
                    "message": "lifecycle",
                    "metadata": {},
                }
            ],
            "transitions": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "previous_state": "active",
                    "new_state": "demoted",
                    "triggering_action": "demote",
                    "reason_codes": ["decay_demote_candidate"],
                    "metadata": {},
                }
            ]
            if concerning
            else [],
            "demotion_decisions": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "approved_for_demotion": concerning,
                    "previous_state": "active",
                    "new_state": "demoted" if concerning else "active",
                    "required_governance_review": True,
                    "rationale": ["decay_demote_candidate"] if concerning else [],
                    "evidence_summary": {},
                    "metadata": {},
                }
            ],
            "retraining_triggers": [
                {
                    "trigger_id": "alpha|2026-04-01|retrain",
                    "as_of": "2026-04-01T00:00:00Z",
                    "strategy_id": "alpha",
                    "source_action": "retrain",
                    "target_candidate_status": "candidate",
                    "governance_required_before_reactivation": True,
                    "reason_codes": ["persistent_forecast_gap"],
                    "handoff_payload": {},
                    "metadata": {},
                }
            ]
            if concerning
            else [],
            "summary": {
                "strategy_count": 1,
                "action_count": 1,
                "transition_count": 1 if concerning else 0,
                "demotion_count": 1 if concerning else 0,
                "retraining_trigger_count": 1 if concerning else 0,
                "watch_count": 0,
                "constrained_count": 0,
                "demoted_count": 1 if concerning else 0,
                "suppressed_action_count": 0,
            },
        },
    )

    _write_json(
        artifact_dir / "paper_risk_controls.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "enabled": True,
            "operating_state": "halted" if concerning else "healthy",
            "schema_version": "paper_risk_controls_v1",
            "triggers": [
                {
                    "as_of": "2026-04-01T00:00:00Z",
                    "scope": "strategy",
                    "scope_id": "alpha",
                    "trigger_type": "drawdown_breach" if concerning else "expected_realized_divergence",
                    "severity": "critical" if concerning else "warning",
                    "threshold": 0.05,
                    "observed_value": 0.12 if concerning else 0.03,
                    "operating_state": "halted" if concerning else "restricted",
                    "message": "risk",
                    "metadata": {},
                }
            ],
            "actions": [],
            "events": [],
            "summary": {"portfolio_drawdown": 0.12 if concerning else 0.01},
        },
    )

    _write_json(
        artifact_dir / "strategy_health_payload.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "strategy_health_payload_v1",
            "rows": [],
            "summary": {"strategy_count": 2},
        },
    )

    _write_json(
        artifact_dir / "realtime_kpi_monitoring.json",
        {
            "as_of": "2026-04-01T00:00:00Z",
            "schema_version": "realtime_kpi_monitoring_v1",
            "metrics": [
                {"as_of": "2026-04-01T00:00:00Z", "metric_name": "drawdown", "metric_value": 0.12 if concerning else 0.01, "unit": "ratio", "scope": "paper_portfolio", "metadata": {}},
                {"as_of": "2026-04-01T00:00:00Z", "metric_name": "gross_exposure", "metric_value": 0.75, "unit": "ratio", "scope": "paper_portfolio", "metadata": {}},
                {"as_of": "2026-04-01T00:00:00Z", "metric_name": "net_exposure", "metric_value": 0.55, "unit": "ratio", "scope": "paper_portfolio", "metadata": {}},
            ],
            "summary": {},
        },
    )

    pd.DataFrame(
        [
            {
                "as_of": "2026-04-01T00:00:00Z",
                "cash": 9000.0,
                "gross_market_value": 1100.0,
                "equity": 10100.0 if not concerning else 9800.0,
            }
        ]
    ).to_csv(artifact_dir / "paper_equity_snapshot.csv", index=False)
    pd.DataFrame([{"symbol": "AAPL", "market_value": 550.0}, {"symbol": "MSFT", "market_value": 550.0}]).to_csv(
        artifact_dir / "paper_positions.csv", index=False
    )
    return artifact_dir


def test_build_daily_system_report_success(tmp_path: Path) -> None:
    artifact_dir = _build_artifact_bundle(tmp_path)

    report = build_daily_system_report(artifact_dir=artifact_dir)

    assert report["portfolio_summary"]["trade_count"] == 2
    assert report["portfolio_summary"]["active_strategy_count"] == 2
    assert report["trade_quality"]["win_rate"] == pytest.approx(1.0)
    assert report["calibration_summary"]["number_of_calibrated_trades"] == 2
    assert report["drift_summary"]["drift_by_type"]["performance"] == 1
    assert report["decay_summary"]["severity_counts"]["healthy"] == 1
    assert report["lifecycle_summary"]["action_type_counts"]["watch"] == 0
    assert report["risk_summary"]["risk_control_state"] == "healthy"
    assert report["evaluation_flags"]["overall_status"] in {"healthy", "mixed"}


def test_build_daily_system_report_handles_missing_optional_artifacts(tmp_path: Path) -> None:
    artifact_dir = _build_artifact_bundle(tmp_path)
    (artifact_dir / "strategy_health_payload.json").unlink()
    (artifact_dir / "paper_positions.csv").unlink()

    report = build_daily_system_report(artifact_dir=artifact_dir)

    assert any("missing optional artifact" in warning for warning in report["warnings"])
    assert report["portfolio_summary"]["active_strategy_count"] == 1


def test_build_daily_system_report_strict_mode_requires_core_artifacts(tmp_path: Path) -> None:
    artifact_dir = _build_artifact_bundle(tmp_path)
    (artifact_dir / "drift_detection_report.json").unlink()

    with pytest.raises(FileNotFoundError, match="Missing required artifacts"):
        build_daily_system_report(artifact_dir=artifact_dir, strict=True)


def test_write_daily_system_report_is_deterministic(tmp_path: Path) -> None:
    artifact_dir = _build_artifact_bundle(tmp_path)
    report = build_daily_system_report(artifact_dir=artifact_dir)

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    write_daily_system_report(report=report, output_json=first)
    write_daily_system_report(report=report, output_json=second)

    assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")


def test_daily_system_report_flags_concerning_behavior_and_cli_output(tmp_path: Path) -> None:
    artifact_dir = _build_artifact_bundle(tmp_path, concerning=True)
    output_json = tmp_path / "daily_system_report.json"
    output_md = tmp_path / "daily_system_report.md"

    exit_code = main(
        [
            "--artifact-dir",
            str(artifact_dir),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )
    payload = json.loads(output_json.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert output_md.exists()
    assert payload["evaluation_flags"]["ev_alignment_flag"] == "concerning"
    assert payload["evaluation_flags"]["calibration_signal_flag"] == "concerning"
    assert payload["evaluation_flags"]["overall_status"] == "concerning"
