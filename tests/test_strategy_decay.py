import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.reporting.calibration import CalibrationScopeSummary, CalibrationSummaryReport
from trading_platform.reporting.drift_detection import DriftSignal, DriftSummaryReport
from trading_platform.reporting.outcome_attribution import (
    TradeAttribution,
    TradeOutcome,
    TradeOutcomeAttributionReport,
)
from trading_platform.reporting.strategy_decay import (
    StrategyDecaySummaryReport,
    build_strategy_decay_summary_report,
    write_strategy_decay_artifacts,
)
from trading_platform.risk.controls import PaperRiskControlReport, RiskControlAction, RiskControlTrigger


def _outcome(
    *,
    trade_id: str,
    strategy_id: str = "alpha",
    realized_net_return: float = 0.03,
    predicted_net_return: float = 0.03,
    regime_label: str = "risk_on",
) -> TradeOutcome:
    return TradeOutcome(
        trade_id=trade_id,
        decision_id=f"decision-{trade_id}",
        strategy_id=strategy_id,
        instrument="AAPL",
        entry_date="2026-03-01",
        exit_date="2026-03-05",
        side="long",
        quantity=10,
        horizon_days=5,
        holding_period_days=4,
        regime_label=regime_label,
        confidence_bucket="high",
        probability_positive=0.8,
        confidence_score=0.8,
        reliability_score=0.7,
        calibration_score=0.8,
        predicted_return=predicted_net_return,
        predicted_gross_return=predicted_net_return + 0.01,
        predicted_cost=0.01,
        predicted_net_return=predicted_net_return,
        realized_gross_return=realized_net_return + 0.01,
        realized_cost=0.01,
        realized_net_return=realized_net_return,
        realized_gross_pnl=(realized_net_return + 0.01) * 1000.0,
        realized_cost_total=10.0,
        realized_net_pnl=realized_net_return * 1000.0,
        entry_reference_price=100.0,
        entry_price=100.0,
        exit_reference_price=100.0 * (1.0 + realized_net_return + 0.01),
        exit_price=100.0 * (1.0 + realized_net_return),
    )


def _attribution(*, trade_id: str, forecast_gap: float = 0.0, cost_error: float = 0.0, execution_error: float = 0.0) -> TradeAttribution:
    return TradeAttribution(
        trade_id=trade_id,
        decision_id=f"decision-{trade_id}",
        strategy_id="alpha",
        instrument="AAPL",
        as_of="2026-04-01",
        forecast_gap=forecast_gap,
        alpha_error=forecast_gap,
        cost_error=cost_error,
        timing_error=0.0,
        execution_error=execution_error,
        sizing_error=0.0,
        regime_mismatch=False,
    )


def _result(
    *,
    outcomes: list[TradeOutcome],
    attributions: list[TradeAttribution],
    drift_signals: list[DriftSignal] | None = None,
    calibration_scope_summaries: list[CalibrationScopeSummary] | None = None,
    risk_triggers: list[RiskControlTrigger] | None = None,
    risk_actions: list[RiskControlAction] | None = None,
) -> PaperTradingRunResult:
    return PaperTradingRunResult(
        as_of="2026-04-01",
        state=PaperPortfolioState(cash=10_000.0),
        latest_prices={},
        latest_scores={},
        latest_target_weights={},
        scheduled_target_weights={},
        orders=[],
        attribution={"trade_rows": [], "strategy_rows": [], "symbol_rows": [], "summary": {}},
        outcome_attribution_report=TradeOutcomeAttributionReport(
            as_of="2026-04-01",
            outcomes=outcomes,
            attributions=attributions,
            aggregates=[],
            summary={"closed_trade_count": len(outcomes)},
        ),
        drift_report=DriftSummaryReport(
            as_of="2026-04-01",
            signals=list(drift_signals or []),
            metric_snapshots=[],
            summary={"signal_count": len(drift_signals or [])},
        ),
        calibration_report=CalibrationSummaryReport(
            as_of="2026-04-01",
            scope_summaries=list(calibration_scope_summaries or []),
            records=[],
            buckets=[],
            adjustments=[],
            summary={},
        ),
        risk_control_report=PaperRiskControlReport(
            as_of="2026-04-01",
            enabled=True,
            operating_state="healthy",
            triggers=list(risk_triggers or []),
            actions=list(risk_actions or []),
            events=[],
            summary={},
        ),
    )


def test_strategy_decay_reports_healthy_case() -> None:
    outcomes = [_outcome(trade_id=f"t{idx}") for idx in range(1, 5)]
    attributions = [_attribution(trade_id=f"t{idx}") for idx in range(1, 5)]
    calibration_scope_summaries = [
        CalibrationScopeSummary(
            as_of="2026-04-01",
            scope="strategy",
            scope_id="alpha",
            sample_count=4,
            confidence_sample_count=4,
            expected_value_sample_count=4,
            mean_raw_confidence_error=0.1,
            mean_calibrated_confidence_error=0.08,
            mean_raw_expected_value_error=0.02,
            mean_calibrated_expected_value_error=0.015,
            sufficient_samples=True,
        )
    ]
    report = build_strategy_decay_summary_report(
        result=_result(
            outcomes=outcomes,
            attributions=attributions,
            calibration_scope_summaries=calibration_scope_summaries,
        )
    )

    assert StrategyDecaySummaryReport.from_dict(report.to_dict()) == report
    assert report.records[0].severity == "healthy"
    assert report.records[0].recommended_action == "monitor"


def test_strategy_decay_handles_insufficient_data() -> None:
    report = build_strategy_decay_summary_report(
        result=_result(
            outcomes=[_outcome(trade_id="t1"), _outcome(trade_id="t2")],
            attributions=[_attribution(trade_id="t1"), _attribution(trade_id="t2")],
        )
    )

    assert report.records[0].sufficient_samples is False
    assert report.records[0].severity == "healthy"
    assert any(row.signal_type == "insufficient_data" for row in report.signals)


def test_strategy_decay_detects_threshold_breach() -> None:
    outcomes = [
        _outcome(trade_id=f"t{idx}", realized_net_return=-0.08, predicted_net_return=0.03)
        for idx in range(1, 5)
    ]
    attributions = [
        _attribution(trade_id=f"t{idx}", forecast_gap=-0.11, cost_error=0.02, execution_error=0.03)
        for idx in range(1, 5)
    ]
    drift_signals = [
        DriftSignal(
            as_of="2026-04-01",
            category="performance",
            metric_name="forecast_gap",
            scope="strategy",
            scope_id="alpha",
            severity="warning",
            recommended_action="constrain",
            comparator_mode="rolling_half_split",
            recent_value=-0.08,
            baseline_value=0.03,
            delta=-0.11,
            relative_delta=-3.66,
            threshold=0.06,
            recent_window_label="recent",
            baseline_window_label="baseline",
            message="drift",
        ),
        DriftSignal(
            as_of="2026-04-01",
            category="execution",
            metric_name="cost_gap",
            scope="strategy",
            scope_id="alpha",
            severity="critical",
            recommended_action="escalate_to_risk_controls",
            comparator_mode="rolling_half_split",
            recent_value=0.03,
            baseline_value=0.0,
            delta=0.03,
            relative_delta=None,
            threshold=0.02,
            recent_window_label="recent",
            baseline_window_label="baseline",
            message="drift",
        ),
    ]
    calibration_scope_summaries = [
        CalibrationScopeSummary(
            as_of="2026-04-01",
            scope="strategy",
            scope_id="alpha",
            sample_count=4,
            confidence_sample_count=4,
            expected_value_sample_count=4,
            mean_raw_confidence_error=0.25,
            mean_calibrated_confidence_error=0.20,
            mean_raw_expected_value_error=0.12,
            mean_calibrated_expected_value_error=0.08,
            sufficient_samples=True,
        )
    ]
    risk_triggers = [
        RiskControlTrigger(
            as_of="2026-04-01",
            scope="strategy",
            scope_id="alpha",
            trigger_type="expected_realized_divergence",
            severity="warning",
            threshold=0.03,
            observed_value=0.11,
            operating_state="restricted",
            message="risk",
        )
    ]
    report = build_strategy_decay_summary_report(
        result=_result(
            outcomes=outcomes,
            attributions=attributions,
            drift_signals=drift_signals,
            calibration_scope_summaries=calibration_scope_summaries,
            risk_triggers=risk_triggers,
        )
    )

    assert report.records[0].severity in {"warning", "critical"}
    assert report.records[0].recommended_action in {"constrain", "demote_candidate"}
    assert any(row.signal_type == "forecast_gap_decay" for row in report.signals)


def test_strategy_decay_artifacts_are_deterministic(tmp_path: Path) -> None:
    outcomes = [_outcome(trade_id=f"t{idx}") for idx in range(1, 5)]
    attributions = [_attribution(trade_id=f"t{idx}") for idx in range(1, 5)]
    report = build_strategy_decay_summary_report(result=_result(outcomes=outcomes, attributions=attributions))
    paths = write_strategy_decay_artifacts(output_dir=tmp_path, report=report)

    payload = json.loads(paths["strategy_decay_report_json_path"].read_text(encoding="utf-8"))
    records_df = pd.read_csv(paths["strategy_decay_records_csv_path"])
    signals_df = pd.read_csv(paths["strategy_decay_signals_csv_path"])

    assert payload["summary"]["strategy_count"] == 1
    assert "strategy_id" in set(records_df.columns)
    assert "signal_type" in set(signals_df.columns)
