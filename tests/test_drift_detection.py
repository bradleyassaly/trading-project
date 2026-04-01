import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperExecutionSimulationOrder, PaperExecutionSimulationReport, PaperPortfolioState, PaperTradingRunResult
from trading_platform.reporting.drift_detection import (
    DriftSummaryReport,
    build_drift_summary_report,
    write_drift_detection_artifacts,
)
from trading_platform.reporting.outcome_attribution import (
    TradeAttribution,
    TradeOutcome,
    TradeOutcomeAttributionReport,
)


def _make_outcome(
    *,
    trade_id: str,
    entry_date: str,
    exit_date: str,
    predicted_net_return: float,
    realized_net_return: float,
    confidence_score: float,
    probability_positive: float,
    confidence_bucket: str,
    regime_label: str,
    instrument: str = "AAPL",
    strategy_id: str = "alpha",
    predicted_cost: float = 0.01,
    realized_cost: float = 0.01,
) -> TradeOutcome:
    return TradeOutcome(
        trade_id=trade_id,
        decision_id=f"decision-{trade_id}",
        strategy_id=strategy_id,
        instrument=instrument,
        entry_date=entry_date,
        exit_date=exit_date,
        side="long",
        quantity=10,
        horizon_days=5,
        holding_period_days=2,
        regime_label=regime_label,
        confidence_bucket=confidence_bucket,
        probability_positive=probability_positive,
        confidence_score=confidence_score,
        reliability_score=0.7,
        calibration_score=probability_positive,
        predicted_return=predicted_net_return,
        predicted_gross_return=predicted_net_return + predicted_cost,
        predicted_cost=predicted_cost,
        predicted_net_return=predicted_net_return,
        realized_gross_return=realized_net_return + realized_cost,
        realized_cost=realized_cost,
        realized_net_return=realized_net_return,
        realized_gross_pnl=(realized_net_return + realized_cost) * 1_000.0,
        realized_cost_total=realized_cost * 1_000.0,
        realized_net_pnl=realized_net_return * 1_000.0,
        entry_reference_price=100.0,
        entry_price=100.0,
        exit_reference_price=100.0 * (1.0 + realized_net_return + realized_cost),
        exit_price=100.0 * (1.0 + realized_net_return),
    )


def _make_attribution(*, trade_id: str, execution_error: float, forecast_gap: float) -> TradeAttribution:
    return TradeAttribution(
        trade_id=trade_id,
        decision_id=f"decision-{trade_id}",
        strategy_id="alpha",
        instrument="AAPL",
        as_of="2026-04-01",
        forecast_gap=forecast_gap,
        alpha_error=forecast_gap,
        cost_error=0.0,
        timing_error=0.0,
        execution_error=execution_error,
        sizing_error=0.0,
        regime_mismatch=False,
    )


def _result(outcomes: list[TradeOutcome], attributions: list[TradeAttribution], *, fill_rate: float = 1.0) -> PaperTradingRunResult:
    execution_quantity = int(fill_rate * 10)
    return PaperTradingRunResult(
        as_of="2026-04-01",
        state=PaperPortfolioState(cash=10_000.0, positions={}, initial_cash_basis=10_000.0),
        latest_prices={"AAPL": 100.0},
        latest_scores={},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[],
        attribution={"strategy_rows": [], "symbol_rows": [], "trade_rows": [], "summary": {}},
        execution_simulation_report=PaperExecutionSimulationReport(
            as_of="2026-04-01",
            orders=[
                PaperExecutionSimulationOrder(
                    symbol="AAPL",
                    side="BUY",
                    requested_quantity=10,
                    executable_quantity=execution_quantity,
                    requested_notional=1_000.0,
                    executable_notional=float(execution_quantity) * 100.0,
                    reference_price=100.0,
                    estimated_fill_price=100.0,
                    filled_fraction=fill_rate,
                    status="executable" if fill_rate >= 1.0 else "partial_fill",
                )
            ],
            summary={"requested_order_count": 1, "partial_fill_order_count": 0 if fill_rate >= 1.0 else 1},
        ),
        outcome_attribution_report=TradeOutcomeAttributionReport(
            as_of="2026-04-01",
            outcomes=outcomes,
            attributions=attributions,
            aggregates=[],
            summary={"closed_trade_count": len(outcomes)},
        ),
    )


def test_build_drift_summary_report_no_drift_baseline_case() -> None:
    outcomes = [
        _make_outcome(
            trade_id=f"t{idx}",
            entry_date=f"2026-03-0{idx}",
            exit_date=f"2026-03-1{idx}",
            predicted_net_return=0.03,
            realized_net_return=0.03,
            confidence_score=0.7,
            probability_positive=1.0,
            confidence_bucket="high",
            regime_label="risk_on",
        )
        for idx in range(1, 5)
    ]
    attributions = [_make_attribution(trade_id=f"t{idx}", execution_error=0.0, forecast_gap=0.0) for idx in range(1, 5)]
    report = build_drift_summary_report(result=_result(outcomes, attributions))

    assert report.summary["signal_count"] == 0
    assert report.signals == []
    assert any(row.metric_name == "forecast_gap" and row.scope == "portfolio" for row in report.metric_snapshots)


def test_build_drift_summary_report_detects_threshold_breaches_and_severity() -> None:
    outcomes = [
        _make_outcome(
            trade_id="t1",
            entry_date="2026-03-01",
            exit_date="2026-03-02",
            predicted_net_return=0.03,
            realized_net_return=0.03,
            confidence_score=0.7,
            probability_positive=1.0,
            confidence_bucket="high",
            regime_label="risk_on",
        ),
        _make_outcome(
            trade_id="t2",
            entry_date="2026-03-03",
            exit_date="2026-03-04",
            predicted_net_return=0.03,
            realized_net_return=0.03,
            confidence_score=0.7,
            probability_positive=1.0,
            confidence_bucket="high",
            regime_label="risk_on",
        ),
        _make_outcome(
            trade_id="t3",
            entry_date="2026-03-05",
            exit_date="2026-03-06",
            predicted_net_return=0.03,
            realized_net_return=-0.08,
            confidence_score=0.2,
            probability_positive=0.2,
            confidence_bucket="low",
            regime_label="risk_off",
            realized_cost=0.06,
        ),
        _make_outcome(
            trade_id="t4",
            entry_date="2026-03-07",
            exit_date="2026-03-08",
            predicted_net_return=0.03,
            realized_net_return=-0.08,
            confidence_score=0.2,
            probability_positive=0.2,
            confidence_bucket="low",
            regime_label="risk_off",
            realized_cost=0.06,
        ),
    ]
    attributions = [
        _make_attribution(trade_id="t1", execution_error=0.0, forecast_gap=0.0),
        _make_attribution(trade_id="t2", execution_error=0.0, forecast_gap=0.0),
        _make_attribution(trade_id="t3", execution_error=0.05, forecast_gap=-0.11),
        _make_attribution(trade_id="t4", execution_error=0.05, forecast_gap=-0.11),
    ]
    report = build_drift_summary_report(result=_result(outcomes, attributions, fill_rate=0.6))

    severity_by_metric = {(row.scope, row.metric_name): row.severity for row in report.signals}
    action_by_metric = {(row.scope, row.metric_name): row.recommended_action for row in report.signals}

    assert severity_by_metric[("portfolio", "forecast_gap")] == "critical"
    assert action_by_metric[("portfolio", "forecast_gap")] == "escalate_to_risk_controls"
    assert severity_by_metric[("portfolio", "fill_rate_gap")] in {"warning", "critical"}
    assert report.summary["signal_count"] >= 4


def test_build_drift_summary_report_supports_strategy_instrument_and_regime_scopes() -> None:
    outcomes = [
        _make_outcome(
            trade_id=f"t{idx}",
            entry_date=f"2026-03-0{idx}",
            exit_date=f"2026-03-1{idx}",
            predicted_net_return=0.02 if idx <= 2 else -0.03,
            realized_net_return=0.02 if idx <= 2 else -0.09,
            confidence_score=0.8 if idx <= 2 else 0.2,
            probability_positive=1.0 if idx <= 2 else 0.2,
            confidence_bucket="high" if idx <= 2 else "low",
            regime_label="risk_on" if idx <= 2 else "risk_off",
            instrument="AAPL",
            strategy_id="alpha",
        )
        for idx in range(1, 5)
    ]
    attributions = [
        _make_attribution(trade_id=f"t{idx}", execution_error=0.0 if idx <= 2 else 0.03, forecast_gap=0.0 if idx <= 2 else -0.06)
        for idx in range(1, 5)
    ]
    report = build_drift_summary_report(result=_result(outcomes, attributions))

    scoped_metrics = {(row.scope, row.scope_id) for row in report.metric_snapshots}
    assert ("strategy", "alpha") in scoped_metrics
    assert ("instrument", "AAPL") in scoped_metrics
    assert ("regime", "risk_on") in scoped_metrics
    assert ("regime", "risk_off") in scoped_metrics


def test_drift_artifacts_are_deterministic_and_round_trip(tmp_path: Path) -> None:
    outcomes = [
        _make_outcome(
            trade_id=f"t{idx}",
            entry_date=f"2026-03-0{idx}",
            exit_date=f"2026-03-1{idx}",
            predicted_net_return=0.03,
            realized_net_return=0.03,
            confidence_score=0.7,
            probability_positive=1.0,
            confidence_bucket="high",
            regime_label="risk_on",
        )
        for idx in range(1, 5)
    ]
    attributions = [_make_attribution(trade_id=f"t{idx}", execution_error=0.0, forecast_gap=0.0) for idx in range(1, 5)]
    report = build_drift_summary_report(result=_result(outcomes, attributions))
    paths = write_drift_detection_artifacts(output_dir=tmp_path, report=report)

    payload = json.loads(paths["drift_detection_report_json_path"].read_text(encoding="utf-8"))
    restored = DriftSummaryReport.from_dict(payload)
    snapshots_df = pd.read_csv(paths["drift_metric_snapshots_csv_path"])
    signals_df = pd.read_csv(paths["drift_signals_csv_path"])

    assert restored.to_dict() == report.to_dict()
    assert len(snapshots_df) == len(report.metric_snapshots)
    assert "metric_name" in set(snapshots_df.columns)
    assert "severity" in set(signals_df.columns)
