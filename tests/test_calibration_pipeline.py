import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.reporting.calibration import (
    CalibrationSummaryReport,
    build_calibration_summary_report,
    write_calibration_artifacts,
)
from trading_platform.reporting.outcome_attribution import TradeOutcome, TradeOutcomeAttributionReport


def _outcome(
    *,
    trade_id: str,
    strategy_id: str = "alpha",
    regime_label: str = "risk_on",
    probability_positive: float = 0.7,
    confidence_score: float = 0.7,
    predicted_net_return: float = 0.03,
    realized_net_return: float = 0.03,
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
        confidence_bucket="high" if probability_positive >= 0.7 else "low",
        probability_positive=probability_positive,
        confidence_score=confidence_score,
        reliability_score=0.6,
        calibration_score=probability_positive,
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


def _result(outcomes: list[TradeOutcome]) -> PaperTradingRunResult:
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
            attributions=[],
            aggregates=[],
            summary={"closed_trade_count": len(outcomes)},
        ),
    )


def test_calibration_pipeline_preserves_raw_values_when_history_is_insufficient() -> None:
    report = build_calibration_summary_report(
        result=_result(
            [
                _outcome(trade_id="t1", probability_positive=0.7, predicted_net_return=0.03, realized_net_return=0.01),
                _outcome(trade_id="t2", probability_positive=0.7, predicted_net_return=0.03, realized_net_return=-0.01),
            ]
        )
    )

    assert report.summary["record_count"] == 2
    assert report.summary["confidence_noop_count"] == 2
    assert report.summary["expected_value_noop_count"] == 2
    assert all(row.raw_confidence_value == row.calibrated_confidence_value for row in report.records)
    assert all(row.raw_expected_value_net == row.calibrated_expected_value_net for row in report.records)


def test_calibration_pipeline_builds_buckets_and_corrections() -> None:
    outcomes = [
        _outcome(trade_id="t1", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.05),
        _outcome(trade_id="t2", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.01),
        _outcome(trade_id="t3", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=-0.01),
        _outcome(trade_id="t4", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=-0.02),
    ]
    report = build_calibration_summary_report(result=_result(outcomes))

    confidence_bucket = next(
        row for row in report.buckets if row.calibration_type == "confidence" and row.scope == "portfolio" and row.bucket_label == "0.80_to_1.00"
    )
    ev_bucket = next(
        row for row in report.buckets if row.calibration_type == "expected_value" and row.scope == "portfolio" and row.bucket_label == "gt_0.02"
    )

    assert confidence_bucket.sample_count == 4
    assert confidence_bucket.correction_delta < 0.0
    assert ev_bucket.sample_count == 4
    assert ev_bucket.correction_delta < 0.0
    assert any(not row.confidence_noop for row in report.records)
    assert any(not row.expected_value_noop for row in report.records)


def test_calibration_pipeline_produces_strategy_and_regime_scope_summaries() -> None:
    outcomes = [
        _outcome(trade_id="a1", strategy_id="alpha", regime_label="risk_on", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.02),
        _outcome(trade_id="a2", strategy_id="alpha", regime_label="risk_on", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.02),
        _outcome(trade_id="a3", strategy_id="alpha", regime_label="risk_on", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.02),
        _outcome(trade_id="a4", strategy_id="alpha", regime_label="risk_on", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.02),
        _outcome(trade_id="b1", strategy_id="beta", regime_label="risk_off", probability_positive=0.2, predicted_net_return=-0.03, realized_net_return=-0.04),
        _outcome(trade_id="b2", strategy_id="beta", regime_label="risk_off", probability_positive=0.2, predicted_net_return=-0.03, realized_net_return=-0.04),
        _outcome(trade_id="b3", strategy_id="beta", regime_label="risk_off", probability_positive=0.2, predicted_net_return=-0.03, realized_net_return=-0.04),
        _outcome(trade_id="b4", strategy_id="beta", regime_label="risk_off", probability_positive=0.2, predicted_net_return=-0.03, realized_net_return=-0.04),
    ]
    report = build_calibration_summary_report(result=_result(outcomes))

    scope_keys = {(row.scope, row.scope_id) for row in report.scope_summaries}
    assert ("portfolio", "portfolio") in scope_keys
    assert ("strategy", "alpha") in scope_keys
    assert ("strategy", "beta") in scope_keys
    assert ("regime", "risk_on") in scope_keys
    assert ("regime", "risk_off") in scope_keys


def test_calibration_artifacts_are_deterministic_and_round_trip(tmp_path: Path) -> None:
    report = build_calibration_summary_report(
        result=_result(
            [
                _outcome(trade_id=f"t{idx}", probability_positive=0.8, predicted_net_return=0.03, realized_net_return=0.02)
                for idx in range(1, 5)
            ]
        )
    )
    paths = write_calibration_artifacts(output_dir=tmp_path, report=report)

    payload = json.loads(paths["calibration_summary_report_json_path"].read_text(encoding="utf-8"))
    restored = CalibrationSummaryReport.from_dict(payload)
    records_df = pd.read_csv(paths["calibration_records_csv_path"])
    buckets_df = pd.read_csv(paths["calibration_buckets_csv_path"])
    summaries_df = pd.read_csv(paths["calibration_scope_summaries_csv_path"])

    assert restored.to_dict() == report.to_dict()
    assert len(records_df) == len(report.records)
    assert "bucket_label" in set(buckets_df.columns)
    assert "scope_id" in set(summaries_df.columns)
