from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.decision_journal.models import TradeDecision
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.reporting.outcome_attribution import (
    TradeOutcomeAttributionReport,
    build_trade_outcome_attribution_report,
    write_trade_outcome_attribution_artifacts,
)


def _result() -> PaperTradingRunResult:
    return PaperTradingRunResult(
        as_of="2025-01-10",
        state=PaperPortfolioState(cash=10_000.0),
        latest_prices={},
        latest_scores={},
        latest_target_weights={},
        scheduled_target_weights={},
        orders=[],
        trade_decision_contracts=[
            TradeDecision(
                decision_id="trade_decision_contract_v1|2025-01-03|AAPL|alpha",
                timestamp="2025-01-03",
                strategy_id="alpha",
                strategy_family="trend",
                candidate_id="candidate-aapl",
                instrument="AAPL",
                side="BUY",
                horizon_days=5,
                predicted_return=0.03,
                expected_value_gross=0.04,
                expected_cost=0.01,
                expected_value_net=0.03,
                probability_positive=0.7,
                confidence_score=0.8,
                reliability_score=0.65,
                calibration_score=0.72,
                regime_label="bull",
            )
        ],
        attribution={
            "trade_rows": [
                {
                    "trade_id": "paper-trade-1",
                    "date": "2025-01-10",
                    "symbol": "AAPL",
                    "strategy_id": "alpha",
                    "side": "long",
                    "quantity": 10,
                    "entry_reference_price": 100.0,
                    "entry_price": 100.2,
                    "exit_reference_price": 108.0,
                    "exit_price": 107.7,
                    "gross_realized_pnl": 80.0,
                    "net_realized_pnl": 74.0,
                    "realized_pnl": 74.0,
                    "total_execution_cost": 6.0,
                    "holding_period_days": 7,
                    "status": "closed",
                    "entry_date": "2025-01-03",
                    "exit_date": "2025-01-10",
                    "entry_decision_id": "trade_decision_contract_v1|2025-01-03|AAPL|alpha",
                    "entry_decision_timestamp": "2025-01-03",
                    "predicted_return": 0.03,
                    "predicted_gross_return": 0.04,
                    "predicted_cost": 0.01,
                    "predicted_net_return": 0.03,
                    "probability_positive": 0.7,
                    "confidence_score": 0.8,
                    "reliability_score": 0.65,
                    "calibration_score": 0.72,
                    "regime_label": "bull",
                    "expected_horizon_days": 5,
                    "entry_reason": "entry_signal",
                    "exit_reason": "target_exit",
                    "target_weight_entry": 1.0,
                    "target_weight_exit": 0.0,
                    "exit_regime_label": "bear",
                }
            ]
        },
    )


def test_trade_outcome_attribution_report_round_trips_and_aggregates() -> None:
    report = build_trade_outcome_attribution_report(result=_result())

    assert TradeOutcomeAttributionReport.from_dict(report.to_dict()) == report
    assert report.summary["closed_trade_count"] == 1
    assert report.outcomes[0].predicted_net_return == 0.03
    assert report.outcomes[0].realized_net_return == 0.074
    assert report.attributions[0].forecast_gap == 0.044
    assert report.attributions[0].regime_mismatch is True
    aggregate_keys = {(row.group_type, row.group_key) for row in report.aggregates}
    assert ("strategy", "alpha") in aggregate_keys
    assert ("instrument", "AAPL") in aggregate_keys
    assert ("regime", "bull") in aggregate_keys
    assert ("confidence_bucket", "high") in aggregate_keys
    assert ("horizon", "5") in aggregate_keys


def test_trade_outcome_attribution_artifacts_are_written(tmp_path: Path) -> None:
    report = build_trade_outcome_attribution_report(result=_result())
    paths = write_trade_outcome_attribution_artifacts(output_dir=tmp_path, report=report)

    assert paths["trade_outcome_attribution_report_json_path"].exists()
    assert paths["trade_outcomes_csv_path"].exists()
    assert paths["trade_outcome_attribution_csv_path"].exists()
    assert paths["trade_outcome_aggregates_csv_path"].exists()
    summary = json.loads(paths["trade_outcome_attribution_summary_json_path"].read_text(encoding="utf-8"))
    outcomes_df = pd.read_csv(paths["trade_outcomes_csv_path"])
    assert summary["closed_trade_count"] == 1
    assert outcomes_df.iloc[0]["instrument"] == "AAPL"
