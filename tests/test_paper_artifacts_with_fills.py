from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.decision_journal.service import build_candidate_journal_for_snapshot
from trading_platform.paper.models import (
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
    PaperTradingRunResult,
)
from trading_platform.paper.service import write_paper_trading_artifacts
from trading_platform.universe_provenance.service import build_universe_provenance_bundle


def test_write_paper_trading_artifacts_writes_fills_and_equity_curve(tmp_path: Path) -> None:
    metadata_dir = tmp_path / "metadata"
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(
            cash=9000.0,
            positions={
                "AAPL": PaperPosition(
                    symbol="AAPL",
                    quantity=10,
                    avg_price=100.0,
                    last_price=110.0,
                )
            },
            last_targets={"AAPL": 1.0},
            initial_cash_basis=10_000.0,
            cumulative_realized_pnl=25.0,
            cumulative_fees=1.0,
        ),
        latest_prices={"AAPL": 110.0},
        latest_scores={"AAPL": 2.0},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[
            PaperOrder(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                reference_price=101.0,
                target_weight=1.0,
                current_quantity=0,
                target_quantity=10,
                notional=1010.0,
                reason="rebalance_to_target",
            )
        ],
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=101.0,
                notional=1010.0,
                commission=1.0,
                slippage_bps=5.0,
                realized_pnl=25.0,
            )
        ],
        skipped_symbols=[],
        diagnostics={
            "ok": True,
            "order_generation": {
                "candidate_trade_rows": [
                    {
                        "date": "2025-01-04",
                        "symbol": "AAPL",
                        "strategy_id": "sma_cross",
                        "signal_family": "trend",
                        "candidate_status": "executed",
                        "candidate_outcome": "executed",
                        "candidate_stage": "execution",
                        "action_reason": "passed_trade_checks",
                        "current_weight": 0.0,
                        "target_weight": 1.0,
                        "ev_adjusted_target_weight": 1.0,
                        "expected_horizon_days": 5,
                        "predicted_return": 0.02,
                        "probability_positive": 0.73,
                        "ev_confidence": 0.7,
                        "ev_reliability": 0.6,
                        "residual_std_final": 0.11,
                        "reliability_calibrated_score": 0.64,
                    },
                    {
                        "date": "2025-01-04",
                        "symbol": "MSFT",
                        "strategy_id": "sma_cross",
                        "signal_family": "trend",
                        "candidate_status": "skipped",
                        "candidate_outcome": "score_band_blocked",
                        "candidate_stage": "score_band",
                        "skip_reason": "blocked_below_entry_threshold",
                        "action_reason": "filtered_by_score_band",
                        "ev_model_fallback_reason": "regression_unavailable",
                        "current_weight": 0.0,
                        "target_weight": 0.0,
                        "expected_horizon_days": 5,
                        "predicted_return": -0.01,
                        "ev_confidence": 0.25,
                        "ev_reliability": 0.3,
                    },
                ],
                "ev_prediction_rows": [
                    {
                        "symbol": "AAPL",
                        "expected_gross_return": 0.03,
                        "expected_cost": 0.01,
                        "expected_net_return": 0.02,
                    }
                ],
            },
            "accounting": {
                "auto_apply_fills": True,
                "fill_application_status": "fills_applied",
                "starting_cash": 10_000.0,
                "ending_cash": 9_000.0,
                "starting_equity": 10_000.0,
                "ending_equity": 10_100.0,
                "fill_count": 1,
                "buy_fill_count": 1,
                "sell_fill_count": 0,
                "cumulative_realized_pnl": 25.0,
                "unrealized_pnl": 100.0,
                "total_pnl": 100.0,
            },
            "target_construction": {
                "multi_strategy_allocation": {
                    "sleeve_contribution": {"core": 1.0},
                    "normalized_capital_weights": {"core": 1.0},
                }
            },
            "strategy_execution_handoff": {
                "activation_applied": True,
                "active_strategy_count": 1,
            },
        },
        decision_bundle=build_candidate_journal_for_snapshot(
            timestamp="2025-01-04",
            run_id="manual|sma_cross|symbols|2025-01-04",
            cycle_id="2025-01-04",
            strategy_id="sma_cross",
            universe_id=None,
            score_map={"AAPL": 2.0},
            latest_prices={"AAPL": 110.0},
            selected_weights={"AAPL": 1.0},
            scheduled_weights={"AAPL": 1.0},
        ),
    )

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path, metadata_dir=metadata_dir)

    assert paths["fills_path"].exists()
    assert paths["equity_snapshot_path"].exists()
    assert paths["candidate_snapshot_csv"].exists()
    assert paths["trade_decision_contracts_v1_csv"].exists()
    assert paths["portfolio_performance_summary_path"].exists()
    assert paths["execution_summary_json_path"].exists()
    assert paths["strategy_contribution_summary_path"].exists()
    assert paths["order_lifecycle_records_json_path"].exists()
    assert paths["order_lifecycle_reconciliation_json_path"].exists()
    assert paths["kpi_payload_json_path"].exists()
    assert paths["trade_explorer_payload_json_path"].exists()
    assert paths["strategy_health_payload_json_path"].exists()
    assert paths["transaction_cost_report_json_path"].exists()
    assert paths["realtime_kpi_monitoring_json_path"].exists()
    assert paths["system_health_payload_json_path"].exists()
    assert paths["paper_risk_controls_json_path"].exists()
    assert paths["drift_detection_report_json_path"].exists()
    assert paths["calibration_summary_report_json_path"].exists()
    assert paths["strategy_decay_report_json_path"].exists()
    assert paths["strategy_lifecycle_report_json_path"].exists()

    fills_df = pd.read_csv(paths["fills_path"])
    equity_df = pd.read_csv(paths["equity_snapshot_path"])
    positions_df = pd.read_csv(paths["positions_path"])
    candidate_df = pd.read_csv(paths["candidate_snapshot_csv"])
    contract_df = pd.read_csv(paths["trade_decision_contracts_v1_csv"])
    order_lifecycle_df = pd.read_csv(paths["order_lifecycle_records_csv_path"])
    reconciliation_df = pd.read_csv(paths["order_lifecycle_reconciliation_mismatches_csv_path"])
    kpi_df = pd.read_csv(paths["kpi_records_csv_path"])
    trade_explorer_df = pd.read_csv(paths["trade_explorer_rows_csv_path"])
    strategy_health_df = pd.read_csv(paths["strategy_health_payload_csv_path"])
    transaction_cost_df = pd.read_csv(paths["transaction_cost_records_csv_path"])
    realtime_monitoring_df = pd.read_csv(paths["realtime_kpi_monitoring_csv_path"])
    system_health_df = pd.read_csv(paths["system_health_checks_csv_path"])
    drift_df = pd.read_csv(paths["drift_metric_snapshots_csv_path"])
    drift_summary = json.loads(paths["drift_detection_summary_json_path"].read_text(encoding="utf-8"))
    calibration_df = pd.read_csv(paths["calibration_records_csv_path"])
    calibration_summary = json.loads(paths["calibration_summary_json_path"].read_text(encoding="utf-8"))
    decay_df = pd.read_csv(paths["strategy_decay_records_csv_path"])
    decay_summary = json.loads(paths["strategy_decay_summary_json_path"].read_text(encoding="utf-8"))
    strategy_lifecycle_df = pd.read_csv(paths["strategy_lifecycle_actions_csv_path"])
    lifecycle_summary = json.loads(paths["strategy_lifecycle_summary_json_path"].read_text(encoding="utf-8"))

    assert len(fills_df) == 1
    assert fills_df.iloc[0]["symbol"] == "AAPL"
    assert float(fills_df.iloc[0]["realized_pnl"]) == 25.0
    assert equity_df.iloc[0]["as_of"] == "2025-01-04"
    assert float(equity_df.iloc[0]["equity"]) == 10100.0
    assert float(equity_df.iloc[0]["unrealized_pnl"]) == 100.0
    assert list(positions_df.columns) == [
        "symbol",
        "quantity",
        "avg_price",
        "last_price",
        "cost_basis",
        "market_value",
        "unrealized_pnl",
        "portfolio_weight",
    ]
    assert candidate_df.iloc[0]["symbol"] == "AAPL"
    assert list(contract_df["instrument"]) == ["AAPL", "MSFT"]
    assert not bool(contract_df.iloc[0]["vetoed"])
    assert bool(contract_df.iloc[1]["vetoed"])
    assert float(contract_df.iloc[0]["probability_positive"]) == 0.73
    assert float(contract_df.iloc[0]["uncertainty_score"]) == 0.11
    assert float(contract_df.iloc[0]["calibration_score"]) == 0.64
    assert contract_df.iloc[0]["rationale_labels"] == "executed|execution|passed_trade_checks"
    assert "has_veto=False" in str(contract_df.iloc[0]["rationale_context"])
    assert "ev_decomposition_status=explicit" in str(contract_df.iloc[0]["metadata"])
    assert "uncertainty_score_source=candidate_row.residual_std_final" in str(contract_df.iloc[0]["metadata"])
    assert "expected_value_gross_source=prediction_row.expected_gross_return" in str(contract_df.iloc[0]["metadata"])
    assert "veto_reason_count=3" in str(contract_df.iloc[1]["rationale_context"])
    assert "ev_decomposition_status=derived" in str(contract_df.iloc[1]["metadata"])
    assert order_lifecycle_df.iloc[0]["final_status"] == "filled"
    assert reconciliation_df.empty
    assert "equity" in set(kpi_df["metric_name"])
    assert list(trade_explorer_df["symbol"]) == ["AAPL", "MSFT"]
    assert list(strategy_health_df["strategy_id"]) == ["sma_cross"]
    assert set(transaction_cost_df["stage"]) == {"estimate", "realized"}
    assert "drawdown" in set(realtime_monitoring_df["metric_name"])
    assert "operating_state_code" in set(kpi_df["metric_name"])
    assert "signal_count" in set(kpi_df["metric_name"])
    assert "record_count" in set(kpi_df["metric_name"])
    assert "strategy_count" in set(kpi_df["metric_name"])
    assert "action_count" in set(kpi_df["metric_name"])
    assert "pipeline_integrity" in set(system_health_df["check_name"])
    assert "metric_name" in set(drift_df.columns)
    assert drift_summary["snapshot_count"] >= 0
    assert "raw_confidence_value" in set(calibration_df.columns)
    assert calibration_summary["record_count"] >= 0
    assert "strategy_id" in set(decay_df.columns)
    assert decay_summary["strategy_count"] >= 0
    assert "action_type" in set(strategy_lifecycle_df.columns)
    assert lifecycle_summary["strategy_count"] >= 0
    assert not any(key.startswith("metadata_") for key in paths)


def test_write_paper_trading_artifacts_refreshes_metadata_sidecars_when_universe_bundle_exists(
    tmp_path: Path,
) -> None:
    metadata_dir = tmp_path / "metadata"
    universe_bundle = build_universe_provenance_bundle(
        symbols=["AAPL"],
        base_universe_id="demo",
        sub_universe_id="demo_screened",
        filter_definitions=[{"filter_name": "include", "filter_type": "symbol_include_list", "symbols": ["AAPL"]}],
        feature_loader=lambda _symbol: pd.DataFrame(
            {"timestamp": pd.date_range("2025-01-01", periods=3), "close": [10.0, 11.0, 12.0]}
        ),
    )
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(cash=10_000.0, positions={}, last_targets={}),
        latest_prices={},
        latest_scores={},
        latest_target_weights={},
        scheduled_target_weights={},
        orders=[],
        universe_bundle=universe_bundle,
    )

    paths = write_paper_trading_artifacts(
        result=result,
        output_dir=tmp_path / "artifacts",
        metadata_dir=metadata_dir,
    )

    assert paths["metadata_sub_universe_snapshot_csv"].exists()
    assert paths["metadata_universe_enrichment_csv"].exists()
    sidecar_df = pd.read_csv(paths["metadata_sub_universe_snapshot_csv"])
    assert sidecar_df.iloc[0]["sub_universe_id"] == "demo_screened"
