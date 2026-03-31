from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.decision_journal.service import (
    build_trade_decision_contracts,
    build_candidate_journal_for_snapshot,
    summarize_entry_reason,
    summarize_exit_reason,
    summarize_selection_context,
    summarize_sizing_context,
    write_decision_journal_artifacts,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import JsonPaperStateStore, run_paper_trading_cycle_for_targets


def test_build_candidate_journal_for_snapshot_serializes_rank_and_rejections() -> None:
    bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|xsec|demo|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="xsec_momentum_topn",
        universe_id="demo",
        score_map={"AAPL": 2.0, "MSFT": 1.0},
        latest_prices={"AAPL": 100.0, "MSFT": 200.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0, "MSFT": 0.0},
        skipped_symbols=["NVDA"],
        skip_reasons={"NVDA": "missing_feature_frame"},
        asset_return_map={"AAPL": 0.04, "MSFT": 0.01},
        selected_rejection_reasons={"MSFT": "outranked_by_other_candidate"},
    )

    assert len(bundle.candidate_evaluations) == 3
    rows = {row.symbol: row for row in bundle.candidate_evaluations}
    assert rows["AAPL"].candidate_status == "selected"
    assert rows["AAPL"].rank == 1
    assert rows["MSFT"].rejection_reason == "outranked_by_other_candidate"
    assert rows["NVDA"].rejection_reason == "missing_feature_frame"
    assert rows["AAPL"].flat_dict()["feature_snapshot"] == "asset_return=0.04|latest_price=100.0"


def test_write_decision_journal_artifacts_writes_flattened_outputs(tmp_path: Path) -> None:
    bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|sma_cross|demo|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="sma_cross",
        universe_id="demo",
        score_map={"AAPL": 1.5},
        latest_prices={"AAPL": 101.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0},
    )

    paths = write_decision_journal_artifacts(bundle=bundle, output_dir=tmp_path)

    assert paths["candidate_snapshot_json"].exists()
    assert paths["candidate_snapshot_csv"].exists()
    candidate_df = pd.read_csv(paths["candidate_snapshot_csv"])
    assert candidate_df.iloc[0]["symbol"] == "AAPL"
    assert candidate_df.iloc[0]["candidate_status"] == "selected"


def test_run_paper_trading_cycle_for_targets_enriches_and_persists_decision_bundle(tmp_path: Path) -> None:
    config = PaperTradingConfig(symbols=["AAPL"], strategy="sma_cross", initial_cash=10_000.0, top_n=1)
    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    base_bundle = build_candidate_journal_for_snapshot(
        timestamp="2025-01-04",
        run_id="manual|sma_cross|symbols|2025-01-04",
        cycle_id="2025-01-04",
        strategy_id="sma_cross",
        universe_id=None,
        score_map={"AAPL": 2.0},
        latest_prices={"AAPL": 100.0},
        selected_weights={"AAPL": 1.0},
        scheduled_weights={"AAPL": 1.0},
    )

    result = run_paper_trading_cycle_for_targets(
        config=config,
        state_store=state_store,
        as_of="2025-01-04",
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 2.0},
        latest_scheduled_weights={"AAPL": 1.0},
        latest_effective_weights={"AAPL": 1.0},
        target_diagnostics={},
        skipped_symbols=[],
        decision_bundle=base_bundle,
        auto_apply_fills=False,
    )

    assert result.decision_bundle is not None
    assert any(row.symbol == "AAPL" and row.candidate_status == "selected" for row in result.decision_bundle.trade_decisions)
    assert [row.instrument for row in result.trade_decision_contracts] == ["AAPL"]
    assert result.order_lifecycle_records[0].intent.symbol == "AAPL"
    assert result.reconciliation_result is not None
    assert result.reconciliation_result.diagnostics["reconciled"] is False

    paths = write_decision_journal_artifacts(bundle=result.decision_bundle, output_dir=tmp_path / "artifacts")
    trade_df = pd.read_csv(paths["trade_decisions_csv"])
    assert trade_df.iloc[0]["symbol"] == "AAPL"
    assert "selected" in summarize_entry_reason(result.decision_bundle.trade_decisions[0])
    assert "selected" in summarize_selection_context(result.decision_bundle.selection_decisions[0])
    assert isinstance(summarize_sizing_context(result.decision_bundle.sizing_decisions[0]), str)


def test_decision_journal_summary_helpers_handle_missing_fields() -> None:
    assert summarize_entry_reason({"candidate_status": "rejected", "rejection_reason": None}) == "no explicit entry rationale"
    assert summarize_exit_reason({"exit_trigger_type": "rebalance", "exit_reason_summary": None}) == "rebalance"
    assert summarize_selection_context({"selection_status": "rejected", "candidate_count": None}) == "rejected"
    assert summarize_sizing_context({"target_quantity": None}) == "no sizing context"


def test_build_trade_decision_contracts_maps_candidate_rows_deterministically() -> None:
    decisions = build_trade_decision_contracts(
        candidate_rows=[
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
                "target_weight": 0.5,
                "ev_adjusted_target_weight": 0.5,
                "expected_horizon_days": 5,
                "predicted_return": 0.02,
                "predicted_return_source": "ev_gate_model",
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
        prediction_rows=[
            {
                "symbol": "AAPL",
                "expected_gross_return": 0.03,
                "expected_cost": 0.01,
                "expected_net_return": 0.02,
            }
        ],
    )

    assert [row.instrument for row in decisions] == ["AAPL", "MSFT"]
    assert decisions[0].vetoed is False
    assert decisions[0].predicted_return == 0.02
    assert decisions[0].expected_value_gross == 0.03
    assert decisions[0].expected_cost == 0.01
    assert decisions[0].expected_value_net == 0.02
    assert decisions[0].probability_positive == 0.73
    assert decisions[0].confidence_score == 0.7
    assert decisions[0].reliability_score == 0.6
    assert decisions[0].uncertainty_score == 0.11
    assert decisions[0].calibration_score == 0.64
    assert decisions[0].metadata["schema_version"] == "trade_decision_contract_v1"
    assert decisions[0].metadata["predicted_return_semantics"] == "primary_trade_return_forecast"
    assert decisions[0].metadata["predicted_return_source"] == "ev_gate_model"
    assert decisions[0].metadata["predicted_return_value_source"] == "candidate_row.predicted_return"
    assert decisions[0].metadata["probability_positive_source"] == "candidate_row.probability_positive"
    assert decisions[0].metadata["confidence_score_source"] == "candidate_row.ev_confidence"
    assert decisions[0].metadata["reliability_score_source"] == "candidate_row.ev_reliability"
    assert decisions[0].metadata["uncertainty_score_source"] == "candidate_row.residual_std_final"
    assert decisions[0].metadata["calibration_score_source"] == "candidate_row.reliability_calibrated_score"
    assert decisions[0].metadata["expected_value_net_source"] == "prediction_row.expected_net_return"
    assert decisions[0].metadata["expected_value_gross_source"] == "prediction_row.expected_gross_return"
    assert decisions[0].metadata["expected_cost_source"] == "prediction_row.expected_cost"
    assert decisions[0].metadata["ev_decomposition_status"] == "explicit"
    assert decisions[0].rationale_summary == "executed | passed_trade_checks"
    assert decisions[0].rationale_labels == ["executed", "execution", "passed_trade_checks"]
    assert decisions[0].rationale_context["has_veto"] is False
    assert decisions[0].metadata["veto_reason_count"] == 0
    assert decisions[1].vetoed is True
    assert "blocked_below_entry_threshold" in decisions[1].veto_reasons
    assert "regression_unavailable" in decisions[1].veto_reasons
    assert decisions[1].rationale_labels == [
        "skipped",
        "score_band_blocked",
        "score_band",
        "filtered_by_score_band",
        "blocked_below_entry_threshold",
        "regression_unavailable",
    ]
    assert decisions[1].rationale_context["has_veto"] is True
    assert decisions[1].rationale_context["veto_reason_count"] == 3
    assert decisions[1].probability_positive is None
    assert decisions[1].uncertainty_score is None
    assert decisions[1].calibration_score is None
    assert decisions[1].metadata["expected_value_net_derived"] is True
    assert decisions[1].metadata["expected_value_gross_derived"] is True
    assert decisions[1].metadata["ev_decomposition_status"] == "derived"


def test_build_trade_decision_contracts_uses_candidate_level_ev_fields_when_prediction_rows_missing() -> None:
    decisions = build_trade_decision_contracts(
        candidate_rows=[
            {
                "date": "2025-01-05",
                "symbol": "NVDA",
                "strategy_id": "sma_cross",
                "candidate_status": "selected",
                "candidate_outcome": "selected",
                "current_weight": 0.0,
                "target_weight": 0.25,
                "expected_horizon_days": 3,
                "predicted_return": 0.011,
                "expected_net_return": 0.012,
                "expected_gross_return": 0.015,
                "expected_cost": 0.003,
                "combined_confidence": 0.44,
                "reliability_calibrated_score": 0.41,
                "residual_std_used": 0.22,
            }
        ],
        prediction_rows=[],
    )

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision.predicted_return == 0.011
    assert decision.expected_value_net == 0.012
    assert decision.expected_value_gross == 0.015
    assert decision.expected_cost == 0.003
    assert decision.metadata["predicted_return_value_source"] == "candidate_row.predicted_return"
    assert decision.metadata["expected_value_net_source"] == "candidate_row.expected_net_return"
    assert decision.metadata["expected_value_gross_source"] == "candidate_row.expected_gross_return"
    assert decision.metadata["expected_cost_source"] == "candidate_row.expected_cost"
    assert decision.confidence_score == 0.44
    assert decision.reliability_score == 0.41
    assert decision.uncertainty_score == 0.22
    assert decision.calibration_score == 0.41
    assert decision.metadata["confidence_score_source"] == "candidate_row.combined_confidence"
    assert decision.metadata["reliability_score_source"] == "candidate_row.reliability_calibrated_score"
    assert decision.metadata["uncertainty_score_source"] == "candidate_row.residual_std_used"
    assert decision.metadata["calibration_score_source"] == "candidate_row.reliability_calibrated_score"
    assert decision.metadata["ev_decomposition_status"] == "explicit"
    assert decision.rationale_labels == ["selected"]


def test_build_trade_decision_contracts_supports_single_veto_reason() -> None:
    decisions = build_trade_decision_contracts(
        candidate_rows=[
            {
                "date": "2025-01-06",
                "symbol": "TSLA",
                "strategy_id": "sma_cross",
                "candidate_status": "skipped",
                "candidate_outcome": "confidence_filtered",
                "action_reason": "filtered_by_confidence",
                "expected_horizon_days": 2,
                "predicted_return": 0.005,
                "ev_confidence": 0.12,
                "ev_reliability": 0.45,
                "was_filtered_by_confidence": True,
            }
        ],
        prediction_rows=[],
    )

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision.vetoed is True
    assert decision.veto_reasons == ["filtered_by_confidence"]
    assert decision.rationale_summary == "skipped | confidence_filtered | filtered_by_confidence"
    assert decision.rationale_labels == ["skipped", "confidence_filtered", "filtered_by_confidence"]
    assert decision.rationale_context["veto_reason_count"] == 1
    assert decision.metadata["veto_reason_count"] == 1
