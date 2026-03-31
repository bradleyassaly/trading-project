from __future__ import annotations

import json

import pytest

from trading_platform.governance.models import (
    LiveReadinessCheckResult,
    LiveReadinessDecision,
    PromotionDecision,
    PromotionGateEvaluation,
    PromotionGateResult,
    StrategyScorecard,
    build_live_readiness_skeleton,
)


def test_strategy_scorecard_round_trip_and_deterministic_serialization() -> None:
    scorecard = StrategyScorecard(
        candidate_id="candidate-1",
        strategy_family="momentum",
        training_period="2024-01-01:2024-06-30",
        validation_period="2024-07-01:2024-09-30",
        prediction_count=125,
        realized_return=0.084,
        expected_return=0.091,
        turnover=0.33,
        slippage_estimate=0.012,
        drawdown=0.08,
        calibration_score=0.62,
        stability_score=0.71,
        regime_robustness_score=0.58,
        readiness_flags=("walk_forward_pass", "paper_ready_candidate"),
        rejection_reasons=(),
        metadata={"z": 3, "a": 1},
    )

    payload = scorecard.to_dict()
    assert payload["candidate_id"] == "candidate-1"
    assert payload["metadata"] == {"a": 1, "z": 3}
    assert payload["readiness_flags"] == ["walk_forward_pass", "paper_ready_candidate"]
    assert json.dumps(payload, sort_keys=True) == json.dumps(scorecard.to_dict(), sort_keys=True)

    restored = StrategyScorecard.from_dict(payload)
    assert restored == scorecard
    assert restored.to_dict() == payload


def test_strategy_scorecard_from_dict_defaults_optional_fields() -> None:
    scorecard = StrategyScorecard.from_dict(
        {
            "candidate_id": "candidate-2",
            "strategy_family": "mean_reversion",
        }
    )

    assert scorecard.training_period is None
    assert scorecard.validation_period is None
    assert scorecard.prediction_count is None
    assert scorecard.readiness_flags == []
    assert scorecard.rejection_reasons == []
    assert scorecard.metadata == {}


def test_strategy_scorecard_validation_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="candidate_id must be a non-empty string"):
        StrategyScorecard(
            candidate_id="",
            strategy_family="momentum",
        )

    with pytest.raises(ValueError, match="strategy_family must be a non-empty string"):
        StrategyScorecard(
            candidate_id="candidate-3",
            strategy_family="",
        )

    with pytest.raises(ValueError, match="prediction_count must be >= 0"):
        StrategyScorecard(
            candidate_id="candidate-4",
            strategy_family="momentum",
            prediction_count=-1,
        )

    with pytest.raises(ValueError, match="turnover must be >= 0"):
        StrategyScorecard(
            candidate_id="candidate-5",
            strategy_family="momentum",
            turnover=-0.1,
        )


def test_strategy_scorecard_normalizes_string_lists() -> None:
    scorecard = StrategyScorecard(
        candidate_id="candidate-6",
        strategy_family="volume_surprise",
        readiness_flags="watchlist",
        rejection_reasons=("insufficient_history", "", "high_turnover"),
    )

    assert scorecard.readiness_flags == ["watchlist"]
    assert scorecard.rejection_reasons == ["insufficient_history", "high_turnover"]


def test_promotion_gate_models_round_trip_deterministically() -> None:
    evaluation = PromotionGateEvaluation(
        candidate_id="momentum|5|1",
        passed=False,
        gate_results=[
            PromotionGateResult(
                gate_name="mean_rank_ic",
                passed=False,
                reason_code="low_mean_rank_ic",
                actual=0.01,
                threshold=0.02,
                comparator=">",
            ),
            PromotionGateResult(
                gate_name="turnover",
                passed=True,
                reason_code="high_turnover",
                actual=0.2,
                threshold=0.75,
                comparator="<=",
            ),
        ],
        rejection_reasons=["low_mean_rank_ic"],
        passed_gate_names=["turnover"],
        failed_gate_names=["mean_rank_ic"],
        metadata={"z": 3, "a": 1},
    )

    payload = evaluation.to_dict()
    assert payload["candidate_id"] == "momentum|5|1"
    assert payload["metadata"] == {"a": 1, "z": 3}
    assert payload["failed_gate_names"] == ["mean_rank_ic"]
    assert json.dumps(payload, sort_keys=True) == json.dumps(evaluation.to_dict(), sort_keys=True)

    restored = PromotionGateEvaluation.from_dict(payload)
    assert restored == evaluation


def test_promotion_gate_models_validate_required_fields() -> None:
    with pytest.raises(ValueError, match="gate_name must be a non-empty string"):
        PromotionGateResult(gate_name="", passed=True, reason_code="reason")

    with pytest.raises(ValueError, match="candidate_id must be a non-empty string"):
        PromotionGateEvaluation(candidate_id="", passed=True, gate_results=[])


def test_promotion_decision_round_trip_and_summary_metadata() -> None:
    evaluation = PromotionGateEvaluation(
        candidate_id="momentum|5|1",
        passed=False,
        gate_results=[
            PromotionGateResult(
                gate_name="mean_rank_ic",
                passed=False,
                reason_code="low_mean_rank_ic",
                actual=0.01,
                threshold=0.02,
                comparator=">",
            )
        ],
        rejection_reasons=["low_mean_rank_ic"],
        passed_gate_names=[],
        failed_gate_names=["mean_rank_ic"],
        metadata={"z": 3, "a": 1},
    )

    decision = PromotionDecision.from_gate_evaluation(evaluation, final_status="reject")
    payload = decision.to_dict()

    assert payload["final_status"] == "reject"
    assert payload["summary_metadata"] == {
        "a": 1,
        "candidate_id": "momentum|5|1",
        "failed_gate_count": 1,
        "gate_count": 1,
        "passed_gate_count": 0,
        "z": 3,
    }
    assert json.dumps(payload, sort_keys=True) == json.dumps(decision.to_dict(), sort_keys=True)

    restored = PromotionDecision.from_dict(payload)
    assert restored == decision


def test_live_readiness_skeleton_is_deterministic_and_live_disabled() -> None:
    decision = build_live_readiness_skeleton(
        "generated_momentum_a",
        summary_metadata={"z": 3, "a": 1},
    )

    payload = decision.to_dict()
    assert payload["strategy_id"] == "generated_momentum_a"
    assert payload["ready_for_live"] is False
    assert payload["live_trading_enabled"] is False
    assert payload["final_status"] == "not_ready"
    assert payload["passed_check_names"] == []
    assert payload["failed_check_names"] == [
        "monitoring_coverage",
        "reconciliation_coverage",
        "execution_support",
        "capital_controls",
        "risk_controls",
        "operator_approval",
    ]
    assert payload["summary_metadata"]["governance_scaffolding_only"] is True
    assert payload["summary_metadata"]["a"] == 1
    assert json.dumps(payload, sort_keys=True) == json.dumps(decision.to_dict(), sort_keys=True)

    restored = LiveReadinessDecision.from_dict(payload)
    assert restored == decision


def test_live_readiness_models_validate_required_fields() -> None:
    with pytest.raises(ValueError, match="check_name must be a non-empty string"):
        LiveReadinessCheckResult(check_name="", passed=False, reason_code="missing")

    with pytest.raises(ValueError, match="status must be one of: ready, missing, blocked, unknown"):
        LiveReadinessCheckResult(check_name="monitoring", passed=False, reason_code="missing", status="bad")

    with pytest.raises(ValueError, match="strategy_id must be a non-empty string"):
        LiveReadinessDecision(strategy_id="", ready_for_live=False)

    with pytest.raises(ValueError, match="final_status must be one of: not_ready, shadow_only, ready_candidate"):
        LiveReadinessDecision(strategy_id="generated_momentum_a", ready_for_live=False, final_status="live")
