from __future__ import annotations

import json

import pytest

from trading_platform.decision_journal import TradeDecision


def test_trade_decision_round_trip_and_deterministic_serialization() -> None:
    decision = TradeDecision(
        decision_id="trade-1",
        timestamp="2025-01-04T15:30:00Z",
        strategy_id="sma_cross",
        strategy_family="trend",
        candidate_id="cand-1",
        instrument="AAPL",
        side="BUY",
        horizon_days=5,
        predicted_return=0.024,
        expected_value_gross=0.031,
        expected_cost=0.007,
        expected_value_net=0.024,
        probability_positive=0.68,
        confidence_score=0.62,
        reliability_score=0.58,
        uncertainty_score=0.14,
        calibration_score=0.61,
        regime_label="risk_on",
        sizing_signal=0.8,
        vetoed=False,
        veto_reasons=("rule_a", "rule_b"),
        rationale_summary="passed all checks",
        rationale_labels=("selected", "passed_all_checks"),
        rationale_context={"status": "selected", "veto_reason_count": 0},
        metadata={"z": 3, "a": 1},
    )

    payload = decision.to_dict()
    assert payload["decision_id"] == "trade-1"
    assert payload["metadata"] == {"a": 1, "z": 3}
    assert payload["veto_reasons"] == ["rule_a", "rule_b"]
    assert payload["rationale_labels"] == ["selected", "passed_all_checks"]
    assert payload["rationale_context"] == {"status": "selected", "veto_reason_count": 0}
    assert payload["probability_positive"] == 0.68
    assert payload["uncertainty_score"] == 0.14
    assert payload["calibration_score"] == 0.61
    assert json.dumps(payload, sort_keys=True) == json.dumps(decision.to_dict(), sort_keys=True)

    restored = TradeDecision.from_dict(payload)
    assert restored == decision
    assert restored.to_dict() == payload


def test_trade_decision_flat_dict_includes_csv_safe_fields() -> None:
    decision = TradeDecision(
        decision_id="trade-2",
        timestamp="2025-01-04T15:30:00Z",
        strategy_id="mean_reversion",
        instrument="MSFT",
        side="SELL",
        horizon_days=3,
        predicted_return=-0.01,
        expected_value_gross=-0.008,
        expected_cost=0.002,
        expected_value_net=-0.01,
        probability_positive=0.31,
        uncertainty_score=0.14,
        vetoed=True,
        veto_reasons=["limit_breached"],
        rationale_summary="skipped | limit_breached",
        rationale_labels=["skipped", "limit_breached"],
        rationale_context={"candidate_status": "skipped", "veto_reason_count": 1},
        metadata={"reason": "risk", "severity": "high"},
    )

    flat = decision.flat_dict()
    assert flat["decision_id"] == "trade-2"
    assert flat["veto_reasons"] == "limit_breached"
    assert flat["rationale_labels"] == "skipped|limit_breached"
    assert "candidate_status=skipped" in str(flat["rationale_context"])
    assert flat["probability_positive"] == 0.31
    assert flat["uncertainty_score"] == 0.14
    assert "severity=high" in str(flat["metadata"])
    assert "reason=risk" in str(flat["metadata"])


def test_trade_decision_validation_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="horizon_days must be > 0"):
        TradeDecision(
            decision_id="trade-3",
            timestamp="2025-01-04T15:30:00Z",
            strategy_id="sma_cross",
            instrument="AAPL",
            side="BUY",
            horizon_days=0,
            predicted_return=0.01,
            expected_value_gross=0.02,
            expected_cost=0.01,
            expected_value_net=0.01,
        )

    with pytest.raises(ValueError, match="instrument must be a non-empty string"):
        TradeDecision(
            decision_id="trade-4",
            timestamp="2025-01-04T15:30:00Z",
            strategy_id="sma_cross",
            instrument="",
            side="BUY",
            horizon_days=5,
            predicted_return=0.01,
            expected_value_gross=0.02,
            expected_cost=0.01,
            expected_value_net=0.01,
        )

    with pytest.raises(TypeError, match="metadata must be a mapping"):
        TradeDecision(
            decision_id="trade-5",
            timestamp="2025-01-04T15:30:00Z",
            strategy_id="sma_cross",
            instrument="AAPL",
            side="BUY",
            horizon_days=5,
            predicted_return=0.01,
            expected_value_gross=0.02,
            expected_cost=0.01,
            expected_value_net=0.01,
            metadata=["not", "a", "dict"],  # type: ignore[arg-type]
        )


def test_trade_decision_from_dict_requires_required_fields() -> None:
    with pytest.raises(KeyError):
        TradeDecision.from_dict(
            {
                "decision_id": "trade-6",
                "timestamp": "2025-01-04T15:30:00Z",
                "strategy_id": "sma_cross",
                "instrument": "AAPL",
                "side": "BUY",
                "horizon_days": 5,
                "expected_value_gross": 0.02,
                "expected_cost": 0.01,
                "expected_value_net": 0.01,
            }
        )


def test_trade_decision_from_dict_defaults_optional_quality_fields_to_none() -> None:
    decision = TradeDecision.from_dict(
        {
            "decision_id": "trade-7",
            "timestamp": "2025-01-04T15:30:00Z",
            "strategy_id": "sma_cross",
            "instrument": "AAPL",
            "side": "BUY",
            "horizon_days": 5,
            "predicted_return": 0.01,
            "expected_value_gross": 0.02,
            "expected_cost": 0.01,
            "expected_value_net": 0.01,
        }
    )

    assert decision.probability_positive is None
    assert decision.confidence_score is None
    assert decision.reliability_score is None
    assert decision.uncertainty_score is None
    assert decision.calibration_score is None
    assert decision.rationale_labels == []
    assert decision.rationale_context == {}


def test_trade_decision_validation_rejects_non_mapping_rationale_context() -> None:
    with pytest.raises(TypeError, match="rationale_context must be a mapping"):
        TradeDecision(
            decision_id="trade-8",
            timestamp="2025-01-04T15:30:00Z",
            strategy_id="sma_cross",
            instrument="AAPL",
            side="BUY",
            horizon_days=5,
            predicted_return=0.01,
            expected_value_gross=0.02,
            expected_cost=0.01,
            expected_value_net=0.01,
            rationale_context=["not", "a", "dict"],  # type: ignore[arg-type]
        )
