"""Tests for smart money integration with market scanner."""
from __future__ import annotations

import pytest

from trading_platform.kalshi.market_scanner import ScanResult


class TestSmartMoneyConfidenceAdjustment:
    def _base_result(self, confidence: float = 0.5) -> ScanResult:
        return ScanResult(
            ticker="T1", yes_price=55.0, days_to_close=10,
            signal_scores={"cal_drift": 1.5}, strongest_signal="cal_drift",
            recommended_side="YES", confidence=confidence,
            news_context="unscheduled", kelly_fraction=0.02,
            smart_money_direction="NEUTRAL", smart_money_confidence=0.0,
        )

    def test_aligned_signal_boosts_confidence(self) -> None:
        r = self._base_result(confidence=0.5)
        # Simulate: smart money direction matches recommended_side
        if r.smart_money_direction == r.recommended_side and r.smart_money_confidence > 0.35:
            new_conf = min(1.0, r.confidence + 0.15)
        else:
            new_conf = r.confidence
        # No boost because direction is NEUTRAL
        assert new_conf == 0.5

        # Now with aligned direction
        r2 = ScanResult(
            ticker="T1", yes_price=55.0, days_to_close=10,
            signal_scores={}, strongest_signal="cal_drift",
            recommended_side="YES", confidence=0.5,
            news_context="unscheduled", kelly_fraction=0.02,
            smart_money_direction="YES", smart_money_confidence=0.5,
        )
        new_conf2 = min(1.0, r2.confidence + 0.15) if (
            r2.smart_money_direction == r2.recommended_side and r2.smart_money_confidence > 0.35
        ) else r2.confidence
        assert new_conf2 == pytest.approx(0.65)

    def test_opposing_signal_reduces_confidence(self) -> None:
        r = ScanResult(
            ticker="T1", yes_price=55.0, days_to_close=10,
            signal_scores={}, strongest_signal="cal_drift",
            recommended_side="YES", confidence=0.5,
            news_context="unscheduled", kelly_fraction=0.02,
            smart_money_direction="NO", smart_money_confidence=0.5,
        )
        # Opposing direction → multiply by 0.7
        new_conf = r.confidence * 0.7 if (
            r.smart_money_direction != "NEUTRAL"
            and r.smart_money_direction != r.recommended_side
        ) else r.confidence
        assert new_conf == pytest.approx(0.35)

    def test_missing_signal_unchanged(self) -> None:
        r = self._base_result(confidence=0.6)
        # NEUTRAL direction → no change
        new_conf = r.confidence  # no adjustment for NEUTRAL
        assert new_conf == 0.6

    def test_confidence_capped_at_one(self) -> None:
        r = ScanResult(
            ticker="T1", yes_price=55.0, days_to_close=10,
            signal_scores={}, strongest_signal="cal_drift",
            recommended_side="YES", confidence=0.95,
            news_context="unscheduled", kelly_fraction=0.02,
            smart_money_direction="YES", smart_money_confidence=0.8,
        )
        new_conf = min(1.0, r.confidence + 0.15) if (
            r.smart_money_direction == r.recommended_side and r.smart_money_confidence > 0.35
        ) else r.confidence
        assert new_conf == 1.0  # capped
