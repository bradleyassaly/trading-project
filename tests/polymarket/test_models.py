"""Tests for PolymarketMarket model."""
from __future__ import annotations

import json
import pytest
from trading_platform.polymarket.models import PolymarketMarket


def _market_dict(**overrides) -> dict:
    base = {
        "id": "abc123",
        "question": "Will X happen?",
        "endDateIso": "2025-01-15T00:00:00Z",
        "closed": True,
        "resolved": True,
        "winnerOutcome": "Yes",
        "volume": 50000.0,
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps(["0.72", "0.28"]),
        "tags": [{"slug": "politics"}, {"slug": "us-elections"}],
        "conditionId": "0xdeadbeef",
        "clobTokenIds": json.dumps(["111111111111", "222222222222"]),
    }
    base.update(overrides)
    return base


class TestPolymarketMarketFromApiDict:
    def test_basic_fields(self):
        m = PolymarketMarket.from_api_dict(_market_dict())
        assert m.id == "abc123"
        assert m.question == "Will X happen?"
        assert m.end_date_iso == "2025-01-15T00:00:00Z"
        assert m.closed is True
        assert m.resolved is True
        assert m.winner_outcome == "Yes"
        assert m.volume == 50000.0
        assert m.condition_id == "0xdeadbeef"

    def test_clob_token_ids_parsed(self):
        m = PolymarketMarket.from_api_dict(_market_dict())
        assert m.clob_token_ids == ["111111111111", "222222222222"]
        assert m.yes_token_id == "111111111111"

    def test_missing_clob_token_ids(self):
        d = _market_dict()
        del d["clobTokenIds"]
        m = PolymarketMarket.from_api_dict(d)
        assert m.clob_token_ids == []
        assert m.yes_token_id is None

    def test_outcomes_parsed_from_json_string(self):
        m = PolymarketMarket.from_api_dict(_market_dict())
        assert m.outcomes == ["Yes", "No"]
        assert m.outcome_prices == [0.72, 0.28]

    def test_outcomes_as_list(self):
        d = _market_dict()
        d["outcomes"] = ["Yes", "No"]
        d["outcomePrices"] = ["0.6", "0.4"]
        m = PolymarketMarket.from_api_dict(d)
        assert m.outcomes == ["Yes", "No"]
        assert m.outcome_prices == [0.6, 0.4]

    def test_tag_slugs_extracted(self):
        m = PolymarketMarket.from_api_dict(_market_dict())
        assert m.tag_slugs == ["politics", "us-elections"]

    def test_missing_tags_gives_empty_list(self):
        m = PolymarketMarket.from_api_dict(_market_dict(tags=None))
        assert m.tag_slugs == []

    def test_missing_outcomes_gives_empty_lists(self):
        m = PolymarketMarket.from_api_dict(
            _market_dict(outcomes="[]", outcomePrices="[]")
        )
        assert m.outcomes == []
        assert m.outcome_prices == []

    def test_missing_volume_defaults_to_zero(self):
        m = PolymarketMarket.from_api_dict(_market_dict(volume=None))
        assert m.volume == 0.0

    def test_missing_condition_id_is_none(self):
        d = _market_dict()
        del d["conditionId"]
        m = PolymarketMarket.from_api_dict(d)
        assert m.condition_id is None


class TestResolutionPrice:
    def test_yes_resolves_to_100(self):
        m = PolymarketMarket.from_api_dict(_market_dict(winnerOutcome="Yes"))
        assert m.resolution_price == 100.0

    def test_no_resolves_to_0(self):
        m = PolymarketMarket.from_api_dict(_market_dict(winnerOutcome="No"))
        assert m.resolution_price == 0.0

    def test_case_insensitive(self):
        assert PolymarketMarket.from_api_dict(_market_dict(winnerOutcome="YES")).resolution_price == 100.0
        assert PolymarketMarket.from_api_dict(_market_dict(winnerOutcome="no")).resolution_price == 0.0

    def test_unresolved_returns_none(self):
        m = PolymarketMarket.from_api_dict(_market_dict(resolved=False, winnerOutcome=None))
        assert m.resolution_price is None

    def test_ambiguous_outcome_returns_none(self):
        m = PolymarketMarket.from_api_dict(_market_dict(winnerOutcome="Maybe"))
        assert m.resolution_price is None

    def test_none_winner_outcome_returns_none(self):
        m = PolymarketMarket.from_api_dict(_market_dict(winnerOutcome=None))
        assert m.resolution_price is None


class TestGammaApiResolutionFields:
    """The Gamma API uses umaResolutionStatus and outcomePrices instead of
    resolved/winnerOutcome. Verify from_api_dict handles this."""

    def _gamma_dict(self, **overrides) -> dict:
        """Market dict matching real Gamma API shape (no resolved/winnerOutcome)."""
        base = {
            "id": "gamma-1",
            "question": "Will X happen?",
            "endDateIso": "2026-03-01T00:00:00Z",
            "closed": True,
            "umaResolutionStatus": "resolved",
            "volume": 10000.0,
            "outcomes": json.dumps(["Yes", "No"]),
            "outcomePrices": json.dumps(["1", "0"]),
            "tags": [{"slug": "politics"}],
            "conditionId": "0xabc",
            "clobTokenIds": json.dumps(["333333333333", "444444444444"]),
        }
        base.update(overrides)
        return base

    def test_resolved_from_uma_status(self):
        m = PolymarketMarket.from_api_dict(self._gamma_dict())
        assert m.resolved is True

    def test_unresolved_uma_status(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(umaResolutionStatus="proposed")
        )
        assert m.resolved is False

    def test_winner_derived_from_outcome_prices_yes(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(outcomePrices=json.dumps(["1", "0"]))
        )
        assert m.winner_outcome == "Yes"
        assert m.resolution_price == 100.0

    def test_winner_derived_from_outcome_prices_no(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(outcomePrices=json.dumps(["0", "1"]))
        )
        assert m.winner_outcome == "No"
        assert m.resolution_price == 0.0

    def test_explicit_winner_outcome_takes_precedence(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(
                winnerOutcome="No",
                outcomePrices=json.dumps(["1", "0"]),
            )
        )
        assert m.winner_outcome == "No"
        assert m.resolution_price == 0.0

    def test_no_clear_winner_in_prices(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(outcomePrices=json.dumps(["0.5", "0.5"]))
        )
        assert m.winner_outcome is None
        assert m.resolution_price is None

    def test_near_one_price_treated_as_winner(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(outcomePrices=json.dumps(["0.97", "0.03"]))
        )
        assert m.winner_outcome == "Yes"
        assert m.resolution_price == 100.0

    def test_below_threshold_not_resolved(self):
        m = PolymarketMarket.from_api_dict(
            self._gamma_dict(outcomePrices=json.dumps(["0.90", "0.10"]))
        )
        assert m.winner_outcome is None
        assert m.resolution_price is None
