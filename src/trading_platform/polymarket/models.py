"""
Polymarket data models.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class PolymarketMarket:
    id: str
    question: str
    end_date_iso: str | None
    closed: bool
    resolved: bool
    winner_outcome: str | None
    volume: float
    outcomes: list[str]
    outcome_prices: list[float]
    tag_slugs: list[str]
    condition_id: str | None = None
    clob_token_ids: list[str] = field(default_factory=list)

    @property
    def yes_token_id(self) -> str | None:
        """Return the YES outcome clobTokenId (index 0), or None."""
        return self.clob_token_ids[0] if self.clob_token_ids else None

    @property
    def resolution_price(self) -> float | None:
        """
        Returns 100.0 if resolved Yes, 0.0 if resolved No, None otherwise.

        Checks ``winner_outcome`` first (when the API provides it), then
        falls back to ``outcome_prices``: the outcome whose price is
        >= 0.95 is treated as the winner.  If no outcome clears that
        threshold the market is considered unresolved.
        """
        if not self.resolved:
            return None

        # Fast path: explicit winner_outcome
        if self.winner_outcome is not None:
            wo = self.winner_outcome.strip().lower()
            if wo == "yes":
                return 100.0
            if wo == "no":
                return 0.0

        # Derive from outcome_prices (Gamma API path)
        if self.outcome_prices and self.outcomes:
            max_price = max(self.outcome_prices)
            if max_price < 0.95:
                return None
            winner_idx = self.outcome_prices.index(max_price)
            if winner_idx < len(self.outcomes):
                winning = self.outcomes[winner_idx].strip().lower()
                if winning == "yes":
                    return 100.0
                if winning == "no":
                    return 0.0

        return None

    @classmethod
    def from_api_dict(cls, d: dict) -> "PolymarketMarket":
        # outcomes and outcomePrices are sometimes JSON-encoded strings
        outcomes_raw = d.get("outcomes", "[]")
        if isinstance(outcomes_raw, str):
            try:
                outcomes: list[str] = json.loads(outcomes_raw)
            except (json.JSONDecodeError, ValueError):
                outcomes = []
        else:
            outcomes = list(outcomes_raw or [])

        prices_raw = d.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                outcome_prices: list[float] = [float(p) for p in json.loads(prices_raw)]
            except (json.JSONDecodeError, ValueError):
                outcome_prices = []
        else:
            outcome_prices = [float(p) for p in (prices_raw or [])]

        # tags is a list of dicts with a "slug" key
        tags_raw = d.get("tags") or []
        if isinstance(tags_raw, list):
            tag_slugs = [
                str(t.get("slug", ""))
                for t in tags_raw
                if isinstance(t, dict) and t.get("slug")
            ]
        else:
            tag_slugs = []

        # The Gamma API uses umaResolutionStatus (not a boolean "resolved")
        # and outcomePrices to indicate winner (not "winnerOutcome").
        resolved_flag = bool(d.get("resolved", False))
        if not resolved_flag:
            resolved_flag = d.get("umaResolutionStatus") == "resolved"

        # Derive winnerOutcome from outcomePrices when API doesn't provide it.
        # The winning outcome has price >= 0.95 (typically exactly 1.0).
        winner_outcome = d.get("winnerOutcome")
        if winner_outcome is None and resolved_flag and outcome_prices and outcomes:
            max_price = max(outcome_prices)
            if max_price >= 0.95:
                idx = outcome_prices.index(max_price)
                if idx < len(outcomes):
                    winner_outcome = outcomes[idx]

        # clobTokenIds: JSON-encoded list of token ID strings
        clob_raw = d.get("clobTokenIds", "[]")
        if isinstance(clob_raw, str):
            try:
                clob_token_ids: list[str] = [str(t) for t in json.loads(clob_raw)]
            except (json.JSONDecodeError, ValueError):
                clob_token_ids = []
        else:
            clob_token_ids = [str(t) for t in (clob_raw or [])]

        return cls(
            id=str(d.get("id", "")),
            question=str(d.get("question", "")),
            end_date_iso=d.get("endDateIso") or d.get("end_date_iso"),
            closed=bool(d.get("closed", False)),
            resolved=resolved_flag,
            winner_outcome=winner_outcome,
            volume=float(d.get("volume") or 0.0),
            outcomes=outcomes,
            outcome_prices=outcome_prices,
            tag_slugs=tag_slugs,
            condition_id=d.get("conditionId"),
            clob_token_ids=clob_token_ids,
        )
