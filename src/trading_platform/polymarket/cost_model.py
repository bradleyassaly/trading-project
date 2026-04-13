"""
Transaction cost model for paper and live trading.

Models spread, slippage, and fees that reduce paper P&L to realistic
levels. Used by both the paper executor (forward) and backtester
(historical) to keep results comparable.

Default parameters are conservative — calibrate from actual CLOB data
once enough live fills are recorded in ``trade_fills``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CostEstimate:
    """Cost breakdown for a single trade entry or exit."""
    raw_price: float
    effective_price: float
    spread_cost: float
    slippage_cost: float
    total_cost: float  # spread + slippage in price terms


# Slippage schedule: stake_usd → slippage_pct
_DEFAULT_SLIPPAGE = {
    50: 0.003,    # $50 stake → 0.3% slippage
    200: 0.005,   # $200 → 0.5%
    500: 0.010,   # $500 → 1.0%
    1000: 0.015,  # $1K → 1.5%
    5000: 0.025,  # $5K → 2.5%
}


class CostModel:
    """Apply spread + slippage costs to paper/backtest trades."""

    def __init__(
        self,
        default_spread: float = 0.02,
        slippage_schedule: dict[float, float] | None = None,
    ) -> None:
        self.default_spread = default_spread
        self.slippage_schedule = slippage_schedule or _DEFAULT_SLIPPAGE

    def entry_cost(
        self, raw_price: float, side: str, stake: float,
        spread: float | None = None,
    ) -> CostEstimate:
        """Compute effective entry price after spread + slippage."""
        half_spread = (spread or self.default_spread) / 2
        slip = self._lookup_slippage(stake)

        if side.upper() in ("YES", "BUY"):
            effective = raw_price + half_spread + slip
        else:
            effective = raw_price - half_spread - slip

        effective = max(0.001, min(0.999, effective))
        return CostEstimate(
            raw_price=raw_price,
            effective_price=round(effective, 6),
            spread_cost=round(half_spread, 6),
            slippage_cost=round(slip, 6),
            total_cost=round(half_spread + slip, 6),
        )

    def exit_cost(
        self, raw_price: float, side: str, stake: float,
        is_resolution: bool = False, spread: float | None = None,
    ) -> CostEstimate:
        """Compute effective exit price after costs.

        Resolution exits (price = 0 or 1) have zero spread/slippage.
        """
        if is_resolution:
            return CostEstimate(
                raw_price=raw_price, effective_price=raw_price,
                spread_cost=0, slippage_cost=0, total_cost=0,
            )

        half_spread = (spread or self.default_spread) / 2
        slip = self._lookup_slippage(stake)

        if side.upper() in ("YES", "BUY"):
            effective = raw_price - half_spread - slip
        else:
            effective = raw_price + half_spread + slip

        effective = max(0.001, min(0.999, effective))
        return CostEstimate(
            raw_price=raw_price,
            effective_price=round(effective, 6),
            spread_cost=round(half_spread, 6),
            slippage_cost=round(slip, 6),
            total_cost=round(half_spread + slip, 6),
        )

    def _lookup_slippage(self, stake: float) -> float:
        """Look up slippage from the schedule (interpolate between tiers)."""
        if stake <= 0:
            return 0
        prev_pct = 0.003
        for threshold, pct in sorted(self.slippage_schedule.items()):
            if stake <= threshold:
                return pct
            prev_pct = pct
        return prev_pct
