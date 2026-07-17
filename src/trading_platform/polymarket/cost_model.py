"""
Transaction cost model for paper and live trading.

Models spread, slippage, and fees that reduce paper P&L to realistic levels.
Used by both the paper executor (forward) and backtester (historical) to keep
results comparable.

2026-07-16 REWRITE — the flat price-blind model (default_spread=0.02, a $50-floor
slippage schedule, exits booked at the transient mid) was proven to fabricate alpha:
paper resolution_decay showed +$11/trade while the SAME signal on real money was
-$0.56/trade, and 78% of paper's take-profit "wins" were on markets that resolved to
$0 — i.e. paper "sold" at momentary mid-spikes it could never fill on thin longshot
books, at ~$0.01 modeled cost. See memory project_2026_07_16_alpha_verdict.

Three realism fixes, all anchored to observed live fills:
  1. PRICE-AWARE half-spread: absolute spread has a floor but the RELATIVE cost
     naturally explodes at the tails, matching the ~13% entry cost seen at p~0.12
     (live entry slippage averaged 5.3% across price levels).
  2. SIZE/impact: unchanged schedule, still small for our $1-16 clips, but retained.
  3. SPIKE-IMPACT haircut (the big one): you cannot realize the full mid gain of a
     large FAVORABLE move on an illiquid book. Given the entry price, only a fraction
     of the (mid - entry) move is realizable. Live captured ~42% of paper's favorable
     move => a ~0.58 impact; the default (0.50) is a touch more generous. Near-
     settlement exits (price <= RES_LO or >= RES_HI) are exempt — those ARE realizable.

Signatures are backward compatible: entry_price on exit_cost is optional (falls back
to the old spread-only behavior when unknown).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CostEstimate:
    """Cost breakdown for a single trade entry or exit."""
    raw_price: float
    effective_price: float
    spread_cost: float
    slippage_cost: float
    total_cost: float  # spread + slippage in price terms


# Slippage schedule: stake_usd -> absolute price impact. Small for our clip sizes;
# grows with notional. (Kept from the original model — size is not the main culprit.)
_DEFAULT_SLIPPAGE = {
    50: 0.003,
    200: 0.005,
    500: 0.010,
    1000: 0.015,
    5000: 0.025,
}

# Exits pinned at/near a binary boundary are settlement-realizable at face value.
RES_LO, RES_HI = 0.03, 0.97


class CostModel:
    """Apply spread + slippage + thin-book impact to paper/backtest trades."""

    def __init__(
        self,
        default_spread: float = 0.02,
        slippage_schedule: dict[float, float] | None = None,
        min_half_spread_abs: float = 0.006,
        rel_half_spread: float = 0.02,
        spike_impact: float = 0.50,
    ) -> None:
        # default_spread kept for API/back-compat; when a caller passes an explicit
        # `spread` we still honour it, otherwise the price-aware model is used.
        self.default_spread = default_spread
        self.slippage_schedule = slippage_schedule or _DEFAULT_SLIPPAGE
        self.min_half_spread_abs = min_half_spread_abs
        self.rel_half_spread = rel_half_spread
        # Fraction of a large favorable move that is NOT realizable on a thin book.
        self.spike_impact = spike_impact

    def _half_spread(self, price: float, spread: float | None) -> float:
        """Price-aware half-spread. Absolute floor + a relative component, so the
        cost as a FRACTION of a longshot price is large (matches live tails)."""
        if spread is not None:
            return spread / 2
        return max(self.min_half_spread_abs, self.rel_half_spread * float(price))

    def entry_cost(
        self, raw_price: float, side: str, stake: float,
        spread: float | None = None,
    ) -> CostEstimate:
        """Effective entry price after spread + slippage (marketable, crosses)."""
        half_spread = self._half_spread(raw_price, spread)
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
        entry_price: float | None = None,
    ) -> CostEstimate:
        """Effective exit price after costs.

        Resolution / near-settlement exits are realizable at face. Otherwise the
        fill crosses the spread AND — if ``entry_price`` is known and the mid moved
        favorably — only a fraction of that move is realizable (thin-book spike
        un-fillability), the dominant correction over the old model.
        """
        if is_resolution or raw_price <= RES_LO or raw_price >= RES_HI:
            return CostEstimate(
                raw_price=raw_price, effective_price=raw_price,
                spread_cost=0, slippage_cost=0, total_cost=0,
            )

        half_spread = self._half_spread(raw_price, spread)
        slip = self._lookup_slippage(stake)
        buy = side.upper() in ("YES", "BUY")

        if buy:
            effective = raw_price - half_spread - slip
        else:
            effective = raw_price + half_spread + slip

        # Spike-impact haircut: cap the realizable price on a large favorable move.
        if entry_price and entry_price > 0:
            if buy and raw_price > entry_price:
                fav = raw_price - entry_price
                capped = entry_price + (1 - self.spike_impact) * fav - half_spread
                effective = min(effective, capped)
            elif (not buy) and raw_price < entry_price:
                fav = entry_price - raw_price
                capped = entry_price - (1 - self.spike_impact) * fav + half_spread
                effective = max(effective, capped)

        effective = max(0.001, min(0.999, effective))
        total = abs(raw_price - effective)
        return CostEstimate(
            raw_price=raw_price,
            effective_price=round(effective, 6),
            spread_cost=round(half_spread, 6),
            slippage_cost=round(slip, 6),
            total_cost=round(total, 6),
        )

    def _lookup_slippage(self, stake: float) -> float:
        """Look up slippage from the schedule (step function by notional)."""
        if stake <= 0:
            return 0
        prev_pct = 0.003
        for threshold, pct in sorted(self.slippage_schedule.items()):
            if stake <= threshold:
                return pct
            prev_pct = pct
        return prev_pct
