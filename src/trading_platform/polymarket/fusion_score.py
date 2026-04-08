"""
Tri-factor fusion score for paper trade gating.

::

    fusion_score = wallet_signal × market_signal × timing_signal

Each factor is roughly in [0, 1] but can exceed 1 when the conviction
multiplier or convergence boost kicks in. The product is bounded above
by ~2.0 and used as a gate before placing a paper trade:

    fusion_score >= 0.6  → full Kelly stake
    0.4 <= fusion < 0.6  → half Kelly stake
    fusion < 0.4         → log only, no trade

Wallet signal
-------------
Combines the wallet's directional win rate (treated as a Bayesian
prior, default 0.5 if unknown), a tier multiplier, and a conviction
multiplier (this trade's size relative to the wallet's average bet).

Market signal
-------------
Liquidity score, dislocation discount (markets near 50/50 have more
room to move), and a staleness penalty.

Timing signal
-------------
How fresh the whale entry is, with a convergence boost if multiple
wallets have entered the same direction recently.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Tier multipliers — actual platform tiers are tier1h / tier1 / tier2.
# Map them to the spec's S/A/B/C/D weights.
_TIER_MULT = {
    "tier1h": 1.00,
    "tier1":  0.85,
    "tier2":  0.65,
    "unknown": 0.40,
    "market":  0.40,
}


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def wallet_signal(
    wallet_wr: float | None,
    wallet_tier: str | None,
    trade_size_usd: float,
    wallet_avg_bet_usd: float,
) -> float:
    """0..1 measure of how strong the wallet evidence is."""
    base = 0.5 if wallet_wr is None else _clamp(wallet_wr, 0.0, 1.0)
    tier_mult = _TIER_MULT.get((wallet_tier or "unknown").lower(), 0.40)

    # Conviction: this bet vs wallet's typical bet
    if wallet_avg_bet_usd and wallet_avg_bet_usd > 0:
        ratio = _clamp(trade_size_usd / wallet_avg_bet_usd, 0.5, 3.0)
    else:
        ratio = 1.0
    # Map [0.5, 3.0] → [0.5, 1.5]
    conviction_mult = 0.5 + (ratio - 0.5) * (1.0 / 2.5)
    conviction_mult = _clamp(conviction_mult, 0.5, 1.5)

    return _clamp(base * tier_mult * conviction_mult, 0.0, 2.0)


def market_signal(
    market_volume_usd: float | None,
    current_price: float | None,
    days_since_last_trade: float | None,
) -> float:
    """0..1 measure of how favourable the market context is."""
    vol = float(market_volume_usd or 0.0)
    liquidity_score = _clamp(vol / 100_000.0, 0.0, 1.0)

    p = 0.5 if current_price is None else _clamp(float(current_price), 0.0, 1.0)
    # Markets near 50/50 (small dislocation) have more room → higher score
    dislocation = abs(p - 0.5)
    dislocation_factor = 1.0 - (dislocation * 0.5)  # 1.0 at 0.5, 0.75 at 0/1

    days = 0.0 if days_since_last_trade is None else float(days_since_last_trade)
    staleness = _clamp(1.0 - (days / 7.0), 0.0, 1.0)

    return _clamp(liquidity_score * dislocation_factor * staleness, 0.0, 1.0)


def timing_signal(
    minutes_since_whale_entry: float | None,
    convergence_count: int = 0,
) -> float:
    """0..1 timing freshness, boosted by multi-wallet convergence."""
    m = 0.0 if minutes_since_whale_entry is None else float(minutes_since_whale_entry)
    if m < 0:
        m = 0.0
    if m < 5:
        base = 1.0
    elif m < 30:
        base = 0.8
    elif m < 120:
        base = 0.5
    else:
        base = 0.2

    if convergence_count >= 2:
        base *= 1.3
    return _clamp(base, 0.0, 2.0)


@dataclass
class FusionResult:
    score: float
    wallet_signal: float
    market_signal: float
    timing_signal: float
    decision: str         # 'auto' | 'half' | 'skip'
    stake_multiplier: float

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def compute_fusion(
    *,
    wallet_wr: float | None = None,
    wallet_tier: str | None = None,
    trade_size_usd: float = 0.0,
    wallet_avg_bet_usd: float = 0.0,
    market_volume_usd: float | None = None,
    current_price: float | None = None,
    days_since_last_trade: float | None = None,
    minutes_since_whale_entry: float | None = None,
    convergence_count: int = 0,
) -> FusionResult:
    """Compute the tri-factor fusion score and decision."""
    w = wallet_signal(wallet_wr, wallet_tier, trade_size_usd, wallet_avg_bet_usd)
    m = market_signal(market_volume_usd, current_price, days_since_last_trade)
    t = timing_signal(minutes_since_whale_entry, convergence_count)
    score = w * m * t

    if score >= 0.6:
        decision = "auto"
        mult = 1.0
    elif score >= 0.4:
        decision = "half"
        mult = 0.5
    else:
        decision = "skip"
        mult = 0.0

    return FusionResult(
        score=round(score, 4),
        wallet_signal=round(w, 4),
        market_signal=round(m, 4),
        timing_signal=round(t, 4),
        decision=decision,
        stake_multiplier=mult,
    )
