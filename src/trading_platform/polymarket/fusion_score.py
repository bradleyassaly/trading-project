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
    *,
    dynamic_tier_multiplier: float | None = None,
) -> float:
    """0..1 measure of how strong the wallet evidence is.

    When ``dynamic_tier_multiplier`` is supplied (from
    :class:`WalletTieringEngine`), it overrides the static
    tier1h/tier1/tier2 lookup. Pass it explicitly to use the per-wallet
    per-category dynamic tier system.
    """
    base = 0.5 if wallet_wr is None else _clamp(wallet_wr, 0.0, 1.0)
    if dynamic_tier_multiplier is not None:
        tier_mult = _clamp(dynamic_tier_multiplier, 0.0, 1.5)
    else:
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
    """0..1 measure of how favourable the market context is.

    Poller-sourced signals don't carry market_volume_usd (only the
    wallet's total volume). Treating None as 0 made liquidity_score 0
    and collapsed the fusion score to 0, which 100%-rejected those
    signals. Treat missing volume as a neutral 0.5 so the gate still
    considers timing + wallet signal rather than hard-failing on it.
    """
    if market_volume_usd is None:
        liquidity_score = 0.5  # neutral — no data shouldn't imply zero liquidity
    else:
        liquidity_score = _clamp(float(market_volume_usd) / 100_000.0, 0.0, 1.0)

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


def market_signal_enhanced(microstructure: dict[str, Any] | None) -> float | None:
    """Enhanced market_signal that uses pmxt microstructure data.

    Returns ``None`` if microstructure is missing/unavailable so the
    caller can fall back to the basic ``market_signal`` formula. When
    available, combines:

    * **liquidity_component** — derived from order_book spread_pct
      (tighter spread = higher score)
    * **volume_component** — derived from velocity.volume_ratio
      (1h volume vs 24h average — surge detection)
    * **timing_component** — derived from baseline.info_already_priced
      and baseline.whale_impact (penalizes "whale is late" patterns)

    Final score = 0.3*liquidity + 0.3*volume + 0.4*timing.
    """
    if not microstructure or not microstructure.get("available"):
        return None

    components: list[tuple[float, float]] = []

    book = microstructure.get("order_book") or {}
    spread_pct = book.get("spread_pct")
    if spread_pct is not None:
        if spread_pct < 0.02:
            liquidity = 1.0
        elif spread_pct < 0.05:
            liquidity = 0.8
        elif spread_pct < 0.10:
            liquidity = 0.5
        else:
            liquidity = 0.2
        components.append((liquidity, 0.3))

    velocity = microstructure.get("velocity") or {}
    volume_ratio = velocity.get("volume_ratio")
    if volume_ratio is not None:
        if volume_ratio > 3.0:
            vol_score = 0.9
        elif volume_ratio > 1.5:
            vol_score = 0.7
        elif volume_ratio > 0.5:
            vol_score = 0.5
        else:
            vol_score = 0.3
        components.append((vol_score, 0.3))

    baseline = microstructure.get("baseline") or {}
    if baseline.get("available"):
        if baseline.get("info_already_priced"):
            timing = 0.2
        elif (baseline.get("whale_impact") or 0) > 0.05:
            timing = 0.6
        else:
            timing = 1.0
        components.append((timing, 0.4))

    if not components:
        return None

    total_weight = sum(w for _, w in components)
    if total_weight <= 0:
        return None
    weighted = sum(score * w for score, w in components)
    return _clamp(weighted / total_weight, 0.0, 1.0)


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
    microstructure: dict[str, Any] | None = None,
    dynamic_tier_multiplier: float | None = None,
) -> FusionResult:
    """Compute the tri-factor fusion score and decision.

    When a pmxt ``microstructure`` dict is supplied (from
    :class:`MarketDataService.get_market_microstructure`), the
    enhanced market signal is used. Falls back to the basic formula
    when microstructure is missing or pmxt is unavailable.

    When ``dynamic_tier_multiplier`` is supplied (from
    :class:`WalletTieringEngine.get_tier_multiplier`), it replaces the
    static tier1h/tier1/tier2 multiplier in wallet_signal.
    """
    w = wallet_signal(
        wallet_wr, wallet_tier, trade_size_usd, wallet_avg_bet_usd,
        dynamic_tier_multiplier=dynamic_tier_multiplier,
    )
    enhanced_m = market_signal_enhanced(microstructure)
    if enhanced_m is not None:
        m = enhanced_m
    else:
        m = market_signal(market_volume_usd, current_price, days_since_last_trade)
    t = timing_signal(minutes_since_whale_entry, convergence_count)
    score = w * m * t

    # Tier expanded 2026-04-15 to add a quarter-Kelly "learn" band.
    # Observed 24h funnel was 1929 fires / 1 placed — most signals died
    # at fusion<0.4 with no data captured. Learn-tier trades at 25% of
    # stake so we build per-signal calibration data on borderline fires
    # without putting meaningful capital on them.
    if score >= 0.6:
        decision = "auto"
        mult = 1.0
    elif score >= 0.4:
        decision = "half"
        mult = 0.5
    elif score >= 0.05:
        # Learn band is generous — 0.05 captures most poller-sourced
        # signals where WalletTieringEngine returns a low multiplier for
        # wallets not yet in the tiering index. We want data from those
        # signals anyway; stake is only 25% of baseline so capital risk
        # is minimal even across many fires.
        decision = "learn"
        mult = 0.25
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
