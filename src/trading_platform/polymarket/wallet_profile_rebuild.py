"""
Rebuild wallet profiles from accumulated trade data.

Recomputes directional win rates, category-specific performance,
wallet type classification (directional vs market maker vs bot),
and equity scores.

Usage::

    from trading_platform.polymarket.wallet_profile_rebuild import rebuild_profiles
    rebuild_profiles()
"""
from __future__ import annotations

import json
import logging
import math
import time
from collections import defaultdict
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB, _now_ts

logger = logging.getLogger(__name__)

# Categories excluded from directional win rate
_EXCLUDED_CATEGORIES = {"tweet_count", "sports"}


def _compute_profit_score(net_pnl: float) -> float:
    """Score based on dollar profit: log10(|pnl|+1) * sign(pnl)."""
    if net_pnl == 0:
        return 0.0
    sign = 1.0 if net_pnl > 0 else -1.0
    return sign * math.log10(abs(net_pnl) + 1)


def rebuild_profiles(*, db: WalletDB | None = None) -> dict[str, Any]:
    """Rebuild all wallet profiles from trade data. Returns summary."""
    from trading_platform.polymarket.wallet_analytics import WalletAnalyticsEngine

    db = db or WalletDB()
    wallets = db.get_all_wallets()
    t0 = time.time()
    analytics_engine = WalletAnalyticsEngine()

    updated = 0
    analytics_count = 0
    for i, wallet in enumerate(wallets):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"  Rebuilding profile {i+1}/{len(wallets)}: {wallet[:16]}...")

        trades = db.get_wallet_trades(wallet, limit=5000)
        if not trades:
            continue

        profile = _compute_profile(trades)

        # Compute analytics (non-blocking — empty dict if insufficient data)
        try:
            analytics = analytics_engine.compute_all(wallet, db._path)
            if analytics:
                profile.update(analytics)
                analytics_count += 1
        except Exception as exc:
            logger.debug("analytics failed for %s: %s", wallet[:12], exc)

        db.upsert_profile(wallet, **profile)
        updated += 1

    elapsed = time.time() - t0
    return {
        "wallets_rebuilt": updated,
        "wallets_with_analytics": analytics_count,
        "elapsed_seconds": round(elapsed, 1),
    }


def _compute_profile(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute all derived fields from trade history."""
    # Category counts
    cat_counts: dict[str, int] = defaultdict(int)
    cat_wins: dict[str, int] = defaultdict(int)
    cat_total: dict[str, int] = defaultdict(int)

    # Per-asset aggregation for directional win rate
    asset_positions: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "net_size": 0.0, "buy_vol": 0.0, "sell_vol": 0.0,
        "category": "other", "resolved": False, "outcome": None,
        "timestamps": [],
    })

    total_size = 0.0
    trade_timestamps: list[int] = []
    trade_sizes: list[float] = []
    pnl_wins: list[float] = []
    pnl_losses: list[float] = []

    for t in trades:
        cat = t.get("category", "other")
        cat_counts[cat] += 1
        asset = t.get("asset", "")
        side = (t.get("side") or "").upper()
        size = t.get("size") or 0.0
        ts = t.get("timestamp") or 0
        pnl = t.get("pnl")

        total_size += size
        if size > 0:
            trade_sizes.append(size)
        if ts:
            trade_timestamps.append(ts)
        if pnl is not None and t.get("market_resolved"):
            if pnl > 0:
                pnl_wins.append(pnl)
            elif pnl < 0:
                pnl_losses.append(abs(pnl))

        ap = asset_positions[asset]
        ap["category"] = cat
        if side == "BUY":
            ap["net_size"] += size
            ap["buy_vol"] += size
        elif side == "SELL":
            ap["net_size"] -= size
            ap["sell_vol"] += size
        if ts:
            ap["timestamps"].append(ts)

        if t.get("market_resolved"):
            ap["resolved"] = True
            ap["outcome"] = t.get("market_outcome")

    # Wallet type classification
    wallet_type = _classify_wallet_type(trades, asset_positions)

    # Directional win rate (excluding tweet_count and sports)
    dir_wins = 0
    dir_total = 0
    for asset, ap in asset_positions.items():
        if ap["category"] in _EXCLUDED_CATEGORIES:
            continue
        if not ap["resolved"] or not ap["outcome"]:
            continue
        dir_total += 1
        # Win = net long and resolved YES, or net short and resolved NO
        net_long = ap["net_size"] > 0
        resolved_yes = ap["outcome"].upper() in ("YES", "1", "TRUE")
        if (net_long and resolved_yes) or (not net_long and not resolved_yes):
            dir_wins += 1

    directional_wr = dir_wins / dir_total if dir_total > 0 else None

    # Category-specific win rates
    crypto_wr = _category_win_rate(asset_positions, "crypto")
    politics_wr = _category_win_rate(asset_positions, "politics")

    # Average position size
    position_sizes = [abs(ap["net_size"]) for ap in asset_positions.values() if abs(ap["net_size"]) > 0]
    avg_pos_size = sum(position_sizes) / len(position_sizes) if position_sizes else 0

    # Equity score: directional_wr * log(resolved_trades + 1) * type_multiplier
    type_mult = 1.0 if wallet_type == "directional" else 0.3
    equity_score = (directional_wr or 0) * math.log10(dir_total + 1) * type_mult

    # P&L metrics
    total_won = sum(pnl_wins)
    total_lost = sum(pnl_losses)
    net_pnl = total_won - total_lost
    avg_win = total_won / len(pnl_wins) if pnl_wins else 0
    avg_loss = total_lost / len(pnl_losses) if pnl_losses else 0
    # Profit factor capped at 10x; null if insufficient losses for a meaningful ratio
    _MAX_PF = 10.0
    if total_lost < 0.01 or len(pnl_losses) < 3:
        profit_factor = None  # insufficient data for meaningful PF
    else:
        profit_factor = min(total_won / total_lost, _MAX_PF)

    # Conviction score: equity * log(avg_position_size + 1)
    conviction_score = equity_score * math.log10(max(avg_pos_size, 1) + 1)

    # Volume tier
    if total_size >= 100_000:
        volume_tier = "whale"
    elif total_size >= 10_000:
        volume_tier = "active"
    elif total_size >= 1_000:
        volume_tier = "casual"
    else:
        volume_tier = "small"

    # Large bet threshold: 75th percentile of trade sizes
    large_bet_threshold = 0.0
    if trade_sizes:
        sorted_sizes = sorted(trade_sizes)
        idx = int(len(sorted_sizes) * 0.75)
        large_bet_threshold = sorted_sizes[min(idx, len(sorted_sizes) - 1)]

    return {
        "directional_win_rate": round(directional_wr, 4) if directional_wr is not None else None,
        "crypto_win_rate": round(crypto_wr, 4) if crypto_wr is not None else None,
        "politics_win_rate": round(politics_wr, 4) if politics_wr is not None else None,
        "category_trades": json.dumps(dict(cat_counts)),
        "wallet_type": wallet_type,
        "avg_position_size_usdc": round(avg_pos_size, 2),
        "total_volume_usdc": round(total_size, 2),
        "total_realized_pnl": round(net_pnl, 2),
        "equity_score": round(equity_score, 4),
        "last_trade_ts": max(trade_timestamps) if trade_timestamps else None,
        "first_seen_ts": min(trade_timestamps) if trade_timestamps else None,
        "resolved_trades": dir_total,
        "total_won_usdc": round(total_won, 2),
        "total_lost_usdc": round(total_lost, 2),
        "net_pnl_usdc": round(net_pnl, 2),
        "avg_win_size_usdc": round(avg_win, 2),
        "avg_loss_size_usdc": round(avg_loss, 2),
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "conviction_score": round(conviction_score, 4),
        "volume_tier": volume_tier,
        "large_bet_threshold": round(large_bet_threshold, 2),
        "profit_score": round(_compute_profit_score(net_pnl), 4),
    }


def _classify_wallet_type(
    trades: list[dict[str, Any]],
    positions: dict[str, dict[str, Any]],
) -> str:
    """Classify wallet as directional, market_maker, arb_bot, or unknown."""
    if not trades:
        return "unknown"

    # Bot detection: >20 trades in <5 minutes
    timestamps = sorted(t.get("timestamp", 0) for t in trades if t.get("timestamp"))
    if len(timestamps) >= 20:
        # Check 20-trade windows
        for j in range(len(timestamps) - 19):
            window = timestamps[j + 19] - timestamps[j]
            if window < 300:  # 5 minutes
                return "arb_bot"

    # Market maker: >80% sells or trades on >10 outcomes of same event
    sell_count = sum(1 for t in trades if (t.get("side") or "").upper() == "SELL")
    if len(trades) > 10 and sell_count / len(trades) > 0.80:
        return "market_maker"

    # Same-event diversification check
    event_outcomes: dict[str, set[str]] = defaultdict(set)
    for t in trades:
        es = t.get("event_slug", "")
        outcome = t.get("outcome", "")
        if es and outcome:
            event_outcomes[es].add(outcome)
    if any(len(outcomes) >= 10 for outcomes in event_outcomes.values()):
        return "market_maker"

    # Directional: avg position > $10
    avg_sizes = [abs(ap["net_size"]) for ap in positions.values() if abs(ap["net_size"]) > 0]
    avg_pos = sum(avg_sizes) / len(avg_sizes) if avg_sizes else 0
    if avg_pos >= 10:
        return "directional"

    return "unknown"


def _category_win_rate(positions: dict[str, dict[str, Any]], category: str) -> float | None:
    wins = 0
    total = 0
    for ap in positions.values():
        if ap["category"] != category or not ap["resolved"] or not ap["outcome"]:
            continue
        total += 1
        net_long = ap["net_size"] > 0
        resolved_yes = ap["outcome"].upper() in ("YES", "1", "TRUE")
        if (net_long and resolved_yes) or (not net_long and not resolved_yes):
            wins += 1
    return wins / total if total > 0 else None
