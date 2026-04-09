"""
Wallet behavioral classification into buckets.

Classifies each wallet based on trading patterns: position sizing,
concentration, buy/sell ratio, and trade frequency.

Usage::

    from trading_platform.polymarket.wallet_buckets import classify_wallets
    classify_wallets()
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB, _now_ts

logger = logging.getLogger(__name__)


def classify_wallets(*, db: WalletDB | None = None) -> dict[str, Any]:
    """Classify all wallets into behavioral buckets. Returns summary."""
    db = db or WalletDB()
    wallets = db.get_all_wallets()
    t0 = time.time()

    counts: dict[str, int] = defaultdict(int)
    classified = 0

    for i, wallet in enumerate(wallets):
        if (i + 1) % 200 == 0 or i == 0:
            print(f"  Classifying {i+1}/{len(wallets)}...")

        trades = db.get_wallet_trades(wallet, limit=5000)
        if not trades:
            continue

        bucket, confidence, metrics = _classify_one(trades)
        # Write to BOTH wallet_type (legacy column some downstream code
        # still reads) AND wallet_bucket (the canonical bucket column).
        # Previously this only wrote wallet_type, leaving wallet_bucket
        # NULL on 100% of rows — see reports/data_validation.md DV-14.
        db.upsert_profile(wallet,
                          wallet_type=bucket,
                          wallet_bucket=bucket,
                          **metrics)
        counts[bucket] += 1
        classified += 1

    elapsed = time.time() - t0
    return {"classified": classified, "buckets": dict(counts), "elapsed": round(elapsed, 1)}


def _classify_one(trades: list[dict[str, Any]]) -> tuple[str, float, dict[str, Any]]:
    """Classify a single wallet. Returns (bucket, confidence, extra_metrics)."""
    total_trades = len(trades)
    if total_trades == 0:
        return "noise", 0.0, {}

    # Compute metrics
    total_vol = sum(t.get("size") or 0 for t in trades)
    avg_bet = total_vol / total_trades if total_trades > 0 else 0

    buy_count = sum(1 for t in trades if (t.get("side") or "").upper() == "BUY")
    buy_pct = buy_count / total_trades

    # Unique markets
    conditions = set(t.get("condition_id") or t.get("asset") for t in trades if t.get("condition_id") or t.get("asset"))
    unique_markets = len(conditions)
    trades_per_market = total_trades / max(unique_markets, 1)

    # Top 3 market concentration
    market_counts: dict[str, int] = defaultdict(int)
    for t in trades:
        key = t.get("condition_id") or t.get("asset") or ""
        if key:
            market_counts[key] += 1
    top3 = sorted(market_counts.values(), reverse=True)[:3]
    top3_concentration = sum(top3) / total_trades if total_trades > 0 else 0

    # Average buy price
    buy_prices = [t.get("price") or 0 for t in trades if (t.get("side") or "").upper() == "BUY" and t.get("price")]
    avg_buy_price = sum(buy_prices) / len(buy_prices) if buy_prices else 0.5

    # Activity in last 30 days
    now = _now_ts()
    cutoff_30d = now - 30 * 86400
    active_30d = sum(1 for t in trades if (t.get("timestamp") or 0) > cutoff_30d)

    # Resolved trades
    resolved = sum(1 for t in trades if t.get("market_resolved"))
    wins = sum(1 for t in trades if t.get("pnl") and t["pnl"] > 0)
    win_rate = wins / resolved if resolved > 0 else None

    # Domain concentration
    cats: dict[str, int] = defaultdict(int)
    for t in trades:
        cats[t.get("category") or "other"] += 1
    top_cat_pct = max(cats.values()) / total_trades if cats else 0

    metrics = {
        "avg_position_size_usdc": round(avg_bet, 2),
        "total_volume_usdc": round(total_vol, 2),
    }

    # Classification priority order
    # Check bot/MM first (already detected by rebuild, but re-check)
    timestamps = sorted(t.get("timestamp", 0) for t in trades if t.get("timestamp"))
    if len(timestamps) >= 20:
        for j in range(len(timestamps) - 19):
            if timestamps[j + 19] - timestamps[j] < 300:
                return "arb_bot", 0.9, metrics

    if total_trades > 10 and (1 - buy_pct) > 0.80:
        return "market_maker", 0.8, metrics

    # Noise check
    if resolved and resolved < 10 and total_trades < 20:
        return "noise", 0.5, metrics
    if win_rate is not None and win_rate < 0.35 and resolved >= 20:
        return "noise", 0.7, metrics
    if active_30d == 0 and total_trades < 50:
        return "noise", 0.4, metrics

    # Contrarian whale (must check before concentrated_whale)
    if avg_bet >= 10000 and avg_buy_price <= 0.50 and buy_pct >= 0.75:
        return "contrarian_whale", 0.85, metrics

    # Concentrated whale
    if avg_bet >= 10000 and buy_pct >= 0.80 and unique_markets <= 50 and top3_concentration >= 0.30:
        return "concentrated_whale", 0.85, metrics

    # Portfolio diversifier
    if avg_bet >= 5000 and unique_markets >= 100 and top3_concentration <= 0.15:
        return "portfolio_diversifier", 0.8, metrics

    # Position builder
    if trades_per_market >= 6.0 and unique_markets <= 30 and avg_bet >= 5000:
        return "position_builder", 0.75, metrics

    # Domain specialist
    if win_rate and win_rate >= 0.60 and resolved >= 20 and avg_bet < 10000 and top_cat_pct >= 0.65:
        return "domain_specialist", 0.7, metrics

    # Volume trader
    if avg_bet < 5000 and total_trades >= 200 and buy_pct >= 0.65:
        return "volume_trader", 0.6, metrics

    # Directional (general)
    if avg_bet >= 10 and buy_pct >= 0.60:
        return "directional", 0.5, metrics

    return "unknown", 0.3, metrics
