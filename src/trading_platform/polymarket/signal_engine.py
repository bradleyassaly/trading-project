"""
Signal compression engine for multi-wallet convergence detection.

Aggregates positions across wallets to find markets where multiple
directional traders are aligned, producing actionable signals.

Usage::

    from trading_platform.polymarket.signal_engine import SignalEngine
    engine = SignalEngine()
    signals = engine.compute_market_signals()
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from trading_platform.polymarket.wallet_db import WalletDB, _now_ts, categorize_slug

logger = logging.getLogger(__name__)

_EXCLUDED_CATEGORIES = {"tweet_count", "sports"}


class SignalEngine:
    """Compute compressed multi-wallet market signals."""

    def __init__(self, db: WalletDB | None = None) -> None:
        self._db = db or WalletDB()

    def compute_market_signals(self) -> list[dict[str, Any]]:
        """Compute signals from current wallet positions. Returns top signals."""
        db = self._db

        # Get all directional wallets
        directional_wallets: dict[str, dict[str, Any]] = {}
        for row in db.get_leaderboard(limit=500):
            if row.get("wallet_type") == "directional":
                directional_wallets[row["wallet"]] = row

        if not directional_wallets:
            logger.info("No directional wallets found")
            return []

        # Get all positions, grouped by asset
        asset_positions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for wallet in directional_wallets:
            for pos in db.get_wallet_positions(wallet):
                asset = pos.get("asset", "")
                if not asset:
                    continue
                slug = pos.get("slug", "")
                cat = categorize_slug(slug)
                if cat in _EXCLUDED_CATEGORIES:
                    continue
                pos["_wallet"] = wallet
                pos["_equity_score"] = directional_wallets[wallet].get("equity_score", 0)
                pos["_conviction_score"] = directional_wallets[wallet].get("conviction_score") or directional_wallets[wallet].get("equity_score", 0)
                pos["_large_bet_threshold"] = directional_wallets[wallet].get("large_bet_threshold", 0)
                pos["_category"] = cat
                asset_positions[asset].append(pos)

        # Compute signals for assets with 2+ directional wallets
        signals = []
        for asset, positions in asset_positions.items():
            if len(positions) < 2:
                continue

            long_vol = 0.0
            short_vol = 0.0
            weighted_long = 0.0
            weighted_short = 0.0
            wallets_long = []
            wallets_short = []
            earliest_ts = None
            latest_ts = None
            title = ""
            slug = ""
            category = ""

            has_oversized_bet = False

            for pos in positions:
                val = pos.get("current_value") or pos.get("initial_value") or 0
                conv = pos.get("_conviction_score", 0)
                threshold = pos.get("_large_bet_threshold", 0)
                wallet = pos["_wallet"]

                # Check for unusually high conviction bet
                if threshold > 0 and val > threshold:
                    has_oversized_bet = True

                # Determine direction from outcome name
                outcome = (pos.get("outcome") or "").upper()
                is_long = outcome in ("YES", "1", "TRUE") or pos.get("size", 0) > 0

                if is_long:
                    long_vol += val
                    weighted_long += val * conv
                    wallets_long.append(wallet)
                else:
                    short_vol += val
                    weighted_short += val * conv
                    wallets_short.append(wallet)

                ts = pos.get("last_updated")
                if ts:
                    if earliest_ts is None or ts < earliest_ts:
                        earliest_ts = ts
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts

                if not title:
                    title = pos.get("title", "")
                    slug = pos.get("slug", "")
                    category = pos.get("_category", "")

            # Direction = whichever side has more directional volume
            if weighted_long >= weighted_short:
                direction = "YES"
                net_vol = long_vol - short_vol
                weighted_net = weighted_long - weighted_short
                dir_count = len(set(wallets_long))
                top_wallet = max(wallets_long, key=lambda w: directional_wallets[w].get("equity_score", 0)) if wallets_long else ""
            else:
                direction = "NO"
                net_vol = short_vol - long_vol
                weighted_net = weighted_short - weighted_long
                dir_count = len(set(wallets_short))
                top_wallet = max(wallets_short, key=lambda w: directional_wallets[w].get("equity_score", 0)) if wallets_short else ""

            total_vol = long_vol + short_vol
            confidence = net_vol / total_vol if total_vol > 0 else 0

            now = _now_ts()
            hours_first = (now - earliest_ts) / 3600 if earliest_ts else None
            hours_last = (now - latest_ts) / 3600 if latest_ts else None

            signal = {
                "token_id": asset,
                "market_title": title,
                "slug": slug,
                "category": category,
                "direction": direction,
                "confidence": round(confidence, 3),
                "net_smart_volume": round(net_vol, 2),
                "weighted_net_volume": round(weighted_net, 2),
                "smart_wallet_count": len(positions),
                "top_wallet_edge": directional_wallets.get(top_wallet, {}).get("equity_score", 0),
                "top_wallet_address": top_wallet,
                "hours_since_first_entry": round(hours_first, 1) if hours_first else None,
                "hours_since_last_trade": round(hours_last, 1) if hours_last else None,
                "directional_wallet_count": dir_count,
                "has_oversized_bet": has_oversized_bet,
                "computed_at": now,
            }
            signals.append(signal)

            # Persist
            db.insert_signal(**signal)

        signals.sort(key=lambda s: s.get("weighted_net_volume", 0), reverse=True)
        logger.info("Computed %d market signals", len(signals))
        return signals[:20]

    def get_actionable_signals(
        self,
        *,
        min_wallets: int = 2,
        min_confidence: float = 0.6,
    ) -> list[dict[str, Any]]:
        """Return filtered actionable signals from the latest computation."""
        all_signals = self._db.get_latest_signals(limit=50)
        return [
            s for s in all_signals
            if (s.get("directional_wallet_count") or 0) >= min_wallets
            and (s.get("confidence") or 0) >= min_confidence
            and s.get("category") not in _EXCLUDED_CATEGORIES
        ][:20]
