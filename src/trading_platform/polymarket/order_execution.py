"""
Order execution strategy for Polymarket CLOB.

Paper trading bypasses this (instant fill at mid-price).
Live trading uses limit orders to minimise spread crossing.

Not wired into any executor yet — activated when transitioning
to live trading (Phase 4, after thesis validation at 70%+).
"""
from __future__ import annotations

from typing import Any


class OrderExecutionStrategy:
    """Compute limit prices and estimate fill quality for CLOB orders."""

    def compute_limit_price(
        self,
        side: str,
        book: dict[str, Any],
        urgency: float = 0.5,
    ) -> float | None:
        """Compute the limit price for an order.

        urgency: 0.0 = patient (at best bid/ask),
                 1.0 = aggressive (cross spread).
        For copy trading, 0.5–0.7 is appropriate — edge decays with time.
        """
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        # min/max, not [0]: raw /book sides arrive sorted descending, so
        # asks[0] is the worst quote (callers may pass unnormalized books)
        best_bid = max(float(b["price"]) for b in bids)
        best_ask = min(float(a["price"]) for a in asks)
        spread = best_ask - best_bid

        if side.upper() in ("BUY", "YES"):
            price = best_bid + spread * urgency
        else:
            price = best_ask - spread * urgency
        return round(price, 4)

    def estimate_fill_quality(
        self,
        side: str,
        size: float,
        book: dict[str, Any],
    ) -> dict[str, Any]:
        """Estimate how our order would fill given current book."""
        levels = book.get("asks" if side.upper() in ("BUY", "YES") else "bids", [])

        filled = 0.0
        total_cost = 0.0
        levels_consumed = 0

        for level in levels:
            level_price = float(level["price"])
            level_size = float(level.get("size", 0))
            level_value = level_size * level_price
            can_fill = min(size - filled, level_value)
            total_cost += can_fill
            filled += can_fill
            levels_consumed += 1
            if filled >= size:
                break

        if filled <= 0:
            return {"fillable": False, "reason": "no_liquidity"}

        avg_price = total_cost / filled
        best_price = float(levels[0]["price"]) if levels else 0
        slippage = abs(avg_price - best_price) / best_price if best_price > 0 else 0

        return {
            "fillable": True,
            "fill_pct": round(filled / size, 4),
            "avg_fill_price": round(avg_price, 4),
            "best_price": best_price,
            "slippage_pct": round(slippage, 4),
            "levels_consumed": levels_consumed,
        }

    @staticmethod
    def should_use_market_order(urgency: float, spread_pct: float) -> bool:
        """Only cross spread when very urgent AND spread is tight."""
        return urgency > 0.8 and spread_pct < 0.02
