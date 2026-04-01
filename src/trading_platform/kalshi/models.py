"""
Kalshi API data models.

All prices are stored as dollar strings (e.g. "0.6500") per the March 2026
Kalshi API migration. The helpers below provide float conversion utilities.

Binary market pricing note:
  YES bid at $X  ≡  NO ask at $(1.00 - X)
  NO  bid at $X  ≡  YES ask at $(1.00 - X)
  Kalshi returns only bids on each side.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def price_to_float(price: str | None) -> float | None:
    """Convert a dollar-string price like '0.6500' to 0.65."""
    if price is None:
        return None
    try:
        return float(price)
    except (ValueError, TypeError):
        return None


def float_to_price(value: float) -> str:
    """Convert 0.65 to '0.6500'."""
    return f"{value:.4f}"


# ── Market ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiMarket:
    ticker: str
    title: str
    subtitle: str | None
    status: str           # unopened | open | closed | settled | paused
    yes_bid: str | None   # dollar string e.g. "0.6500"
    yes_ask: str | None
    no_bid: str | None
    no_ask: str | None
    volume: int | None
    open_interest: int | None
    close_time: str | None   # ISO timestamp
    event_ticker: str | None
    series_ticker: str | None
    category: str | None
    liquidity: str | None    # dollar string
    raw: dict[str, Any] = field(default_factory=dict, compare=False)


# ── Order Book ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiOrderBookLevel:
    price: str    # dollar string
    quantity: int

    @property
    def price_float(self) -> float:
        return float(self.price)


@dataclass
class KalshiOrderBook:
    ticker: str
    yes_bids: list[KalshiOrderBookLevel]   # YES side bids (best = first)
    no_bids: list[KalshiOrderBookLevel]    # NO side bids (best = first)
    fetched_at: str | None = None

    @property
    def best_yes_bid(self) -> float | None:
        return self.yes_bids[0].price_float if self.yes_bids else None

    @property
    def best_no_bid(self) -> float | None:
        return self.no_bids[0].price_float if self.no_bids else None

    @property
    def mid_price(self) -> float | None:
        """Midpoint between best YES bid and implied YES ask (1 - best NO bid)."""
        yes = self.best_yes_bid
        no = self.best_no_bid
        if yes is None or no is None:
            return None
        yes_ask_implied = 1.0 - no
        return round((yes + yes_ask_implied) / 2, 4)


# ── Trades ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiTrade:
    trade_id: str
    ticker: str
    side: str         # yes | no  (taker side)
    yes_price: str    # dollar string
    no_price: str     # dollar string
    count: int
    created_time: str  # ISO timestamp


# ── Positions ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiPosition:
    ticker: str
    market_exposure: int      # net contracts from market's perspective
    position: int             # net contracts you hold (positive=YES, negative=NO)
    resting_orders_count: int
    total_traded: int
    fees_paid: str            # dollar string
    realized_pnl: str | None  # dollar string
    unrealized_pnl: str | None


# ── Orders ────────────────────────────────────────────────────────────────────

@dataclass
class KalshiOrderRequest:
    ticker: str
    side: str           # yes | no
    action: str         # buy | sell
    count: int
    yes_price: str | None = None   # dollar string e.g. "0.5600"
    no_price: str | None = None
    time_in_force: str = "good_till_canceled"
    client_order_id: str | None = None
    post_only: bool = False
    reduce_only: bool = False


@dataclass(frozen=True)
class KalshiOrderStatus:
    order_id: str
    client_order_id: str | None
    ticker: str
    side: str
    action: str
    status: str          # resting | canceled | executed
    order_type: str
    yes_price: str | None
    no_price: str | None
    count: int
    remaining_count: int
    amend_count: int
    created_time: str | None
    close_time: str | None
    fees: str | None


# ── Fills ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiFill:
    fill_id: str | None
    order_id: str
    ticker: str
    side: str
    action: str
    count: int
    yes_price: str
    no_price: str
    created_time: str | None
    fees: str | None
    is_taker: bool = False
