"""Canonical price-space convention for live_trades.

Established empirically 2026-07-07 (scripts probe over 38 SELL entries,
36/38 unambiguous): ``live_trades.fill_price`` and ``entry_price`` are ALWAYS
stored in YES-space — the price of the YES token — regardless of trade
direction. This holds even for SELL-direction trades, where we actually hold
the NO token: the stored number is the YES price (≈ 1 − NO_price).

The recurring "price-space" bugs (5 ledger-corrupting incidents through
2026-07-06, plus the audit's C1/C2/C3) are NOT indeterminate storage — they
are individual call sites that either (a) read fill_price as the held-token
price directly, or (b) write a held-token-space price (e.g. a data-api fill,
which is quoted in the traded token's own space) into the column without
converting to YES-space first.

This module is the single source of truth. Every site that reasons about the
economic value of a position MUST go through it rather than open-coding
``1 - fill_price``.

Definitions (``yes`` = a YES-space price in [0,1], ``direction`` in
{"BUY","YES"} vs {"SELL","NO"}):

    held_token_price(direction, yes)  -> price of the token we HOLD, own space
    cost_per_share(direction, yes)    -> alias: entry cost of one held share
    to_yes_space(direction, held)     -> convert a held-token-space price to
                                          YES-space for storage
    position_cost(direction, shares, yes)      -> shares * held cost
    position_value(direction, shares, yes_now) -> shares * held mark now
    realized_pnl(direction, shares, yes_entry, yes_exit) -> signed $ PnL
"""
from __future__ import annotations

_YES_SIDES = ("BUY", "YES")

# Floor used everywhere a held-token price could otherwise hit 0 and blow up
# a division or produce a nonsensical zero cost basis.
_FLOOR = 0.001


def _is_yes(direction: str | None) -> bool:
    return (direction or "BUY").upper() in _YES_SIDES


def held_token_price(direction: str | None, yes_price: float) -> float:
    """Price of the token we hold, in its own space.

    BUY/YES → the YES price itself. SELL/NO → the NO price = 1 − YES.
    """
    yp = float(yes_price)
    return max(yp, _FLOOR) if _is_yes(direction) else max(1.0 - yp, _FLOOR)


# Entry cost of one held share is just the held-token price at entry.
cost_per_share = held_token_price


def to_yes_space(direction: str | None, held_price: float) -> float:
    """Convert a held-token-space price (e.g. a data-api fill quoted in the
    traded token's own space) into the YES-space storage convention."""
    hp = float(held_price)
    return hp if _is_yes(direction) else (1.0 - hp)


def position_cost(direction: str | None, shares: float, yes_entry: float) -> float:
    return float(shares) * held_token_price(direction, yes_entry)


def position_value(direction: str | None, shares: float, yes_now: float) -> float:
    return float(shares) * held_token_price(direction, yes_now)


def realized_pnl(direction: str | None, shares: float,
                 yes_entry: float, yes_exit: float) -> float:
    """Signed realized $ PnL: (held exit price − held entry price) × shares.

    Both prices are YES-space inputs; the held-token conversion is applied
    consistently to entry and exit so the sign is correct for both directions.
    """
    entry_held = held_token_price(direction, yes_entry)
    exit_held = held_token_price(direction, yes_exit)
    return float(shares) * (exit_held - entry_held)
