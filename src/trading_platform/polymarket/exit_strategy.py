"""
Exit strategy rules — DOCUMENTED BUT NOT WIRED IN PAPER TRADING.

Paper trades hold to resolution for clean hypothesis validation. These
rules activate only when transitioning to live trading (Phase 4). The
rules are documented now so the live executor can implement them when
the thesis is validated (70%+ accuracy on 50+ trades).

Usage (future live executor)::

    from trading_platform.polymarket.exit_strategy import ExitStrategy
    strategy = ExitStrategy()
    should_exit, reason = strategy.should_exit(trade, current_price)
    if should_exit:
        executor.close_position(trade.id, reason=reason)
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass
class ExitRule:
    name: str
    description: str
    threshold: float
    active: bool = True


# Default exit rules for live trading.
DEFAULT_RULES = [
    ExitRule(
        name="STOP_LOSS",
        description="Exit if unrealized loss exceeds threshold",
        threshold=-0.30,
    ),
    ExitRule(
        name="TAKE_PROFIT",
        description="Exit if unrealized gain exceeds threshold",
        threshold=0.80,
    ),
    ExitRule(
        name="TIME_DECAY",
        description="Exit if held > 14 days with < 5% gain",
        threshold=336,  # hours
    ),
    ExitRule(
        name="WHALE_EXIT",
        description="Exit if the copied wallet exits their position",
        threshold=0,
        active=False,  # TODO: detect wallet exits from trade poller
    ),
]


class ExitStrategy:
    """Evaluate whether an open position should be exited.

    NOT wired into the paper executor. Paper trades hold to resolution.
    """

    def __init__(self, rules: list[ExitRule] | None = None) -> None:
        self.rules = rules or list(DEFAULT_RULES)

    def should_exit(
        self,
        trade: dict[str, Any],
        current_price: float,
    ) -> tuple[bool, str]:
        """Check all active rules. Returns (should_exit, reason)."""
        entry = float(trade.get("entry_price") or 0)
        side = trade.get("side", "YES")
        entry_ts = trade.get("entry_ts") or int(time.time())
        now = int(time.time())

        # Compute unrealized return
        if side in ("YES", "BUY"):
            unrealized = (current_price - entry) / max(entry, 0.01)
        else:
            unrealized = (entry - current_price) / max(1 - entry, 0.01)

        hours_held = (now - entry_ts) / 3600

        for rule in self.rules:
            if not rule.active:
                continue

            if rule.name == "STOP_LOSS" and unrealized < rule.threshold:
                return True, f"STOP_LOSS: {unrealized*100:.0f}% loss (threshold {rule.threshold*100:.0f}%)"

            if rule.name == "TAKE_PROFIT" and unrealized > rule.threshold:
                return True, f"TAKE_PROFIT: {unrealized*100:.0f}% gain (threshold {rule.threshold*100:.0f}%)"

            if rule.name == "TIME_DECAY" and hours_held > rule.threshold and abs(unrealized) < 0.05:
                return True, (
                    f"TIME_DECAY: {hours_held/24:.0f} days held, "
                    f"only {unrealized*100:.1f}% return"
                )

        return False, "HOLD"

    def get_rules_summary(self) -> list[dict[str, Any]]:
        """For the API / dashboard."""
        return [
            {
                "name": r.name,
                "description": r.description,
                "threshold": r.threshold,
                "active": r.active,
            }
            for r in self.rules
        ]
