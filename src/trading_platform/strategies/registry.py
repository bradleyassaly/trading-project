from __future__ import annotations

from backtesting import Strategy

from trading_platform.strategies.breakout_hold import BreakoutHold
from trading_platform.strategies.momentum_hold import MomentumHold
from trading_platform.strategies.sma_cross import SmaCross

STRATEGY_REGISTRY: dict[str, type[Strategy]] = {
    "sma_cross": SmaCross,
    "momentum_hold": MomentumHold,
    "breakout_hold": BreakoutHold,
}
