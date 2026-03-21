from __future__ import annotations

import pandas as pd
from backtesting import Strategy


class BreakoutHold(Strategy):
    entry_lookback: int = 55
    exit_lookback: int = 20
    momentum_lookback: int | None = None

    def init(self) -> None:
        close = self.data.Close
        self.breakout_high = self.I(
            lambda x: pd.Series(x).shift(1).rolling(self.entry_lookback).max(),
            close,
        )
        self.breakout_low = self.I(
            lambda x: pd.Series(x).shift(1).rolling(self.exit_lookback).min(),
            close,
        )
        if self.momentum_lookback is not None:
            self.trailing_return = self.I(
                lambda x: pd.Series(x).pct_change(self.momentum_lookback),
                close,
            )
        else:
            self.trailing_return = None

    def next(self) -> None:
        price = float(self.data.Close[-1])
        breakout_high = self.breakout_high[-1]
        breakout_low = self.breakout_low[-1]
        momentum_ok = True
        if self.trailing_return is not None:
            momentum_ok = pd.notna(self.trailing_return[-1]) and self.trailing_return[-1] > 0

        if self.position and pd.notna(breakout_low) and price < breakout_low:
            self.position.close()
            return

        if not self.position and momentum_ok and pd.notna(breakout_high) and price > breakout_high:
            self.buy()
