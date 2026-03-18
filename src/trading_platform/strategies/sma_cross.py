from __future__ import annotations

import pandas as pd
from backtesting import Strategy


class SmaCross(Strategy):
    fast: int = 20
    slow: int = 100

    def init(self) -> None:
        close = self.data.Close
        self.sma_fast = self.I(lambda x: pd.Series(x).rolling(self.fast).mean(), close)
        self.sma_slow = self.I(lambda x: pd.Series(x).rolling(self.slow).mean(), close)

    def next(self) -> None:
        if self.sma_fast[-1] > self.sma_slow[-1] and not self.position:
            self.buy()
        elif self.sma_fast[-1] < self.sma_slow[-1] and self.position:
            self.position.close()