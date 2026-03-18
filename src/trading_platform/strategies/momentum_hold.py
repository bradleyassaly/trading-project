from __future__ import annotations

import pandas as pd
from backtesting import Strategy


class MomentumHold(Strategy):
    lookback: int = 20

    def init(self) -> None:
        close = self.data.Close
        self.momentum = self.I(
            lambda x: pd.Series(x).pct_change(self.lookback),
            close,
        )

    def next(self) -> None:
        if self.momentum[-1] > 0 and not self.position:
            self.buy()
        elif self.momentum[-1] <= 0 and self.position:
            self.position.close()