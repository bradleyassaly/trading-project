import pandas as pd
from backtesting import Backtest, Strategy

from trading_platform.settings import FEATURE_DATA_DIR


def sma(values, n: int):
    return pd.Series(values).rolling(n).mean()


class SMACross(Strategy):
    fast = 20
    slow = 100

    def init(self):
        self.fast_ma = self.I(sma, self.data.Close, self.fast)
        self.slow_ma = self.I(sma, self.data.Close, self.slow)

    def next(self):
        # skip warmup / NaN region
        if pd.isna(self.fast_ma[-1]) or pd.isna(self.slow_ma[-1]):
            return

        if self.fast_ma[-1] > self.slow_ma[-1]:
            if not self.position:
                self.buy()
        elif self.fast_ma[-1] < self.slow_ma[-1]:
            if self.position:
                self.position.close()


def run_backtest(symbol: str):
    df = pd.read_parquet(FEATURE_DATA_DIR / f"{symbol}.parquet")

    # make sure timestamp is the index
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()

    # backtesting.py expects these exact column names
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })

    # keep only required columns
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()

    bt = Backtest(df, SMACross, cash=10_000, commission=0.001)
    stats = bt.run()
    return stats