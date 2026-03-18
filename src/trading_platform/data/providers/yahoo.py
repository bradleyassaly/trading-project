from __future__ import annotations

import pandas as pd
import yfinance as yf


class YahooBarDataProvider:
    @property
    def provider_name(self) -> str:
        return "yahoo"

    def __init__(self, auto_adjust: bool = False, progress: bool = False) -> None:
        self.auto_adjust = auto_adjust
        self.progress = progress

    def fetch_bars(
        self,
        symbol: str,
        start: str,
        end: str | None = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        df = yf.download(
            symbol,
            start=start,
            end=end,
            interval=interval,
            auto_adjust=self.auto_adjust,
            progress=self.progress,
        )

        if df.empty:
            raise ValueError(
                f"No data returned from Yahoo for symbol={symbol}, "
                f"start={start}, end={end}, interval={interval}"
            )

        return df