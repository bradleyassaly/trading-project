from __future__ import annotations

from typing import Protocol

import pandas as pd


class BarDataProvider(Protocol):
    @property
    def provider_name(self) -> str:
        ...

    def fetch_bars(
        self,
        symbol: str,
        start: str,
        end: str | None = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        ...