from __future__ import annotations

import pandas as pd

from trading_platform.data.canonical import normalize_research_frame


def normalize_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    working = normalize_research_frame(df, require_close=True)
    return working.sort_values("timestamp").set_index("timestamp")


def make_base_signal_frame(df: pd.DataFrame) -> pd.DataFrame:
    working = normalize_price_frame(df)

    out = pd.DataFrame(index=working.index)
    out["close"] = working["close"]
    out["asset_return"] = working["close"].pct_change().fillna(0.0)

    return out
