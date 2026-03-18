from __future__ import annotations

import pandas as pd


def normalize_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()

    if "Date" in working.columns:
        working["Date"] = pd.to_datetime(working["Date"])
        working = working.sort_values("Date").set_index("Date")
    elif "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"])
        working = working.sort_values("timestamp").set_index("timestamp")
    else:
        working.index = pd.to_datetime(working.index)
        working = working.sort_index()

    rename_map = {}
    for col in working.columns:
        lower = str(col).lower()
        if lower == "open":
            rename_map[col] = "open"
        elif lower == "high":
            rename_map[col] = "high"
        elif lower == "low":
            rename_map[col] = "low"
        elif lower == "close":
            rename_map[col] = "close"
        elif lower == "volume":
            rename_map[col] = "volume"

    working = working.rename(columns=rename_map)

    if "close" not in working.columns:
        raise ValueError(f"Expected close column, found: {list(working.columns)}")

    return working


def make_base_signal_frame(df: pd.DataFrame) -> pd.DataFrame:
    working = normalize_price_frame(df)

    out = pd.DataFrame(index=working.index)
    out["close"] = working["close"]
    out["asset_return"] = working["close"].pct_change().fillna(0.0)

    return out