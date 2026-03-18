from __future__ import annotations

import pandas as pd


from trading_platform.signals.common import make_base_signal_frame, normalize_price_frame


def generate_signal_frame(
    df: pd.DataFrame,
    *,
    fast: int = 20,
    slow: int = 100,
    **_: object,
) -> pd.DataFrame:
    if fast >= slow:
        raise ValueError(f"fast must be less than slow, got fast={fast}, slow={slow}")

    working = normalize_price_frame(df)
    out = make_base_signal_frame(working)

    out["fast_ma"] = working["close"].rolling(fast).mean()
    out["slow_ma"] = working["close"].rolling(slow).mean()
    out["score"] = out["fast_ma"] / out["slow_ma"] - 1.0
    out["position"] = (out["fast_ma"] > out["slow_ma"]).astype(float)
    out["position"] = out["position"].fillna(0.0)

    return out[["close", "asset_return", "score", "position"]]

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