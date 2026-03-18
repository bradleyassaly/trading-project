from __future__ import annotations

import pandas as pd

from trading_platform.signals.common import make_base_signal_frame, normalize_price_frame

def generate_signal_frame(
    df: pd.DataFrame,
    *,
    lookback: int = 20,
    **_: object,
) -> pd.DataFrame:
    if lookback <= 0:
        raise ValueError(f"lookback must be positive, got {lookback}")

    working = normalize_price_frame(df)
    out = make_base_signal_frame(working)

    out["momentum"] = working["close"] / working["close"].shift(lookback) - 1.0
    out["position"] = (out["momentum"] > 0.0).astype(float)
    out["position"] = out["position"].fillna(0.0)

    return out[["close", "asset_return", "position"]]