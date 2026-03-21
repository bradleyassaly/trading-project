from __future__ import annotations

import pandas as pd

from trading_platform.signals.common import make_base_signal_frame, normalize_price_frame


def generate_signal_frame(
    df: pd.DataFrame,
    *,
    lookback_bars: int = 126,
    skip_bars: int = 0,
    **_: object,
) -> pd.DataFrame:
    if lookback_bars <= 0:
        raise ValueError(f"lookback_bars must be positive, got {lookback_bars}")
    if skip_bars < 0:
        raise ValueError(f"skip_bars must be >= 0, got {skip_bars}")

    working = normalize_price_frame(df)
    out = make_base_signal_frame(working)

    reference_close = working["close"].shift(skip_bars) if skip_bars > 0 else working["close"]
    out["score"] = reference_close / reference_close.shift(lookback_bars) - 1.0
    out["position"] = out["score"].notna().astype(float)
    return out[["close", "asset_return", "score", "position"]]
