from __future__ import annotations

import pandas as pd

from trading_platform.signals.registry import SIGNAL_REGISTRY


def build_signal_frame(
    df: pd.DataFrame,
    strategy: str,
    *,
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
) -> pd.DataFrame:
    try:
        signal_fn = SIGNAL_REGISTRY[strategy]
    except KeyError as e:
        raise ValueError(f"Unsupported strategy: {strategy}") from e

    return signal_fn(
        df,
        fast=fast,
        slow=slow,
        lookback=lookback,
    )