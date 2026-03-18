from __future__ import annotations

import pandas as pd

from trading_platform.signals.loaders import load_feature_frame
from trading_platform.signals.registry import SIGNAL_REGISTRY


def generate_signals_for_symbol(
    symbol: str,
    strategy: str,
    *,
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
) -> pd.DataFrame:
    if strategy not in SIGNAL_REGISTRY:
        raise ValueError(f"Unsupported strategy: {strategy}")

    df = load_feature_frame(symbol)
    signal_fn = SIGNAL_REGISTRY[strategy]

    return signal_fn(
        df,
        fast=fast,
        slow=slow,
        lookback=lookback,
    )