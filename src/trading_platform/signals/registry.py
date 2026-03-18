from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from trading_platform.signals.momentum_hold import generate_signal_frame as momentum_hold_signal_frame
from trading_platform.signals.sma_cross import generate_signal_frame as sma_cross_signal_frame

SignalGenerator = Callable[..., pd.DataFrame]

SIGNAL_REGISTRY: dict[str, SignalGenerator] = {
    "sma_cross": sma_cross_signal_frame,
    "momentum_hold": momentum_hold_signal_frame,
}