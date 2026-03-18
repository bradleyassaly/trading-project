from __future__ import annotations

from collections.abc import Callable

import polars as pl

from trading_platform.features.momentum import add_momentum_features
from trading_platform.features.trend import add_trend_features
from trading_platform.features.volatility import add_volatility_features
from trading_platform.features.volume import add_volume_features

FeatureBuilder = Callable[[pl.DataFrame], pl.DataFrame]

FEATURE_BUILDERS: dict[str, FeatureBuilder] = {
    "momentum": add_momentum_features,
    "trend": add_trend_features,
    "volatility": add_volatility_features,
    "volume": add_volume_features,
}

DEFAULT_FEATURE_GROUPS: list[str] = [
    "trend",
    "momentum",
    "volatility",
    "volume",
]