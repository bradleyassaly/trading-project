"""
Kalshi signal family definitions for binary prediction market alpha research.

Three signal families, each compatible with the platform's signal family convention:
a named group that emits a scored series from feature columns produced by
``kalshi.features.build_kalshi_features``.

Signal direction convention:
  +1 → higher raw score means BUY YES (expect price to rise toward 100)
  −1 → higher raw score means SELL YES (expect price to fall toward 0)

After applying direction the returned signal is always "buy if positive, avoid if negative".
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

KALSHI_SIGNAL_FAMILY_NAMES = (
    "kalshi_calibration_drift",
    "kalshi_volume_spike",
    "kalshi_time_decay",
)


@dataclass(frozen=True)
class KalshiSignalFamily:
    name: str
    feature_col: str
    alt_feature_cols: tuple[str, ...] = ()
    direction: int = 1
    description: str = ""

    def score(self, df: pd.DataFrame) -> pd.Series:
        for col in (self.feature_col, *self.alt_feature_cols):
            if col in df.columns:
                raw = pd.to_numeric(df[col], errors="coerce")
                return raw * self.direction
        return pd.Series(dtype=float, index=df.index, name=self.name)


KALSHI_CALIBRATION_DRIFT = KalshiSignalFamily(
    name="kalshi_calibration_drift",
    feature_col="calibration_drift_z",
    direction=-1,
    description=(
        "Probability calibration drift: fades extreme z-score moves in log-odds space. "
        "Large positive z → price has overshot upward → expect mean-reversion → signal = SELL YES. "
        "Large negative z → price has overshot downward → signal = BUY YES."
    ),
)

KALSHI_VOLUME_SPIKE = KalshiSignalFamily(
    name="kalshi_volume_spike",
    feature_col="extreme_volume",
    alt_feature_cols=("volume_spike",),
    direction=1,
    description=(
        "Volume spike detection: follows informed-money direction. "
        "A statistically unusual volume spike at an extreme probability level "
        "indicates that participants with information are trading. Signal follows the spike."
    ),
)

KALSHI_TIME_DECAY = KalshiSignalFamily(
    name="kalshi_time_decay",
    feature_col="tension",
    alt_feature_cols=("price_var_proxy",),
    direction=-1,
    description=(
        "Time-decay tension: fades markets with high uncertainty-per-unit-time. "
        "High tension = high price_var_proxy relative to remaining days → "
        "uncertainty premium → fade toward resolution direction. Signal = SELL tension."
    ),
)

ALL_KALSHI_SIGNAL_FAMILIES: list[KalshiSignalFamily] = [
    KALSHI_CALIBRATION_DRIFT,
    KALSHI_VOLUME_SPIKE,
    KALSHI_TIME_DECAY,
]

_FAMILY_BY_NAME: dict[str, KalshiSignalFamily] = {f.name: f for f in ALL_KALSHI_SIGNAL_FAMILIES}


def get_kalshi_signal_family(name: str) -> KalshiSignalFamily:
    if name not in _FAMILY_BY_NAME:
        raise ValueError(
            f"Unknown Kalshi signal family: {name!r}. "
            f"Choose from: {', '.join(_FAMILY_BY_NAME)}"
        )
    return _FAMILY_BY_NAME[name]


def compute_kalshi_signal(df: pd.DataFrame, family: KalshiSignalFamily) -> pd.Series:
    return family.score(df)
