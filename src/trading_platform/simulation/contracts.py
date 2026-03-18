from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class SingleAssetSimulationResult:
    timeseries: pd.DataFrame
    summary: dict[str, float]


@dataclass
class PortfolioSimulationResult:
    timeseries: pd.DataFrame
    weights: pd.DataFrame
    positions: pd.DataFrame
    summary: dict[str, float]