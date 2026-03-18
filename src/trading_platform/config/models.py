from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class IngestConfig:
    symbol: str
    start: str = "2010-01-01"
    end: str | None = None
    interval: str = "1d"


@dataclass(frozen=True)
class FeatureConfig:
    symbol: str
    feature_groups: list[str] | None = None


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    cash: float = 10_000
    commission: float = 0.0


@dataclass(frozen=True)
class ResearchWorkflowConfig:
    symbol: str
    start: str = "2010-01-01"
    end: str | None = None
    interval: str = "1d"
    feature_groups: list[str] | None = None
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    cash: float = 10_000
    commission: float = 0.0