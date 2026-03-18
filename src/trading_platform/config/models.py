from __future__ import annotations

from dataclasses import dataclass, field

from trading_platform.features.registry import FEATURE_BUILDERS

VALID_INTERVALS = {
    "1d",
    "1h",
    "30m",
    "15m",
    "5m",
    "1m",
}

VALID_STRATEGIES = {
    "sma_cross",
    "momentum",
    "buy_and_hold",
}


def _validate_symbol(symbol: str) -> None:
    if not symbol or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")


@dataclass(frozen=True)
class IngestConfig:
    symbol: str
    start: str = "2010-01-01"
    end: str | None = None
    interval: str = "1d"

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.interval not in VALID_INTERVALS:
            raise ValueError(
                f"Unsupported interval: {self.interval}. "
                f"Supported intervals: {sorted(VALID_INTERVALS)}"
            )


@dataclass(frozen=True)
class FeatureConfig:
    symbol: str
    feature_groups: list[str] | None = None

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.feature_groups is not None:
            unknown = sorted(set(self.feature_groups) - set(FEATURE_BUILDERS.keys()))
            if unknown:
                raise ValueError(
                    f"Unknown feature groups: {unknown}. "
                    f"Available groups: {sorted(FEATURE_BUILDERS.keys())}"
                )


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str
    strategy: str = "sma_cross"
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None
    cash: float = 10_000
    commission: float = 0.0

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.strategy not in VALID_STRATEGIES:
            raise ValueError(
                f"Unsupported strategy: {self.strategy}. "
                f"Supported strategies: {sorted(VALID_STRATEGIES)}"
            )

        if self.cash <= 0:
            raise ValueError("cash must be > 0")

        if self.commission < 0:
            raise ValueError("commission must be >= 0")

        if self.strategy == "sma_cross":
            if self.fast is None or self.slow is None:
                raise ValueError("sma_cross strategy requires both fast and slow")
            if self.fast <= 0 or self.slow <= 0:
                raise ValueError("fast and slow must be > 0")
            if self.fast >= self.slow:
                raise ValueError("fast must be < slow for sma_cross")

        if self.strategy == "momentum":
            if self.lookback is None:
                raise ValueError("momentum strategy requires lookback")
            if self.lookback <= 0:
                raise ValueError("lookback must be > 0")


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

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.interval not in VALID_INTERVALS:
            raise ValueError(
                f"Unsupported interval: {self.interval}. "
                f"Supported intervals: {sorted(VALID_INTERVALS)}"
            )

        if self.feature_groups is not None:
            unknown = sorted(set(self.feature_groups) - set(FEATURE_BUILDERS.keys()))
            if unknown:
                raise ValueError(
                    f"Unknown feature groups: {unknown}. "
                    f"Available groups: {sorted(FEATURE_BUILDERS.keys())}"
                )

        if self.strategy not in VALID_STRATEGIES:
            raise ValueError(
                f"Unsupported strategy: {self.strategy}. "
                f"Supported strategies: {sorted(VALID_STRATEGIES)}"
            )

        if self.cash <= 0:
            raise ValueError("cash must be > 0")

        if self.commission < 0:
            raise ValueError("commission must be >= 0")

        if self.strategy == "sma_cross":
            if self.fast is None or self.slow is None:
                raise ValueError("sma_cross strategy requires both fast and slow")
            if self.fast <= 0 or self.slow <= 0:
                raise ValueError("fast and slow must be > 0")
            if self.fast >= self.slow:
                raise ValueError("fast must be < slow for sma_cross")

        if self.strategy == "momentum":
            if self.lookback is None:
                raise ValueError("momentum strategy requires lookback")
            if self.lookback <= 0:
                raise ValueError("lookback must be > 0")

@dataclass(frozen=True)
class ParameterSweepConfig:
    symbol: str
    start: str = "2010-01-01"
    end: str | None = None
    interval: str = "1d"
    feature_groups: list[str] | None = None
    strategy: str = "sma_cross"
    fast_values: list[int] = field(default_factory=list)
    slow_values: list[int] = field(default_factory=list)
    lookback_values: list[int] = field(default_factory=list)
    cash: float = 10_000
    commission: float = 0.0
    rank_metric: str = "Return [%]"

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.interval not in VALID_INTERVALS:
            raise ValueError(
                f"Unsupported interval: {self.interval}. "
                f"Supported intervals: {sorted(VALID_INTERVALS)}"
            )

        if self.feature_groups is not None:
            unknown = sorted(set(self.feature_groups) - set(FEATURE_BUILDERS.keys()))
            if unknown:
                raise ValueError(
                    f"Unknown feature groups: {unknown}. "
                    f"Available groups: {sorted(FEATURE_BUILDERS.keys())}"
                )

        if self.strategy not in VALID_STRATEGIES:
            raise ValueError(
                f"Unsupported strategy: {self.strategy}. "
                f"Supported strategies: {sorted(VALID_STRATEGIES)}"
            )

        if self.cash <= 0:
            raise ValueError("cash must be > 0")

        if self.commission < 0:
            raise ValueError("commission must be >= 0")

        if self.strategy == "sma_cross":
            if not self.fast_values or not self.slow_values:
                raise ValueError("sma_cross sweep requires fast_values and slow_values")

        if self.strategy == "momentum":
            if not self.lookback_values:
                raise ValueError("momentum sweep requires lookback_values")

@dataclass(frozen=True)
class WalkForwardConfig:
    symbol: str
    strategy: str = "sma_cross"
    start: str = "2010-01-01"
    end: str | None = None
    interval: str = "1d"
    feature_groups: list[str] | None = None

    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None

    cash: float = 10_000
    commission: float = 0.0

    train_window_bars: int = 252 * 2
    test_window_bars: int = 63
    step_bars: int = 63
    min_required_bars: int = 252

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol)

        if self.interval not in VALID_INTERVALS:
            raise ValueError(
                f"Unsupported interval: {self.interval}. "
                f"Supported intervals: {sorted(VALID_INTERVALS)}"
            )

        if self.feature_groups is not None:
            unknown = sorted(set(self.feature_groups) - set(FEATURE_BUILDERS.keys()))
            if unknown:
                raise ValueError(
                    f"Unknown feature groups: {unknown}. "
                    f"Available groups: {sorted(FEATURE_BUILDERS.keys())}"
                )

        if self.strategy not in VALID_STRATEGIES:
            raise ValueError(
                f"Unsupported strategy: {self.strategy}. "
                f"Supported strategies: {sorted(VALID_STRATEGIES)}"
            )

        if self.cash <= 0:
            raise ValueError("cash must be > 0")

        if self.commission < 0:
            raise ValueError("commission must be >= 0")

        if self.train_window_bars <= 0:
            raise ValueError("train_window_bars must be > 0")
        if self.test_window_bars <= 0:
            raise ValueError("test_window_bars must be > 0")
        if self.step_bars <= 0:
            raise ValueError("step_bars must be > 0")
        if self.min_required_bars <= 0:
            raise ValueError("min_required_bars must be > 0")

        if self.strategy == "sma_cross":
            if self.fast is None or self.slow is None:
                raise ValueError("sma_cross strategy requires both fast and slow")
            if self.fast <= 0 or self.slow <= 0:
                raise ValueError("fast and slow must be > 0")
            if self.fast >= self.slow:
                raise ValueError("fast must be < slow for sma_cross")

        if self.strategy == "momentum":
            if self.lookback is None:
                raise ValueError("momentum strategy requires lookback")
            if self.lookback <= 0:
                raise ValueError("lookback must be > 0")