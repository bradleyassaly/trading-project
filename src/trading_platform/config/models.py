from __future__ import annotations

from dataclasses import dataclass, field

from trading_platform.features.registry import FEATURE_BUILDERS
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


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

VALID_WALK_FORWARD_MODES = {
    "fixed",
    "optimize",
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

    # fixed-parameter mode fields
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None

    # optimize mode fields
    walk_forward_mode: str = "fixed"
    rank_metric: str = "Return [%]"
    fast_values: list[int] = field(default_factory=list)
    slow_values: list[int] = field(default_factory=list)
    lookback_values: list[int] = field(default_factory=list)

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

        if self.walk_forward_mode not in VALID_WALK_FORWARD_MODES:
            raise ValueError(
                f"Unsupported walk_forward_mode: {self.walk_forward_mode}. "
                f"Supported modes: {sorted(VALID_WALK_FORWARD_MODES)}"
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

        if self.walk_forward_mode == "fixed":
            if self.strategy == "sma_cross":
                if self.fast is None or self.slow is None:
                    raise ValueError("fixed sma_cross walk-forward requires both fast and slow")
                if self.fast <= 0 or self.slow <= 0:
                    raise ValueError("fast and slow must be > 0")
                if self.fast >= self.slow:
                    raise ValueError("fast must be < slow for sma_cross")

            if self.strategy == "momentum":
                if self.lookback is None:
                    raise ValueError("fixed momentum walk-forward requires lookback")
                if self.lookback <= 0:
                    raise ValueError("lookback must be > 0")

        if self.walk_forward_mode == "optimize":
            if self.strategy == "sma_cross":
                if not self.fast_values or not self.slow_values:
                    raise ValueError(
                        "optimize sma_cross walk-forward requires fast_values and slow_values"
                    )

            if self.strategy == "momentum":
                if not self.lookback_values:
                    raise ValueError(
                        "optimize momentum walk-forward requires lookback_values"
                    )

@dataclass
class UniverseConfig:
    symbols: list[str]
    rebalance_frequency: str = "M"
    min_required_bars: int = 252


@dataclass
class PortfolioConstructionConfig:
    method: str = "top_n"
    top_n: int = 10
    long_short: bool = False
    bottom_n: int = 0
    max_weight: float = 0.10
    gross_target: float = 1.0
    net_target: float = 1.0
    transaction_cost_bps: float = 0.0


@dataclass
class UniverseWalkForwardConfig:
    feature_path: Path
    universe: UniverseConfig
    walk_forward: "WalkForwardConfig"
    backtest: "BacktestConfig"
    portfolio: PortfolioConstructionConfig
    output_dir: Path | None = None


@dataclass
class PortfolioConstructionResult:
    weights_df: pd.DataFrame
    portfolio_returns_df: pd.DataFrame
    summary: dict


@dataclass
class UniverseWalkForwardResult:
    fold_results_df: pd.DataFrame
    oos_scores_df: pd.DataFrame
    portfolio_result: PortfolioConstructionResult
    summary: dict


@dataclass(frozen=True)
class MultiStrategySleeveConfig:
    sleeve_name: str
    preset_name: str
    target_capital_weight: float
    enabled: bool = True
    min_capital_weight: float | None = None
    max_capital_weight: float | None = None
    rebalance_priority: int | None = None
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.sleeve_name or not self.sleeve_name.strip():
            raise ValueError("sleeve_name must be a non-empty string")
        if not self.preset_name or not self.preset_name.strip():
            raise ValueError("preset_name must be a non-empty string")
        if self.target_capital_weight < 0:
            raise ValueError("target_capital_weight must be >= 0")
        if self.min_capital_weight is not None and self.min_capital_weight < 0:
            raise ValueError("min_capital_weight must be >= 0")
        if self.max_capital_weight is not None and self.max_capital_weight < 0:
            raise ValueError("max_capital_weight must be >= 0")
        if (
            self.min_capital_weight is not None
            and self.max_capital_weight is not None
            and self.min_capital_weight > self.max_capital_weight
        ):
            raise ValueError("min_capital_weight must be <= max_capital_weight")
        if self.min_capital_weight is not None and self.target_capital_weight < self.min_capital_weight:
            raise ValueError("target_capital_weight must be >= min_capital_weight")
        if self.max_capital_weight is not None and self.target_capital_weight > self.max_capital_weight:
            raise ValueError("target_capital_weight must be <= max_capital_weight")


@dataclass(frozen=True)
class MultiStrategyGroupCap:
    group: str
    max_weight: float

    def __post_init__(self) -> None:
        if not self.group or not self.group.strip():
            raise ValueError("group must be a non-empty string")
        if self.max_weight < 0:
            raise ValueError("max_weight must be >= 0")


@dataclass(frozen=True)
class MultiStrategyPortfolioConfig:
    sleeves: list[MultiStrategySleeveConfig]
    gross_leverage_cap: float = 1.0
    net_exposure_cap: float = 1.0
    max_position_weight: float = 1.0
    max_symbol_concentration: float = 1.0
    sector_caps: list[MultiStrategyGroupCap] = field(default_factory=list)
    turnover_cap: float | None = None
    cash_reserve_pct: float = 0.0
    group_map_path: str | None = None
    rebalance_timestamp: str | None = None
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.sleeves:
            raise ValueError("sleeves must contain at least one sleeve")
        if self.gross_leverage_cap < 0:
            raise ValueError("gross_leverage_cap must be >= 0")
        if self.net_exposure_cap < 0:
            raise ValueError("net_exposure_cap must be >= 0")
        if self.max_position_weight < 0:
            raise ValueError("max_position_weight must be >= 0")
        if self.max_symbol_concentration < 0:
            raise ValueError("max_symbol_concentration must be >= 0")
        if self.turnover_cap is not None and self.turnover_cap < 0:
            raise ValueError("turnover_cap must be >= 0")
        if not 0.0 <= self.cash_reserve_pct < 1.0:
            raise ValueError("cash_reserve_pct must be in [0, 1)")

    @property
    def enabled_sleeves(self) -> list[MultiStrategySleeveConfig]:
        return [sleeve for sleeve in self.sleeves if sleeve.enabled]
