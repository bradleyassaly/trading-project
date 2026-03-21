from __future__ import annotations

from typing import Any

import pandas as pd
from backtesting import Backtest

from trading_platform.research.diagnostics import diagnostics_from_legacy_stats
from trading_platform.settings import FEATURES_DIR
from trading_platform.strategies.registry import STRATEGY_REGISTRY


def _validate_strategy_params(
    strategy: str,
    fast: int,
    slow: int,
    lookback: int,
    lookback_bars: int,
    skip_bars: int,
    top_n: int,
    rebalance_bars: int,
    entry_lookback: int,
    exit_lookback: int,
    momentum_lookback: int | None,
    cash: float,
    commission: float,
) -> None:
    if cash <= 0:
        raise ValueError("cash must be > 0")
    if commission < 0:
        raise ValueError("commission must be >= 0")

    if strategy == "sma_cross":
        if fast <= 0:
            raise ValueError("fast must be > 0")
        if slow <= 0:
            raise ValueError("slow must be > 0")
        if fast >= slow:
            raise ValueError("fast must be less than slow")
    elif strategy == "momentum_hold":
        if lookback <= 0:
            raise ValueError("lookback must be > 0")
    elif strategy == "breakout_hold":
        if entry_lookback <= 0:
            raise ValueError("entry_lookback must be > 0")
        if exit_lookback <= 0:
            raise ValueError("exit_lookback must be > 0")
        if momentum_lookback is not None and momentum_lookback <= 0:
            raise ValueError("momentum_lookback must be > 0 when provided")

    if strategy not in STRATEGY_REGISTRY:
        raise ValueError(
            f"Unknown strategy: {strategy}. "
            f"Available: {sorted(STRATEGY_REGISTRY.keys())}"
        )


def _normalize_backtest_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    rename_map = {}
    for col in df.columns:
        lower = str(col).lower()
        if lower in {"date", "timestamp", "datetime"}:
            rename_map[col] = "Date"
        elif lower == "open":
            rename_map[col] = "Open"
        elif lower == "high":
            rename_map[col] = "High"
        elif lower == "low":
            rename_map[col] = "Low"
        elif lower == "close":
            rename_map[col] = "Close"
        elif lower == "volume":
            rename_map[col] = "Volume"

    df = df.rename(columns=rename_map)

    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns for backtest: {sorted(missing)}. "
            f"Available columns: {list(df.columns)}"
        )

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
    else:
        df.index = pd.to_datetime(df.index)

    df = df.sort_index()
    return df


def run_backtest_on_df(
    df: pd.DataFrame,
    symbol: str,
    strategy: str = "sma_cross",
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
    lookback_bars: int = 126,
    skip_bars: int = 0,
    top_n: int = 3,
    rebalance_bars: int = 21,
    entry_lookback: int = 55,
    exit_lookback: int = 20,
    momentum_lookback: int | None = None,
    cash: float = 10_000,
    commission: float = 0.001,
) -> dict[str, Any]:
    _validate_strategy_params(
        strategy=strategy,
        fast=fast,
        slow=slow,
        lookback=lookback,
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
        entry_lookback=entry_lookback,
        exit_lookback=exit_lookback,
        momentum_lookback=momentum_lookback,
        cash=cash,
        commission=commission,
    )

    df = _normalize_backtest_df(df)
    strategy_cls = STRATEGY_REGISTRY[strategy]

    bt = Backtest(
        df,
        strategy_cls,
        cash=cash,
        commission=commission,
        exclusive_orders=True,
        finalize_trades=True,
    )

    if strategy == "sma_cross":
        stats = bt.run(fast=fast, slow=slow)
    elif strategy == "momentum_hold":
        stats = bt.run(lookback=lookback)
    elif strategy == "breakout_hold":
        stats = bt.run(
            entry_lookback=entry_lookback,
            exit_lookback=exit_lookback,
            momentum_lookback=momentum_lookback,
        )
    else:
        stats = bt.run()

    result = dict(stats)
    result["symbol"] = symbol
    result["strategy"] = strategy
    result["fast"] = fast
    result["slow"] = slow
    result["lookback"] = lookback
    result["lookback_bars"] = lookback_bars
    result["skip_bars"] = skip_bars
    result["top_n"] = top_n
    result["rebalance_bars"] = rebalance_bars
    result["entry_lookback"] = entry_lookback
    result["exit_lookback"] = exit_lookback
    result["momentum_lookback"] = momentum_lookback
    result["cash"] = cash
    result["commission"] = commission
    result.update(diagnostics_from_legacy_stats(result))
    return result


def run_backtest(
    symbol: str,
    strategy: str = "sma_cross",
    fast: int = 20,
    slow: int = 100,
    lookback: int = 20,
    lookback_bars: int = 126,
    skip_bars: int = 0,
    top_n: int = 3,
    rebalance_bars: int = 21,
    entry_lookback: int = 55,
    exit_lookback: int = 20,
    momentum_lookback: int | None = None,
    cash: float = 10_000,
    commission: float = 0.001,
) -> dict[str, Any]:
    path = FEATURES_DIR / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Feature file not found for {symbol}: {path}. Run the features step first."
        )

    df = pd.read_parquet(path)
    return run_backtest_on_df(
        df=df,
        symbol=symbol,
        strategy=strategy,
        fast=fast,
        slow=slow,
        lookback=lookback,
        lookback_bars=lookback_bars,
        skip_bars=skip_bars,
        top_n=top_n,
        rebalance_bars=rebalance_bars,
        entry_lookback=entry_lookback,
        exit_lookback=exit_lookback,
        momentum_lookback=momentum_lookback,
        cash=cash,
        commission=commission,
    )
