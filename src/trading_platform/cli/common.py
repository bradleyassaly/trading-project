from __future__ import annotations

import argparse
import pandas as pd

from trading_platform.features.registry import DEFAULT_FEATURE_GROUPS, FEATURE_BUILDERS
from trading_platform.strategies.registry import STRATEGY_REGISTRY
from trading_platform.universes.definitions import UNIVERSE_DEFINITIONS

UNIVERSES = UNIVERSE_DEFINITIONS


def add_strategy_choice_argument(
    parser: argparse.ArgumentParser,
    *,
    help_text: str,
    default: str = "sma_cross",
) -> None:
    parser.add_argument(
        "--strategy",
        type=str,
        default=default,
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help=help_text,
    )

def add_symbol_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="One or more ticker symbols",
    )
    parser.add_argument(
        "--universe",
        type=str,
        choices=sorted(UNIVERSES.keys()) if UNIVERSES else None,
        help="Named universe of symbols",
    )

def resolve_symbols(args: argparse.Namespace) -> list[str]:
    if getattr(args, "symbols", None):
        return list(dict.fromkeys([s.upper() for s in args.symbols]))

    if getattr(args, "universe", None):
        return UNIVERSES[args.universe]

    raise SystemExit("Provide either --symbols or --universe")

def print_symbol_list(symbols: list[str], max_items: int = 10) -> str:
    if len(symbols) <= max_items:
        return ", ".join(symbols)
    return f"{', '.join(symbols[:max_items])}, ... ({len(symbols)} total)"

def add_shared_symbol_args(parser) -> None:
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the run.",
    )
    parser.add_argument(
        "--universe",
        default=None,
        help="Named universe to trade instead of passing --symbols.",
    )

def add_strategy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to run",
    )
    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window")
    parser.add_argument("--lookback", type=int, default=20, help="Momentum lookback")
    parser.add_argument("--cash", type=float, default=10_000, help="Starting cash")
    parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate",
    )

def add_feature_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--feature-groups",
        nargs="+",
        default=DEFAULT_FEATURE_GROUPS,
        choices=sorted(FEATURE_BUILDERS.keys()),
        help="Feature groups to build",
    )

def compound_return_pct(series: pd.Series) -> float:
    clean = series.dropna()
    if clean.empty:
        return float("nan")
    growth = (1 + clean / 100.0).prod()
    return (growth - 1) * 100.0

def compute_buy_and_hold_return_pct(df: pd.DataFrame) -> float:
    working = df.copy()

    if "Date" in working.columns:
        working["Date"] = pd.to_datetime(working["Date"])
        working = working.sort_values("Date")
    elif "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"])
        working = working.sort_values("timestamp")
    else:
        working = working.sort_index()

    rename_map = {}
    for col in working.columns:
        if str(col).lower() == "close":
            rename_map[col] = "Close"

    working = working.rename(columns=rename_map)

    if "Close" not in working.columns:
        raise ValueError(f"Benchmark requires Close column. Available: {list(working.columns)}")

    close = working["Close"].dropna()
    if len(close) < 2:
        return float("nan")

    return (close.iloc[-1] / close.iloc[0] - 1.0) * 100.0
