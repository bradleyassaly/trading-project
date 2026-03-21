from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from trading_platform.cli.presets import get_preset_choices
from trading_platform.features.registry import DEFAULT_FEATURE_GROUPS, FEATURE_BUILDERS
from trading_platform.signals.loaders import load_feature_frame, resolve_feature_frame_path
from trading_platform.strategies.registry import STRATEGY_REGISTRY
from trading_platform.universes.definitions import UNIVERSE_DEFINITIONS

UNIVERSES = UNIVERSE_DEFINITIONS
XSEC_RESEARCH_STRATEGIES = {"xsec_momentum_topn"}
XSEC_BENCHMARK_CHOICES = ["equal_weight"]
XSEC_PORTFOLIO_CONSTRUCTION_MODES = ["pure_topn", "transition"]


def add_preset_argument(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "--preset",
        type=str,
        default=None,
        choices=get_preset_choices(),
        help=help_text,
    )


def add_strategy_choice_argument(
    parser: argparse.ArgumentParser,
    *,
    help_text: str,
    default: str = "sma_cross",
    include_xsec: bool = False,
) -> None:
    parser.add_argument(
        "--strategy",
        type=str,
        default=default,
        choices=get_strategy_choices(include_xsec=include_xsec),
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
    has_symbols = bool(getattr(args, "symbols", None))
    has_universe = bool(getattr(args, "universe", None))

    if has_symbols == has_universe:
        raise SystemExit("Provide exactly one of --symbols or --universe")

    if has_symbols:
        return list(dict.fromkeys([s.upper() for s in args.symbols]))

    if has_universe:
        return UNIVERSES[args.universe]

    raise SystemExit("Provide exactly one of --symbols or --universe")

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

def add_strategy_arguments(parser: argparse.ArgumentParser, *, include_xsec: bool = False) -> None:
    parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=get_strategy_choices(include_xsec=include_xsec),
        help="Strategy to run",
    )
    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window")
    parser.add_argument("--lookback", type=int, default=20, help="Momentum lookback")
    parser.add_argument("--entry-lookback", type=int, default=55, help="Breakout entry lookback in bars")
    parser.add_argument("--exit-lookback", type=int, default=20, help="Breakout exit lookback in bars")
    parser.add_argument("--momentum-lookback", type=int, default=None, help="Optional breakout momentum filter lookback in bars")
    parser.add_argument("--cash", type=float, default=10_000, help="Starting cash")
    parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate",
    )


def build_strategy_params(args: argparse.Namespace) -> dict[str, object]:
    return {
        "fast": getattr(args, "fast", 20),
        "slow": getattr(args, "slow", 100),
        "lookback": getattr(args, "lookback", 20),
        "lookback_bars": getattr(args, "lookback_bars", 126),
        "skip_bars": getattr(args, "skip_bars", 0),
        "top_n": getattr(args, "top_n", 3),
        "rebalance_bars": getattr(args, "rebalance_bars", 21),
        "max_position_weight": getattr(args, "max_position_weight", None),
        "min_avg_dollar_volume": getattr(args, "min_avg_dollar_volume", None),
        "max_names_per_sector": getattr(args, "max_names_per_sector", None),
        "turnover_buffer_bps": getattr(args, "turnover_buffer_bps", 0.0),
        "max_turnover_per_rebalance": getattr(args, "max_turnover_per_rebalance", None),
        "weighting_scheme": getattr(args, "weighting_scheme", "equal"),
        "vol_lookback_bars": getattr(args, "vol_lookback_bars", 20),
        "portfolio_construction_mode": getattr(args, "portfolio_construction_mode", "pure_topn"),
        "entry_lookback": getattr(args, "entry_lookback", 55),
        "exit_lookback": getattr(args, "exit_lookback", 20),
        "momentum_lookback": getattr(args, "momentum_lookback", None),
    }


def resolve_turnover_cost(args: argparse.Namespace) -> float:
    cost_bps = getattr(args, "cost_bps", None)
    if cost_bps is not None:
        return float(cost_bps) / 10_000.0
    return float(getattr(args, "commission", 0.0))


def normalize_paper_weighting_scheme(weighting_scheme: str) -> str:
    if weighting_scheme == "inv_vol":
        return "inverse_vol"
    return weighting_scheme


def get_strategy_choices(*, include_xsec: bool = False) -> list[str]:
    choices = set(STRATEGY_REGISTRY.keys())
    if include_xsec:
        choices.update(XSEC_RESEARCH_STRATEGIES)
    return sorted(choices)


def add_xsec_research_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--lookback-bars", type=int, default=126, help="Cross-sectional momentum lookback in bars")
    parser.add_argument("--skip-bars", type=int, default=0, help="Bars to skip before measuring trailing momentum")
    parser.add_argument("--top-n", type=int, default=3, help="Number of symbols to hold for cross-sectional top-N research")
    parser.add_argument("--rebalance-bars", type=int, default=21, help="Rebalance interval in bars for cross-sectional top-N research")
    parser.add_argument("--portfolio-construction-mode", type=str, default="pure_topn", choices=XSEC_PORTFOLIO_CONSTRUCTION_MODES, help="Use pure_topn for research-clean top-N portfolios or transition for gradual deployable transitions.")
    parser.add_argument("--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum 20-bar average dollar volume required for xsec eligibility.")
    parser.add_argument("--max-names-per-sector", type=int, default=None, help="Optional maximum number of selected names per sector when sector metadata is available.")
    parser.add_argument("--turnover-buffer-bps", type=float, default=0.0, help="Optional minimum momentum-score improvement, expressed in bps of score gap, required to replace an existing xsec holding.")
    parser.add_argument("--max-turnover-per-rebalance", type=float, default=None, help="Optional cap on absolute turnover per xsec rebalance.")
    parser.add_argument("--weighting-scheme", type=str, default="equal", choices=["equal", "inv_vol"], help="How to size selected xsec holdings.")
    parser.add_argument("--vol-lookback-bars", type=int, default=20, help="Lookback window for inverse-vol xsec weighting.")
    parser.add_argument("--benchmark", type=str, default="equal_weight", choices=XSEC_BENCHMARK_CHOICES, help="Benchmark type for cross-sectional research")


def add_xsec_paper_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--lookback-bars", type=int, default=126, help="Cross-sectional momentum lookback in bars for paper runs")
    parser.add_argument("--skip-bars", type=int, default=0, help="Bars to skip before measuring trailing xsec momentum in paper runs")
    parser.add_argument("--rebalance-bars", type=int, default=21, help="Rebalance interval in bars for xsec paper runs")
    parser.add_argument("--portfolio-construction-mode", type=str, default="pure_topn", choices=XSEC_PORTFOLIO_CONSTRUCTION_MODES, help="Use pure_topn for the research-clean baseline or transition for the deployable overlay.")
    parser.add_argument("--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight in paper runs.")
    parser.add_argument("--max-names-per-sector", type=int, default=None, help="Optional maximum selected names per sector for xsec paper runs.")
    parser.add_argument("--turnover-buffer-bps", type=float, default=0.0, help="Optional score-gap buffer, expressed in bps, required before replacing an existing xsec holding.")
    parser.add_argument("--max-turnover-per-rebalance", type=float, default=None, help="Optional cap on absolute turnover per xsec rebalance in paper runs.")
    parser.add_argument("--vol-lookback-bars", type=int, default=20, help="Lookback window used by inv_vol weighting for xsec paper runs.")
    parser.add_argument("--benchmark", type=str, default=None, choices=XSEC_BENCHMARK_CHOICES, help="Optional benchmark label to persist with xsec paper-run summaries.")


def add_xsec_live_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--lookback-bars", type=int, default=126, help="Cross-sectional momentum lookback in bars for live dry-runs")
    parser.add_argument("--skip-bars", type=int, default=0, help="Bars to skip before measuring trailing xsec momentum in live dry-runs")
    parser.add_argument("--rebalance-bars", type=int, default=21, help="Rebalance interval in bars for xsec live dry-runs")
    parser.add_argument("--portfolio-construction-mode", type=str, default="pure_topn", choices=XSEC_PORTFOLIO_CONSTRUCTION_MODES, help="Use pure_topn for the research-clean baseline or transition for the deployable overlay.")
    parser.add_argument("--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight in live dry-runs.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum average dollar volume required for xsec eligibility in live dry-runs.")
    parser.add_argument("--max-names-per-sector", type=int, default=None, help="Optional maximum selected names per sector for xsec live dry-runs.")
    parser.add_argument("--turnover-buffer-bps", type=float, default=0.0, help="Optional score-gap buffer, expressed in bps, required before replacing an existing xsec holding.")
    parser.add_argument("--max-turnover-per-rebalance", type=float, default=None, help="Optional cap on absolute turnover per xsec rebalance in live dry-runs.")
    parser.add_argument("--vol-lookback-bars", type=int, default=20, help="Lookback window used by inv_vol weighting for xsec live dry-runs.")
    parser.add_argument("--benchmark", type=str, default=None, choices=XSEC_BENCHMARK_CHOICES, help="Optional benchmark label to persist with xsec live dry-run summaries.")

def add_date_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=str, default=None, help="Optional inclusive start date in YYYY-MM-DD format")
    parser.add_argument("--end", type=str, default=None, help="Optional inclusive end date in YYYY-MM-DD format")

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

def prepare_research_frame(
    symbol: str,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, object]:
    feature_path = resolve_feature_frame_path(symbol)
    df = load_feature_frame(symbol)
    working = df.copy()

    if "Date" in working.columns:
        working["Date"] = pd.to_datetime(working["Date"])
        date_col = "Date"
    elif "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"])
        date_col = "timestamp"
    else:
        working.index = pd.to_datetime(working.index)
        working = working.reset_index().rename(columns={"index": "Date"})
        date_col = "Date"

    working = working.sort_values(date_col).reset_index(drop=True)
    if start:
        working = working[working[date_col] >= pd.Timestamp(start)]
    if end:
        working = working[working[date_col] <= pd.Timestamp(end)]
    working = working.reset_index(drop=True)
    if working.empty:
        raise ValueError(
            f"No rows available for {symbol} after applying date range start={start!r} end={end!r}"
        )

    effective_start = pd.Timestamp(working[date_col].min()).date().isoformat()
    effective_end = pd.Timestamp(working[date_col].max()).date().isoformat()
    return {
        "df": working,
        "path": Path(feature_path),
        "date_col": date_col,
        "effective_start": effective_start,
        "effective_end": effective_end,
        "rows": int(len(working)),
    }
