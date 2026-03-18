from __future__ import annotations

import argparse

from trading_platform.cli.commands.features import cmd_features
from trading_platform.cli.commands.ingest import cmd_ingest
from trading_platform.cli.commands.list_strategies import cmd_list_strategies
from trading_platform.cli.commands.list_universes import cmd_list_universes
from trading_platform.cli.commands.pipeline import cmd_pipeline
from trading_platform.cli.commands.portfolio import cmd_portfolio
from trading_platform.cli.commands.research import cmd_research
from trading_platform.cli.commands.sweep import cmd_sweep
from trading_platform.cli.commands.walkforward import cmd_walkforward
from trading_platform.cli.common import (
    add_feature_arguments,
    add_shared_symbol_args,
    add_strategy_arguments,
)
from trading_platform.strategies.registry import STRATEGY_REGISTRY


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading_platform.cli",
        description="Trading platform command line interface",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Download raw OHLCV data")
    add_shared_symbol_args(ingest_parser)
    ingest_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    features_parser = subparsers.add_parser("features", help="Build feature datasets")
    add_shared_symbol_args(features_parser)
    add_feature_arguments(features_parser)
    features_parser.set_defaults(func=cmd_features)

    research_parser = subparsers.add_parser("research", help="Run backtests")
    add_shared_symbol_args(research_parser)
    add_strategy_arguments(research_parser)
    research_parser.set_defaults(func=cmd_research)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run ingest -> features -> research in one command",
    )
    add_shared_symbol_args(pipeline_parser)
    add_feature_arguments(pipeline_parser)
    add_strategy_arguments(pipeline_parser)
    pipeline_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    pipeline_parser.set_defaults(func=cmd_pipeline)

    universes_parser = subparsers.add_parser(
        "list-universes",
        help="Show available named universes",
    )
    universes_parser.set_defaults(func=cmd_list_universes)

    strategies_parser = subparsers.add_parser(
        "list-strategies",
        help="Show available strategies",
    )
    strategies_parser.set_defaults(func=cmd_list_strategies)

    sweep_parser = subparsers.add_parser(
        "sweep",
        help="Run parameter sweep for a strategy",
    )
    add_shared_symbol_args(sweep_parser)
    sweep_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to sweep",
    )
    sweep_parser.add_argument("--fast-values", type=int, nargs="+")
    sweep_parser.add_argument("--slow-values", type=int, nargs="+")
    sweep_parser.add_argument("--lookback-values", type=int, nargs="+")
    sweep_parser.add_argument("--cash", type=float, default=10_000)
    sweep_parser.add_argument("--commission", type=float, default=0.001)
    sweep_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/sweep_results.csv",
        help="CSV output path for sweep summary",
    )
    sweep_parser.set_defaults(func=cmd_sweep)

    walk_parser = subparsers.add_parser(
        "walkforward",
        help="Run walk-forward validation",
    )
    add_shared_symbol_args(walk_parser)
    walk_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to validate",
    )
    walk_parser.add_argument("--fast-values", type=int, nargs="+")
    walk_parser.add_argument("--slow-values", type=int, nargs="+")
    walk_parser.add_argument("--lookback-values", type=int, nargs="+")
    walk_parser.add_argument("--train-years", type=int, default=5)
    walk_parser.add_argument("--test-years", type=int, default=1)
    walk_parser.add_argument("--min-train-rows", type=int, default=252)
    walk_parser.add_argument("--min-test-rows", type=int, default=126)
    walk_parser.add_argument(
        "--select-by",
        type=str,
        default="Sharpe Ratio",
        choices=["Sharpe Ratio", "Return [%]"],
        help="Metric used to choose best params on the train window",
    )
    walk_parser.add_argument("--cash", type=float, default=10_000)
    walk_parser.add_argument("--commission", type=float, default=0.001)
    walk_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/walkforward_results.csv",
        help="CSV output path",
    )
    walk_parser.set_defaults(func=cmd_walkforward)

    portfolio_parser = subparsers.add_parser(
        "portfolio",
        help="Run equal-weight portfolio backtest across multiple symbols",
    )
    add_shared_symbol_args(portfolio_parser)
    add_strategy_arguments(portfolio_parser)
    portfolio_parser.add_argument(
        "--output-dir",
        type=str,
        default="data/experiments/portfolio",
        help="Directory for portfolio outputs",
    )
    portfolio_parser.set_defaults(func=cmd_portfolio)

    return parser