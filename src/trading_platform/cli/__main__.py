from __future__ import annotations

import argparse

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import (
    UNIVERSES,
    add_symbol_arguments,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.data.ingest import ingest_symbol
from trading_platform.experiments.tracker import log_experiment
from trading_platform.features.build import build_features


def add_shared_symbol_args(parser: argparse.ArgumentParser) -> None:
    add_symbol_arguments(parser)


def cmd_ingest(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = ingest_symbol(symbol, start=args.start)
        print(f"[OK] {symbol}: saved raw data to {path}")


def cmd_features(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = build_features(symbol)
        print(f"[OK] {symbol}: saved features to {path}")


def cmd_research(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running research for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        stats = run_backtest(symbol)
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(
            f"[OK] {symbol}: "
            f"Return[%]={ret}, Sharpe={sharpe}, MaxDD[%]={max_dd}, Experiment={exp_id}"
        )


def cmd_pipeline(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running pipeline for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        raw_path = ingest_symbol(symbol, start=args.start)
        feat_path = build_features(symbol)
        stats = run_backtest(symbol)
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(f"[OK] {symbol}")
        print(f"  raw: {raw_path}")
        print(f"  features: {feat_path}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {exp_id}")


def cmd_list_universes(args: argparse.Namespace) -> None:
    for name, symbols in UNIVERSES.items():
        print(f"{name}: {', '.join(symbols)}")


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
    features_parser.set_defaults(func=cmd_features)

    research_parser = subparsers.add_parser("research", help="Run backtests")
    add_shared_symbol_args(research_parser)
    research_parser.set_defaults(func=cmd_research)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run ingest -> features -> research in one command",
    )
    add_shared_symbol_args(pipeline_parser)
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

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()