from __future__ import annotations

import argparse
from typing import Iterable


UNIVERSES: dict[str, list[str]] = {
    "core": ["SPY", "QQQ", "IWM"],
    "megacap": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"],
    "sectors": ["XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLY"],
}


def add_symbol_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--symbol",
        type=str,
        help="Single ticker symbol, e.g. SPY",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="One or more ticker symbols, e.g. --symbols SPY QQQ IWM",
    )
    parser.add_argument(
        "--universe",
        choices=sorted(UNIVERSES.keys()),
        help="Named ticker universe",
    )


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    provided = [bool(args.symbol), bool(args.symbols), bool(args.universe)]
    if sum(provided) != 1:
        raise SystemExit(
            "Provide exactly one of: --symbol, --symbols, or --universe"
        )

    if args.symbol:
        symbols = [args.symbol]
    elif args.symbols:
        symbols = list(args.symbols)
    else:
        symbols = UNIVERSES[args.universe]

    normalized = []
    seen = set()
    for symbol in symbols:
        s = symbol.strip().upper()
        if s and s not in seen:
            normalized.append(s)
            seen.add(s)

    if not normalized:
        raise SystemExit("No valid symbols were provided.")

    return normalized


def print_symbol_list(symbols: Iterable[str]) -> str:
    return ", ".join(symbols)