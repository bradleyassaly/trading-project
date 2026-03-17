from __future__ import annotations

import argparse

from trading_platform.cli.common import (
    add_symbol_arguments,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.data.ingest import ingest_symbol


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download raw OHLCV data for one or more ticker symbols."
    )
    add_symbol_arguments(parser)
    parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = ingest_symbol(symbol, start=args.start)
        print(f"[OK] {symbol}: saved raw data to {path}")


if __name__ == "__main__":
    main()