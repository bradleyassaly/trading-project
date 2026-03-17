from __future__ import annotations

import argparse

from trading_platform.cli.common import (
    add_symbol_arguments,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.features.build import build_features


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build feature datasets for one or more ticker symbols."
    )
    add_symbol_arguments(parser)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = build_features(symbol)
        print(f"[OK] {symbol}: saved features to {path}")


if __name__ == "__main__":
    main()