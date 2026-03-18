from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.features.build import build_features


def cmd_features(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = build_features(symbol, feature_groups=args.feature_groups)
        print(f"[OK] {symbol}: saved features to {path}")