from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.data.ingest import ingest_symbol


def cmd_ingest(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = ingest_symbol(symbol, start=args.start)
        print(f"[OK] {symbol}: saved raw data to {path}")