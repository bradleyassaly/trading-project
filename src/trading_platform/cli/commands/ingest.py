from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.services.ingest_service import run_ingest


def cmd_ingest(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = run_ingest(
            symbol=args.symbol,
            start=args.start,
            end=getattr(args, "end", None),
            interval=getattr(args, "interval", "1d"),
        )
        print(f"[OK] {symbol}: saved raw data to {path}")