from __future__ import annotations

import argparse

from trading_platform.universes.registry import get_universe_symbols, list_universes


def cmd_list_universes(args: argparse.Namespace) -> None:
    for name in list_universes():
        symbols = get_universe_symbols(name)
        print(f"{name}: {', '}")
