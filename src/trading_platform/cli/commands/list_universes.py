from __future__ import annotations

import argparse

from trading_platform.cli.common import UNIVERSES


def cmd_list_universes(args: argparse.Namespace) -> None:
    for name, symbols in UNIVERSES.items():
        print(f"{name}: {', '.join(symbols)}")