from __future__ import annotations

import argparse

from trading_platform.strategies.registry import STRATEGY_REGISTRY


def cmd_list_strategies(args: argparse.Namespace) -> None:
    for name in sorted(STRATEGY_REGISTRY.keys()):
        print(name)