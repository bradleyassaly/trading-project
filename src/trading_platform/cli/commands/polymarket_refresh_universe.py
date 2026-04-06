"""
CLI command: trading-cli data polymarket refresh-universe

Fetches top markets per category from Polymarket Gamma API and saves
to data/polymarket/market_universe.json.
"""
from __future__ import annotations

import argparse


def cmd_polymarket_refresh_universe(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.market_universe import MarketUniverse

    max_per_cat = getattr(args, "max_per_category", 25)
    print(f"Refreshing market universe (max {max_per_cat} per category)...")

    universe = MarketUniverse()
    universe.refresh(max_per_category=max_per_cat)

    print(f"Saved to data/polymarket/market_universe.json")
    print(f"Total condition IDs: {len(universe.get_all_condition_ids())}")
