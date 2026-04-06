"""CLI: trading-cli data polymarket backfill-historical-trades"""
from __future__ import annotations

import argparse


def cmd_polymarket_backfill_historical(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.historical_backfill import backfill
    backfill(
        limit=int(getattr(args, "limit", None) or 200),
        min_volume=float(getattr(args, "min_volume", None) or 5000),
        max_trades_per_market=int(getattr(args, "max_trades_per_market", None) or 10000),
    )
