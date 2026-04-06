"""CLI: trading-cli data polymarket backfill-goldsky-fills"""
from __future__ import annotations

import argparse


def cmd_polymarket_backfill_goldsky(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.goldsky_backfill import backfill_goldsky_fills
    backfill_goldsky_fills(
        limit=int(getattr(args, "limit", None) or 100),
        min_volume=float(getattr(args, "min_volume", None) or 10000),
        strategy=getattr(args, "strategy", None) or "balanced",
        skip_existing=getattr(args, "skip_existing", False),
    )
