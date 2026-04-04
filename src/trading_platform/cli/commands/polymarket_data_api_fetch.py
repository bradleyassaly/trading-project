"""
CLI: trading-cli data polymarket data-api-fetch
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _project_relative(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else PROJECT_ROOT / path


def cmd_polymarket_data_api_fetch(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.data_api_fetcher import PolymarketDataApiFetcher

    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/polymarket/data_api_trades")
    hours_back = int(getattr(args, "hours_back", None) or 168)
    condition_id = getattr(args, "condition_id", None)

    fetcher = PolymarketDataApiFetcher()

    if condition_id:
        print(f"Fetching trades for market {condition_id}...")
        count = fetcher.fetch_market_trades(condition_id, output_dir)
        print(f"[DONE] {count} trades written")
    else:
        print(f"Fetching all recent trades (last {hours_back}h)...")
        count = fetcher.fetch_recent_trades(output_dir, hours_back=hours_back)
        print(f"[DONE] {count} trades written to {output_dir}")
