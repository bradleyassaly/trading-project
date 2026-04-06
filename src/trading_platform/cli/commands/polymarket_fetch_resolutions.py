"""CLI: trading-cli data polymarket fetch-resolutions"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_fetch_resolutions(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.gamma_resolution_fetcher import GammaResolutionFetcher

    days = int(getattr(args, "days", None) or 90)
    output = PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"

    print(f"Fetching Polymarket resolutions from Gamma API (last {days} days)...")
    fetcher = GammaResolutionFetcher()
    count = fetcher.fetch_all_resolved(output, since_days=days)
    print(f"[DONE] {count} resolutions written to {output}")
