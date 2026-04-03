"""
CLI command: trading-cli data polymarket live-collect

Connects to the Polymarket CLOB WebSocket, streams live price updates
for the top open markets by volume, and stores ticks in SQLite with
hourly parquet bar exports.

Usage
-----
    trading-cli data polymarket live-collect
    trading-cli data polymarket live-collect --config configs/polymarket.yaml
    trading-cli data polymarket live-collect --max-markets 50
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _project_relative(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else PROJECT_ROOT / p


def cmd_polymarket_live_collect(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.client import PolymarketClient, PolymarketConfig
    from trading_platform.polymarket.models import PolymarketMarket
    from trading_platform.polymarket.live_collector import (
        LiveMarketInfo,
        PolymarketLiveCollector,
        PolymarketLiveCollectorConfig,
    )

    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    max_markets = int(getattr(args, "max_markets", None) or cfg_raw.get("live_max_markets", 100))
    sleep_sec = float(cfg_raw.get("request_sleep_sec", 0.05))

    client = PolymarketClient(PolymarketConfig(request_sleep_sec=sleep_sec))

    # Fetch open (not closed) markets
    print("Fetching open markets from Gamma API...")
    raw_markets = client.list_markets(limit=500, active=True, closed=False, archived=False)
    markets = [PolymarketMarket.from_api_dict(m) for m in raw_markets]
    markets = [m for m in markets if m.yes_token_id and not m.closed]
    markets.sort(key=lambda m: m.volume, reverse=True)
    markets = markets[:max_markets]

    if not markets:
        print("[ERROR] No open markets found with clobTokenIds.")
        return

    live_infos = [
        LiveMarketInfo(
            market_id=m.id,
            question=m.question,
            yes_token_id=m.yes_token_id,
            volume=m.volume,
        )
        for m in markets
    ]

    db_path = str(_project_relative(cfg_raw.get("live_db_path", "data/polymarket/live/prices.db")))
    bars_dir = str(_project_relative(cfg_raw.get("live_hourly_bars_dir", "data/polymarket/live/hourly_bars")))

    config = PolymarketLiveCollectorConfig(
        db_path=db_path,
        hourly_bars_dir=bars_dir,
    )

    print(f"Polymarket Live Collector")
    print(f"  markets    : {len(live_infos)}")
    print(f"  db path    : {config.db_path}")
    print(f"  bars dir   : {config.hourly_bars_dir}")
    print(f"  ws url     : {config.ws_url}")
    print()
    for i, m in enumerate(live_infos[:5]):
        print(f"  [{i+1}] {m.question[:70]}  (vol={m.volume:,.0f})")
    if len(live_infos) > 5:
        print(f"  ... and {len(live_infos) - 5} more")
    print()

    collector = PolymarketLiveCollector(config, live_infos)
    asyncio.run(collector.run())
