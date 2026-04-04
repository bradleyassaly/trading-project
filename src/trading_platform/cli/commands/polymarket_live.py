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
    from datetime import timedelta

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

    ms_cfg = cfg_raw.get("market_selection", {})
    max_markets = int(
        getattr(args, "max_markets", None) or ms_cfg.get("max_markets", 75)
    )
    min_volume = float(ms_cfg.get("min_volume") or cfg_raw.get("live_min_volume", 10_000))
    horizon_days = int(ms_cfg.get("end_date_max_days") or cfg_raw.get("live_lookback_days", 30))
    sleep_sec = float(cfg_raw.get("request_sleep_sec", 0.05))

    client = PolymarketClient(PolymarketConfig(request_sleep_sec=sleep_sec))

    from datetime import datetime, timezone
    today = datetime.now(tz=timezone.utc)
    end_date_min = today.strftime("%Y-%m-%d")
    end_date_max = (today + timedelta(days=horizon_days)).strftime("%Y-%m-%d")

    # Fetch top markets by volume with 30-day horizon (no tag filter)
    print(f"Fetching top {max_markets} open markets by volume resolving within {horizon_days} days...")
    print(f"  date range : {end_date_min} → {end_date_max}")
    print(f"  min volume : {min_volume:,.0f}")

    try:
        raw_pages = client.get_all_markets(
            closed=False, active=True,
            end_date_min=end_date_min, end_date_max=end_date_max,
        )
    except Exception as exc:
        print(f"[ERROR] Failed to fetch markets: {exc}")
        return

    _SPORTS_KEYWORDS = {"vs.", "vs ", "nba", "nfl", "nhl", "mlb", "spread", "o/u",
                         "rebounds", "assists", "points", "touchdowns", "goals",
                         "winner:", "game ", "match ", "set winner"}

    all_markets = [PolymarketMarket.from_api_dict(m) for m in raw_pages]
    all_markets = [
        m for m in all_markets
        if m.yes_token_id and m.volume >= min_volume
        and not any(kw in m.question.lower() for kw in _SPORTS_KEYWORDS)
    ]
    all_markets.sort(key=lambda m: m.volume, reverse=True)
    all_markets = all_markets[:max_markets]
    print(f"  found      : {len(all_markets)} qualifying markets")

    if not all_markets:
        print("[ERROR] No open markets found matching criteria.")
        return

    live_infos = [
        LiveMarketInfo(
            market_id=m.id,
            question=m.question,
            yes_token_id=m.yes_token_id,
            volume=m.volume,
            end_date_iso=m.end_date_iso,
        )
        for m in all_markets
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
