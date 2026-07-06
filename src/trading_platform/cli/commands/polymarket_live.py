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

    # 2026-04-25: configure logging at the live-collect entry point.
    # Without this every `logger.info(...)` call inside the signal
    # engine + paper executor was being silently dropped (Python's
    # default root logger handles only WARNING+), so [DISPATCH],
    # [CAT_GATE], [EXEC_GATE], [SIDE_GATE], [STAKE_BOOST] and every
    # other diagnostic marker was invisible. Cause of the long-running
    # 0.4% signal→paper conversion blackout was a mix of (a) hard
    # sports block + (b) silent gates we couldn't see.
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="live_collect")
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
    # 2026-07-06: 75 → 150. The chain-direct fast lane (2-4s detection)
    # only covers subscribed markets; everything else waits for the
    # poller (~5 min p50). Doubling volume coverage widens the fast lane
    # at negligible ws cost.
    max_markets = int(
        getattr(args, "max_markets", None) or ms_cfg.get("max_markets", 150)
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

    _EXCLUDE_KEYWORDS = {"vs.", "vs ", "nba", "nfl", "nhl", "mlb", "spread", "o/u",
                          "rebounds", "assists", "points", "touchdowns", "goals",
                          "winner:", "game ", "match ", "set winner",
                          "2028", "2029", "2030", "nomination"}

    all_markets = [PolymarketMarket.from_api_dict(m) for m in raw_pages]
    all_markets = [
        m for m in all_markets
        if m.yes_token_id and m.volume >= min_volume
        and not any(kw in m.question.lower() for kw in _EXCLUDE_KEYWORDS)
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
            condition_id=m.condition_id,
        )
        for m in all_markets
    ]

    # Merge wallet-derived markets — markets where our watched wallets traded
    # recently. These MUST be subscribed regardless of volume filters.
    try:
        from trading_platform.polymarket.market_universe import MarketUniverse as _MU
        _u = _MU()
        if _u.load_cached():
            wallet_cids = _u.get_wallet_derived_markets(days_back=14)
            existing_cids = {li.condition_id for li in live_infos if li.condition_id}
            new_cids = wallet_cids - existing_cids
            print(f"Wallet-derived markets: {len(wallet_cids)} total, {len(new_cids)} new")

            added = 0
            # 2026-07-06: 300 → 500 — wallet-derived markets are where
            # watched whales actually trade, i.e. exactly the markets the
            # fast lane exists for.
            for cid in list(new_cids)[:500]:  # cap extra subscriptions
                entry = _u._by_condition.get(cid)
                if entry and entry.get("token_ids"):
                    live_infos.append(LiveMarketInfo(
                        market_id=cid,
                        question=entry.get("question") or cid[:20],
                        yes_token_id=entry["token_ids"][0],
                        volume=entry.get("volume", 0),
                        end_date_iso=entry.get("end_date_iso", ""),
                        condition_id=cid,
                    ))
                    added += 1
            print(f"Added {added} wallet-derived markets to subscription")
    except Exception as exc:
        print(f"[WARN] Wallet-derived merge failed: {exc}")

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

    # Initialize whale detection pipeline
    tripwire = None
    signal_engine = None
    paper_executor = None
    try:
        from trading_platform.polymarket.market_universe import MarketUniverse
        from trading_platform.polymarket.whale_tripwire import WhaleTripwire
        from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor

        universe = MarketUniverse()
        if not universe.load_cached():
            print("Refreshing market universe...")
            universe.refresh(max_per_category=25)

        tripwire = WhaleTripwire(universe=universe)
        signal_engine = WhaleSignalEngine()
        paper_executor = PolymarketPaperExecutor()
        print(f"  whale detection: ENABLED (tier1={len(tripwire.watched_tier1)}, tier2={len(tripwire.watched_tier2)})")
    except Exception as exc:
        print(f"  whale detection: DISABLED ({exc})")

    collector = PolymarketLiveCollector(
        config, live_infos,
        whale_tripwire=tripwire,
        signal_engine=signal_engine,
        paper_executor=paper_executor,
    )
    asyncio.run(collector.run())
