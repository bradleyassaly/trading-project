"""
CLI command: trading-cli data kalshi live-candles

Fetches hourly candlestick data for open Kalshi Economics/Politics
markets via the authenticated series endpoint.

Usage
-----
    trading-cli data kalshi live-candles --lookback-days 30
    trading-cli data kalshi live-candles --config configs/kalshi.yaml --loop
"""
from __future__ import annotations

import argparse
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


def cmd_kalshi_live_candles(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.auth import KalshiConfig
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.live_candle_collector import KalshiLiveCandleCollector

    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    hist_cfg = cfg_raw.get("historical_ingest", {})
    series_tickers = list(hist_cfg.get("direct_series_tickers") or [
        "KXCPI", "KXFED", "KXGDP", "KXJOBS", "KXPCE", "KXINFL",
    ])
    lookback_days = int(getattr(args, "lookback_days", None) or 30)
    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/kalshi/live")
    loop_mode = getattr(args, "loop", False)
    sleep_sec = float(hist_cfg.get("authenticated_request_sleep_sec", 0.15))

    # Build authenticated client
    kalshi_config = KalshiConfig.from_mapping(
        cfg_raw.get("auth", {}),
        env_fallback=True,
        demo=bool(cfg_raw.get("environment", {}).get("demo", False)),
        allow_missing=False,
        source_label=f"Kalshi auth ({getattr(args, 'config', None) or 'env'})",
    )
    client = KalshiClient(kalshi_config, authenticated_sleep_sec=sleep_sec)

    collector = KalshiLiveCandleCollector(
        client,
        series_tickers=series_tickers,
        sleep_sec=sleep_sec,
    )

    print("Kalshi Live Candle Collector")
    print(f"  series     : {', '.join(series_tickers)}")
    print(f"  lookback   : {lookback_days} days")
    print(f"  output     : {output_dir}")
    print(f"  loop       : {'yes' if loop_mode else 'no'}")
    print()

    if loop_mode:
        interval = int(getattr(args, "interval", None) or 60)
        collector.run_loop(output_dir, lookback_days=lookback_days, interval_minutes=interval)
    else:
        result = collector.run_once(output_dir, lookback_days=lookback_days)
        print("[DONE] Live candle collection complete.")
        print(f"  Series scanned        : {result.series_scanned}")
        print(f"  Events found          : {result.events_found}")
        print(f"  Markets found         : {result.markets_found}")
        print(f"  Markets with candles  : {result.markets_with_candles}")
        print(f"  Candles fetched       : {result.candles_fetched}")
        print(f"  Feature files written : {result.feature_files_written}")
