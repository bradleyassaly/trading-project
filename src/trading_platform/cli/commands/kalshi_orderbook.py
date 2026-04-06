"""
CLI: trading-cli data kalshi snapshot-orderbooks
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def cmd_kalshi_snapshot_orderbooks(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.auth import KalshiConfig
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.orderbook_collector import KalshiOrderBookCollector

    # Load config
    config_path = getattr(args, "config", None) or "configs/kalshi.yaml"
    cfg_raw: dict[str, Any] = {}
    try:
        cfg_raw = _load_yaml(config_path)
    except Exception as exc:
        print(f"[WARN] Could not load config {config_path}: {exc}")

    # Build authenticated client
    kalshi_config = KalshiConfig.from_mapping(
        cfg_raw.get("auth", {}),
        env_fallback=True,
        demo=bool(cfg_raw.get("environment", {}).get("demo", False)),
        allow_missing=False,
        source_label=f"Kalshi auth ({config_path})",
    )
    sleep_sec = float(cfg_raw.get("historical_ingest", {}).get("authenticated_request_sleep_sec", 0.15))
    client = KalshiClient(kalshi_config, authenticated_sleep_sec=sleep_sec)
    collector = KalshiOrderBookCollector(client)

    # Determine tickers
    tickers_arg = getattr(args, "tickers", None)
    if tickers_arg:
        tickers = [t.strip() for t in tickers_arg.split(",") if t.strip()]
    else:
        # Load from config — need to discover actual market tickers from series
        series = list(cfg_raw.get("historical_ingest", {}).get("direct_series_tickers") or [])
        if series:
            print(f"Loading open markets for {len(series)} series...")
            tickers = []
            for s in series:
                try:
                    events, _ = client.get_events_raw(series_ticker=s, status="open", limit=200)
                    for ev in events:
                        et = ev.get("event_ticker") or ev.get("ticker") or ""
                        if not et:
                            continue
                        markets, _ = client.get_markets_raw(event_ticker=et, status="open", limit=200)
                        for m in markets:
                            t = m.get("ticker", "")
                            if t:
                                tickers.append(t)
                except Exception as exc:
                    logger.debug("Failed to load tickers for %s: %s", s, exc)
        else:
            print("[ERROR] No tickers specified and no series in config")
            return

    if not tickers:
        print("[ERROR] No tickers found")
        return

    loop = getattr(args, "loop", False)
    interval = int(getattr(args, "interval", None) or 300)

    print(f"Kalshi Orderbook Snapshots")
    print(f"  Tickers: {len(tickers)}")
    print()

    if loop:
        print(f"Running every {interval}s. Ctrl+C to stop.")
        while True:
            results = collector.snapshot_all(tickers)
            taken = sum(1 for df in results.values() if not df.empty)
            print(f"  [{time.strftime('%H:%M:%S')}] {taken}/{len(tickers)} snapshots taken")
            time.sleep(interval)
    else:
        results = collector.snapshot_all(tickers)
        for ticker, df in results.items():
            if df.empty:
                print(f"  {ticker}: no orderbook data")
                continue
            features = collector.compute_book_features(ticker)
            if features:
                print(f"  {ticker}: spread={features.get('spread', 0):.4f} "
                      f"depth_imbalance={features.get('depth_imbalance', 0):.4f} "
                      f"yes_bid={features.get('best_yes_bid', 0):.2f} "
                      f"no_bid={features.get('best_no_bid', 0):.2f}")
            else:
                yes_count = len(df[df["side"] == "YES"]) if "side" in df.columns else 0
                no_count = len(df[df["side"] == "NO"]) if "side" in df.columns else 0
                print(f"  {ticker}: {yes_count} YES levels, {no_count} NO levels (need 2+ snapshots for features)")
        print(f"\nDone. {sum(1 for df in results.values() if not df.empty)}/{len(tickers)} orderbooks fetched.")
