"""
CLI command: trading-cli data kalshi historical-ingest

Downloads all resolved Kalshi markets from the past year via the public
historical API (no auth required), builds trade parquets, computes feature
parquets (including base rate and optional Metaculus features), and writes
a run manifest.

Usage
-----
    trading-cli data kalshi historical-ingest
    trading-cli data kalshi historical-ingest --config configs/kalshi.yaml
    trading-cli data kalshi historical-ingest --lookback-days 180
    trading-cli data kalshi historical-ingest --no-base-rate
    trading-cli data kalshi historical-ingest --metaculus --sleep 0.3
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def cmd_kalshi_historical_ingest(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.auth import KalshiConfig, LIVE_BASE_URL
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.historical_ingest import HistoricalIngestConfig, HistoricalIngestPipeline

    # Load YAML config for defaults; CLI flags override
    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    hist_cfg = cfg_raw.get("historical_ingest", {})

    # Build HistoricalIngestConfig from YAML + CLI overrides
    config = HistoricalIngestConfig(
        raw_markets_dir=getattr(args, "raw_markets_dir", None) or hist_cfg.get("raw_markets_dir", "data/kalshi/raw/markets"),
        raw_trades_dir=getattr(args, "raw_trades_dir", None) or hist_cfg.get("raw_trades_dir", "data/kalshi/raw/trades"),
        trades_parquet_dir=hist_cfg.get("trades_parquet_dir", "data/kalshi/trades"),
        features_dir=getattr(args, "output_dir", None) or hist_cfg.get("features_dir", "data/kalshi/features"),
        resolution_csv_path=hist_cfg.get("resolution_csv_path", "data/kalshi/raw/resolution.csv"),
        manifest_path=hist_cfg.get("manifest_path", "data/kalshi/raw/ingest_manifest.json"),
        lookback_days=int(getattr(args, "lookback_days", None) or hist_cfg.get("lookback_days", 365)),
        feature_period=getattr(args, "period", None) or hist_cfg.get("feature_period", "1h"),
        min_trades=int(hist_cfg.get("min_trades", 5)),
        request_sleep_sec=float(getattr(args, "sleep", None) or hist_cfg.get("request_sleep_sec", 0.2)),
        run_base_rate=not getattr(args, "no_base_rate", False),
        base_rate_db_path=hist_cfg.get("base_rate_db_path", "data/kalshi/base_rates/base_rate_db.json"),
        run_metaculus=getattr(args, "metaculus", False) or hist_cfg.get("run_metaculus", False),
        metaculus_matches_path=hist_cfg.get("metaculus_matches_path", "data/kalshi/metaculus/matches.json"),
        metaculus_min_confidence=float(hist_cfg.get("metaculus_min_confidence", 0.70)),
        market_page_size=200,
        trade_page_size=1000,
        ticker_filter=list(getattr(args, "tickers", None) or []),
    )

    print(f"Kalshi Historical Ingest")
    print(f"  lookback : {config.lookback_days} days")
    print(f"  period   : {config.feature_period}")
    print(f"  sleep    : {config.request_sleep_sec}s/request")
    print(f"  base rate: {'enabled' if config.run_base_rate else 'disabled'}")
    print(f"  metaculus: {'enabled' if config.run_metaculus else 'disabled'}")
    if config.ticker_filter:
        print(f"  tickers  : {', '.join(config.ticker_filter)}")

    # Build a minimal client that can hit public historical endpoints.
    # Historical endpoints don't require auth, but the client still needs
    # a config object.  We pass dummy credentials; they are never used
    # because historical methods use _get_public() which skips auth.
    try:
        kalshi_config = KalshiConfig.from_env()
    except (ValueError, Exception):
        # No credentials configured — still fine for public historical endpoints
        kalshi_config = _build_public_only_config(cfg_raw)

    client = KalshiClient(kalshi_config)

    from trading_platform.kalshi.historical_ingest import HistoricalIngestPipeline
    pipeline = HistoricalIngestPipeline(client, config)

    print("\nStarting download... (this may take several minutes for 365 days of data)")
    result = pipeline.run()

    print(f"\n[DONE] Historical ingest complete.")
    print(f"  Markets downloaded         : {result.markets_downloaded}")
    print(f"  Markets with trades        : {result.markets_with_trades}")
    print(f"  Markets skipped (no trades): {result.markets_skipped_no_trades}")
    print(f"  Markets failed             : {result.markets_failed}")
    print(f"  Total trades               : {result.total_trades}")
    print(f"  Resolution records         : {result.resolution_count}")
    print(f"  Feature files written      : {result.feature_files_written}")
    if result.date_range_start:
        print(f"  Date range                 : {result.date_range_start} → {result.date_range_end}")
    print(f"  Manifest                   : {result.manifest_path}")


def _build_public_only_config(cfg_raw: dict[str, Any]):
    """Build a KalshiConfig with dummy credentials for public-only access."""
    from trading_platform.kalshi.auth import KalshiConfig, LIVE_BASE_URL, DEMO_BASE_URL
    demo = cfg_raw.get("environment", {}).get("demo", True)
    # Use a placeholder private key that will never be called
    # (historical methods use _get_public which skips auth)
    placeholder_pem = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "PLACEHOLDER_NOT_USED_FOR_PUBLIC_ENDPOINTS\n"
        "-----END RSA PRIVATE KEY-----"
    )
    return KalshiConfig(
        api_key_id="public-only",
        private_key_pem=placeholder_pem,
        demo=bool(demo),
    )
