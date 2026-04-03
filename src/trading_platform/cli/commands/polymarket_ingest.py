"""
CLI command: trading-cli data polymarket ingest

Downloads closed/resolved Polymarket markets from the Gamma API for configured
tag slugs, fetches hourly price history from the CLOB API, generates feature
parquets, and writes resolution.csv + ingest_manifest.json.

Usage
-----
    trading-cli data polymarket ingest
    trading-cli data polymarket ingest --config configs/polymarket.yaml
    trading-cli data polymarket ingest --lookback-days 30 --min-volume 500
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


def _project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def _project_relative(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else PROJECT_ROOT / p


def cmd_polymarket_ingest(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.client import PolymarketClient, PolymarketConfig
    from trading_platform.polymarket.historical_ingest import (
        PolymarketIngestConfig,
        PolymarketIngestPipeline,
    )

    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    cfg = cfg_raw  # top-level is flat for polymarket.yaml

    tag_slugs = getattr(args, "tag_slugs", None) or cfg.get("tag_slugs") or [
        "politics", "economics", "science", "world"
    ]
    lookback_days = int(
        getattr(args, "lookback_days", None) or cfg.get("lookback_days", 90)
    )
    min_volume = float(
        getattr(args, "min_volume", None) or cfg.get("min_volume", 1000.0)
    )
    sleep_sec = float(
        getattr(args, "sleep", None) or cfg.get("request_sleep_sec", 0.05)
    )
    sort_newest_raw = getattr(args, "sort_newest_first", None)
    if sort_newest_raw is not None:
        sort_newest_first = bool(sort_newest_raw)
    else:
        sort_newest_first = bool(cfg.get("sort_newest_first", True))

    config = PolymarketIngestConfig(
        tag_slugs=list(tag_slugs),
        lookback_days=lookback_days,
        min_volume=min_volume,
        raw_markets_dir=str(_project_relative(
            getattr(args, "raw_markets_dir", None) or cfg.get("raw_markets_dir", "data/polymarket/raw/markets")
        )),
        raw_prices_dir=str(_project_relative(
            cfg.get("raw_prices_dir", "data/polymarket/raw/prices")
        )),
        features_dir=str(_project_relative(
            getattr(args, "output_dir", None) or cfg.get("features_dir", "data/polymarket/features")
        )),
        resolution_csv_path=str(_project_relative(
            cfg.get("resolution_csv_path", "data/polymarket/resolution.csv")
        )),
        manifest_path=str(_project_relative(
            cfg.get("manifest_path", "data/polymarket/raw/ingest_manifest.json")
        )),
        feature_period=str(cfg.get("feature_period", "1h")),
        sort_newest_first=sort_newest_first,
        max_markets_per_tag=int(cfg.get("max_markets_per_tag", 500)),
    )

    client = PolymarketClient(PolymarketConfig(request_sleep_sec=sleep_sec))

    print("Polymarket Historical Ingest")
    print(f"  tag slugs  : {', '.join(config.tag_slugs)}")
    print(f"  lookback   : {config.lookback_days} days")
    print(f"  min volume : {config.min_volume:.0f}")
    print(f"  sort newest: {config.sort_newest_first}")
    print(f"  sleep      : {sleep_sec}s/request")
    print(f"  raw dir    : {config.raw_markets_dir}")
    print(f"  features   : {config.features_dir}")
    print(f"  resolution : {config.resolution_csv_path}")
    print()

    pipeline = PolymarketIngestPipeline(client, config)
    result = pipeline.run()

    print("[DONE] Polymarket ingest complete.")
    print(f"  Markets fetched           : {result.markets_fetched}")
    print(f"  Markets processed         : {result.markets_processed}")
    print(f"  Markets skipped (volume)  : {result.markets_skipped_volume}")
    print(f"  Markets skipped (no cid)  : {result.markets_skipped_no_condition_id}")
    print(f"  Markets skipped (no price): {result.markets_skipped_no_prices}")
    print(f"  Markets failed            : {result.markets_failed}")
    print(f"  Feature files written     : {result.feature_files_written}")
    print(f"  Resolution records        : {result.resolution_records}")
    if result.date_range_start:
        print(f"  Date range                : {result.date_range_start} → {result.date_range_end}")
    print(f"  Manifest                  : {result.manifest_path}")
    print(f"  Resolution CSV            : {result.resolution_csv_path}")
    if result.tag_breakdown:
        print("  Tag breakdown:")
        for slug, count in result.tag_breakdown.items():
            print(f"    {slug}: {count}")
