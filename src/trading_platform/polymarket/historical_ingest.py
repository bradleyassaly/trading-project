"""
Polymarket historical ingest pipeline.

Fetches closed+resolved markets from the Gamma API for configured tag slugs,
downloads price history from the CLOB API, generates feature parquets, and
writes resolution.csv + ingest_manifest.json.

Usage
-----
    from trading_platform.polymarket.client import PolymarketClient
    from trading_platform.polymarket.historical_ingest import (
        PolymarketIngestConfig, PolymarketIngestPipeline,
    )
    client = PolymarketClient()
    config = PolymarketIngestConfig()
    result = PolymarketIngestPipeline(client, config).run()
"""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PolymarketIngestConfig:
    # ── Market discovery ──────────────────────────────────────────────────────
    tag_slugs: list[str] = field(
        default_factory=lambda: ["politics", "economics", "science", "world"]
    )
    lookback_days: int = 90
    min_volume: float = 1000.0
    sort_newest_first: bool = True
    max_markets_per_tag: int = 500

    # ── Output paths ──────────────────────────────────────────────────────────
    raw_markets_dir: str = "data/polymarket/raw/markets"
    raw_prices_dir: str = "data/polymarket/raw/prices"
    features_dir: str = "data/polymarket/features"
    resolution_csv_path: str = "data/polymarket/resolution.csv"
    manifest_path: str = "data/polymarket/raw/ingest_manifest.json"

    # ── Feature generation ────────────────────────────────────────────────────
    feature_period: str = "1h"


@dataclass
class PolymarketIngestResult:
    markets_fetched: int = 0
    markets_skipped_volume: int = 0
    markets_skipped_no_condition_id: int = 0
    markets_skipped_no_prices: int = 0
    markets_failed: int = 0
    markets_processed: int = 0
    feature_files_written: int = 0
    resolution_records: int = 0
    tag_breakdown: dict[str, int] = field(default_factory=dict)
    date_range_start: str | None = None
    date_range_end: str | None = None
    manifest_path: str = ""
    resolution_csv_path: str = ""


class PolymarketIngestPipeline:
    """
    Download closed/resolved Polymarket markets, fetch price history, and
    produce feature parquets + resolution.csv.
    """

    def __init__(self, client: Any, config: PolymarketIngestConfig) -> None:
        self.client = client
        self.config = config

    def run(self) -> PolymarketIngestResult:
        cfg = self.config
        result = PolymarketIngestResult()

        # ── Prepare output directories ─────────────────────────────────────
        raw_markets_dir = Path(cfg.raw_markets_dir)
        raw_prices_dir = Path(cfg.raw_prices_dir)
        features_dir = Path(cfg.features_dir)
        resolution_csv_path = Path(cfg.resolution_csv_path)
        manifest_path = Path(cfg.manifest_path)

        for d in (raw_markets_dir, raw_prices_dir, features_dir,
                  resolution_csv_path.parent, manifest_path.parent):
            d.mkdir(parents=True, exist_ok=True)

        # ── Fetch markets across all tag slugs ─────────────────────────────
        cutoff_ts: float = 0.0
        end_date_min: str | None = None
        if cfg.lookback_days > 0:
            from datetime import timedelta
            cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(days=cfg.lookback_days)
            cutoff_ts = cutoff_dt.timestamp()
            # When sort_newest_first is enabled, use the Gamma API's
            # end_date_min filter to skip old markets server-side.
            # We intentionally avoid order/ascending here because those
            # params break tag_slug filtering on the Gamma API.
            if cfg.sort_newest_first:
                end_date_min = cutoff_dt.strftime("%Y-%m-%d")

        seen_ids: set[str] = set()
        all_markets: list[dict[str, Any]] = []

        for slug in cfg.tag_slugs:
            logger.info("Fetching closed markets for tag_slug=%s", slug)
            offset = 0
            count_for_slug = 0
            while True:
                try:
                    page, next_offset = self.client.get_markets(
                        tag_slug=slug, closed=True, limit=100, offset=offset,
                        end_date_min=end_date_min,
                    )
                except Exception as exc:
                    logger.warning("Failed to fetch markets for tag_slug=%s: %s", slug, exc)
                    break

                if not page:
                    break

                for m in page:
                    market_id = str(m.get("id", ""))
                    if not market_id or market_id in seen_ids:
                        continue
                    seen_ids.add(market_id)
                    all_markets.append(m)
                    count_for_slug += 1

                if cfg.max_markets_per_tag > 0 and count_for_slug >= cfg.max_markets_per_tag:
                    logger.info(
                        "  tag_slug=%s: hit max_markets_per_tag=%d — stopping",
                        slug, cfg.max_markets_per_tag,
                    )
                    break

                if next_offset is None:
                    break
                offset = next_offset

            result.tag_breakdown[slug] = count_for_slug
            logger.info("  tag_slug=%s: %d unique markets", slug, count_for_slug)

        result.markets_fetched = len(all_markets)
        logger.info("Total unique markets fetched: %d", result.markets_fetched)

        # ── Process each market ────────────────────────────────────────────
        from trading_platform.polymarket.models import PolymarketMarket
        from trading_platform.polymarket.features import PolymarketFeatureGenerator

        feature_gen = PolymarketFeatureGenerator(features_dir)
        resolution_rows: list[dict[str, Any]] = []
        end_dates: list[datetime] = []

        skipped_unresolved = 0
        skipped_no_resolution_price = 0
        logged_sample = 0

        for raw in all_markets:
            market = PolymarketMarket.from_api_dict(raw)

            # Volume filter
            if cfg.min_volume > 0 and market.volume < cfg.min_volume:
                result.markets_skipped_volume += 1
                continue

            # Must be resolved with a clear Yes/No outcome
            if not market.resolved:
                skipped_unresolved += 1
                continue
            if market.resolution_price is None:
                skipped_no_resolution_price += 1
                continue

            if logged_sample < 3:
                logger.info(
                    "  sample market: id=%s resolved=%s winner=%s price=%s q=%s",
                    market.id, market.resolved, market.winner_outcome,
                    market.resolution_price, market.question[:80],
                )
                logged_sample += 1

            # Lookback filter on end_date_iso
            if cutoff_ts > 0 and market.end_date_iso:
                try:
                    end_str = market.end_date_iso
                    if end_str.endswith("Z"):
                        end_str = end_str[:-1] + "+00:00"
                    end_dt = datetime.fromisoformat(end_str)
                    if end_dt.timestamp() < cutoff_ts:
                        continue
                except (ValueError, AttributeError):
                    pass

            # Save raw market JSON
            raw_path = raw_markets_dir / f"{market.id}.json"
            raw_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")

            # Fetch price history
            if not market.yes_token_id:
                logger.warning("Market %s has no clobTokenId — skipping price history", market.id)
                result.markets_skipped_no_condition_id += 1
                continue

            try:
                price_history = self.client.get_price_history(market.yes_token_id)
            except Exception as exc:
                logger.warning("Failed to fetch price history for %s: %s", market.id, exc)
                result.markets_skipped_no_prices += 1
                continue

            if not price_history:
                logger.warning("Empty price history for market %s — skipping", market.id)
                result.markets_skipped_no_prices += 1
                continue

            # Save raw price history
            prices_path = raw_prices_dir / f"{market.id}.json"
            prices_path.write_text(json.dumps(price_history, indent=2), encoding="utf-8")

            # Generate features
            try:
                feature_gen.generate_and_write(market, price_history, period=cfg.feature_period)
                result.feature_files_written += 1
            except Exception as exc:
                logger.warning("Feature generation failed for %s: %s", market.id, exc)
                result.markets_failed += 1
                continue

            # Collect resolution row
            resolution_rows.append({
                "ticker": market.id,
                "resolution_price": market.resolution_price,
                "resolves_yes": market.resolution_price == 100.0,
                "question": market.question,
                "volume": market.volume,
                "end_date_iso": market.end_date_iso or "",
            })
            result.markets_processed += 1

            if market.end_date_iso:
                try:
                    end_str = market.end_date_iso
                    if end_str.endswith("Z"):
                        end_str = end_str[:-1] + "+00:00"
                    end_dates.append(datetime.fromisoformat(end_str))
                except (ValueError, AttributeError):
                    pass

        result.resolution_records = len(resolution_rows)
        logger.info(
            "Processing summary: volume_skip=%d unresolved=%d no_resolution_price=%d "
            "no_cid=%d no_prices=%d failed=%d processed=%d",
            result.markets_skipped_volume, skipped_unresolved,
            skipped_no_resolution_price, result.markets_skipped_no_condition_id,
            result.markets_skipped_no_prices, result.markets_failed,
            result.markets_processed,
        )

        # ── Write resolution.csv ───────────────────────────────────────────
        if resolution_rows:
            fieldnames = ["ticker", "resolution_price", "resolves_yes", "question",
                          "volume", "end_date_iso"]
            with resolution_csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(resolution_rows)

        result.resolution_csv_path = str(resolution_csv_path)

        if end_dates:
            result.date_range_start = min(end_dates).isoformat()
            result.date_range_end = max(end_dates).isoformat()

        # ── Write manifest ─────────────────────────────────────────────────
        manifest: dict[str, Any] = {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "markets_fetched": result.markets_fetched,
            "markets_processed": result.markets_processed,
            "markets_failed": result.markets_failed,
            "markets_skipped_volume": result.markets_skipped_volume,
            "markets_skipped_no_condition_id": result.markets_skipped_no_condition_id,
            "markets_skipped_no_prices": result.markets_skipped_no_prices,
            "feature_files_written": result.feature_files_written,
            "resolution_records": result.resolution_records,
            "tag_breakdown": result.tag_breakdown,
            "tag_slugs": cfg.tag_slugs,
            "lookback_days": cfg.lookback_days,
            "min_volume": cfg.min_volume,
            "date_range_start": result.date_range_start,
            "date_range_end": result.date_range_end,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        result.manifest_path = str(manifest_path)

        return result
