"""
Kalshi live-filtered recent ingest pipeline.

This mode is intended for category-specific research where the historical
archive cursor scan is a poor fit. It treats the authenticated live
``/markets`` endpoint as the primary source for recent filtered markets and
optionally augments the dataset with direct historical lookups for explicitly
named older tickers.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.ingest.status import IngestStatusTracker
from trading_platform.kalshi.historical_ingest import (
    KALSHI_INGEST_STAGE_NAMES,
    HistoricalIngestConfig,
    HistoricalIngestPipeline,
    HistoricalIngestResult,
    _market_key,
    _market_time_value,
    _normalise_candlestick_rows,
    _safe_volume,
    _trades_to_dataframe,
)

logger = logging.getLogger(__name__)
DEFAULT_RECENT_EXCLUDED_MARKET_TYPE_PATTERNS = ("CROSSCATEGORY", "SPORTSMULTIGAME", "EXTENDED")


@dataclass
class RecentIngestConfig(HistoricalIngestConfig):
    """Configuration for live-filtered recent ingest plus direct ticker backfill."""

    recent_ingest_enabled: bool = True
    recent_ingest_statuses: list[str] = field(default_factory=lambda: ["settled"])
    recent_ingest_categories: list[str] = field(default_factory=list)
    recent_ingest_limit: int = 200
    preferred_research_ingest_mode: str = "live_recent_filtered"
    direct_historical_tickers: list[str] = field(default_factory=list)
    recent_ingest_series_tickers: list[str] = field(default_factory=list)
    recent_ingest_event_tickers: list[str] = field(default_factory=list)
    economics_series: list[str] = field(default_factory=list)
    politics_series: list[str] = field(default_factory=list)
    exclude_market_type_patterns: list[str] = field(default_factory=lambda: list(DEFAULT_RECENT_EXCLUDED_MARKET_TYPE_PATTERNS))
    disable_market_type_filter: bool = False


class RecentIngestPipeline(HistoricalIngestPipeline):
    """Reuse the existing Kalshi artifact-processing flow with live market discovery."""

    def __init__(self, client: Any, config: RecentIngestConfig | None = None) -> None:
        super().__init__(client, config or RecentIngestConfig())
        self.config: RecentIngestConfig

    def _build_status_tracker(self) -> IngestStatusTracker:
        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return IngestStatusTracker(
            run_id=run_id,
            pipeline_name="kalshi_recent_ingest",
            mode="live_recent_filtered",
            lookback_days=self.config.lookback_days,
            stage_names=KALSHI_INGEST_STAGE_NAMES,
            output_root=Path(self.config.status_artifacts_root) / run_id,
            log_prefix="kalshi_recent_ingest",
        )

    def _build_recent_query_specs(self) -> list[dict[str, str | None]]:
        return self._resolve_recent_query_plan()["query_specs"]

    def _live_fetch_limit(self) -> int | None:
        limit = int(self.config.recent_ingest_limit or 0)
        return limit if limit > 0 else None

    def _live_page_size(self, total_fetched: int) -> int:
        page_size = max(1, int(getattr(self.config, "market_page_size", 200) or 200))
        fetch_limit = self._live_fetch_limit()
        if fetch_limit is None:
            return page_size
        remaining = fetch_limit - total_fetched
        if remaining <= 0:
            return 0
        return min(page_size, remaining)

    def _series_category_lookup(self) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for series in self.config.economics_series:
            normalized = str(series).strip().upper()
            if normalized:
                lookup[normalized] = "Economics"
        for series in self.config.politics_series:
            normalized = str(series).strip().upper()
            if normalized:
                lookup[normalized] = "Politics"
        return lookup

    def _resolve_recent_query_plan(self) -> dict[str, Any]:
        statuses = self.config.recent_ingest_statuses or ["settled"]
        requested_categories = [str(category).strip() for category in self.config.recent_ingest_categories if str(category).strip()]
        raw_series_values = self.config.recent_ingest_series_tickers or []
        raw_event_values = self.config.recent_ingest_event_tickers or []
        specs: list[dict[str, str | None]] = []
        seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
        conflicts: list[dict[str, Any]] = []
        inferred_series_categories: dict[str, str] = {}
        category_ignored_for_series = 0
        requested_category_set = {category.lower() for category in requested_categories}
        series_lookup = self._series_category_lookup()
        normalized_series_values = [
            normalized
            for series in raw_series_values
            if series is not None and (normalized := str(series).strip())
        ]
        normalized_event_values = [
            normalized
            for event in raw_event_values
            if event is not None and (normalized := str(event).strip())
        ] or [None]

        if not normalized_series_values:
            categories = requested_categories or [None]
            for status in statuses:
                normalized_status = str(status).strip() or None
                for category in categories:
                    normalized_category = str(category).strip() if category else None
                    for normalized_event in normalized_event_values:
                        key = (normalized_status, normalized_category, None, normalized_event)
                        if key in seen:
                            continue
                        seen.add(key)
                        specs.append(
                            {
                                "status": normalized_status,
                                "category": normalized_category,
                                "series_ticker": None,
                                "event_ticker": normalized_event,
                            }
                        )
            return {
                "query_specs": specs,
                "mode": "category_only" if requested_categories else "unfiltered",
                "requested_categories": requested_categories,
                "requested_series_tickers": [],
                "inferred_series_categories": inferred_series_categories,
                "filter_conflicts": conflicts,
                "category_ignored_for_series_count": category_ignored_for_series,
            }

        for series in normalized_series_values:
            inferred_category = series_lookup.get(series.upper())
            if inferred_category:
                inferred_series_categories[series] = inferred_category
            if requested_categories:
                if inferred_category and inferred_category.lower() not in requested_category_set:
                    conflicts.append(
                        {
                            "series_ticker": series,
                            "requested_categories": requested_categories,
                            "inferred_category": inferred_category,
                            "resolution": "ignored_requested_category_used_inferred_category",
                        }
                    )
                    category_ignored_for_series += 1
                    effective_categories: list[str | None] = [inferred_category]
                elif inferred_category and inferred_category.lower() in requested_category_set:
                    effective_categories = [inferred_category]
                else:
                    conflicts.append(
                        {
                            "series_ticker": series,
                            "requested_categories": requested_categories,
                            "inferred_category": None,
                            "resolution": "ignored_requested_category_for_unknown_series",
                        }
                    )
                    category_ignored_for_series += 1
                    effective_categories = [None]
            else:
                effective_categories = [inferred_category] if inferred_category else [None]

            for status in statuses:
                normalized_status = str(status).strip() or None
                for effective_category in effective_categories:
                    for normalized_event in normalized_event_values:
                        key = (
                            normalized_status,
                            effective_category,
                            series,
                            normalized_event,
                        )
                        if key in seen:
                            continue
                        seen.add(key)
                        specs.append(
                            {
                                "status": normalized_status,
                                "category": effective_category,
                                "series_ticker": series,
                                "event_ticker": normalized_event,
                            }
                        )

        return {
            "query_specs": specs,
            "mode": "series_driven",
            "requested_categories": requested_categories,
            "requested_series_tickers": normalized_series_values,
            "inferred_series_categories": inferred_series_categories,
            "filter_conflicts": conflicts,
            "category_ignored_for_series_count": category_ignored_for_series,
        }

    def _annotate_market(
        self,
        market: dict[str, Any],
        *,
        source_tier: str,
        source_endpoint: str,
        source_mode: str,
        ingested_at: str,
    ) -> dict[str, Any]:
        annotated = dict(market)
        ticker = str(_market_key(annotated, "ticker", "market_ticker") or "")
        if ticker and not annotated.get("ticker"):
            annotated["ticker"] = ticker
        annotated["source_tier"] = source_tier
        annotated["source_endpoint"] = source_endpoint
        annotated["source_mode"] = source_mode
        annotated["ingested_at"] = ingested_at
        return annotated

    def _missing_core_fields(self, market: dict[str, Any]) -> list[str]:
        missing: list[str] = []
        if not str(_market_key(market, "ticker", "market_ticker") or "").strip():
            missing.append("ticker")
        if not str(market.get("status") or "").strip():
            missing.append("status")
        return missing

    def _market_type_filter_patterns(self) -> list[str]:
        if self.config.disable_market_type_filter:
            return []
        return [
            normalized
            for pattern in self.config.exclude_market_type_patterns
            if pattern is not None and (normalized := str(pattern).strip().upper())
        ]

    def _recent_market_type_filter_reason(self, market: dict[str, Any]) -> str | None:
        patterns = self._market_type_filter_patterns()
        if not patterns:
            return None
        ticker = str(_market_key(market, "ticker", "market_ticker") or "").upper()
        if not ticker:
            return None
        for token in patterns:
            if token in ticker:
                return f"market_type:{token}"
        return None

    def _filter_recent_page_markets(
        self,
        markets: list[dict[str, Any]],
        *,
        start_dt: datetime,
        end_dt: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        retained: list[dict[str, Any]] = []
        diagnostics: dict[str, Any] = {
            "fetched": len(markets),
            "retained": 0,
            "discarded": 0,
            "discarded_by_reason": {
                "category": 0,
                "series_pattern": 0,
                "min_volume": 0,
                "market_type": 0,
                "synthetic": 0,
                "missing_core_fields": 0,
                "lookback": 0,
                "no_trade_data": 0,
            },
            "retained_sample_tickers": [],
            "discarded_samples": [],
        }
        for market in markets:
            ticker = str(_market_key(market, "ticker", "market_ticker") or "<missing>")
            market_volume = _safe_volume(market)
            missing_fields = self._missing_core_fields(market)
            if missing_fields:
                diagnostics["discarded_by_reason"]["missing_core_fields"] += 1
                logger.info(
                    "Excluding Kalshi recent market %s: reason=missing_core_fields volume=%.2f fields=%s",
                    ticker,
                    market_volume,
                    ",".join(missing_fields),
                )
                if len(diagnostics["discarded_samples"]) < 5:
                    diagnostics["discarded_samples"].append(
                        {"ticker": ticker, "reason": "missing_core_fields", "fields": ",".join(missing_fields)}
                    )
                continue
            market_time = _market_time_value(market)
            if market_time is not None and (market_time < start_dt or market_time > end_dt):
                diagnostics["discarded_by_reason"]["lookback"] += 1
                logger.info(
                    "Excluding Kalshi recent market %s: reason=lookback volume=%.2f",
                    ticker,
                    market_volume,
                )
                if len(diagnostics["discarded_samples"]) < 5:
                    diagnostics["discarded_samples"].append({"ticker": ticker, "reason": "lookback"})
                continue
            market_type_reason = self._recent_market_type_filter_reason(market)
            if market_type_reason is not None:
                diagnostics["discarded_by_reason"]["market_type"] += 1
                logger.info(
                    "Excluding Kalshi recent market %s: reason=%s volume=%.2f",
                    ticker,
                    market_type_reason,
                    market_volume,
                )
                if len(diagnostics["discarded_samples"]) < 5:
                    diagnostics["discarded_samples"].append({"ticker": ticker, "reason": market_type_reason})
                continue
            reason = self._early_filter_reason(market)
            if reason is not None:
                diagnostics["discarded_by_reason"][reason] += 1
                logger.info(
                    "Excluding Kalshi recent market %s: reason=%s volume=%.2f",
                    ticker,
                    reason,
                    market_volume,
                )
                if len(diagnostics["discarded_samples"]) < 5:
                    diagnostics["discarded_samples"].append({"ticker": ticker, "reason": reason})
                continue
            retained.append(market)
            if len(diagnostics["retained_sample_tickers"]) < 5:
                diagnostics["retained_sample_tickers"].append(ticker)
        diagnostics["retained"] = len(retained)
        diagnostics["discarded"] = len(markets) - len(retained)
        return retained, diagnostics

    def _download_market_universe(
        self,
        *,
        checkpoint: dict[str, Any],
        start_dt: datetime,
        end_dt: datetime,
        cutoff_ts: dict[str, datetime | None],
        on_retained_market=None,
        tracker: IngestStatusTracker | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        query_plan = self._resolve_recent_query_plan()
        diagnostics: dict[str, Any] = {
            "pages_fetched": 0,
            "total_markets_fetched": 0,
            "total_markets_retained": 0,
            "total_markets_discarded": 0,
            "discarded_by_category": 0,
            "discarded_by_series_pattern": 0,
            "discarded_by_min_volume": 0,
            "discarded_by_market_type": 0,
            "discarded_synthetic": 0,
            "excluded_no_trade_data": 0,
            "excluded_missing_core_fields": 0,
            "excluded_by_series": 0,
            "excluded_by_market_type": 0,
            "excluded_by_lookback": 0,
            "retained_sample_tickers": [],
            "last_cursor": None,
            "pages_with_retained_markets": 0,
            "pages_without_retained_markets": 0,
            "pagination_stop_reason": None,
            "first_retained_processing_ticker": None,
            "first_retained_processing_source": None,
            "first_retained_processing_page": None,
            "live_query_specs": query_plan["query_specs"],
            "filter_resolution": {
                "mode": query_plan["mode"],
                "requested_categories": query_plan["requested_categories"],
                "requested_series_tickers": query_plan["requested_series_tickers"],
                "inferred_series_categories": query_plan["inferred_series_categories"],
                "filter_conflicts": query_plan["filter_conflicts"],
                "category_ignored_for_series_count": query_plan["category_ignored_for_series_count"],
            },
            "direct_historical_tickers_requested": list(cfg.direct_historical_tickers),
            "source_mode_counts": {
                "live_recent_filtered": 0,
                "direct_historical_ticker": 0,
                "archive_scan": 0,
            },
            "direct_historical_tickers_fetched": 0,
            "live_recent_limit": cfg.recent_ingest_limit,
            "market_type_filter_enabled": not cfg.disable_market_type_filter and bool(self._market_type_filter_patterns()),
            "market_type_filter_patterns": self._market_type_filter_patterns(),
        }
        if checkpoint.get("market_download_complete"):
            diagnostics["pagination_stop_reason"] = "checkpoint_complete"
            return diagnostics

        logger.info(
            "Kalshi recent-ingest market-type filter %s patterns=%s",
            "enabled" if diagnostics["market_type_filter_enabled"] else "disabled",
            diagnostics["market_type_filter_patterns"] or [],
        )

        self._update_checkpoint_progress(
            checkpoint,
            current_stage="market_universe_fetch",
            stage_counters={
                "recent_query_specs": diagnostics["live_query_specs"],
                "direct_historical_tickers_requested": list(cfg.direct_historical_tickers),
            },
        )

        ingested_at = end_dt.isoformat()
        total_live_markets_fetched = 0
        limit_reached = False
        for spec_index, spec in enumerate(diagnostics["live_query_specs"], start=1):
            if not cfg.recent_ingest_enabled:
                break
            cursor: str | None = None
            while True:
                page_request_limit = self._live_page_size(total_live_markets_fetched)
                if page_request_limit == 0:
                    diagnostics["pagination_stop_reason"] = "recent_limit_reached"
                    limit_reached = True
                    break
                raw_markets, next_cursor = self.client.get_markets_raw(
                    status=spec["status"],
                    category=spec["category"],
                    series_ticker=spec["series_ticker"],
                    event_ticker=spec["event_ticker"],
                    limit=page_request_limit,
                    cursor=cursor,
                )
                page_markets_raw = raw_markets[:page_request_limit]
                page_markets = [
                    self._annotate_market(
                        market,
                        source_tier="live",
                        source_endpoint="/markets",
                        source_mode="live_recent_filtered",
                        ingested_at=ingested_at,
                    )
                    for market in page_markets_raw
                ]
                retained_markets, page_diagnostics = self._filter_recent_page_markets(
                    page_markets,
                    start_dt=start_dt,
                    end_dt=end_dt,
                )
                page_unique_retained = 0
                for market in retained_markets:
                    if not self._enqueue_retained_market(checkpoint, market):
                        continue
                    page_unique_retained += 1
                    diagnostics["total_markets_retained"] += 1
                    diagnostics["source_mode_counts"]["live_recent_filtered"] += 1
                    if len(diagnostics["retained_sample_tickers"]) < 5:
                        diagnostics["retained_sample_tickers"].append(str(market.get("ticker", "")))
                    self._update_checkpoint_progress(
                        checkpoint,
                        current_stage="market_universe_fetch",
                        stage_counters={
                            "last_recent_query": spec,
                            "last_recent_query_index": spec_index,
                            "pages_fetched": diagnostics["pages_fetched"] + 1,
                            "total_markets_retained": diagnostics["total_markets_retained"],
                        },
                    )
                    self._save_checkpoint(
                        checkpoint,
                        tracker=tracker,
                        message=f"recent market queued: {market.get('ticker', '')}",
                    )
                    if on_retained_market is not None:
                        if diagnostics["first_retained_processing_ticker"] is None:
                            diagnostics["first_retained_processing_ticker"] = market.get("ticker", "")
                            diagnostics["first_retained_processing_source"] = "live_recent_filtered"
                            diagnostics["first_retained_processing_page"] = diagnostics["pages_fetched"] + 1
                        on_retained_market(market)

                total_live_markets_fetched += len(page_markets)
                diagnostics["pages_fetched"] += 1
                diagnostics["total_markets_fetched"] += len(page_markets)
                diagnostics["total_markets_discarded"] += page_diagnostics["discarded"]
                diagnostics["discarded_by_category"] += page_diagnostics["discarded_by_reason"]["category"]
                diagnostics["discarded_by_series_pattern"] += page_diagnostics["discarded_by_reason"]["series_pattern"]
                diagnostics["discarded_by_min_volume"] += page_diagnostics["discarded_by_reason"]["min_volume"]
                diagnostics["discarded_by_market_type"] += page_diagnostics["discarded_by_reason"]["market_type"]
                diagnostics["discarded_synthetic"] += page_diagnostics["discarded_by_reason"]["synthetic"]
                diagnostics["excluded_missing_core_fields"] = diagnostics.get("excluded_missing_core_fields", 0) + page_diagnostics["discarded_by_reason"]["missing_core_fields"]
                diagnostics["excluded_by_lookback"] = diagnostics.get("excluded_by_lookback", 0) + page_diagnostics["discarded_by_reason"]["lookback"]
                diagnostics["excluded_no_trade_data"] = diagnostics.get("excluded_no_trade_data", 0) + page_diagnostics["discarded_by_reason"]["no_trade_data"]
                diagnostics["excluded_by_series"] = diagnostics.get("excluded_by_series", 0) + page_diagnostics["discarded_by_reason"]["series_pattern"]
                diagnostics["excluded_by_market_type"] = diagnostics.get("excluded_by_market_type", 0) + page_diagnostics["discarded_by_reason"]["market_type"]
                diagnostics["last_cursor"] = next_cursor
                live_fetch_limit = self._live_fetch_limit()
                if live_fetch_limit is not None and total_live_markets_fetched >= live_fetch_limit:
                    diagnostics["pagination_stop_reason"] = "recent_limit_reached"
                    limit_reached = True
                logger.info(
                    "Kalshi recent-ingest page=%s records=%s total_fetched=%s stop_reason=%s",
                    diagnostics["pages_fetched"],
                    len(page_markets),
                    total_live_markets_fetched,
                    diagnostics["pagination_stop_reason"] or "continuing",
                )
                self._log_market_fetch_progress(
                    source="live_recent_filtered",
                    page_number=diagnostics["pages_fetched"],
                    cursor=next_cursor,
                    page_diagnostics={
                        **page_diagnostics,
                        "retained": page_unique_retained,
                        "fetched": len(page_markets),
                        "query_spec": spec,
                        "page_records": len(page_markets),
                        "total_fetched_so_far": total_live_markets_fetched,
                        "stop_reason": diagnostics["pagination_stop_reason"] or "continuing",
                    },
                    total_fetched=diagnostics["total_markets_fetched"],
                    total_retained=diagnostics["total_markets_retained"],
                    progress_log_interval_pages=1,
                    retained_sample=diagnostics["retained_sample_tickers"],
                    tracker=tracker,
                    pages_with_retained_markets=diagnostics["pages_with_retained_markets"],
                    pages_without_retained_markets=diagnostics["pages_without_retained_markets"],
                )
                if page_unique_retained > 0:
                    diagnostics["pages_with_retained_markets"] += 1
                else:
                    diagnostics["pages_without_retained_markets"] += 1
                if limit_reached:
                    break
                if not next_cursor:
                    break
                cursor = next_cursor
            if limit_reached:
                break

        for ticker in cfg.direct_historical_tickers:
            normalized_ticker = str(ticker).strip()
            if not normalized_ticker:
                continue
            market = self.client.get_historical_market(normalized_ticker)
            annotated_market = self._annotate_market(
                market,
                source_tier="historical",
                source_endpoint=f"/historical/markets/{normalized_ticker}",
                source_mode="direct_historical_ticker",
                ingested_at=ingested_at,
            )
            diagnostics["direct_historical_tickers_fetched"] += 1
            diagnostics["total_markets_fetched"] += 1
            retained_markets, page_diagnostics = self._filter_recent_page_markets(
                [annotated_market],
                start_dt=start_dt,
                end_dt=end_dt,
            )
            diagnostics["total_markets_discarded"] += page_diagnostics["discarded"]
            diagnostics["discarded_by_category"] += page_diagnostics["discarded_by_reason"]["category"]
            diagnostics["discarded_by_series_pattern"] += page_diagnostics["discarded_by_reason"]["series_pattern"]
            diagnostics["discarded_by_min_volume"] += page_diagnostics["discarded_by_reason"]["min_volume"]
            diagnostics["discarded_by_market_type"] += page_diagnostics["discarded_by_reason"]["market_type"]
            diagnostics["discarded_synthetic"] += page_diagnostics["discarded_by_reason"]["synthetic"]
            diagnostics["excluded_missing_core_fields"] = diagnostics.get("excluded_missing_core_fields", 0) + page_diagnostics["discarded_by_reason"]["missing_core_fields"]
            diagnostics["excluded_by_lookback"] = diagnostics.get("excluded_by_lookback", 0) + page_diagnostics["discarded_by_reason"]["lookback"]
            diagnostics["excluded_no_trade_data"] = diagnostics.get("excluded_no_trade_data", 0) + page_diagnostics["discarded_by_reason"]["no_trade_data"]
            diagnostics["excluded_by_series"] = diagnostics.get("excluded_by_series", 0) + page_diagnostics["discarded_by_reason"]["series_pattern"]
            diagnostics["excluded_by_market_type"] = diagnostics.get("excluded_by_market_type", 0) + page_diagnostics["discarded_by_reason"]["market_type"]
            if not retained_markets:
                continue
            if not self._enqueue_retained_market(checkpoint, retained_markets[0]):
                continue
            diagnostics["total_markets_retained"] += 1
            diagnostics["source_mode_counts"]["direct_historical_ticker"] += 1
            if len(diagnostics["retained_sample_tickers"]) < 5:
                diagnostics["retained_sample_tickers"].append(normalized_ticker)
            self._update_checkpoint_progress(
                checkpoint,
                current_stage="market_universe_fetch",
                stage_counters={
                    "last_direct_historical_ticker": normalized_ticker,
                    "total_markets_retained": diagnostics["total_markets_retained"],
                },
            )
            self._save_checkpoint(
                checkpoint,
                tracker=tracker,
                message=f"direct historical market queued: {normalized_ticker}",
            )
            if on_retained_market is not None:
                if diagnostics["first_retained_processing_ticker"] is None:
                    diagnostics["first_retained_processing_ticker"] = normalized_ticker
                    diagnostics["first_retained_processing_source"] = "direct_historical_ticker"
                    diagnostics["first_retained_processing_page"] = diagnostics["pages_fetched"]
                on_retained_market(retained_markets[0])

        checkpoint["market_download_complete"] = True
        checkpoint["live_market_cursor"] = None
        diagnostics["pagination_stop_reason"] = diagnostics["pagination_stop_reason"] or (
            "cursor_exhausted" if cfg.recent_ingest_enabled else "direct_historical_complete"
        )
        diagnostics["zero_results_due_to_filter_conflicts"] = bool(
            diagnostics["total_markets_retained"] == 0 and diagnostics["filter_resolution"]["filter_conflicts"]
        )
        if diagnostics["total_markets_fetched"] > 0 and diagnostics["excluded_by_market_type"] == diagnostics["total_markets_fetched"]:
            logger.warning(
                "Kalshi recent-ingest market-type filter removed all fetched markets: fetched=%s excluded_by_market_type=%s patterns=%s",
                diagnostics["total_markets_fetched"],
                diagnostics["excluded_by_market_type"],
                diagnostics["market_type_filter_patterns"],
            )
        self._update_checkpoint_progress(
            checkpoint,
            current_stage="market_universe_fetch",
            last_completed_stage="market_universe_fetch",
            pagination_stop_reason=diagnostics["pagination_stop_reason"],
            stage_counters={
                "pages_fetched": diagnostics["pages_fetched"],
                "source_mode_counts": diagnostics["source_mode_counts"],
                "filter_resolution": diagnostics["filter_resolution"],
                "zero_results_due_to_filter_conflicts": diagnostics["zero_results_due_to_filter_conflicts"],
            },
        )
        self._save_checkpoint(checkpoint, tracker=tracker, message="recent market universe download complete")
        return diagnostics

    def _process_market_artifacts(self, market: dict[str, Any], **kwargs: Any) -> str:
        from trading_platform.kalshi.features import build_kalshi_features, write_feature_parquet

        checkpoint = kwargs["checkpoint"]
        cutoff_ts = kwargs["cutoff_ts"]
        start_dt = kwargs["start_dt"]
        end_dt = kwargs["end_dt"]
        raw_trades_dir: Path = kwargs["raw_trades_dir"]
        raw_candles_dir: Path = kwargs["raw_candles_dir"]
        trades_parquet_dir: Path = kwargs["trades_parquet_dir"]
        normalized_candles_dir: Path = kwargs["normalized_candles_dir"]
        features_dir: Path = kwargs["features_dir"]
        processed_tickers: set[str] = kwargs["processed_tickers"]
        skipped_or_failed_tickers: list[dict[str, Any]] = kwargs["skipped_or_failed_tickers"]
        all_close_times: list[datetime] = kwargs["all_close_times"]
        metrics: dict[str, int] = kwargs["metrics"]
        tracker: IngestStatusTracker | None = kwargs.get("tracker")

        cfg = self.config
        ticker = market.get("ticker", "")
        if not ticker:
            return "skipped"
        if ticker in processed_tickers:
            logger.info("Skipping already-processed market %s from checkpoint.", ticker)
            self._remove_pending_market(checkpoint, ticker)
            return "skipped"

        self._update_checkpoint_progress(
            checkpoint,
            current_stage="retained_market_processing",
            current_market_ticker=ticker,
        )
        self._save_checkpoint(checkpoint, tracker=tracker, message=f"started retained market processing: {ticker}")
        if tracker is not None:
            retained_started = tracker.run.retained_markets_started + 1
            tracker.update_stage(
                "retained_market_processing",
                item_count_completed=metrics["markets_with_trades"],
                item_count_failed=metrics["markets_failed"],
                message=f"starting market {ticker}",
                counters={"active_ticker": ticker},
                run_counters={
                    "retained_markets_started": retained_started,
                    "processed_ticker_count": len(processed_tickers),
                },
                log_line=self._log_stage_progress(
                    "retained_market_processing",
                    "running",
                    started=retained_started,
                    completed=metrics["markets_with_trades"],
                    failed=metrics["markets_failed"],
                    raw_written=tracker.run.raw_market_files_written,
                    normalized_written=tracker.run.normalized_outputs_written,
                    ticker=ticker,
                ),
            )
        if not self._processing_started_logged:
            logger.info("Kalshi retained-market processing started with %s.", ticker)
            self._processing_started_logged = True

        self._write_raw_market(market)
        if tracker is not None:
            tracker.increment_run_counters(raw_market_files_written=1)

        close_time = _market_time_value(market)
        if close_time is not None:
            all_close_times.append(close_time)

        try:
            raw_trades = self._fetch_market_trades(market, cutoff_ts)
        except Exception as exc:
            logger.warning("Failed to fetch trades for %s: %s", ticker, exc)
            metrics["markets_failed"] += 1
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trades", "error": str(exc)})
            self._remove_pending_market(checkpoint, ticker)
            self._update_checkpoint_progress(checkpoint, current_stage="retained_market_processing", current_market_ticker=None)
            self._save_checkpoint(checkpoint, tracker=tracker, message=f"failed retained market processing: {ticker}")
            return "failed"

        trades_df = _trades_to_dataframe(raw_trades)
        (raw_trades_dir / f"{ticker}.json").write_text(json.dumps(raw_trades, indent=2, default=str), encoding="utf-8")
        try:
            trades_df.write_parquet(trades_parquet_dir / f"{ticker}.parquet")
            if tracker is not None:
                tracker.increment_run_counters(normalized_outputs_written=1)
        except Exception as exc:
            logger.warning("Trade parquet write failed for %s: %s", ticker, exc)
            metrics["markets_failed"] += 1
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trade_parquet", "error": str(exc)})
            self._remove_pending_market(checkpoint, ticker)
            self._update_checkpoint_progress(checkpoint, current_stage="retained_market_processing", current_market_ticker=None)
            self._save_checkpoint(checkpoint, tracker=tracker, message=f"failed retained market processing: {ticker}")
            return "failed"

        try:
            raw_candles = self._fetch_market_candles(
                market,
                cutoff_ts,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        except Exception as exc:
            logger.warning("Candlestick fetch failed for %s: %s", ticker, exc)
            raw_candles = []
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "candles", "error": str(exc)})

        (raw_candles_dir / f"{ticker}.json").write_text(json.dumps(raw_candles, indent=2, default=str), encoding="utf-8")
        candles_df = _normalise_candlestick_rows(raw_candles)
        try:
            candles_df.write_parquet(normalized_candles_dir / f"{ticker}.parquet")
            metrics["total_candlesticks"] += len(candles_df)
            if tracker is not None:
                tracker.increment_run_counters(normalized_outputs_written=1)
        except Exception as exc:
            logger.warning("Candle parquet write failed for %s: %s", ticker, exc)
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "candle_parquet", "error": str(exc)})

        if len(raw_trades) > 0:
            metrics["markets_with_trades"] += 1
            metrics["total_trades"] += len(raw_trades)
            if len(raw_trades) < cfg.min_trades:
                logger.info(
                    "Skipping feature generation for %s: only %d trades (min %d required for signal-quality features).",
                    ticker,
                    len(raw_trades),
                    cfg.min_trades,
                )
                skipped_or_failed_tickers.append(
                    {"ticker": ticker, "stage": "feature_min_trades", "trade_count": len(raw_trades), "min_trades": cfg.min_trades}
                )
            else:
                try:
                    yes_prices = trades_df.get_column("yes_price").drop_nulls()
                    last_yes_price = float(yes_prices[-1]) if len(yes_prices) > 0 else 50.0
                    if last_yes_price <= 1.0:
                        last_yes_price *= 100.0
                    extra_features = self._compute_extra_features(market, last_yes_price)
                    feat_df = build_kalshi_features(
                        trades_df,
                        ticker=ticker,
                        period=cfg.feature_period,
                        close_time=close_time,
                        timestamp_col="traded_at",
                        price_col="yes_price",
                        count_col="count",
                        extra_scalar_features=extra_features if extra_features else None,
                        market_context={
                            "title": market.get("title"),
                            "series_ticker": market.get("series_ticker"),
                            "base_rate_db_path": cfg.base_rate_db_path,
                            "side_col": "side",
                        },
                    )
                    write_feature_parquet(feat_df, features_dir, ticker)
                    metrics["feature_files_written"] += 1
                    if tracker is not None:
                        tracker.increment_run_counters(normalized_outputs_written=1)
                except Exception as exc:
                    logger.warning("Feature build failed for %s: %s", ticker, exc)
                    skipped_or_failed_tickers.append({"ticker": ticker, "stage": "features", "error": str(exc)})
        else:
            logger.info("No trade history for %s; retaining market-level artifacts without feature generation.", ticker)

        processed_tickers.add(ticker)
        checkpoint["processed_tickers"] = sorted(processed_tickers)
        checkpoint["failed_tickers"].pop(ticker, None)
        self._remove_pending_market(checkpoint, ticker)
        self._update_checkpoint_progress(
            checkpoint,
            current_stage="retained_market_processing",
            last_completed_stage="retained_market_processing",
            current_market_ticker=None,
        )
        self._save_checkpoint(checkpoint, tracker=tracker, message=f"processed ticker checkpointed: {ticker}")
        if tracker is not None:
            tracker.update_stage(
                "retained_market_processing",
                item_count_completed=metrics["markets_with_trades"],
                item_count_failed=metrics["markets_failed"],
                message=f"completed market {ticker}",
                counters={
                    "last_completed_ticker": ticker,
                    "markets_skipped": metrics["markets_skipped"],
                    "total_trades": metrics["total_trades"],
                    "total_candlesticks": metrics["total_candlesticks"],
                    "feature_files_written": metrics["feature_files_written"],
                },
                run_counters={
                    "markets_completed": len(processed_tickers),
                    "processed_ticker_count": len(processed_tickers),
                    "markets_failed": metrics["markets_failed"],
                },
            )
        return "processed"

    def _build_run_summary(self, **kwargs: Any) -> dict[str, Any]:
        summary = super()._build_run_summary(**kwargs)
        summary.setdefault("filter_diagnostics", {})
        summary["filter_diagnostics"]["excluded_by_market_type"] = summary.get("page_diagnostics_summary", {}).get("excluded_by_market_type", 0)
        summary["recent_ingest"] = {
            "enabled": self.config.recent_ingest_enabled,
            "statuses": list(self.config.recent_ingest_statuses),
            "categories": list(self.config.recent_ingest_categories),
            "series_tickers": list(self.config.recent_ingest_series_tickers),
            "event_tickers": list(self.config.recent_ingest_event_tickers),
            "economics_series": list(self.config.economics_series),
            "politics_series": list(self.config.politics_series),
            "limit": self.config.recent_ingest_limit,
            "min_volume": self.config.min_volume,
            "market_type_filter_enabled": not self.config.disable_market_type_filter and bool(self._market_type_filter_patterns()),
            "exclude_market_type_patterns": list(self.config.exclude_market_type_patterns),
            "preferred_research_ingest_mode": self.config.preferred_research_ingest_mode,
            "direct_historical_tickers": list(self.config.direct_historical_tickers),
            "filter_resolution": summary.get("page_diagnostics_summary", {}).get("filter_resolution", {}),
            "zero_results_due_to_filter_conflicts": summary.get("page_diagnostics_summary", {}).get("zero_results_due_to_filter_conflicts", False),
        }
        return summary

    def run(self) -> HistoricalIngestResult:
        return self._run_streaming()
