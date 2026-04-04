"""
Kalshi historical data ingestion pipeline.

The ingest is intentionally split into:

    data/kalshi/raw/...         source-of-truth API payloads
    data/kalshi/normalized/...  reproducible normalized research inputs
    data/kalshi/features/...    derived feature artifacts

The pipeline is cutoff-aware. It fetches archived settled markets through
``/historical/*`` endpoints and combines them with post-cutoff settled data
still available on the live endpoints so a lookback window remains complete
as Kalshi advances the historical boundary.
"""
from __future__ import annotations

import csv
import json
import logging
import random
import re
import time
from collections.abc import Callable
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import requests
from trading_platform.ingest.status import IngestStatusTracker

logger = logging.getLogger(__name__)

KALSHI_INGEST_STAGE_NAMES = [
    "initialization",
    "checkpoint_load",
    "cutoff_discovery",
    "market_universe_fetch",
    "retained_market_processing",
    "normalization",
    "checkpoint_write",
    "final_summary",
]
KALSHI_RESUME_RECOVERY_MODES = {"automatic", "backup_only", "cursor_reset_only", "fail_fast"}


class KalshiIngestFailFastError(RuntimeError):
    """Raised when ingest stops deliberately to avoid unbounded live-bridge churn."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class KalshiResumeCursorRecoveryError(RuntimeError):
    """Raised when a saved resume cursor cannot be recovered safely."""

    def __init__(
        self,
        *,
        endpoint: str,
        cursor: str,
        attempts: int,
        recovery_actions: list[str],
        last_http_status: int | None,
        message: str,
    ) -> None:
        super().__init__(message)
        self.endpoint = endpoint
        self.cursor = cursor
        self.attempts = attempts
        self.recovery_actions = recovery_actions
        self.last_http_status = last_http_status

# ── Resolution helpers ────────────────────────────────────────────────────────

def _result_to_price(result: str | None) -> float | None:
    """Convert Kalshi result string to a resolution price (0 or 100)."""
    if result == "yes":
        return 100.0
    if result == "no":
        return 0.0
    return None


def _resolution_price_from_market(raw_market: dict[str, Any]) -> float | None:
    result_price = _result_to_price(raw_market.get("result"))
    if result_price is not None:
        return result_price
    for price_field in ("settlement_value_dollars", "resolution_price", "yes_settlement_value_dollars"):
        value = raw_market.get(price_field)
        if value is None:
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed <= 1.0:
            parsed *= 100.0
        return parsed
    return None


def _parse_trade_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert one raw historical trade dict to a normalised row."""
    yes_price_raw = raw.get("yes_price_dollars") or raw.get("yes_price")
    no_price_raw = raw.get("no_price_dollars") or raw.get("no_price")
    try:
        yes_price = float(yes_price_raw) if yes_price_raw is not None else None
    except (TypeError, ValueError):
        yes_price = None
    try:
        no_price = float(no_price_raw) if no_price_raw is not None else None
    except (TypeError, ValueError):
        no_price = None

    ts_str = raw.get("created_time", "")
    traded_at: datetime | None = None
    if ts_str:
        try:
            traded_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except ValueError:
            traded_at = None

    count_raw = raw.get("count_fp", raw.get("count", 0))
    try:
        count = float(count_raw)
    except (TypeError, ValueError):
        count = 0.0

    return {
        "trade_id": raw.get("trade_id", ""),
        "ticker": raw.get("ticker", ""),
        "side": raw.get("taker_side", raw.get("side", "")),
        "yes_price": yes_price,
        "no_price": no_price,
        "count": count,
        "traded_at": traded_at,
    }


def _trades_to_dataframe(raw_trades: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert raw trade dicts to a Polars DataFrame suitable for ``build_kalshi_features``."""
    if not raw_trades:
        return pl.DataFrame(schema={
            "trade_id": pl.Utf8,
            "ticker": pl.Utf8,
            "side": pl.Utf8,
            "yes_price": pl.Float64,
            "no_price": pl.Float64,
            "count": pl.Float64,
            "traded_at": pl.Datetime,
        })
    rows = [_parse_trade_row(t) for t in raw_trades]
    return pl.from_dicts(rows, schema_overrides={
        "yes_price": pl.Float64,
        "no_price": pl.Float64,
        "count": pl.Float64,
        "traded_at": pl.Datetime(time_zone="UTC"),
    })


def _parse_iso_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _market_close_time_in_range(
    market: dict[str, Any],
    *,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
) -> bool:
    close_time = _market_time_value(market)
    if close_time is None:
        return False
    close_ts = int(close_time.timestamp())
    if min_close_ts is not None and close_ts < min_close_ts:
        return False
    if max_close_ts is not None and close_ts > max_close_ts:
        return False
    return True


def _is_synthetic_ticker(ticker: str | None) -> bool:
    return str(ticker or "").startswith("SYNTH-")


def _safe_volume(market: dict[str, Any]) -> float:
    """Return the market volume as a float; 0.0 when missing or non-numeric."""
    v = market.get("volume")
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _market_key(market: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in market and market.get(key) is not None:
            return market.get(key)
    return None


def _market_time_raw(market: dict[str, Any]) -> Any:
    return _market_key(
        market,
        "close_time",
        "close_date",
        "expiration_time",
        "expiration_ts",
        "end_date",
    )


def _market_time_value(market: dict[str, Any]) -> datetime | None:
    return _parse_iso_ts(_market_time_raw(market))


def _normalise_market_row(raw_market: dict[str, Any]) -> dict[str, Any]:
    resolution_price = _resolution_price_from_market(raw_market)
    mapped_time = _market_time_raw(raw_market)
    return {
        "ticker": _market_key(raw_market, "ticker", "market_ticker") or "",
        "title": raw_market.get("title", ""),
        "subtitle": raw_market.get("subtitle"),
        "series_ticker": _market_key(raw_market, "series_ticker", "seriesTicker"),
        "event_ticker": _market_key(raw_market, "event_ticker", "eventTicker"),
        "status": raw_market.get("status"),
        "category": _market_key(raw_market, "category", "market_category"),
        "close_time": mapped_time,
        "expiration_time": _market_key(raw_market, "expiration_time", "expiration_ts"),
        "settlement_time": _market_key(raw_market, "settlement_time", "settled_time"),
        "result": raw_market.get("result"),
        "resolution_price": resolution_price,
        "settlement_value_dollars": raw_market.get("settlement_value_dollars"),
        "yes_bid": _market_key(raw_market, "yes_bid_dollars", "yes_bid"),
        "yes_ask": _market_key(raw_market, "yes_ask_dollars", "yes_ask"),
        "no_bid": _market_key(raw_market, "no_bid_dollars", "no_bid"),
        "no_ask": _market_key(raw_market, "no_ask_dollars", "no_ask"),
        "last_price": _market_key(raw_market, "last_price_dollars", "last_price"),
        "yes_price": _market_key(raw_market, "yes_price_dollars", "yes_price"),
        "no_price": _market_key(raw_market, "no_price_dollars", "no_price"),
        "volume": raw_market.get("volume"),
        "open_interest": raw_market.get("open_interest"),
        "liquidity_dollars": raw_market.get("liquidity_dollars") or raw_market.get("liquidity"),
        "source_endpoint": raw_market.get("source_endpoint"),
        "source_mode": raw_market.get("source_mode"),
        "source_tier": raw_market.get("source_tier"),
        "ingested_at": raw_market.get("ingested_at"),
    }


def _normalise_candlestick_rows(raw_candles: list[dict[str, Any]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for raw in raw_candles:
        timestamp = (
            _parse_iso_ts(raw.get("end_period_ts"))
            or _parse_iso_ts(raw.get("end_ts"))
            or _parse_iso_ts(raw.get("period_end"))
            or _parse_iso_ts(raw.get("timestamp"))
            or _parse_iso_ts(raw.get("start_period_ts"))
        )
        if timestamp is None:
            continue

        def _float_from(*keys: str) -> float | None:
            for key in keys:
                value = raw.get(key)
                if value is None:
                    continue
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    continue
                if parsed <= 1.0:
                    parsed *= 100.0
                return parsed
            return None

        open_price = _float_from("open_price_dollars", "open_price", "open")
        high_price = _float_from("high_price_dollars", "high_price", "high")
        low_price = _float_from("low_price_dollars", "low_price", "low")
        close_price = _float_from("close_price_dollars", "close_price", "close")
        if open_price is None or high_price is None or low_price is None or close_price is None:
            continue

        volume_value = raw.get("volume")
        if volume_value is None:
            volume_value = raw.get("count")
        if volume_value is None:
            volume_value = raw.get("count_fp")
        try:
            volume = float(volume_value) if volume_value is not None else 0.0
        except (TypeError, ValueError):
            volume = 0.0

        rows.append(
            {
                "timestamp": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "dollar_volume": close_price * volume,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime(time_zone="UTC"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "dollar_volume": pl.Float64,
            }
        )
    return pl.from_dicts(rows, schema_overrides={"timestamp": pl.Datetime(time_zone="UTC")}).sort("timestamp")


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class HistoricalIngestConfig:
    """Configuration for the historical ingest pipeline."""

    # Output directories
    raw_markets_dir: str = "data/kalshi/raw/markets"
    raw_trades_dir: str = "data/kalshi/raw/trades"
    raw_candles_dir: str = "data/kalshi/raw/candles"
    trades_parquet_dir: str = "data/kalshi/normalized/trades"
    normalized_candles_dir: str = "data/kalshi/normalized/candles"
    normalized_markets_path: str = "data/kalshi/normalized/markets.parquet"
    features_dir: str = "data/kalshi/features/real"
    resolution_csv_path: str = "data/kalshi/normalized/resolution.csv"
    legacy_resolution_csv_path: str = "data/kalshi/resolution.csv"
    manifest_path: str = "data/kalshi/raw/ingest_manifest.json"
    checkpoint_path: str = "data/kalshi/raw/ingest_checkpoint.json"
    summary_path: str = "data/kalshi/raw/ingest_summary.json"
    status_artifacts_root: str = "artifacts/kalshi_ingest"
    checkpoint_backup_path: str | None = None

    # Lookback window
    lookback_days: int = 365

    # Feature generation
    feature_period: str = "1h"
    min_trades: int = 5    # skip markets with fewer trades than this
    candle_period_interval: int = 60  # minutes: 1, 60, or 1440

    # Rate limiting
    request_sleep_sec: float = 0.05   # 20 req/sec
    authenticated_request_sleep_sec: float = 0.072
    authenticated_rate_limit_max_retries: int = 5
    authenticated_rate_limit_backoff_base_sec: float = 0.5
    authenticated_rate_limit_backoff_max_sec: float = 8.0
    authenticated_rate_limit_jitter_max_sec: float = 0.25
    max_live_pages_without_retained_markets: int = 25
    max_raw_markets_without_processing: int = 2000
    resume_cursor_max_retries: int = 3
    resume_cursor_backoff_base_sec: float = 1.0
    resume_cursor_backoff_max_sec: float = 10.0
    resume_cursor_jitter_max_sec: float = 0.5
    resume_recovery_mode: str = "automatic"

    # Optional signal enrichment
    run_base_rate: bool = True
    base_rate_db_path: str = "data/kalshi/base_rates/base_rate_db.json"

    run_metaculus: bool = False    # requires pre-built matches file
    metaculus_matches_path: str = "data/kalshi/metaculus/matches.json"
    metaculus_min_confidence: float = 0.70

    # Pagination
    market_page_size: int = 1000
    trade_page_size: int = 1000

    # Optional ticker filter (empty = all)
    ticker_filter: list[str] = field(default_factory=list)
    resume: bool = True
    resume_mode: str = "latest"
    resume_checkpoint_path: str | None = None

    # ── Market filtering ──────────────────────────────────────────────────────
    # Defaults are all "disabled" (empty/zero) so the pipeline runs without
    # any filtering unless explicitly configured via YAML or CLI.

    # Regex patterns matched against series_ticker (or ticker when series is absent).
    # Any market whose series matches at least one pattern is excluded.
    # Example: ["KXBTC", "KXETH"] removes Bitcoin and Ethereum price-bracket series.
    excluded_series_patterns: list[str] = field(default_factory=list)

    # Skip any event_ticker that appears more than this many times.
    # Bracket markets (e.g. BTC at every $250 increment) produce hundreds of
    # markets per event; setting this to 5 removes them.  0 = disabled.
    max_markets_per_event: int = 0

    # Skip markets whose total traded volume is below this threshold.
    # Illiquid / untraded markets add noise to the feature store.  0 = disabled.
    min_volume: float = 0.0

    # Allowlist of category strings (case-insensitive).  When non-empty only
    # markets in these categories are processed.  Example: ["Economics", "Politics"].
    preferred_categories: list[str] = field(default_factory=list)

    # When True (default) and preferred_categories is non-empty, use the
    # /events endpoint to discover matching events by category, then fetch
    # markets per event via /markets?event_ticker=X.  This replaces the
    # cursor-based /markets?status=settled live bridge which does NOT filter
    # by category server-side.  Set to False to revert to the old blind
    # pagination approach.
    use_events_for_category_filter: bool = True

    # When True, skip /historical/markets pagination entirely.
    # Use alongside use_events_for_category_filter=True so the pipeline
    # discovers markets only via /events?category=X, with no historical
    # archive scan.
    skip_historical_pagination: bool = True

    # When True (default), use the live /markets endpoint with category/series
    # filters (and individual /historical/markets/{ticker} lookups) instead of
    # blind pagination through /historical/markets.  Falls back to paginated
    # scanning (capped at 50 pages) if fewer than 20 markets are returned.
    use_direct_series_fetch: bool = True
    direct_series_tickers: list[str] = field(default_factory=list)

    # Specific market tickers to fetch individually via /historical/markets/{ticker}.
    # Used for older markets that predate the live API's settled-market window.
    # Example: ["KXINFL-25DEC", "KXFED-25DEC"]
    known_tickers: list[str] = field(default_factory=list)


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class HistoricalIngestResult:
    """Summary of a completed historical ingest run."""
    markets_downloaded: int
    markets_with_trades: int
    markets_skipped_no_trades: int
    markets_failed: int
    total_trades: int
    total_candlesticks: int
    resolution_count: int
    date_range_start: str | None
    date_range_end: str | None
    feature_files_written: int
    normalized_markets_written: int
    manifest_path: Path
    summary_path: Path
    status_artifact_path: Path | None = None
    run_summary_artifact_path: Path | None = None


# ── Pipeline ──────────────────────────────────────────────────────────────────

class HistoricalIngestPipeline:
    """
    Downloads Kalshi resolved market history and builds a local research dataset.

    :param client:  :class:`~trading_platform.kalshi.client.KalshiClient` instance.
    :param config:  :class:`HistoricalIngestConfig` controlling paths and options.
    """

    def __init__(self, client: Any, config: HistoricalIngestConfig | None = None) -> None:
        self.client = client
        self.config = config or HistoricalIngestConfig()
        self._processing_started_logged = False
        self._checkpoint_write_count = 0
        self._loaded_backup_checkpoint: dict[str, Any] | None = None

        # Lazy-load signal helpers
        self._base_rate_signal: Any = None
        self._metaculus_signal: Any = None

    def _build_status_tracker(self) -> IngestStatusTracker:
        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return IngestStatusTracker(
            run_id=run_id,
            pipeline_name="kalshi_historical_ingest",
            mode="historical_streaming",
            lookback_days=self.config.lookback_days,
            stage_names=KALSHI_INGEST_STAGE_NAMES,
            output_root=Path(self.config.status_artifacts_root) / run_id,
            log_prefix="kalshi_historical_ingest",
        )

    def _checkpoint_file_path(self) -> Path:
        if self.config.resume_checkpoint_path:
            return Path(self.config.resume_checkpoint_path)
        return Path(self.config.checkpoint_path)

    def _checkpoint_backup_file_path(self) -> Path:
        if self.config.checkpoint_backup_path:
            return Path(self.config.checkpoint_backup_path)
        checkpoint_path = self._checkpoint_file_path()
        return checkpoint_path.with_name(f"{checkpoint_path.stem}.bak{checkpoint_path.suffix}")

    @staticmethod
    def _default_checkpoint_state() -> dict[str, Any]:
        return {
            "schema_version": 2,
            "run_id": None,
            "resumed_from_run_id": None,
            "last_completed_stage": None,
            "current_stage": None,
            "historical_market_cursor": None,
            "live_market_cursor": None,
            "market_download_complete": False,
            "processed_tickers": [],
            "pending_retained_markets": [],
            "failed_tickers": {},
            "stage_counters": {},
            "resume_counters": {
                "replayed_work_skipped": 0,
                "replayed_work_replayed": 0,
            },
            "resume_cursor_retry_count": 0,
            "resume_cursor_last_http_status": None,
            "resume_recovery_action": None,
            "resumed_from_backup_checkpoint": False,
            "resumed_with_cursor_reset": False,
            "backup_checkpoint_recovery_attempted": False,
            "cursor_reset_recovery_attempted": False,
            "pagination_stop_reason": None,
            "current_market_ticker": None,
            "updated_at": None,
        }

    def _normalise_checkpoint(self, checkpoint: dict[str, Any]) -> dict[str, Any]:
        normalized = self._default_checkpoint_state()
        normalized.update(checkpoint)
        normalized["processed_tickers"] = sorted({str(ticker) for ticker in checkpoint.get("processed_tickers", [])})
        removed_pending_count = 0
        pending = []
        seen_pending: set[str] = set()
        for market in checkpoint.get("pending_retained_markets", []):
            if not isinstance(market, dict):
                removed_pending_count += 1
                continue
            ticker = str(_market_key(market, "ticker", "market_ticker") or "")
            if not ticker or ticker in normalized["processed_tickers"] or ticker in seen_pending:
                removed_pending_count += 1
                continue
            seen_pending.add(ticker)
            pending.append(market)
        normalized["pending_retained_markets"] = pending
        failed_tickers = checkpoint.get("failed_tickers", {})
        normalized["failed_tickers"] = failed_tickers if isinstance(failed_tickers, dict) else {}
        resume_counters = checkpoint.get("resume_counters", {})
        if not isinstance(resume_counters, dict):
            resume_counters = {}
        normalized["resume_counters"] = {
            "replayed_work_skipped": int(resume_counters.get("replayed_work_skipped", 0) or 0) + removed_pending_count,
            "replayed_work_replayed": int(resume_counters.get("replayed_work_replayed", 0) or 0),
        }
        normalized["resume_cursor_retry_count"] = int(checkpoint.get("resume_cursor_retry_count", 0) or 0)
        normalized["resume_cursor_last_http_status"] = checkpoint.get("resume_cursor_last_http_status")
        normalized["resume_recovery_action"] = checkpoint.get("resume_recovery_action")
        normalized["resumed_from_backup_checkpoint"] = bool(checkpoint.get("resumed_from_backup_checkpoint", False))
        normalized["resumed_with_cursor_reset"] = bool(checkpoint.get("resumed_with_cursor_reset", False))
        normalized["backup_checkpoint_recovery_attempted"] = bool(checkpoint.get("backup_checkpoint_recovery_attempted", False))
        normalized["cursor_reset_recovery_attempted"] = bool(checkpoint.get("cursor_reset_recovery_attempted", False))
        stage_counters = checkpoint.get("stage_counters", {})
        normalized["stage_counters"] = stage_counters if isinstance(stage_counters, dict) else {}
        return normalized

    def _update_checkpoint_progress(
        self,
        checkpoint: dict[str, Any],
        *,
        current_stage: str | None = None,
        last_completed_stage: str | None = None,
        current_market_ticker: str | None = None,
        pagination_stop_reason: str | None = None,
        stage_counters: dict[str, Any] | None = None,
    ) -> None:
        if current_stage is not None:
            checkpoint["current_stage"] = current_stage
        if last_completed_stage is not None:
            checkpoint["last_completed_stage"] = last_completed_stage
        if current_market_ticker is not None or current_market_ticker is None:
            checkpoint["current_market_ticker"] = current_market_ticker
        if pagination_stop_reason is not None:
            checkpoint["pagination_stop_reason"] = pagination_stop_reason
        if stage_counters:
            checkpoint["stage_counters"].update(stage_counters)
        checkpoint["updated_at"] = datetime.now(UTC).isoformat()

    @staticmethod
    def _market_exists_in_pending(checkpoint: dict[str, Any], ticker: str) -> bool:
        return any(str(_market_key(market, "ticker", "market_ticker") or "") == ticker for market in checkpoint.get("pending_retained_markets", []))

    def _enqueue_retained_market(self, checkpoint: dict[str, Any], market: dict[str, Any]) -> bool:
        ticker = str(_market_key(market, "ticker", "market_ticker") or "")
        if not ticker:
            return False
        if ticker in checkpoint.get("processed_tickers", []) or self._market_exists_in_pending(checkpoint, ticker):
            return False
        checkpoint["pending_retained_markets"].append(market)
        return True

    def _remove_pending_market(self, checkpoint: dict[str, Any], ticker: str) -> None:
        checkpoint["pending_retained_markets"] = [
            market for market in checkpoint.get("pending_retained_markets", [])
            if str(_market_key(market, "ticker", "market_ticker") or "") != ticker
        ]

    @staticmethod
    def _http_status_from_exception(exc: Exception) -> int | None:
        response = getattr(exc, "response", None)
        if response is None:
            return None
        return getattr(response, "status_code", None)

    def _is_retryable_resume_cursor_error(self, exc: Exception) -> bool:
        if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
            return True
        status_code = self._http_status_from_exception(exc)
        return status_code in {502, 503, 504}

    def _validate_resume_recovery_mode(self) -> str:
        mode = str(self.config.resume_recovery_mode or "automatic").strip().lower()
        if mode not in KALSHI_RESUME_RECOVERY_MODES:
            raise ValueError(
                f"Unsupported resume_recovery_mode={self.config.resume_recovery_mode!r}; "
                f"expected one of {sorted(KALSHI_RESUME_RECOVERY_MODES)}"
            )
        return mode

    def _resume_cursor_delay(self, attempt: int) -> float:
        exponential_delay = min(
            self.config.resume_cursor_backoff_base_sec * (2 ** max(attempt - 1, 0)),
            self.config.resume_cursor_backoff_max_sec,
        )
        return exponential_delay + random.uniform(0.0, self.config.resume_cursor_jitter_max_sec)

    def _merge_checkpoints_for_resume(
        self,
        primary: dict[str, Any],
        backup: dict[str, Any],
    ) -> dict[str, Any]:
        merged = self._normalise_checkpoint(backup)
        merged["processed_tickers"] = sorted(
            set(merged.get("processed_tickers", [])) | set(primary.get("processed_tickers", []))
        )
        merged_pending = list(merged.get("pending_retained_markets", []))
        merged_seen = {
            str(_market_key(market, "ticker", "market_ticker") or "")
            for market in merged_pending
        }
        for market in primary.get("pending_retained_markets", []):
            ticker = str(_market_key(market, "ticker", "market_ticker") or "")
            if not ticker or ticker in merged["processed_tickers"] or ticker in merged_seen:
                continue
            merged_pending.append(market)
            merged_seen.add(ticker)
        merged["pending_retained_markets"] = merged_pending
        merged_failed = dict(merged.get("failed_tickers", {}))
        for ticker, details in primary.get("failed_tickers", {}).items():
            merged_failed[ticker] = details
        merged["failed_tickers"] = merged_failed
        merged["resume_counters"] = {
            "replayed_work_skipped": max(
                int(primary.get("resume_counters", {}).get("replayed_work_skipped", 0) or 0),
                int(merged.get("resume_counters", {}).get("replayed_work_skipped", 0) or 0),
            ),
            "replayed_work_replayed": max(
                int(primary.get("resume_counters", {}).get("replayed_work_replayed", 0) or 0),
                int(merged.get("resume_counters", {}).get("replayed_work_replayed", 0) or 0),
            ),
        }
        return merged

    def _load_backup_checkpoint(self) -> dict[str, Any] | None:
        if self._loaded_backup_checkpoint is not None:
            return self._normalise_checkpoint(self._loaded_backup_checkpoint)
        backup_path = self._checkpoint_backup_file_path()
        if not backup_path.exists():
            return None
        try:
            return self._normalise_checkpoint(json.loads(backup_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load backup checkpoint %s: %s", backup_path, exc)
            return None

    def _fetch_live_page_with_resume_recovery(
        self,
        *,
        checkpoint: dict[str, Any],
        tracker: IngestStatusTracker | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        resume_recovery_mode = self._validate_resume_recovery_mode()
        cursor = checkpoint.get("live_market_cursor")
        is_resume_cursor = bool(cursor)
        attempts = 0
        last_http_status: int | None = None
        recovery_actions: list[str] = []
        while True:
            try:
                raw_markets, next_cursor = self.client.get_markets_raw(
                    status="settled",
                    limit=200,
                    cursor=cursor,
                )
                checkpoint["resume_cursor_retry_count"] = attempts
                if last_http_status is not None:
                    checkpoint["resume_cursor_last_http_status"] = last_http_status
                if tracker is not None:
                    tracker.set_run_counters(
                        resume_cursor=cursor,
                        resume_cursor_retry_count=attempts,
                        resume_cursor_last_http_status=checkpoint.get("resume_cursor_last_http_status"),
                        resume_recovery_action=recovery_actions[-1] if recovery_actions else None,
                        resumed_from_backup_checkpoint=bool(checkpoint.get("resumed_from_backup_checkpoint", False)),
                        resumed_with_cursor_reset=bool(checkpoint.get("resumed_with_cursor_reset", False)),
                        backup_checkpoint_recovery_attempted=bool(checkpoint.get("backup_checkpoint_recovery_attempted", False)),
                        cursor_reset_recovery_attempted=bool(checkpoint.get("cursor_reset_recovery_attempted", False)),
                    )
                return raw_markets, next_cursor
            except Exception as exc:
                if not is_resume_cursor or not self._is_retryable_resume_cursor_error(exc):
                    raise
                attempts += 1
                last_http_status = self._http_status_from_exception(exc)
                checkpoint["resume_cursor_retry_count"] = attempts
                checkpoint["resume_cursor_last_http_status"] = last_http_status
                if tracker is not None:
                    tracker.set_run_counters(
                        resume_cursor=cursor,
                        resume_cursor_retry_count=attempts,
                        resume_cursor_last_http_status=last_http_status,
                        backup_checkpoint_recovery_attempted=bool(checkpoint.get("backup_checkpoint_recovery_attempted", False)),
                        cursor_reset_recovery_attempted=bool(checkpoint.get("cursor_reset_recovery_attempted", False)),
                    )
                if attempts <= self.config.resume_cursor_max_retries:
                    delay = self._resume_cursor_delay(attempts)
                    logger.warning(
                        "Kalshi live resume cursor fetch failed for /markets cursor=%s status=%s; retrying in %.2fs (attempt %d/%d).",
                        cursor,
                        last_http_status or "timeout",
                        delay,
                        attempts,
                        self.config.resume_cursor_max_retries,
                    )
                    time.sleep(delay)
                    continue
                backup_allowed = resume_recovery_mode in {"automatic", "backup_only"}
                cursor_reset_allowed = resume_recovery_mode in {"automatic", "cursor_reset_only"}
                if backup_allowed:
                    checkpoint["backup_checkpoint_recovery_attempted"] = True
                    backup_checkpoint = self._load_backup_checkpoint()
                    if backup_checkpoint is not None and backup_checkpoint.get("live_market_cursor") != cursor:
                        merged = self._merge_checkpoints_for_resume(checkpoint, backup_checkpoint)
                        merged["resumed_from_backup_checkpoint"] = True
                        merged["resume_recovery_action"] = "backup_checkpoint_fallback"
                        merged["resume_cursor_retry_count"] = attempts
                        merged["resume_cursor_last_http_status"] = last_http_status
                        checkpoint.clear()
                        checkpoint.update(merged)
                        cursor = checkpoint.get("live_market_cursor")
                        is_resume_cursor = bool(cursor)
                        recovery_actions.append("backup_checkpoint_fallback")
                        self._save_checkpoint(checkpoint, tracker=tracker, message="resume cursor recovered via backup checkpoint")
                        if tracker is not None:
                            tracker.set_run_counters(
                                resume_cursor=cursor,
                                resume_cursor_retry_count=attempts,
                                resume_cursor_last_http_status=last_http_status,
                                resume_recovery_action="backup_checkpoint_fallback",
                                resumed_from_backup_checkpoint=True,
                                backup_checkpoint_recovery_attempted=True,
                            )
                        attempts = 0
                        continue
                    recovery_actions.append(
                        "backup_checkpoint_unavailable_or_same_cursor"
                        if backup_checkpoint is None or backup_checkpoint.get("live_market_cursor") == cursor
                        else "backup_checkpoint_attempted"
                    )
                if cursor_reset_allowed:
                    checkpoint["cursor_reset_recovery_attempted"] = True
                    checkpoint["live_market_cursor"] = None
                    checkpoint["resumed_with_cursor_reset"] = True
                    checkpoint["resume_recovery_action"] = "cursor_reset"
                    checkpoint["resume_cursor_retry_count"] = attempts
                    checkpoint["resume_cursor_last_http_status"] = last_http_status
                    recovery_actions.append("cursor_reset")
                    self._save_checkpoint(checkpoint, tracker=tracker, message="resume cursor cleared after repeated failures")
                    if tracker is not None:
                        tracker.set_run_counters(
                            resume_cursor=None,
                            resume_cursor_retry_count=attempts,
                            resume_cursor_last_http_status=last_http_status,
                            resume_recovery_action="cursor_reset",
                            resumed_with_cursor_reset=True,
                            backup_checkpoint_recovery_attempted=bool(checkpoint.get("backup_checkpoint_recovery_attempted", False)),
                            cursor_reset_recovery_attempted=True,
                        )
                    cursor = None
                    is_resume_cursor = False
                    continue
                recovery_actions_text = ", ".join(recovery_actions) if recovery_actions else "none"
                raise KalshiResumeCursorRecoveryError(
                    endpoint="/markets?status=settled",
                    cursor=str(cursor),
                    attempts=attempts,
                    recovery_actions=recovery_actions,
                    last_http_status=last_http_status,
                    message=(
                        "Kalshi resume cursor recovery failed for endpoint /markets?status=settled "
                        f"cursor={cursor} after {attempts} attempts; last_http_status={last_http_status}; "
                        f"recovery_actions_attempted={recovery_actions_text}"
                    ),
                ) from exc

    @staticmethod
    def _log_stage_progress(stage_name: str, status: str, **fields: Any) -> str:
        parts = [f"[kalshi_historical_ingest] stage={stage_name} status={status}"]
        for key, value in fields.items():
            if value is None:
                continue
            parts.append(f"{key}={value}")
        return " ".join(parts)

    def _update_checkpoint_status(
        self,
        tracker: IngestStatusTracker | None,
        checkpoint: dict[str, Any],
        *,
        message: str,
    ) -> None:
        if tracker is None:
            return
        self._checkpoint_write_count += 1
        tracker.update_stage(
            "checkpoint_write",
            current_stage=False,
            item_count_completed=self._checkpoint_write_count,
            message=message,
            counters={
                "checkpoint_path": self.config.checkpoint_path,
                "market_download_complete": bool(checkpoint.get("market_download_complete")),
                "processed_ticker_count": len(checkpoint.get("processed_tickers", [])),
                "historical_market_cursor": checkpoint.get("historical_market_cursor"),
                "live_market_cursor": checkpoint.get("live_market_cursor"),
            },
        )

    @staticmethod
    def _top_error_categories(skipped_or_failed_tickers: list[dict[str, Any]]) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for item in skipped_or_failed_tickers:
            stage = str(item.get("stage") or "unknown")
            counts[stage] += 1
        return dict(counts.most_common(5))

    def _drain_pending_retained_markets(
        self,
        *,
        checkpoint: dict[str, Any],
        tracker: IngestStatusTracker | None,
        cutoff_ts: dict[str, datetime | None],
        start_dt: datetime,
        end_dt: datetime,
        raw_trades_dir: Path,
        raw_candles_dir: Path,
        trades_parquet_dir: Path,
        normalized_candles_dir: Path,
        features_dir: Path,
        processed_tickers: set[str],
        skipped_or_failed_tickers: list[dict[str, Any]],
        all_close_times: list[datetime],
        metrics: dict[str, int],
    ) -> None:
        for market in list(checkpoint.get("pending_retained_markets", [])):
            ticker = str(_market_key(market, "ticker", "market_ticker") or "")
            if not ticker:
                continue
            if ticker in processed_tickers:
                self._remove_pending_market(checkpoint, ticker)
                checkpoint["resume_counters"]["replayed_work_skipped"] += 1
                self._update_checkpoint_progress(
                    checkpoint,
                    current_stage="retained_market_processing",
                    current_market_ticker=None,
                )
                self._save_checkpoint(checkpoint, tracker=tracker, message=f"removed already-processed pending market: {ticker}")
                continue
            checkpoint["resume_counters"]["replayed_work_replayed"] += 1
            self._process_market_artifacts(
                market,
                checkpoint=checkpoint,
                cutoff_ts=cutoff_ts,
                start_dt=start_dt,
                end_dt=end_dt,
                raw_trades_dir=raw_trades_dir,
                raw_candles_dir=raw_candles_dir,
                trades_parquet_dir=trades_parquet_dir,
                normalized_candles_dir=normalized_candles_dir,
                features_dir=features_dir,
                processed_tickers=processed_tickers,
                skipped_or_failed_tickers=skipped_or_failed_tickers,
                all_close_times=all_close_times,
                metrics=metrics,
                tracker=tracker,
            )

    def _init_signals(self) -> None:
        """Initialise optional signal helpers (lazy, so import errors are silent)."""
        cfg = self.config
        if cfg.run_base_rate and self._base_rate_signal is None:
            try:
                from trading_platform.kalshi.signals_base_rate import KalshiBaseRateSignal
                self._base_rate_signal = KalshiBaseRateSignal(cfg.base_rate_db_path)
            except Exception as exc:
                logger.warning("Could not load base rate signal: %s", exc)

        if cfg.run_metaculus and self._metaculus_signal is None:
            try:
                from trading_platform.kalshi.signals_metaculus import KalshiMetaculusSignal
                self._metaculus_signal = KalshiMetaculusSignal(
                    matches_path=cfg.metaculus_matches_path,
                    min_confidence=cfg.metaculus_min_confidence,
                )
            except Exception as exc:
                logger.warning("Could not load Metaculus signal: %s", exc)

    def _make_dirs(self) -> None:
        cfg = self.config
        for d in [
            cfg.raw_markets_dir,
            cfg.raw_trades_dir,
            cfg.raw_candles_dir,
            cfg.trades_parquet_dir,
            cfg.normalized_candles_dir,
            cfg.features_dir,
            str(Path(cfg.normalized_markets_path).parent),
            str(Path(cfg.resolution_csv_path).parent),
            str(Path(cfg.legacy_resolution_csv_path).parent),
            str(Path(cfg.checkpoint_path).parent),
            str(Path(cfg.summary_path).parent),
            cfg.status_artifacts_root,
            str(self._checkpoint_backup_file_path().parent),
        ]:
            Path(d).mkdir(parents=True, exist_ok=True)

    def _load_checkpoint(self) -> dict[str, Any]:
        checkpoint_path = self._checkpoint_file_path()
        backup_path = self._checkpoint_backup_file_path()
        self._loaded_backup_checkpoint = None
        if not self.config.resume or self.config.resume_mode == "fresh":
            return self._default_checkpoint_state()
        if backup_path.exists():
            try:
                self._loaded_backup_checkpoint = self._normalise_checkpoint(json.loads(backup_path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Could not load checkpoint %s: %s", backup_path, exc)
        for candidate in (checkpoint_path, backup_path):
            if not candidate.exists():
                continue
            try:
                return self._normalise_checkpoint(json.loads(candidate.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Could not load checkpoint %s: %s", candidate, exc)
        return self._default_checkpoint_state()

    def _save_checkpoint(
        self,
        checkpoint: dict[str, Any],
        *,
        tracker: IngestStatusTracker | None = None,
        message: str = "checkpoint updated",
    ) -> None:
        checkpoint_path = self._checkpoint_file_path()
        backup_path = self._checkpoint_backup_file_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self._normalise_checkpoint(checkpoint), indent=2, default=str)
        tmp_path = checkpoint_path.with_name(f"{checkpoint_path.name}.tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(checkpoint_path)
        backup_path.write_text(payload, encoding="utf-8")
        self._update_checkpoint_status(tracker, checkpoint, message=message)

    def _write_raw_market(self, market: dict[str, Any]) -> None:
        ticker = str(_market_key(market, "ticker", "market_ticker") or "")
        if not ticker:
            return
        path = Path(self.config.raw_markets_dir) / f"{ticker}.json"
        path.write_text(json.dumps(market, indent=2, default=str), encoding="utf-8")

    def _iter_downloaded_markets(self) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        for path in sorted(Path(self.config.raw_markets_dir).glob("*.json")):
            try:
                markets.append(json.loads(path.read_text(encoding="utf-8")))
            except json.JSONDecodeError as exc:
                logger.warning("Skipping unreadable raw market file %s: %s", path, exc)
        if self.config.ticker_filter:
            tickers = set(self.config.ticker_filter)
            markets = [market for market in markets if market.get("ticker", "") in tickers]
        return markets

    def _write_market_indexes(self, markets: list[dict[str, Any]]) -> tuple[int, int]:
        rows = [_normalise_market_row(market) for market in markets if market.get("ticker")]
        market_frame = pl.from_dicts(rows) if rows else pl.DataFrame()
        market_path = Path(self.config.normalized_markets_path)
        market_path.parent.mkdir(parents=True, exist_ok=True)
        market_frame.write_parquet(market_path)

        resolution_rows = [
            {
                "ticker": row["ticker"],
                "resolution_price": row["resolution_price"],
                "result": row["result"],
                "close_time": row["close_time"],
                "source_tier": row["source_tier"],
            }
            for row in rows
            if row["resolution_price"] is not None
        ]
        for resolution_path in (Path(self.config.resolution_csv_path), Path(self.config.legacy_resolution_csv_path)):
            resolution_path.parent.mkdir(parents=True, exist_ok=True)
            with resolution_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["ticker", "resolution_price", "result", "close_time", "source_tier"],
                )
                writer.writeheader()
                writer.writerows(resolution_rows)
        return len(rows), len(resolution_rows)

    def _fetch_cutoff_timestamps(self) -> dict[str, datetime | None]:
        try:
            payload = self.client.get_historical_cutoff()
        except Exception as exc:
            logger.warning("Could not fetch historical cutoff timestamps: %s", exc)
            return {
                "market_settled_ts": None,
                "trades_created_ts": None,
                "orders_updated_ts": None,
            }
        return {
            "market_settled_ts": _parse_iso_ts(payload.get("market_settled_ts")),
            "trades_created_ts": _parse_iso_ts(payload.get("trades_created_ts")),
            "orders_updated_ts": _parse_iso_ts(payload.get("orders_updated_ts")),
        }

    def _compute_extra_features(
        self,
        market: dict[str, Any],
        last_yes_price: float,
    ) -> dict[str, float]:
        """Build the extra_scalar_features dict for a market."""
        extra: dict[str, float] = {}

        if self._base_rate_signal is not None:
            try:
                title = market.get("title", "")
                series = market.get("series_ticker", "")
                features = self._base_rate_signal.compute_for_market(title, series, last_yes_price)
                extra.update(features)
            except Exception as exc:
                logger.debug("Base rate computation failed for %s: %s", market.get("ticker"), exc)

        if self._metaculus_signal is not None:
            try:
                ticker = market.get("ticker", "")
                features = self._metaculus_signal.compute_for_market(ticker, last_yes_price)
                extra.update(features)
            except Exception as exc:
                logger.debug("Metaculus computation failed for %s: %s", market.get("ticker"), exc)

        return extra

    def _early_filter_reason(self, market: dict[str, Any]) -> str | None:
        cfg = self.config
        if _is_synthetic_ticker(_market_key(market, "ticker", "market_ticker")):
            return "synthetic"
        if cfg.preferred_categories:
            allowed = {c.lower() for c in cfg.preferred_categories}
            category = str(_market_key(market, "category", "market_category") or "").lower()
            if category not in allowed:
                return "category"
        if cfg.excluded_series_patterns:
            series_key = str(_market_key(market, "series_ticker", "seriesTicker", "ticker", "market_ticker") or "")
            compiled = [re.compile(pattern, re.IGNORECASE) for pattern in cfg.excluded_series_patterns]
            if any(pattern.search(series_key) for pattern in compiled):
                return "series_pattern"
        if cfg.min_volume > 0 and _safe_volume(market) < cfg.min_volume:
            return "min_volume"
        return None

    def _filter_markets_for_ingest_page(
        self,
        markets: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        retained: list[dict[str, Any]] = []
        discarded_by_reason: Counter[str] = Counter()
        retained_sample_tickers: list[str] = []
        discarded_samples: list[dict[str, str]] = []
        for market in markets:
            reason = self._early_filter_reason(market)
            if reason is not None:
                discarded_by_reason[reason] += 1
                if len(discarded_samples) < 5:
                    discarded_samples.append(
                        {
                            "ticker": str(_market_key(market, "ticker", "market_ticker") or "<missing>"),
                            "reason": reason,
                        }
                    )
                continue
            retained.append(market)
            if len(retained_sample_tickers) < 5:
                retained_sample_tickers.append(str(_market_key(market, "ticker", "market_ticker") or "<missing>"))
        diagnostics = {
            "fetched": len(markets),
            "retained": len(retained),
            "discarded": len(markets) - len(retained),
            "discarded_by_reason": {
                "category": discarded_by_reason.get("category", 0),
                "series_pattern": discarded_by_reason.get("series_pattern", 0),
                "min_volume": discarded_by_reason.get("min_volume", 0),
                "synthetic": discarded_by_reason.get("synthetic", 0),
            },
            "retained_sample_tickers": retained_sample_tickers,
            "discarded_samples": discarded_samples,
        }
        return retained, diagnostics

    def _log_market_fetch_progress(
        self,
        *,
        source: str,
        page_number: int,
        cursor: str | None,
        page_diagnostics: dict[str, Any],
        total_fetched: int,
        total_retained: int,
        progress_log_interval_pages: int,
        retained_sample: list[str],
        tracker: IngestStatusTracker | None = None,
        pages_with_retained_markets: int = 0,
        pages_without_retained_markets: int = 0,
        stop_reason_hint: str | None = None,
    ) -> None:
        if page_number == 1 or page_number % progress_log_interval_pages == 0 or not cursor:
            log_line = self._log_stage_progress(
                "market_universe_fetch",
                "running",
                source=source,
                page=page_number,
                pages_seen=page_number,
                page_fetched=page_diagnostics["fetched"],
                page_retained=page_diagnostics["retained"],
                page_discarded=page_diagnostics["discarded"],
                pages_with_retained=pages_with_retained_markets,
                pages_without_retained=pages_without_retained_markets,
                retained_markets_seen=total_retained,
                fetched_total=total_fetched,
                cursor=cursor or "<end>",
                stop_hint=stop_reason_hint,
                retained_sample=(page_diagnostics.get("retained_sample_tickers") or retained_sample[:5] or ["<none>"]),
                discarded_sample=(page_diagnostics.get("discarded_samples") or ["<none>"]),
            )
            logger.info("%s discard_reasons=%s", log_line, page_diagnostics["discarded_by_reason"])
        if tracker is not None:
            tracker.update_stage(
                "market_universe_fetch",
                item_count_completed=total_fetched,
                message=f"{source} page {page_number} processed",
                counters={
                    "source": source,
                    "current_cursor": cursor,
                    "last_page_number": page_number,
                    "page_retained": page_diagnostics["retained"],
                    "page_discarded": page_diagnostics["discarded"],
                    "discard_reasons": page_diagnostics["discarded_by_reason"],
                    "retained_sample_tickers": page_diagnostics.get("retained_sample_tickers") or retained_sample[:5],
                    "discarded_samples": page_diagnostics.get("discarded_samples") or [],
                },
                run_counters={
                    "pages_seen": pages_with_retained_markets + pages_without_retained_markets,
                    "pages_with_retained_markets": pages_with_retained_markets,
                    "pages_without_retained_markets": pages_without_retained_markets,
                    "retained_markets_seen": total_retained,
                },
                log_line=None,
            )

    def _fetch_live_markets_by_events(
        self,
        *,
        start_dt: datetime,
        end_dt: datetime,
        cutoff_ts: dict[str, datetime | None],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Fetch post-cutoff settled markets via the /events endpoint.

        NOTE: The Kalshi ``/events`` endpoint does not actually filter by ``category``
        server-side (the parameter is silently ignored).  This method is therefore
        best used with ``preferred_categories`` as a client-side allowlist: it
        paginates through all events and applies the category check per market in
        ``_filter_markets_for_ingest_page``.  For known Economics/Politics series,
        prefer ``use_direct_series_fetch=True`` + ``direct_series_tickers`` which
        avoids scanning the full event universe.

        Returns a flat list of retained markets and a diagnostics dict.
        """
        cfg = self.config
        live_start_ts = int(start_dt.timestamp())
        live_end_ts = int(end_dt.timestamp())
        if cutoff_ts.get("market_settled_ts") is not None:
            live_start_ts = max(live_start_ts, int(cutoff_ts["market_settled_ts"].timestamp()))

        all_retained: list[dict[str, Any]] = []
        total_fetched = 0
        total_retained = 0
        event_count = 0
        pages_fetched = 0

        for category in cfg.preferred_categories:
            cursor: str | None = None
            while True:
                events, cursor = self.client.get_events_raw(
                    category=category,
                    with_nested_markets=False,
                    limit=200,
                    cursor=cursor,
                )
                pages_fetched += 1
                event_count += len(events)
                for event in events:
                    event_ticker = event.get("event_ticker") or event.get("ticker")
                    if not event_ticker:
                        continue
                    markets_page, _ = self.client.get_markets_raw(
                        status="settled",
                        event_ticker=event_ticker,
                        limit=200,
                    )
                    page_markets = []
                    for market in markets_page:
                        if not _market_close_time_in_range(
                            market,
                            min_close_ts=live_start_ts,
                            max_close_ts=live_end_ts,
                        ):
                            continue
                        market["source_tier"] = "live"
                        market["ingested_at"] = end_dt.isoformat()
                        page_markets.append(market)
                    total_fetched += len(page_markets)
                    retained, _ = self._filter_markets_for_ingest_page(page_markets)
                    all_retained.extend(retained)
                    total_retained += len(retained)
                if not cursor:
                    break

        logger.info(
            "Events-based category fetch complete: categories=%s pages=%d events=%d fetched=%d retained=%d",
            cfg.preferred_categories,
            pages_fetched,
            event_count,
            total_fetched,
            total_retained,
        )
        return all_retained, {
            "total_markets_fetched": total_fetched,
            "total_markets_retained": total_retained,
            "event_count": event_count,
            "pages_fetched": pages_fetched,
        }

    def _fetch_markets_by_series(
        self,
        *,
        start_dt: datetime,
        end_dt: datetime,
        cutoff_ts: dict[str, datetime | None],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Fetch markets for each known series ticker directly via series_ticker param.

        Returns a flat list of all retained markets and a diagnostics dict.
        """
        cfg = self.config
        min_close_ts = int(start_dt.timestamp())
        max_close_ts = int(end_dt.timestamp())
        historical_max_close_ts = max_close_ts
        if cutoff_ts["market_settled_ts"] is not None:
            historical_max_close_ts = min(max_close_ts, int(cutoff_ts["market_settled_ts"].timestamp()) - 1)

        all_retained: list[dict[str, Any]] = []
        total_fetched = 0
        total_retained = 0
        for series in cfg.direct_series_tickers:
            # Step 1: fetch events for this series via authenticated endpoint
            # (/events?series_ticker=X&status=settled filters correctly,
            # unlike /historical/markets which ignores series_ticker).
            event_tickers: list[str] = []
            ev_cursor: str | None = None
            while True:
                events, ev_cursor = self.client.get_events_raw(
                    series_ticker=series,
                    status="settled",
                    with_nested_markets=False,
                    limit=200,
                    cursor=ev_cursor,
                )
                for ev in events:
                    et = ev.get("event_ticker") or ev.get("ticker") or ""
                    if et:
                        event_tickers.append(et)
                if not ev_cursor:
                    break
            logger.info("Direct series %s: found %d events", series, len(event_tickers))

            # Step 2: fetch markets for each event
            for event_ticker in event_tickers:
                mkt_cursor: str | None = None
                while True:
                    markets, mkt_cursor = self.client.get_markets_raw(
                        event_ticker=event_ticker,
                        status="settled",
                        limit=200,
                        cursor=mkt_cursor,
                    )
                    page_markets = []
                    for market in markets:
                        market["source_tier"] = "live_direct_series"
                        market["ingested_at"] = end_dt.isoformat()
                        page_markets.append(market)
                    total_fetched += len(page_markets)
                    retained_markets, _ = self._filter_markets_for_ingest_page(page_markets)
                    all_retained.extend(retained_markets)
                    total_retained += len(retained_markets)
                    logger.debug(
                        "Direct series %s event %s: fetched=%d retained=%d",
                        series, event_ticker, len(page_markets), len(retained_markets),
                    )
                    if not mkt_cursor:
                        break

        diagnostics: dict[str, Any] = {
            "total_markets_fetched": total_fetched,
            "total_markets_retained": total_retained,
            "series_count": len(cfg.direct_series_tickers),
        }
        logger.info(
            "Direct series fetch complete: %d series, %d fetched, %d retained",
            len(cfg.direct_series_tickers),
            total_fetched,
            total_retained,
        )
        return all_retained, diagnostics

    def _download_market_universe(
        self,
        *,
        checkpoint: dict[str, Any],
        start_dt: datetime,
        end_dt: datetime,
        cutoff_ts: dict[str, datetime | None],
        on_retained_market: Callable[[dict[str, Any]], None] | None = None,
        tracker: IngestStatusTracker | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        min_close_ts = int(start_dt.timestamp())
        max_close_ts = int(end_dt.timestamp())
        progress_log_interval_pages = 10
        diagnostics: dict[str, Any] = {
            "pages_fetched": 0,
            "total_markets_fetched": 0,
            "total_markets_retained": 0,
            "total_markets_discarded": 0,
            "discarded_by_category": 0,
            "discarded_by_series_pattern": 0,
            "discarded_by_min_volume": 0,
            "discarded_synthetic": 0,
            "retained_sample_tickers": [],
            "last_cursor": None,
            "pages_with_retained_markets": 0,
            "pages_without_retained_markets": 0,
            "pagination_stop_reason": None,
            "first_retained_processing_ticker": None,
            "first_retained_processing_source": None,
            "first_retained_processing_page": None,
        }
        consecutive_live_zero_retained_pages = 0
        if checkpoint.get("market_download_complete"):
            diagnostics["pagination_stop_reason"] = "checkpoint_complete"
            return diagnostics
        self._update_checkpoint_progress(checkpoint, current_stage="market_universe_fetch")

        historical_max_close_ts = max_close_ts
        if cutoff_ts["market_settled_ts"] is not None:
            historical_max_close_ts = min(max_close_ts, int(cutoff_ts["market_settled_ts"].timestamp()) - 1)

        # ── Direct series fetch (fast path) ──────────────────────────────────
        # Fetch known Economics/Politics series tickers directly instead of
        # scanning all markets page by page.  Falls back to full pagination
        # (capped at 50 pages) when direct fetch returns fewer than 20 markets.
        live_start_dt = start_dt
        if cutoff_ts["market_settled_ts"] is not None:
            live_start_dt = max(start_dt, cutoff_ts["market_settled_ts"])

        def _queue_retained_markets(
            markets: list[dict[str, Any]],
            *,
            source_name: str,
            page_number: int,
        ) -> None:
            for market in markets:
                if len(diagnostics["retained_sample_tickers"]) < 5:
                    diagnostics["retained_sample_tickers"].append(market.get("ticker", ""))
                self._enqueue_retained_market(checkpoint, market)
                self._update_checkpoint_progress(
                    checkpoint,
                    current_stage="market_universe_fetch",
                    stage_counters={
                        "pages_fetched": diagnostics["pages_fetched"],
                        "total_markets_retained": diagnostics["total_markets_retained"],
                    },
                )
                self._save_checkpoint(checkpoint, tracker=tracker, message=f"retained market queued: {market.get('ticker', '')}")
                if on_retained_market is not None:
                    if diagnostics["first_retained_processing_ticker"] is None:
                        diagnostics["first_retained_processing_ticker"] = market.get("ticker", "")
                        diagnostics["first_retained_processing_source"] = source_name
                        diagnostics["first_retained_processing_page"] = page_number
                    on_retained_market(market)

        events_mode_enabled = bool(cfg.preferred_categories and cfg.use_events_for_category_filter)
        events_only_mode = events_mode_enabled and not cfg.use_direct_series_fetch
        # When direct series fetch is enabled, skip the events-based category
        # scan entirely — direct_series_tickers is the sole discovery mechanism.
        if events_mode_enabled and not (cfg.use_direct_series_fetch and cfg.direct_series_tickers):
            events_markets, events_diag = self._fetch_live_markets_by_events(
                start_dt=live_start_dt,
                end_dt=end_dt,
                cutoff_ts=cutoff_ts,
            )
            diagnostics["pages_fetched"] += events_diag.get("pages_fetched", 0)
            diagnostics["total_markets_fetched"] += events_diag["total_markets_fetched"]
            diagnostics["total_markets_retained"] += events_diag["total_markets_retained"]
            if events_diag.get("pages_fetched", 0) > 0:
                if events_diag["total_markets_retained"] > 0:
                    diagnostics["pages_with_retained_markets"] += events_diag["pages_fetched"]
                else:
                    diagnostics["pages_without_retained_markets"] += events_diag["pages_fetched"]
            _queue_retained_markets(
                events_markets,
                source_name="live_events",
                page_number=0,
            )
            checkpoint["live_market_cursor"] = None
            if cfg.skip_historical_pagination or events_only_mode:
                checkpoint["historical_market_cursor"] = None
                diagnostics["pagination_stop_reason"] = (
                    "events_only_mode"
                    if events_only_mode
                    else "events_fetch_skip_historical_pagination"
                )
                checkpoint["market_download_complete"] = True
                self._update_checkpoint_progress(
                    checkpoint,
                    current_stage="market_universe_fetch",
                    last_completed_stage="market_universe_fetch",
                    current_market_ticker=None,
                    pagination_stop_reason=diagnostics["pagination_stop_reason"],
                )
                self._save_checkpoint(
                    checkpoint,
                    tracker=tracker,
                    message=(
                        "market universe fetch complete (events-only discovery)"
                        if events_only_mode
                        else "market universe fetch complete (skip_historical_pagination=True)"
                    ),
                )
                logger.info(
                    "Kalshi market-universe fetch complete: pages=%d fetched=%d retained=%d discarded=%d sample=%s",
                    diagnostics["pages_fetched"],
                    diagnostics["total_markets_fetched"],
                    diagnostics["total_markets_retained"],
                    diagnostics["total_markets_discarded"],
                    diagnostics["retained_sample_tickers"] or ["<none>"],
                )
                return diagnostics

        direct_fetch_done = False
        max_historical_fallback_pages = 50
        if cfg.use_direct_series_fetch and cfg.direct_series_tickers:
            direct_markets, direct_diag = self._fetch_markets_by_series(
                start_dt=start_dt,
                end_dt=end_dt,
                cutoff_ts=cutoff_ts,
            )
            diagnostics["total_markets_fetched"] += direct_diag["total_markets_fetched"]
            diagnostics["total_markets_retained"] += direct_diag["total_markets_retained"]
            _queue_retained_markets(
                direct_markets,
                source_name="direct_series",
                page_number=0,
            )
            if direct_diag["total_markets_retained"] >= 20:
                # Sufficient markets found via direct fetch — skip historical pagination.
                direct_fetch_done = True
                checkpoint["historical_market_cursor"] = None
                self._update_checkpoint_progress(checkpoint, current_stage="market_universe_fetch")
                self._save_checkpoint(checkpoint, tracker=tracker, message="historical pagination skipped (direct series fetch sufficient)")
                logger.info(
                    "Direct series fetch returned %d markets (>= 20); skipping full historical pagination.",
                    direct_diag["total_markets_retained"],
                )
            else:
                logger.info(
                    "Direct series fetch returned only %d markets (< 20); falling back to historical pagination (max %d pages).",
                    direct_diag["total_markets_retained"],
                    max_historical_fallback_pages,
                )

        cursor = checkpoint.get("historical_market_cursor")
        historical_pages = 0
        if cfg.skip_historical_pagination:
            # Skip /historical/markets scan entirely — the events-based live
            # fetch (or direct-series fetch) is the sole discovery mechanism.
            checkpoint["historical_market_cursor"] = None
            self._update_checkpoint_progress(checkpoint, current_stage="market_universe_fetch")
            self._save_checkpoint(checkpoint, tracker=tracker, message="historical pagination skipped (skip_historical_pagination=True)")
        elif not direct_fetch_done and historical_max_close_ts >= min_close_ts:
            while True:
                markets, cursor = self.client.get_historical_markets(
                    limit=cfg.market_page_size,
                    cursor=cursor,
                    min_close_ts=min_close_ts,
                    max_close_ts=historical_max_close_ts,
                    sleep=cfg.request_sleep_sec,
                )
                historical_pages += 1
                diagnostics["pages_fetched"] += 1
                page_markets = []
                for market in markets:
                    market["source_tier"] = "historical"
                    market["ingested_at"] = end_dt.isoformat()
                    page_markets.append(market)
                retained_markets, page_diagnostics = self._filter_markets_for_ingest_page(page_markets)
                diagnostics["total_markets_fetched"] += page_diagnostics["fetched"]
                diagnostics["total_markets_retained"] += page_diagnostics["retained"]
                diagnostics["total_markets_discarded"] += page_diagnostics["discarded"]
                diagnostics["discarded_by_category"] += page_diagnostics["discarded_by_reason"]["category"]
                diagnostics["discarded_by_series_pattern"] += page_diagnostics["discarded_by_reason"]["series_pattern"]
                diagnostics["discarded_by_min_volume"] += page_diagnostics["discarded_by_reason"]["min_volume"]
                diagnostics["discarded_synthetic"] += page_diagnostics["discarded_by_reason"]["synthetic"]
                diagnostics["last_cursor"] = cursor
                if page_diagnostics["retained"] > 0:
                    diagnostics["pages_with_retained_markets"] += 1
                else:
                    diagnostics["pages_without_retained_markets"] += 1
                _queue_retained_markets(
                    retained_markets,
                    source_name="historical",
                    page_number=historical_pages,
                )
                self._log_market_fetch_progress(
                    source="historical",
                    page_number=historical_pages,
                    cursor=cursor,
                    page_diagnostics=page_diagnostics,
                    total_fetched=diagnostics["total_markets_fetched"],
                    total_retained=diagnostics["total_markets_retained"],
                    progress_log_interval_pages=progress_log_interval_pages,
                    retained_sample=diagnostics["retained_sample_tickers"],
                    tracker=tracker,
                    pages_with_retained_markets=diagnostics["pages_with_retained_markets"],
                    pages_without_retained_markets=diagnostics["pages_without_retained_markets"],
                )
                checkpoint["historical_market_cursor"] = cursor
                self._update_checkpoint_progress(
                    checkpoint,
                    current_stage="market_universe_fetch",
                    stage_counters={"pages_fetched": diagnostics["pages_fetched"]},
                )
                self._save_checkpoint(checkpoint, tracker=tracker, message="historical market cursor updated")
                if not cursor:
                    break
                if cfg.use_direct_series_fetch and historical_pages >= max_historical_fallback_pages:
                    logger.info(
                        "Historical pagination fallback cap reached (%d pages); stopping historical scan.",
                        max_historical_fallback_pages,
                    )
                    break
        elif not direct_fetch_done:
            checkpoint["historical_market_cursor"] = None
            self._update_checkpoint_progress(checkpoint, current_stage="market_universe_fetch")
            self._save_checkpoint(checkpoint, tracker=tracker, message="historical market cursor skipped")

        live_start_dt = start_dt
        if cutoff_ts["market_settled_ts"] is not None:
            live_start_dt = max(start_dt, cutoff_ts["market_settled_ts"])

        # ── Live market fetch: events-based (category-filtered) or cursor pagination ──
        # When preferred_categories is set and use_events_for_category_filter is True,
        # use /events?category=X to discover matching events, then fetch each event's
        # settled markets.  This replaces blind cursor pagination through /markets which
        # does NOT filter by category server-side.
        if events_mode_enabled:
            checkpoint["live_market_cursor"] = None
            diagnostics["pagination_stop_reason"] = "events_based_fetch_complete"
        else:
            cursor = checkpoint.get("live_market_cursor")
            live_pages = 0
            while True:
                checkpoint["live_market_cursor"] = cursor
                raw_markets, cursor = self._fetch_live_page_with_resume_recovery(
                    checkpoint=checkpoint,
                    tracker=tracker,
                )
                live_pages += 1
                diagnostics["pages_fetched"] += 1
                page_close_times = [
                    close_time
                    for close_time in (_parse_iso_ts(_market_key(market, "close_time", "expiration_time")) for market in raw_markets)
                    if close_time is not None
                ]
                oldest_page_close_time = min(page_close_times) if page_close_times else None
                newest_page_close_time = max(page_close_times) if page_close_times else None
                range_filtered_markets = [
                    market
                    for market in raw_markets
                    if _market_close_time_in_range(
                        market,
                        min_close_ts=int(live_start_dt.timestamp()),
                        max_close_ts=max_close_ts,
                    )
                ]
                page_markets = []
                for market in range_filtered_markets:
                    market["source_tier"] = "live"
                    market["ingested_at"] = end_dt.isoformat()
                    page_markets.append(market)
                retained_markets, page_diagnostics = self._filter_markets_for_ingest_page(page_markets)
                diagnostics["total_markets_fetched"] += page_diagnostics["fetched"]
                diagnostics["total_markets_retained"] += page_diagnostics["retained"]
                diagnostics["total_markets_discarded"] += page_diagnostics["discarded"]
                diagnostics["discarded_by_category"] += page_diagnostics["discarded_by_reason"]["category"]
                diagnostics["discarded_by_series_pattern"] += page_diagnostics["discarded_by_reason"]["series_pattern"]
                diagnostics["discarded_by_min_volume"] += page_diagnostics["discarded_by_reason"]["min_volume"]
                diagnostics["discarded_synthetic"] += page_diagnostics["discarded_by_reason"]["synthetic"]
                diagnostics["last_cursor"] = cursor
                if page_diagnostics["retained"] > 0:
                    diagnostics["pages_with_retained_markets"] += 1
                else:
                    diagnostics["pages_without_retained_markets"] += 1
                for market in retained_markets:
                    if len(diagnostics["retained_sample_tickers"]) < 5:
                        diagnostics["retained_sample_tickers"].append(market.get("ticker", ""))
                    self._enqueue_retained_market(checkpoint, market)
                    self._update_checkpoint_progress(
                        checkpoint,
                        current_stage="market_universe_fetch",
                        stage_counters={
                            "pages_fetched": diagnostics["pages_fetched"],
                            "total_markets_retained": diagnostics["total_markets_retained"],
                        },
                    )
                    self._save_checkpoint(checkpoint, tracker=tracker, message=f"retained market queued: {market.get('ticker', '')}")
                    if on_retained_market is not None:
                        if diagnostics["first_retained_processing_ticker"] is None:
                            diagnostics["first_retained_processing_ticker"] = market.get("ticker", "")
                            diagnostics["first_retained_processing_source"] = "live"
                            diagnostics["first_retained_processing_page"] = live_pages
                        on_retained_market(market)
                processed_after_page = len(checkpoint.get("processed_tickers", []))
                if page_diagnostics["retained"] == 0:
                    consecutive_live_zero_retained_pages += 1
                else:
                    consecutive_live_zero_retained_pages = 0
                self._log_market_fetch_progress(
                    source="live",
                    page_number=live_pages,
                    cursor=cursor,
                    page_diagnostics=page_diagnostics,
                    total_fetched=diagnostics["total_markets_fetched"],
                    total_retained=diagnostics["total_markets_retained"],
                    progress_log_interval_pages=progress_log_interval_pages,
                    retained_sample=diagnostics["retained_sample_tickers"],
                    tracker=tracker,
                    pages_with_retained_markets=diagnostics["pages_with_retained_markets"],
                    pages_without_retained_markets=diagnostics["pages_without_retained_markets"],
                )
                checkpoint["live_market_cursor"] = cursor
                self._update_checkpoint_progress(
                    checkpoint,
                    current_stage="market_universe_fetch",
                    stage_counters={"pages_fetched": diagnostics["pages_fetched"]},
                )
                self._save_checkpoint(checkpoint, tracker=tracker, message="live market cursor updated")
                if oldest_page_close_time is not None and oldest_page_close_time < live_start_dt and not range_filtered_markets:
                    diagnostics["pagination_stop_reason"] = "aged_out_pages"
                    logger.info(
                        "Stopping live settled-market pagination after page %d because page close-time range %s -> %s is older than ingest window start %s.",
                        live_pages,
                        oldest_page_close_time.isoformat(),
                        newest_page_close_time.isoformat() if newest_page_close_time is not None else "<unknown>",
                        live_start_dt.isoformat(),
                    )
                    checkpoint["live_market_cursor"] = None
                    self._update_checkpoint_progress(checkpoint, current_stage="market_universe_fetch", pagination_stop_reason="aged_out_pages")
                    self._save_checkpoint(checkpoint, tracker=tracker, message="live market cursor aged out")
                    break
                if (
                    consecutive_live_zero_retained_pages >= cfg.max_live_pages_without_retained_markets
                    and processed_after_page == 0
                ):
                    diagnostics["pagination_stop_reason"] = "fail_fast_zero_retained_pages"
                    error = KalshiIngestFailFastError(
                        "zero_retained_pages",
                        "Kalshi live bridge fetched "
                        f"{live_pages} live pages without any retained markets entering processing. "
                        "Check preferred_categories / excluded_series_patterns / min_volume against the live /markets payload.",
                    )
                    error.diagnostics = dict(diagnostics)
                    raise error
                if diagnostics["total_markets_retained"] > cfg.max_raw_markets_without_processing and processed_after_page == 0:
                    diagnostics["pagination_stop_reason"] = "fail_fast_retained_without_processing"
                    error = KalshiIngestFailFastError(
                        "retained_without_processing",
                        "Kalshi ingest fail-fast: retained raw-market attempts exceeded "
                        f"{cfg.max_raw_markets_without_processing} before any market finished processing. "
                        "This usually means live settled pagination is traversing too much of the universe or retained markets are never reaching normalization.",
                    )
                    error.diagnostics = dict(diagnostics)
                    raise error
                if not cursor:
                    diagnostics["pagination_stop_reason"] = "cursor_exhausted"
                    break

        checkpoint["market_download_complete"] = True
        self._update_checkpoint_progress(
            checkpoint,
            current_stage="market_universe_fetch",
            last_completed_stage="market_universe_fetch",
            current_market_ticker=None,
            pagination_stop_reason=diagnostics.get("pagination_stop_reason"),
        )
        self._save_checkpoint(checkpoint, tracker=tracker, message="market universe fetch complete")
        logger.info(
            "Kalshi market-universe fetch complete: pages=%d fetched=%d retained=%d discarded=%d sample=%s",
            diagnostics["pages_fetched"],
            diagnostics["total_markets_fetched"],
            diagnostics["total_markets_retained"],
            diagnostics["total_markets_discarded"],
            diagnostics["retained_sample_tickers"] or ["<none>"],
        )
        return diagnostics

    def _fetch_market_trades(self, market: dict[str, Any], cutoff_ts: dict[str, datetime | None]) -> list[dict[str, Any]]:
        ticker = market.get("ticker", "")
        if not ticker:
            return []
        cfg = self.config
        trades: list[dict[str, Any]] = []
        trade_cutoff = cutoff_ts.get("trades_created_ts")
        if trade_cutoff is None:
            source_tier = str(market.get("source_tier", "historical"))
            if source_tier == "live":
                live_trades = self.client.get_all_trades_raw(ticker, limit=cfg.trade_page_size)
                for trade in live_trades:
                    trade["source_tier"] = "live"
                return live_trades
            historical_trades = self.client.get_all_historical_trades(
                ticker=ticker,
                limit=cfg.trade_page_size,
                sleep=cfg.request_sleep_sec,
            )
            for trade in historical_trades:
                trade["source_tier"] = "historical"
            return historical_trades

        historical_trades = self.client.get_all_historical_trades(
            ticker=ticker,
            limit=cfg.trade_page_size,
            max_ts=int(trade_cutoff.timestamp()) - 1,
            sleep=cfg.request_sleep_sec,
        )
        for trade in historical_trades:
            trade["source_tier"] = "historical"
        trades.extend(historical_trades)

        live_trades = self.client.get_all_trades_raw(
            ticker=ticker,
            min_ts=int(trade_cutoff.timestamp()),
            limit=cfg.trade_page_size,
        )
        for trade in live_trades:
            trade["source_tier"] = "live"
        trades.extend(live_trades)

        deduped: dict[str, dict[str, Any]] = {}
        for trade in trades:
            trade_id = str(trade.get("trade_id") or f"{trade.get('ticker')}::{trade.get('created_time')}::{trade.get('yes_price_dollars')}")
            deduped[trade_id] = trade
        return list(deduped.values())

    def _fetch_market_candles(
        self,
        market: dict[str, Any],
        cutoff_ts: dict[str, datetime | None],
        *,
        start_dt: datetime,
        end_dt: datetime,
    ) -> list[dict[str, Any]]:
        ticker = market.get("ticker", "")
        if not ticker:
            return []
        params = {
            "start_ts": int(start_dt.timestamp()),
            "end_ts": int(end_dt.timestamp()),
            "period_interval": self.config.candle_period_interval,
        }
        close_time = _parse_iso_ts(market.get("close_time"))
        use_historical = bool(
            cutoff_ts.get("market_settled_ts") is None
            or (close_time is not None and cutoff_ts["market_settled_ts"] is not None and close_time < cutoff_ts["market_settled_ts"])
            or str(market.get("source_tier")) == "historical"
        )
        if use_historical:
            # Historical candlestick endpoint is not available on free API tiers.
            # Log a warning and return empty — use `data kalshi live-candles` instead.
            logger.debug(
                "Skipping historical candle fetch for %s — endpoint not available on this API tier. "
                "Use 'trading-cli data kalshi live-candles' for open markets.",
                ticker,
            )
            return []
        series_ticker = market.get("series_ticker") or market.get("seriesTicker")
        candles = self.client.get_market_candlesticks_raw(
            ticker,
            start_ts=params["start_ts"],
            end_ts=params["end_ts"],
            period_interval=params["period_interval"],
            series_ticker=series_ticker,
        )
        for candle in candles:
            candle["source_tier"] = "live"
        return candles

    def _apply_market_filters_with_diagnostics(self, markets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Apply configured filters to the raw market list.

        Filters are applied in this order:
        1. preferred_categories allowlist (when non-empty)
        2. excluded_series_patterns regex exclusions
        3. min_volume threshold
        4. max_markets_per_event bracket detection

        A summary line is always logged so the operator knows what was dropped.
        """
        cfg = self.config
        total = len(markets)
        filtered = list(markets)
        diagnostics: dict[str, Any] = {
            "total_markets_before_filters": total,
            "excluded_by_category": 0,
            "excluded_by_series_pattern": 0,
            "excluded_by_min_volume": 0,
            "excluded_by_bracket": 0,
            "retained_markets": total,
            "excluded_markets_total": 0,
            "effective_filter_config": {
                "preferred_categories": list(cfg.preferred_categories),
                "excluded_series_patterns": list(cfg.excluded_series_patterns),
                "min_volume": cfg.min_volume,
                "max_markets_per_event": cfg.max_markets_per_event,
            },
        }

        # 1. Category allowlist
        if cfg.preferred_categories:
            allowed = {c.lower() for c in cfg.preferred_categories}
            before = len(filtered)
            filtered = [
                m for m in filtered
                if str(m.get("category") or "").lower() in allowed
            ]
            diagnostics["excluded_by_category"] = before - len(filtered)
            logger.debug(
                "After preferred_categories filter (%s): %d → %d markets",
                cfg.preferred_categories,
                before,
                len(filtered),
            )

        # 2. Excluded series patterns
        if cfg.excluded_series_patterns:
            compiled = [re.compile(p, re.IGNORECASE) for p in cfg.excluded_series_patterns]

            def _series_key(m: dict[str, Any]) -> str:
                return str(m.get("series_ticker") or m.get("ticker") or "")

            before = len(filtered)
            filtered = [
                m for m in filtered
                if not any(pat.search(_series_key(m)) for pat in compiled)
            ]
            diagnostics["excluded_by_series_pattern"] = before - len(filtered)
            logger.debug(
                "After excluded_series_patterns filter: %d → %d markets",
                before,
                len(filtered),
            )

        # 3. Minimum volume
        if cfg.min_volume > 0:
            before = len(filtered)
            filtered = [m for m in filtered if _safe_volume(m) >= cfg.min_volume]
            diagnostics["excluded_by_min_volume"] = before - len(filtered)
            logger.debug(
                "After min_volume filter (>= %.0f): %d → %d markets",
                cfg.min_volume,
                before,
                len(filtered),
            )

        # 4. Max markets per event (bracket detection)
        if cfg.max_markets_per_event > 0:
            event_counts: Counter[str] = Counter(
                str(m.get("event_ticker") or m.get("series_ticker") or m.get("ticker") or "")
                for m in filtered
            )
            bracket_events = {
                event for event, count in event_counts.items()
                if count > cfg.max_markets_per_event
            }
            if bracket_events:
                before = len(filtered)
                filtered = [
                    m for m in filtered
                    if str(m.get("event_ticker") or m.get("series_ticker") or m.get("ticker") or "")
                    not in bracket_events
                ]
                diagnostics["excluded_by_bracket"] = before - len(filtered)
                logger.debug(
                    "After max_markets_per_event filter (<= %d): removed %d bracket events, %d → %d markets",
                    cfg.max_markets_per_event,
                    len(bracket_events),
                    before,
                    len(filtered),
                )

        diagnostics["retained_markets"] = len(filtered)
        diagnostics["excluded_markets_total"] = total - len(filtered)

        logger.info(
            "Market filter summary: Found %d total markets, filtering to %d after exclusions "
            "(preferred_categories=%s, excluded_series=%s, min_volume=%.0f, max_markets_per_event=%d)",
            total,
            len(filtered),
            cfg.preferred_categories or "all",
            cfg.excluded_series_patterns or "none",
            cfg.min_volume,
            cfg.max_markets_per_event,
        )
        return filtered, diagnostics

    def _apply_market_filters(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        filtered, _ = self._apply_market_filters_with_diagnostics(markets)
        return filtered

    def _process_market_artifacts(
        self,
        market: dict[str, Any],
        *,
        checkpoint: dict[str, Any],
        cutoff_ts: dict[str, datetime | None],
        start_dt: datetime,
        end_dt: datetime,
        raw_trades_dir: Path,
        raw_candles_dir: Path,
        trades_parquet_dir: Path,
        normalized_candles_dir: Path,
        features_dir: Path,
        processed_tickers: set[str],
        skipped_or_failed_tickers: list[dict[str, Any]],
        all_close_times: list[datetime],
        metrics: dict[str, int],
        tracker: IngestStatusTracker | None = None,
    ) -> str:
        from trading_platform.kalshi.features import build_kalshi_features, write_feature_parquet

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
                counters={
                    "active_ticker": ticker,
                    "min_trades": cfg.min_trades,
                },
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
            if tracker is not None:
                tracker.update_stage(
                    "retained_market_processing",
                    message=f"first retained market processing started with {ticker}",
                    counters={"first_processing_ticker": ticker},
                    run_counters={"first_retained_processing_ticker": ticker},
                    current_stage=True,
                )
        self._write_raw_market(market)
        if tracker is not None:
            tracker.increment_run_counters(raw_market_files_written=1)

        close_time_str = _market_key(market, "close_time", "expiration_time") or ""
        close_time = _parse_iso_ts(close_time_str)
        if close_time is not None:
            all_close_times.append(close_time)

        logger.info("Processing Kalshi market %s (%s)", ticker, market.get("title", ""))
        try:
            raw_trades = self._fetch_market_trades(market, cutoff_ts)
        except Exception as exc:
            logger.warning("Failed to fetch trades for %s: %s", ticker, exc)
            metrics["markets_failed"] += 1
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trades", "error": str(exc)})
            checkpoint["failed_tickers"][ticker] = {
                "attempts": int(checkpoint["failed_tickers"].get(ticker, {}).get("attempts", 0)) + 1,
                "last_error": str(exc),
                "last_stage": "trades",
                "updated_at": datetime.now(UTC).isoformat(),
            }
            self._remove_pending_market(checkpoint, ticker)
            self._update_checkpoint_progress(
                checkpoint,
                current_stage="retained_market_processing",
                current_market_ticker=None,
            )
            self._save_checkpoint(checkpoint, tracker=tracker, message=f"failed retained market processing: {ticker}")
            if tracker is not None:
                tracker.update_stage(
                    "retained_market_processing",
                    item_count_completed=metrics["markets_with_trades"],
                    item_count_failed=metrics["markets_failed"],
                    message=f"trade fetch failed for {ticker}",
                    counters={"last_failed_ticker": ticker, "last_failure_stage": "trades"},
                    run_counters={
                        "markets_failed": metrics["markets_failed"],
                        "processed_ticker_count": len(processed_tickers),
                    },
                )
            return "failed"

        if len(raw_trades) < cfg.min_trades:
            logger.debug("Skipping %s: only %d trades (min %d).", ticker, len(raw_trades), cfg.min_trades)
            metrics["markets_skipped"] += 1
            skipped_or_failed_tickers.append(
                {"ticker": ticker, "stage": "min_trades", "trade_count": len(raw_trades), "min_trades": cfg.min_trades}
            )
            self._remove_pending_market(checkpoint, ticker)
            checkpoint["failed_tickers"].pop(ticker, None)
            self._update_checkpoint_progress(
                checkpoint,
                current_stage="retained_market_processing",
                current_market_ticker=None,
            )
            self._save_checkpoint(checkpoint, tracker=tracker, message=f"skipped retained market: {ticker}")
            if tracker is not None:
                tracker.update_stage(
                    "retained_market_processing",
                    item_count_completed=metrics["markets_with_trades"],
                    item_count_failed=metrics["markets_failed"],
                    message=f"skipped {ticker} for min_trades",
                    counters={"last_skipped_ticker": ticker, "last_skip_reason": "min_trades"},
                    run_counters={"processed_ticker_count": len(processed_tickers)},
                )
            return "skipped"

        metrics["markets_with_trades"] += 1
        metrics["total_trades"] += len(raw_trades)
        (raw_trades_dir / f"{ticker}.json").write_text(json.dumps(raw_trades, indent=2, default=str), encoding="utf-8")

        try:
            trades_df = _trades_to_dataframe(raw_trades)
            trades_df.write_parquet(trades_parquet_dir / f"{ticker}.parquet")
            if tracker is not None:
                tracker.increment_run_counters(normalized_outputs_written=1)
        except Exception as exc:
            logger.warning("Trade parquet write failed for %s: %s", ticker, exc)
            metrics["markets_failed"] += 1
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trade_parquet", "error": str(exc)})
            checkpoint["failed_tickers"][ticker] = {
                "attempts": int(checkpoint["failed_tickers"].get(ticker, {}).get("attempts", 0)) + 1,
                "last_error": str(exc),
                "last_stage": "trade_parquet",
                "updated_at": datetime.now(UTC).isoformat(),
            }
            self._remove_pending_market(checkpoint, ticker)
            self._update_checkpoint_progress(
                checkpoint,
                current_stage="retained_market_processing",
                current_market_ticker=None,
            )
            self._save_checkpoint(checkpoint, tracker=tracker, message=f"failed retained market processing: {ticker}")
            if tracker is not None:
                tracker.update_stage(
                    "retained_market_processing",
                    item_count_completed=metrics["markets_with_trades"],
                    item_count_failed=metrics["markets_failed"],
                    message=f"trade parquet failed for {ticker}",
                    counters={"last_failed_ticker": ticker, "last_failure_stage": "trade_parquet"},
                    run_counters={
                        "markets_failed": metrics["markets_failed"],
                        "processed_ticker_count": len(processed_tickers),
                    },
                )
            return "failed"

        try:
            raw_candles = self._fetch_market_candles(
                market,
                cutoff_ts,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            (raw_candles_dir / f"{ticker}.json").write_text(json.dumps(raw_candles, indent=2, default=str), encoding="utf-8")
            candles_df = _normalise_candlestick_rows(raw_candles)
            candles_df.write_parquet(normalized_candles_dir / f"{ticker}.parquet")
            metrics["total_candlesticks"] += len(candles_df)
            if tracker is not None:
                tracker.increment_run_counters(normalized_outputs_written=1)
        except Exception as exc:
            logger.warning("Candlestick fetch/write failed for %s: %s", ticker, exc)
            skipped_or_failed_tickers.append({"ticker": ticker, "stage": "candles", "error": str(exc)})

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
                log_line=self._log_stage_progress(
                    "retained_market_processing",
                    "running",
                    started=tracker.run.retained_markets_started,
                    completed=len(processed_tickers),
                    failed=metrics["markets_failed"],
                    raw_written=tracker.run.raw_market_files_written,
                    normalized_written=tracker.run.normalized_outputs_written,
                    ticker=ticker,
                ),
            )
        return "completed"

    def _build_run_summary(
        self,
        *,
        tracker: IngestStatusTracker,
        now_utc: datetime,
        cutoff_ts: dict[str, datetime | None],
        download_diagnostics: dict[str, Any],
        filter_diagnostics: dict[str, Any],
        normalized_markets_written: int,
        resolution_count: int,
        metrics: dict[str, int],
        date_range_start: str | None,
        date_range_end: str | None,
        skipped_or_failed_tickers: list[dict[str, Any]],
        checkpoint: dict[str, Any],
        fail_fast_triggered: bool,
        fail_fast_reason: str | None,
        stop_reason: str | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        effective_stop_reason = stop_reason or download_diagnostics.get("pagination_stop_reason") or "completed"
        return {
            "generated_at": now_utc.isoformat(),
            "run_id": tracker.run.run_id,
            "pipeline_name": tracker.run.pipeline_name,
            "lookback_days": cfg.lookback_days,
            "request_sleep_sec": cfg.request_sleep_sec,
            "authenticated_request_sleep_sec": cfg.authenticated_request_sleep_sec,
            "authenticated_rate_limit": {
                "max_retries": cfg.authenticated_rate_limit_max_retries,
                "backoff_base_sec": cfg.authenticated_rate_limit_backoff_base_sec,
                "backoff_max_sec": cfg.authenticated_rate_limit_backoff_max_sec,
                "jitter_max_sec": cfg.authenticated_rate_limit_jitter_max_sec,
            },
            "streaming_fail_fast": {
                "max_live_pages_without_retained_markets": cfg.max_live_pages_without_retained_markets,
                "max_raw_markets_without_processing": cfg.max_raw_markets_without_processing,
            },
            "feature_period": cfg.feature_period,
            "market_page_size": cfg.market_page_size,
            "trade_page_size": cfg.trade_page_size,
            "output_layout": {
                "raw_markets_dir": cfg.raw_markets_dir,
                "raw_trades_dir": cfg.raw_trades_dir,
                "raw_candles_dir": cfg.raw_candles_dir,
                "normalized_trades_dir": cfg.trades_parquet_dir,
                "normalized_candles_dir": cfg.normalized_candles_dir,
                "normalized_markets_path": cfg.normalized_markets_path,
                "features_dir": cfg.features_dir,
                "resolution_csv_path": cfg.resolution_csv_path,
                "legacy_resolution_csv_path": cfg.legacy_resolution_csv_path,
                "checkpoint_path": cfg.checkpoint_path,
                "status_artifacts_root": cfg.status_artifacts_root,
            },
            "cutoff_timestamps": {
                key: value.isoformat() if isinstance(value, datetime) else None
                for key, value in cutoff_ts.items()
            },
            "markets_downloaded": download_diagnostics.get("total_markets_fetched", 0),
            "markets_after_filters": filter_diagnostics.get("retained_markets", 0),
            "markets_excluded_by_filters": download_diagnostics.get("total_markets_discarded", 0),
            "filter_config": {
                "preferred_categories": cfg.preferred_categories,
                "excluded_series_patterns": cfg.excluded_series_patterns,
                "min_volume": cfg.min_volume,
                "max_markets_per_event": cfg.max_markets_per_event,
            },
            "filter_diagnostics": filter_diagnostics,
            "page_diagnostics_summary": download_diagnostics,
            "normalized_markets_written": normalized_markets_written,
            "markets_with_trades": metrics["markets_with_trades"],
            "markets_skipped_no_trades": metrics["markets_skipped"],
            "markets_failed": metrics["markets_failed"],
            "total_trades": metrics["total_trades"],
            "total_candlesticks": metrics["total_candlesticks"],
            "resolution_count": resolution_count,
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
            "feature_files_written": metrics["feature_files_written"],
            "pages_seen": tracker.run.pages_seen,
            "pages_with_retained_markets": tracker.run.pages_with_retained_markets,
            "pages_without_retained_markets": tracker.run.pages_without_retained_markets,
            "retained_markets_seen": tracker.run.retained_markets_seen,
            "retained_markets_started": tracker.run.retained_markets_started,
            "markets_completed": tracker.run.markets_completed,
            "processed_ticker_count": tracker.run.processed_ticker_count,
            "raw_market_files_written": tracker.run.raw_market_files_written,
            "normalized_outputs_written": tracker.run.normalized_outputs_written,
            "fail_fast_triggered": fail_fast_triggered,
            "fail_fast_reason": fail_fast_reason,
            "stop_reason": effective_stop_reason,
            "resumed_from_run_id": tracker.run.resumed_from_run_id,
            "resumed_from_checkpoint": tracker.run.resumed_from_checkpoint,
            "resumed_stage": tracker.run.resumed_stage,
            "resumed_processed_ticker_count": tracker.run.resumed_processed_ticker_count,
            "replayed_work_skipped": tracker.run.replayed_work_skipped,
            "replayed_work_replayed": tracker.run.replayed_work_replayed,
            "configured_resume_recovery_mode": tracker.run.configured_resume_recovery_mode,
            "resume_cursor": tracker.run.resume_cursor,
            "resume_cursor_retry_count": tracker.run.resume_cursor_retry_count,
            "resume_cursor_last_http_status": tracker.run.resume_cursor_last_http_status,
            "resume_recovery_action": tracker.run.resume_recovery_action,
            "resumed_from_backup_checkpoint": tracker.run.resumed_from_backup_checkpoint,
            "resumed_with_cursor_reset": tracker.run.resumed_with_cursor_reset,
            "backup_checkpoint_recovery_attempted": tracker.run.backup_checkpoint_recovery_attempted,
            "cursor_reset_recovery_attempted": tracker.run.cursor_reset_recovery_attempted,
            "first_retained_processing_milestone": {
                "ticker": download_diagnostics.get("first_retained_processing_ticker"),
                "source": download_diagnostics.get("first_retained_processing_source"),
                "page": download_diagnostics.get("first_retained_processing_page"),
            },
            "checkpoint_summary": {
                "checkpoint_writes": self._checkpoint_write_count,
                "processed_tickers": checkpoint.get("processed_tickers", []),
                "market_download_complete": bool(checkpoint.get("market_download_complete")),
                "pending_retained_markets": [
                    str(_market_key(market, "ticker", "market_ticker") or "")
                    for market in checkpoint.get("pending_retained_markets", [])
                ],
                "failed_tickers": checkpoint.get("failed_tickers", {}),
            },
            "top_error_categories": self._top_error_categories(skipped_or_failed_tickers),
            "skipped_or_failed": skipped_or_failed_tickers,
            "status_artifact_path": str(tracker.status_path),
            "run_summary_artifact_path": str(tracker.summary_path),
        }

    def _run_streaming(self) -> HistoricalIngestResult:
        cfg = self.config
        now_utc = datetime.now(UTC)
        start_dt = now_utc - timedelta(days=cfg.lookback_days)
        tracker = self._build_status_tracker()
        raw_trades_dir = Path(cfg.raw_trades_dir)
        raw_candles_dir = Path(cfg.raw_candles_dir)
        trades_parquet_dir = Path(cfg.trades_parquet_dir)
        normalized_candles_dir = Path(cfg.normalized_candles_dir)
        features_dir = Path(cfg.features_dir)
        checkpoint: dict[str, Any] = {}
        skipped_or_failed_tickers: list[dict[str, Any]] = []
        processed_tickers: set[str] = set()
        all_close_times: list[datetime] = []
        cutoff_ts: dict[str, datetime | None] = {
            "market_settled_ts": None,
            "trades_created_ts": None,
            "orders_updated_ts": None,
        }
        download_diagnostics: dict[str, Any] = {"pagination_stop_reason": None}
        filter_diagnostics: dict[str, Any] = {}
        normalized_markets_written = 0
        resolution_count = 0
        date_range_start: str | None = None
        date_range_end: str | None = None
        self._checkpoint_write_count = 0

        metrics = {
            "markets_with_trades": 0,
            "markets_skipped": 0,
            "markets_failed": 0,
            "total_trades": 0,
            "total_candlesticks": 0,
            "feature_files_written": 0,
        }
        tracker.start_run(current_stage="initialization")
        tracker.set_run_counters(configured_resume_recovery_mode=self._validate_resume_recovery_mode())
        tracker.start_stage("initialization", message="initializing Kalshi historical ingest")
        try:
            self._make_dirs()
            self._init_signals()
            tracker.complete_stage("initialization", message="directories and signals initialized")

            tracker.start_stage("checkpoint_load", message="loading checkpoint state")
            checkpoint = self._load_checkpoint()
            processed_tickers = set(str(ticker) for ticker in checkpoint.get("processed_tickers", []))
            resumed_from_run_id = checkpoint.get("run_id")
            checkpoint["resumed_from_run_id"] = resumed_from_run_id
            checkpoint["run_id"] = tracker.run.run_id
            tracker.set_run_counters(
                resumed_from_run_id=resumed_from_run_id,
                resumed_from_checkpoint=str(self._checkpoint_file_path()) if self.config.resume and self.config.resume_mode != "fresh" else None,
                resumed_stage=checkpoint.get("current_stage") or checkpoint.get("last_completed_stage"),
                resumed_processed_ticker_count=len(processed_tickers),
                replayed_work_skipped=int(checkpoint.get("resume_counters", {}).get("replayed_work_skipped", 0)),
                replayed_work_replayed=int(checkpoint.get("resume_counters", {}).get("replayed_work_replayed", 0)),
                processed_ticker_count=len(processed_tickers),
                markets_completed=len(processed_tickers),
                resume_cursor=checkpoint.get("live_market_cursor"),
                resume_cursor_retry_count=int(checkpoint.get("resume_cursor_retry_count", 0) or 0),
                resume_cursor_last_http_status=checkpoint.get("resume_cursor_last_http_status"),
                resume_recovery_action=checkpoint.get("resume_recovery_action"),
                resumed_from_backup_checkpoint=bool(checkpoint.get("resumed_from_backup_checkpoint", False)),
                resumed_with_cursor_reset=bool(checkpoint.get("resumed_with_cursor_reset", False)),
                configured_resume_recovery_mode=self._validate_resume_recovery_mode(),
                backup_checkpoint_recovery_attempted=bool(checkpoint.get("backup_checkpoint_recovery_attempted", False)),
                cursor_reset_recovery_attempted=bool(checkpoint.get("cursor_reset_recovery_attempted", False)),
            )
            tracker.complete_stage(
                "checkpoint_load",
                message="checkpoint loaded",
                counters={
                    "processed_ticker_count": len(processed_tickers),
                    "market_download_complete": bool(checkpoint.get("market_download_complete")),
                    "pending_retained_markets": len(checkpoint.get("pending_retained_markets", [])),
                    "resume_mode": self.config.resume_mode,
                },
            )
            tracker.start_stage("checkpoint_write", current_stage=False, message="checkpoint tracking initialized")
            self._save_checkpoint(checkpoint, tracker=tracker, message="checkpoint normalized for resume")

            tracker.start_stage("cutoff_discovery", message="fetching cutoff timestamps")
            cutoff_ts = self._fetch_cutoff_timestamps()
            tracker.complete_stage(
                "cutoff_discovery",
                message="cutoff timestamps loaded",
                counters={key: value.isoformat() if isinstance(value, datetime) else None for key, value in cutoff_ts.items()},
            )

            logger.info(
                "Starting Kalshi historical ingest: lookback_days=%d raw_markets_dir=%s raw_trades_dir=%s raw_candles_dir=%s normalized_trades_dir=%s normalized_candles_dir=%s features_dir=%s",
                cfg.lookback_days,
                cfg.raw_markets_dir,
                cfg.raw_trades_dir,
                cfg.raw_candles_dir,
                cfg.trades_parquet_dir,
                cfg.normalized_candles_dir,
                cfg.features_dir,
            )
            logger.info("Fetching settled Kalshi markets closed between %s and %s", start_dt.date(), now_utc.date())

            tracker.start_stage("market_universe_fetch", message="fetching and filtering market universe")
            tracker.start_stage("retained_market_processing", message="waiting for first retained market")
            self._drain_pending_retained_markets(
                checkpoint=checkpoint,
                tracker=tracker,
                cutoff_ts=cutoff_ts,
                start_dt=start_dt,
                end_dt=now_utc,
                raw_trades_dir=raw_trades_dir,
                raw_candles_dir=raw_candles_dir,
                trades_parquet_dir=trades_parquet_dir,
                normalized_candles_dir=normalized_candles_dir,
                features_dir=features_dir,
                processed_tickers=processed_tickers,
                skipped_or_failed_tickers=skipped_or_failed_tickers,
                all_close_times=all_close_times,
                metrics=metrics,
            )
            tracker.set_run_counters(
                replayed_work_skipped=int(checkpoint.get("resume_counters", {}).get("replayed_work_skipped", 0)),
                replayed_work_replayed=int(checkpoint.get("resume_counters", {}).get("replayed_work_replayed", 0)),
                processed_ticker_count=len(processed_tickers),
                markets_completed=len(processed_tickers),
            )
            checkpoint_complete_before_run = bool(checkpoint.get("market_download_complete"))
            download_diagnostics = self._download_market_universe(
                checkpoint=checkpoint,
                start_dt=start_dt,
                end_dt=now_utc,
                cutoff_ts=cutoff_ts,
                tracker=tracker,
                on_retained_market=(
                    None
                    if checkpoint_complete_before_run
                    else lambda market: self._process_market_artifacts(
                        market,
                        checkpoint=checkpoint,
                        cutoff_ts=cutoff_ts,
                        start_dt=start_dt,
                        end_dt=now_utc,
                        raw_trades_dir=raw_trades_dir,
                        raw_candles_dir=raw_candles_dir,
                        trades_parquet_dir=trades_parquet_dir,
                        normalized_candles_dir=normalized_candles_dir,
                        features_dir=features_dir,
                        processed_tickers=processed_tickers,
                        skipped_or_failed_tickers=skipped_or_failed_tickers,
                        all_close_times=all_close_times,
                        metrics=metrics,
                        tracker=tracker,
                    )
                ),
            )
            tracker.complete_stage(
                "market_universe_fetch",
                message=f"market universe fetch stopped via {download_diagnostics.get('pagination_stop_reason') or 'unknown'}",
                counters=download_diagnostics,
            )

            tracker.start_stage("normalization", message="writing normalized market indexes")
            all_markets = [market for market in self._iter_downloaded_markets() if not _is_synthetic_ticker(market.get("ticker"))]
            normalized_markets_written, resolution_count = self._write_market_indexes(all_markets)
            tracker.increment_run_counters(normalized_outputs_written=3)
            markets_to_process, late_filter_diagnostics = self._apply_market_filters_with_diagnostics(all_markets)
            filter_diagnostics = {
                "total_markets_before_filters": download_diagnostics.get("total_markets_fetched", len(all_markets)),
                "retained_markets": len(markets_to_process),
                "excluded_markets_total": download_diagnostics.get("total_markets_discarded", 0),
                "excluded_by_category": download_diagnostics.get("discarded_by_category", 0),
                "excluded_by_series_pattern": download_diagnostics.get("discarded_by_series_pattern", 0),
                "excluded_by_series": download_diagnostics.get("excluded_by_series", download_diagnostics.get("discarded_by_series_pattern", 0)),
                "excluded_by_min_volume": download_diagnostics.get("discarded_by_min_volume", 0),
                "excluded_by_bracket": late_filter_diagnostics.get("excluded_by_bracket", 0),
                "excluded_no_trade_data": download_diagnostics.get("excluded_no_trade_data", 0),
                "excluded_missing_core_fields": download_diagnostics.get("excluded_missing_core_fields", 0),
                "excluded_by_lookback": download_diagnostics.get("excluded_by_lookback", 0),
                "effective_filter_config": late_filter_diagnostics.get("effective_filter_config", {}),
                "pages_fetched": download_diagnostics.get("pages_fetched", 0),
                "pages_with_retained_markets": download_diagnostics.get("pages_with_retained_markets", 0),
                "pages_without_retained_markets": download_diagnostics.get("pages_without_retained_markets", 0),
                "retained_sample_tickers": download_diagnostics.get("retained_sample_tickers", []),
                "last_cursor": download_diagnostics.get("last_cursor"),
                "pagination_stop_reason": download_diagnostics.get("pagination_stop_reason"),
                "first_retained_processing_ticker": download_diagnostics.get("first_retained_processing_ticker"),
                "first_retained_processing_source": download_diagnostics.get("first_retained_processing_source"),
                "first_retained_processing_page": download_diagnostics.get("first_retained_processing_page"),
            }
            tracker.complete_stage("normalization", message="normalized market indexes written", counters={"normalized_markets_written": normalized_markets_written, "resolution_count": resolution_count})

            if not all_markets:
                logger.warning("No Kalshi markets matched early ingest filters after %d fetched markets. Exiting cleanly.", download_diagnostics.get("total_markets_fetched", 0))

            if checkpoint_complete_before_run:
                for market in markets_to_process:
                    self._process_market_artifacts(
                        market,
                        checkpoint=checkpoint,
                        cutoff_ts=cutoff_ts,
                        start_dt=start_dt,
                        end_dt=now_utc,
                        raw_trades_dir=raw_trades_dir,
                        raw_candles_dir=raw_candles_dir,
                        trades_parquet_dir=trades_parquet_dir,
                        normalized_candles_dir=normalized_candles_dir,
                        features_dir=features_dir,
                        processed_tickers=processed_tickers,
                        skipped_or_failed_tickers=skipped_or_failed_tickers,
                        all_close_times=all_close_times,
                        metrics=metrics,
                        tracker=tracker,
                    )

            tracker.complete_stage(
                "retained_market_processing",
                message="retained market processing complete" if tracker.run.retained_markets_started else "no retained markets entered processing",
                counters={"markets_skipped": metrics["markets_skipped"], "feature_files_written": metrics["feature_files_written"]},
            )
            tracker.complete_stage("checkpoint_write", current_stage=False, message="checkpoint writes complete", counters={"checkpoint_writes": self._checkpoint_write_count})

            if all_close_times:
                date_range_start = min(all_close_times).date().isoformat()
                date_range_end = max(all_close_times).date().isoformat()

            summary = self._build_run_summary(
                tracker=tracker,
                now_utc=now_utc,
                cutoff_ts=cutoff_ts,
                download_diagnostics=download_diagnostics,
                filter_diagnostics=filter_diagnostics,
                normalized_markets_written=normalized_markets_written,
                resolution_count=resolution_count,
                metrics=metrics,
                date_range_start=date_range_start,
                date_range_end=date_range_end,
                skipped_or_failed_tickers=skipped_or_failed_tickers,
                checkpoint=checkpoint,
                fail_fast_triggered=False,
                fail_fast_reason=None,
            )
            tracker.start_stage("final_summary", message="writing final summary artifacts")
            manifest_path = Path(cfg.manifest_path)
            summary_path = Path(cfg.summary_path)
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
            summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
            logger.info("Manifest written to %s", manifest_path)
            logger.info("Summary written to %s", summary_path)
            tracker.complete_stage("final_summary", message="final summary written", counters={"manifest_path": str(manifest_path), "summary_path": str(summary_path)})
            tracker.complete_run(stop_reason=summary["stop_reason"], extra_summary=summary)
            return HistoricalIngestResult(
                markets_downloaded=download_diagnostics.get("total_markets_fetched", len(all_markets)),
                markets_with_trades=metrics["markets_with_trades"],
                markets_skipped_no_trades=metrics["markets_skipped"],
                markets_failed=metrics["markets_failed"],
                total_trades=metrics["total_trades"],
                total_candlesticks=metrics["total_candlesticks"],
                resolution_count=resolution_count,
                date_range_start=date_range_start,
                date_range_end=date_range_end,
                feature_files_written=metrics["feature_files_written"],
                normalized_markets_written=normalized_markets_written,
                manifest_path=manifest_path,
                summary_path=summary_path,
                status_artifact_path=tracker.status_path,
                run_summary_artifact_path=tracker.summary_path,
            )
        except KalshiIngestFailFastError as exc:
            download_diagnostics = getattr(exc, "diagnostics", download_diagnostics)
            tracker.fail_stage("market_universe_fetch", error_summary=str(exc), message=f"market universe fetch failed fast: {exc.reason}")
            tracker.fail_run(
                current_stage="market_universe_fetch",
                error_summary=str(exc),
                stop_reason=download_diagnostics.get("pagination_stop_reason") or exc.reason,
                fail_fast_reason=exc.reason,
                extra_summary=self._build_run_summary(
                    tracker=tracker,
                    now_utc=now_utc,
                    cutoff_ts=cutoff_ts,
                    download_diagnostics=download_diagnostics,
                    filter_diagnostics=filter_diagnostics,
                    normalized_markets_written=normalized_markets_written,
                    resolution_count=resolution_count,
                    metrics=metrics,
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    skipped_or_failed_tickers=skipped_or_failed_tickers,
                    checkpoint=checkpoint,
                    fail_fast_triggered=True,
                    fail_fast_reason=exc.reason,
                ),
            )
            raise
        except KalshiResumeCursorRecoveryError as exc:
            tracker.fail_stage("market_universe_fetch", error_summary=str(exc), message="resume cursor recovery failed")
            tracker.fail_run(
                current_stage="market_universe_fetch",
                error_summary=str(exc),
                stop_reason="resume_cursor_recovery_failed",
                extra_summary=self._build_run_summary(
                    tracker=tracker,
                    now_utc=now_utc,
                    cutoff_ts=cutoff_ts,
                    download_diagnostics=download_diagnostics,
                    filter_diagnostics=filter_diagnostics,
                    normalized_markets_written=normalized_markets_written,
                    resolution_count=resolution_count,
                    metrics=metrics,
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    skipped_or_failed_tickers=skipped_or_failed_tickers,
                    checkpoint=checkpoint,
                    fail_fast_triggered=False,
                    fail_fast_reason=None,
                    stop_reason="resume_cursor_recovery_failed",
                ),
            )
            raise
        except Exception as exc:
            tracker.fail_stage(tracker.run.current_stage or "initialization", error_summary=str(exc), message=f"run failed in {tracker.run.current_stage or 'initialization'}")
            tracker.fail_run(
                current_stage=tracker.run.current_stage,
                error_summary=str(exc),
                stop_reason="failed",
                extra_summary=self._build_run_summary(
                    tracker=tracker,
                    now_utc=now_utc,
                    cutoff_ts=cutoff_ts,
                    download_diagnostics=download_diagnostics,
                    filter_diagnostics=filter_diagnostics,
                    normalized_markets_written=normalized_markets_written,
                    resolution_count=resolution_count,
                    metrics=metrics,
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    skipped_or_failed_tickers=skipped_or_failed_tickers,
                    checkpoint=checkpoint,
                    fail_fast_triggered=False,
                    fail_fast_reason=None,
                    stop_reason="failed",
                ),
            )
            raise

    def run(self) -> HistoricalIngestResult:
        """
        Execute the full historical ingest pipeline.

        :returns: :class:`HistoricalIngestResult` with run summary.
        """
        return self._run_streaming()

        from trading_platform.kalshi.features import build_kalshi_features, write_feature_parquet

        cfg = self.config
        self._make_dirs()
        self._init_signals()

        checkpoint = self._load_checkpoint()
        now_utc = datetime.now(UTC)
        start_dt = now_utc - timedelta(days=cfg.lookback_days)
        raw_trades_dir = Path(cfg.raw_trades_dir)
        raw_candles_dir = Path(cfg.raw_candles_dir)
        trades_parquet_dir = Path(cfg.trades_parquet_dir)
        normalized_candles_dir = Path(cfg.normalized_candles_dir)
        features_dir = Path(cfg.features_dir)

        logger.info(
            "Starting Kalshi historical ingest: lookback_days=%d raw_markets_dir=%s raw_trades_dir=%s raw_candles_dir=%s normalized_trades_dir=%s normalized_candles_dir=%s features_dir=%s",
            cfg.lookback_days,
            cfg.raw_markets_dir,
            cfg.raw_trades_dir,
            cfg.raw_candles_dir,
            cfg.trades_parquet_dir,
            cfg.normalized_candles_dir,
            cfg.features_dir,
        )
        logger.info("Fetching settled Kalshi markets closed between %s and %s", start_dt.date(), now_utc.date())

        # ── Step 1: Download all resolved markets ────────────────────────────
        cutoff_ts = self._fetch_cutoff_timestamps()
        self._download_market_universe(
            checkpoint=checkpoint,
            start_dt=start_dt,
            end_dt=now_utc,
            cutoff_ts=cutoff_ts,
        )
        all_markets = [market for market in self._iter_downloaded_markets() if not _is_synthetic_ticker(market.get("ticker"))]
        normalized_markets_written, resolution_count = self._write_market_indexes(all_markets)

        # ── Step 2: Apply market filters ─────────────────────────────────────
        # Filtering happens AFTER market indexes are written so the resolution
        # CSV and normalized markets parquet remain complete.  Only the expensive
        # trade / feature processing step (step 4) is restricted to filtered markets.
        markets_to_process, filter_diagnostics = self._apply_market_filters_with_diagnostics(all_markets)

        skipped_or_failed_tickers: list[dict[str, Any]] = []
        processed_tickers = set(str(ticker) for ticker in checkpoint.get("processed_tickers", []))

        # ── Step 3: Write resolution CSV ────────────────────────────────────
        markets_with_trades = 0

        # ── Step 4: Download trades + build features ─────────────────────────
        markets_with_trades = 0
        markets_skipped = 0
        markets_failed = 0
        total_trades = 0
        total_candlesticks = 0
        feature_files_written = 0
        all_close_times: list[datetime] = []

        for market in markets_to_process:
            ticker = market.get("ticker", "")
            if not ticker:
                continue
            if ticker in processed_tickers:
                logger.info("Skipping already-processed market %s from checkpoint.", ticker)
                continue

            # Collect close time for date range reporting
            close_time_str = market.get("close_time", "")
            close_time = _parse_iso_ts(close_time_str)
            if close_time is not None:
                all_close_times.append(close_time)

            # ── Fetch trades ─────────────────────────────────────────────────
            logger.info("Processing Kalshi market %s (%s)", ticker, market.get("title", ""))
            try:
                raw_trades = self._fetch_market_trades(market, cutoff_ts)
            except Exception as exc:
                logger.warning("Failed to fetch trades for %s: %s", ticker, exc)
                markets_failed += 1
                skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trades", "error": str(exc)})
                continue

            if len(raw_trades) < cfg.min_trades:
                logger.debug("Skipping %s: only %d trades (min %d).", ticker, len(raw_trades), cfg.min_trades)
                markets_skipped += 1
                skipped_or_failed_tickers.append(
                    {"ticker": ticker, "stage": "min_trades", "trade_count": len(raw_trades), "min_trades": cfg.min_trades}
                )
                continue

            markets_with_trades += 1
            total_trades += len(raw_trades)

            (raw_trades_dir / f"{ticker}.json").write_text(json.dumps(raw_trades, indent=2, default=str), encoding="utf-8")

            # ── Convert to parquet ────────────────────────────────────────────
            try:
                trades_df = _trades_to_dataframe(raw_trades)
                trades_df.write_parquet(trades_parquet_dir / f"{ticker}.parquet")
            except Exception as exc:
                logger.warning("Trade parquet write failed for %s: %s", ticker, exc)
                markets_failed += 1
                skipped_or_failed_tickers.append({"ticker": ticker, "stage": "trade_parquet", "error": str(exc)})
                continue

            # ── Build features ────────────────────────────────────────────────
            try:
                raw_candles = self._fetch_market_candles(
                    market,
                    cutoff_ts,
                    start_dt=start_dt,
                    end_dt=now_utc,
                )
                (raw_candles_dir / f"{ticker}.json").write_text(json.dumps(raw_candles, indent=2, default=str), encoding="utf-8")
                candles_df = _normalise_candlestick_rows(raw_candles)
                candles_df.write_parquet(normalized_candles_dir / f"{ticker}.parquet")
                total_candlesticks += len(candles_df)
            except Exception as exc:
                logger.warning("Candlestick fetch/write failed for %s: %s", ticker, exc)
                skipped_or_failed_tickers.append({"ticker": ticker, "stage": "candles", "error": str(exc)})

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
                feature_files_written += 1
            except Exception as exc:
                logger.warning("Feature build failed for %s: %s", ticker, exc)
                skipped_or_failed_tickers.append({"ticker": ticker, "stage": "features", "error": str(exc)})

            processed_tickers.add(ticker)
            checkpoint["processed_tickers"] = sorted(processed_tickers)
            self._save_checkpoint(checkpoint)

        date_range_start: str | None = None
        date_range_end: str | None = None
        if all_close_times:
            date_range_start = min(all_close_times).date().isoformat()
            date_range_end = max(all_close_times).date().isoformat()

        summary = {
            "generated_at": now_utc.isoformat(),
            "lookback_days": cfg.lookback_days,
            "request_sleep_sec": cfg.request_sleep_sec,
            "authenticated_request_sleep_sec": cfg.authenticated_request_sleep_sec,
            "authenticated_rate_limit": {
                "max_retries": cfg.authenticated_rate_limit_max_retries,
                "backoff_base_sec": cfg.authenticated_rate_limit_backoff_base_sec,
                "backoff_max_sec": cfg.authenticated_rate_limit_backoff_max_sec,
                "jitter_max_sec": cfg.authenticated_rate_limit_jitter_max_sec,
            },
            "streaming_fail_fast": {
                "max_live_pages_without_retained_markets": cfg.max_live_pages_without_retained_markets,
                "max_raw_markets_without_processing": cfg.max_raw_markets_without_processing,
            },
            "feature_period": cfg.feature_period,
            "market_page_size": cfg.market_page_size,
            "trade_page_size": cfg.trade_page_size,
            "output_layout": {
                "raw_markets_dir": cfg.raw_markets_dir,
                "raw_trades_dir": cfg.raw_trades_dir,
                "raw_candles_dir": cfg.raw_candles_dir,
                "normalized_trades_dir": cfg.trades_parquet_dir,
                "normalized_candles_dir": cfg.normalized_candles_dir,
                "normalized_markets_path": cfg.normalized_markets_path,
                "features_dir": cfg.features_dir,
                "resolution_csv_path": cfg.resolution_csv_path,
                "legacy_resolution_csv_path": cfg.legacy_resolution_csv_path,
                "checkpoint_path": cfg.checkpoint_path,
            },
            "cutoff_timestamps": {
                key: value.isoformat() if isinstance(value, datetime) else None
                for key, value in cutoff_ts.items()
            },
            "markets_downloaded": len(all_markets),
            "markets_after_filters": len(markets_to_process),
            "markets_excluded_by_filters": len(all_markets) - len(markets_to_process),
            "filter_config": {
                "preferred_categories": cfg.preferred_categories,
                "excluded_series_patterns": cfg.excluded_series_patterns,
                "min_volume": cfg.min_volume,
                "max_markets_per_event": cfg.max_markets_per_event,
            },
            "filter_diagnostics": filter_diagnostics,
            "normalized_markets_written": normalized_markets_written,
            "markets_with_trades": markets_with_trades,
            "markets_skipped_no_trades": markets_skipped,
            "markets_failed": markets_failed,
            "total_trades": total_trades,
            "total_candlesticks": total_candlesticks,
            "resolution_count": resolution_count,
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
            "feature_files_written": feature_files_written,
            "skipped_or_failed": skipped_or_failed_tickers,
        }
        manifest_path = Path(cfg.manifest_path)
        summary_path = Path(cfg.summary_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        logger.info("Manifest written to %s", manifest_path)
        logger.info("Summary written to %s", summary_path)

        return HistoricalIngestResult(
            markets_downloaded=len(all_markets),
            markets_with_trades=markets_with_trades,
            markets_skipped_no_trades=markets_skipped,
            markets_failed=markets_failed,
            total_trades=total_trades,
            total_candlesticks=total_candlesticks,
            resolution_count=resolution_count,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            feature_files_written=feature_files_written,
            normalized_markets_written=normalized_markets_written,
            manifest_path=manifest_path,
            summary_path=summary_path,
        )
