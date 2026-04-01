"""
Kalshi historical data ingestion pipeline.

Downloads all resolved markets from the past year via the Kalshi public
historical API (no auth required) and builds a local research dataset:

    data/kalshi/raw/markets/<ticker>.json    — raw market JSON from API
    data/kalshi/raw/trades/<ticker>.json     — raw trade list JSON from API
    data/kalshi/trades/<ticker>.parquet      — trade DataFrame for feature gen
    data/kalshi/features/<ticker>.parquet    — feature parquet (all groups)
    data/kalshi/raw/resolution.csv           — ticker → resolution_price (0/100)
    data/kalshi/raw/ingest_manifest.json     — run summary

Rate limiting
-------------
Historical endpoints are public but conservative rate limiting is applied:
``request_sleep_sec`` (default 0.2 → 5 req/sec) is applied between every
paginated API call.  This can be overridden per-run via config or CLI.

Graceful degradation
--------------------
If a market has no trade history (thin/illiquid market), it is skipped and
counted in ``skipped_no_trades``.  The pipeline never raises on individual
market failures — it logs and continues.

Resolution encoding
-------------------
``result == "yes"``  → ``resolution_price = 100``
``result == "no"``   → ``resolution_price = 0``
Any other value      → skipped (not included in resolution.csv)

Base rate and Metaculus integration
------------------------------------
When ``run_base_rate=True``, the pipeline loads the base rate database and
injects ``base_rate_prior``, ``base_rate_edge``, and ``base_rate_confidence``
as extra scalar columns into each feature parquet.

When ``run_metaculus=True``, the pipeline loads pre-computed Metaculus
matches (from ``metaculus_matches_path``) and injects those features too.
Metaculus matches are NOT fetched live during ingest (that is done by the
separate ``kalshi-full-backtest`` CLI command which can optionally refresh
them first).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# ── Resolution helpers ────────────────────────────────────────────────────────

def _result_to_price(result: str | None) -> float | None:
    """Convert Kalshi result string to a resolution price (0 or 100)."""
    if result == "yes":
        return 100.0
    if result == "no":
        return 0.0
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

    return {
        "trade_id": raw.get("trade_id", ""),
        "ticker": raw.get("ticker", ""),
        "side": raw.get("taker_side", raw.get("side", "")),
        "yes_price": yes_price,
        "no_price": no_price,
        "count": int(raw.get("count", 0)),
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
            "count": pl.Int64,
            "traded_at": pl.Datetime,
        })
    rows = [_parse_trade_row(t) for t in raw_trades]
    return pl.from_dicts(rows, schema_overrides={
        "yes_price": pl.Float64,
        "no_price": pl.Float64,
        "count": pl.Int64,
        "traded_at": pl.Datetime(time_zone="UTC"),
    })


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class HistoricalIngestConfig:
    """Configuration for the historical ingest pipeline."""

    # Output directories
    raw_markets_dir: str = "data/kalshi/raw/markets"
    raw_trades_dir: str = "data/kalshi/raw/trades"
    trades_parquet_dir: str = "data/kalshi/trades"
    features_dir: str = "data/kalshi/features"
    resolution_csv_path: str = "data/kalshi/raw/resolution.csv"
    manifest_path: str = "data/kalshi/raw/ingest_manifest.json"

    # Lookback window
    lookback_days: int = 365

    # Feature generation
    feature_period: str = "1h"
    min_trades: int = 5    # skip markets with fewer trades than this

    # Rate limiting
    request_sleep_sec: float = 0.2   # 5 req/sec

    # Optional signal enrichment
    run_base_rate: bool = True
    base_rate_db_path: str = "data/kalshi/base_rates/base_rate_db.json"

    run_metaculus: bool = False    # requires pre-built matches file
    metaculus_matches_path: str = "data/kalshi/metaculus/matches.json"
    metaculus_min_confidence: float = 0.70

    # Pagination
    market_page_size: int = 200
    trade_page_size: int = 1000

    # Optional ticker filter (empty = all)
    ticker_filter: list[str] = field(default_factory=list)


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class HistoricalIngestResult:
    """Summary of a completed historical ingest run."""
    markets_downloaded: int
    markets_with_trades: int
    markets_skipped_no_trades: int
    markets_failed: int
    total_trades: int
    resolution_count: int
    date_range_start: str | None
    date_range_end: str | None
    feature_files_written: int
    manifest_path: Path


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

        # Lazy-load signal helpers
        self._base_rate_signal: Any = None
        self._metaculus_signal: Any = None

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
            cfg.trades_parquet_dir,
            cfg.features_dir,
        ]:
            Path(d).mkdir(parents=True, exist_ok=True)

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

    def run(self) -> HistoricalIngestResult:
        """
        Execute the full historical ingest pipeline.

        :returns: :class:`HistoricalIngestResult` with run summary.
        """
        from trading_platform.kalshi.features import build_kalshi_features, write_feature_parquet

        cfg = self.config
        self._make_dirs()
        self._init_signals()

        # Compute lookback window
        now_utc = datetime.now(UTC)
        start_dt = now_utc - timedelta(days=cfg.lookback_days)
        min_close_ts = int(start_dt.timestamp())
        max_close_ts = int(now_utc.timestamp())

        logger.info(
            "Fetching historical markets closed between %s and %s",
            start_dt.date(), now_utc.date()
        )

        # ── Step 1: Download all resolved markets ────────────────────────────
        all_markets: list[dict[str, Any]] = self.client.get_all_historical_markets(
            min_close_ts=min_close_ts,
            max_close_ts=max_close_ts,
        )

        # Apply optional ticker filter
        if cfg.ticker_filter:
            filter_set = set(cfg.ticker_filter)
            all_markets = [m for m in all_markets if m.get("ticker", "") in filter_set]

        logger.info("Downloaded %d resolved markets.", len(all_markets))

        # ── Step 2: Save raw market JSONs + collect resolution data ──────────
        resolution_rows: list[dict[str, Any]] = []
        raw_markets_dir = Path(cfg.raw_markets_dir)

        for market in all_markets:
            ticker = market.get("ticker", "")
            if not ticker:
                continue
            # Save raw JSON
            market_path = raw_markets_dir / f"{ticker}.json"
            market_path.write_text(json.dumps(market, indent=2, default=str), encoding="utf-8")

            result = market.get("result")
            resolution_price = _result_to_price(result)
            if resolution_price is not None:
                resolution_rows.append({
                    "ticker": ticker,
                    "resolution_price": resolution_price,
                    "result": result,
                    "close_time": market.get("close_time", ""),
                })

        # ── Step 3: Write resolution CSV ────────────────────────────────────
        import csv
        resolution_path = Path(cfg.resolution_csv_path)
        resolution_path.parent.mkdir(parents=True, exist_ok=True)
        if resolution_rows:
            with open(resolution_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["ticker", "resolution_price", "result", "close_time"])
                writer.writeheader()
                writer.writerows(resolution_rows)
        logger.info("Wrote %d resolution records to %s.", len(resolution_rows), resolution_path)

        # ── Step 4: Download trades + build features ─────────────────────────
        markets_with_trades = 0
        markets_skipped = 0
        markets_failed = 0
        total_trades = 0
        feature_files_written = 0
        all_close_times: list[datetime] = []

        raw_trades_dir = Path(cfg.raw_trades_dir)
        trades_parquet_dir = Path(cfg.trades_parquet_dir)
        features_dir = Path(cfg.features_dir)

        for market in all_markets:
            ticker = market.get("ticker", "")
            if not ticker:
                continue

            # Collect close time for date range reporting
            close_time_str = market.get("close_time", "")
            close_time: datetime | None = None
            if close_time_str:
                try:
                    close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                    all_close_times.append(close_time)
                except ValueError:
                    pass

            # ── Fetch trades ─────────────────────────────────────────────────
            try:
                time.sleep(cfg.request_sleep_sec)
                raw_trades = self.client.get_all_historical_trades(ticker=ticker)
            except Exception as exc:
                logger.warning("Failed to fetch trades for %s: %s", ticker, exc)
                markets_failed += 1
                continue

            if len(raw_trades) < cfg.min_trades:
                logger.debug("Skipping %s: only %d trades (min %d).", ticker, len(raw_trades), cfg.min_trades)
                markets_skipped += 1
                continue

            markets_with_trades += 1
            total_trades += len(raw_trades)

            # Save raw trades JSON
            trades_json_path = raw_trades_dir / f"{ticker}.json"
            trades_json_path.write_text(
                json.dumps(raw_trades, indent=2, default=str), encoding="utf-8"
            )

            # ── Convert to parquet ────────────────────────────────────────────
            try:
                trades_df = _trades_to_dataframe(raw_trades)
                trades_parquet_path = trades_parquet_dir / f"{ticker}.parquet"
                trades_df.write_parquet(trades_parquet_path)
            except Exception as exc:
                logger.warning("Trade parquet write failed for %s: %s", ticker, exc)
                markets_failed += 1
                continue

            # ── Build features ────────────────────────────────────────────────
            try:
                # Determine last yes-price for scalar signal computation
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
                )
                write_feature_parquet(feat_df, features_dir, ticker)
                feature_files_written += 1
            except Exception as exc:
                logger.warning("Feature build failed for %s: %s", ticker, exc)
                # Don't count as failed — trades were saved successfully

        # ── Step 5: Write manifest ────────────────────────────────────────────
        date_range_start: str | None = None
        date_range_end: str | None = None
        if all_close_times:
            date_range_start = min(all_close_times).date().isoformat()
            date_range_end = max(all_close_times).date().isoformat()

        manifest = {
            "generated_at": now_utc.isoformat(),
            "lookback_days": cfg.lookback_days,
            "markets_downloaded": len(all_markets),
            "markets_with_trades": markets_with_trades,
            "markets_skipped_no_trades": markets_skipped,
            "markets_failed": markets_failed,
            "total_trades": total_trades,
            "resolution_count": len(resolution_rows),
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
            "feature_files_written": feature_files_written,
            "feature_period": cfg.feature_period,
            "request_sleep_sec": cfg.request_sleep_sec,
        }
        manifest_path = Path(cfg.manifest_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        logger.info("Manifest written to %s", manifest_path)

        return HistoricalIngestResult(
            markets_downloaded=len(all_markets),
            markets_with_trades=markets_with_trades,
            markets_skipped_no_trades=markets_skipped,
            markets_failed=markets_failed,
            total_trades=total_trades,
            resolution_count=len(resolution_rows),
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            feature_files_written=feature_files_written,
            manifest_path=manifest_path,
        )
