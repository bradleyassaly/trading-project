"""
Kalshi live candle collector.

Fetches hourly candlestick data for open Economics/Politics markets
via the authenticated ``/series/{series}/markets/{ticker}/candlesticks``
endpoint, then builds feature parquets using the same pipeline as the
historical ingest.

The historical candlestick endpoint (``/historical/markets/…``) is not
available on free API tiers, so this collector works forward-only on
currently open markets.

Usage::

    from trading_platform.kalshi.live_candle_collector import KalshiLiveCandleCollector
    collector = KalshiLiveCandleCollector(client, series_tickers=["KXCPI", "KXFED"])
    result = collector.run_once("data/kalshi/live", lookback_days=30)
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _extract_dollar(obj: dict[str, Any], key: str) -> float | None:
    """Safely extract a dollar string like ``"0.2600"`` as a float."""
    val = obj.get(key)
    if val is None:
        return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _candles_to_trades(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Kalshi live API candle format to trades rows.

    The live API returns nested candles like::

        {
          "end_period_ts": 1772686800,       # Unix timestamp (int)
          "price": {"close_dollars": "0.26", ...},
          "volume_fp": "1.00",
          ...
        }

    We extract one trade row per candle with the close price.
    """
    rows: list[dict[str, Any]] = []
    for c in candles:
        # Parse timestamp (Unix int or ISO string)
        ts_raw = c.get("end_period_ts")
        if ts_raw is None:
            continue
        try:
            if isinstance(ts_raw, (int, float)):
                dt = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
            else:
                raw = str(ts_raw).strip()
                try:
                    dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
                except ValueError:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, TypeError, OSError):
            continue

        # Extract price — priority: bid/ask midpoint > price.close_dollars > price.previous_dollars
        price_val = None

        # 1. Bid/ask midpoint (most reliable for live markets)
        bid_obj = c.get("yes_bid")
        ask_obj = c.get("yes_ask")
        bid_val = _extract_dollar(bid_obj, "close_dollars") if isinstance(bid_obj, dict) else None
        ask_val = _extract_dollar(ask_obj, "close_dollars") if isinstance(ask_obj, dict) else None
        if bid_val is not None and ask_val is not None:
            price_val = (bid_val + ask_val) / 2.0
        elif bid_val is not None:
            price_val = bid_val
        elif ask_val is not None:
            price_val = ask_val

        # 2. Trade price from price object
        if price_val is None:
            price_obj = c.get("price")
            if isinstance(price_obj, dict):
                for key in ("close_dollars", "mean_dollars", "open_dollars", "previous_dollars"):
                    price_val = _extract_dollar(price_obj, key)
                    if price_val is not None:
                        break

        # 3. Flat fallback
        if price_val is None:
            for key in ("close_price_dollars", "close_price", "close"):
                try:
                    price_val = float(c[key])
                    break
                except (KeyError, TypeError, ValueError):
                    continue

        if price_val is None or price_val <= 0:
            continue

        # Volume
        vol = 1
        vol_raw = c.get("volume_fp") or c.get("volume")
        if vol_raw is not None:
            try:
                vol = max(1, int(float(vol_raw)))
            except (TypeError, ValueError):
                pass

        rows.append({
            "traded_at": dt,
            "yes_price": price_val,
            "count": vol,
        })
    return rows


@dataclass
class LiveCandleResult:
    series_scanned: int = 0
    events_found: int = 0
    markets_found: int = 0
    markets_with_candles: int = 0
    candles_fetched: int = 0
    feature_files_written: int = 0


class KalshiLiveCandleCollector:
    """Collect candles for open Kalshi markets via the live series endpoint."""

    def __init__(
        self,
        client: Any,
        *,
        series_tickers: list[str] | None = None,
        period_interval: int = 60,
        sleep_sec: float = 0.15,
    ) -> None:
        self.client = client
        self.series_tickers = series_tickers or [
            "KXCPI", "KXFED", "KXGDP", "KXJOBS", "KXPCE", "KXINFL",
        ]
        self.period_interval = period_interval
        self.sleep_sec = sleep_sec

    def run_once(
        self,
        output_dir: str | Path,
        *,
        lookback_days: int = 30,
    ) -> LiveCandleResult:
        from trading_platform.kalshi.features import build_kalshi_features

        output_dir = Path(output_dir)
        candles_dir = output_dir / "candles"
        features_dir = output_dir / "features"
        candles_dir.mkdir(parents=True, exist_ok=True)
        features_dir.mkdir(parents=True, exist_ok=True)

        result = LiveCandleResult()
        now = int(time.time())
        start_ts = now - lookback_days * 86400

        for series in self.series_tickers:
            result.series_scanned += 1
            logger.info("Scanning series %s for open events...", series)

            # Get open events for this series
            event_tickers: list[str] = []
            cursor: str | None = None
            while True:
                events, cursor = self.client.get_events_raw(
                    series_ticker=series,
                    status="open",
                    with_nested_markets=False,
                    limit=200,
                    cursor=cursor,
                )
                for ev in events:
                    et = ev.get("event_ticker") or ev.get("ticker") or ""
                    if et:
                        event_tickers.append(et)
                if not cursor:
                    break
            result.events_found += len(event_tickers)

            # Get open markets for each event
            for event_ticker in event_tickers:
                mkt_cursor: str | None = None
                while True:
                    markets, mkt_cursor = self.client.get_markets_raw(
                        event_ticker=event_ticker,
                        status="open",
                        limit=200,
                        cursor=mkt_cursor,
                    )
                    for market in markets:
                        ticker = market.get("ticker", "")
                        if not ticker:
                            continue
                        result.markets_found += 1

                        # Fetch candles
                        try:
                            time.sleep(self.sleep_sec)
                            candles = self.client.get_market_candlesticks_raw(
                                ticker,
                                period_interval=self.period_interval,
                                start_ts=start_ts,
                                end_ts=now,
                                series_ticker=series,
                            )
                        except Exception as exc:
                            logger.warning("Candle fetch failed for %s: %s", ticker, exc)
                            continue

                        if not candles:
                            logger.debug("No candles for %s", ticker)
                            continue

                        result.markets_with_candles += 1
                        result.candles_fetched += len(candles)

                        # Save raw candles
                        (candles_dir / f"{ticker}.json").write_text(
                            json.dumps(candles, indent=2, default=str), encoding="utf-8",
                        )

                        # Convert candles to trades and build features
                        try:
                            trades_rows = _candles_to_trades(candles)
                            if len(trades_rows) < 3:
                                continue

                            import polars as pl
                            trades_df = pl.DataFrame(trades_rows)

                            # Extract close_time for days_to_close computation
                            close_time = None
                            ct_raw = market.get("close_time") or market.get("expected_expiration_time")
                            if ct_raw:
                                try:
                                    close_time = datetime.fromisoformat(
                                        str(ct_raw).replace("Z", "+00:00")
                                    )
                                except (ValueError, AttributeError):
                                    pass

                            feat_df = build_kalshi_features(
                                trades_df,
                                ticker=ticker,
                                period="1h",
                                close_time=close_time,
                                feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                            )
                            if feat_df.is_empty():
                                continue
                            feat_df.write_parquet(features_dir / f"{ticker}.parquet")
                            result.feature_files_written += 1
                            logger.info(
                                "%s: %d candles → feature parquet written",
                                ticker, len(candles),
                            )
                        except Exception as exc:
                            logger.warning("Feature build failed for %s: %s", ticker, exc)

                    if not mkt_cursor:
                        break

        return result

    def run_loop(
        self,
        output_dir: str | Path,
        *,
        lookback_days: int = 30,
        interval_minutes: int = 60,
    ) -> None:
        """Run collection in a loop with a sleep interval."""
        while True:
            try:
                result = self.run_once(output_dir, lookback_days=lookback_days)
                logger.info(
                    "Live candle collection complete: %d series, %d markets, %d candles, %d features",
                    result.series_scanned, result.markets_found,
                    result.candles_fetched, result.feature_files_written,
                )
            except Exception as exc:
                logger.error("Live candle collection failed: %s", exc)
            logger.info("Sleeping %d minutes before next collection...", interval_minutes)
            time.sleep(interval_minutes * 60)
