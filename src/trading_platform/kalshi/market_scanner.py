"""
Kalshi market scanner — scans open markets, runs signals, returns recommendations.

Fetches candles for open Economics/Politics markets, builds features,
runs all 8 signal families, and returns scored results for paper trading.

Usage::

    from trading_platform.kalshi.market_scanner import KalshiMarketScanner
    scanner = KalshiMarketScanner(client)
    results = scanner.scan(["KXFED", "KXCPI"])
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScanResult:
    ticker: str
    yes_price: float
    days_to_close: int
    signal_scores: dict[str, float]
    strongest_signal: str
    recommended_side: str  # "YES" or "NO"
    confidence: float
    news_context: str  # "pre_event", "scheduled_release", "unscheduled"
    kelly_fraction: float


class KalshiMarketScanner:
    """Scan open Kalshi markets and score with all signal families."""

    def __init__(
        self,
        client: Any,
        *,
        sleep_sec: float = 0.15,
        period_interval: int = 60,
    ) -> None:
        self.client = client
        self.sleep_sec = sleep_sec
        self.period_interval = period_interval

    def scan(
        self,
        series_list: list[str],
        *,
        lookback_days: int = 30,
    ) -> list[ScanResult]:
        from trading_platform.kalshi.features import build_kalshi_features
        from trading_platform.kalshi.signal_registry import known_kalshi_signal_families
        from trading_platform.kalshi.live_candle_collector import _candles_to_trades
        from trading_platform.signals.news_tagger import EconomicNewsCalendar

        families = known_kalshi_signal_families()
        now_ts = int(time.time())
        start_ts = now_ts - lookback_days * 86400
        results: list[ScanResult] = []

        # Build news calendar from the tickers we'll scan
        all_tickers: list[str] = []

        for series in series_list:
            # Fetch open events
            try:
                events, _ = self.client.get_events_raw(
                    series_ticker=series, status="open",
                    with_nested_markets=False, limit=200,
                )
            except Exception as exc:
                logger.warning("Failed to fetch events for %s: %s", series, exc)
                continue

            for event in events:
                event_ticker = event.get("event_ticker") or event.get("ticker") or ""
                if not event_ticker:
                    continue

                # Fetch open markets for this event
                try:
                    markets, _ = self.client.get_markets_raw(
                        event_ticker=event_ticker, status="open", limit=200,
                    )
                except Exception as exc:
                    logger.warning("Failed to fetch markets for %s: %s", event_ticker, exc)
                    continue

                for market in markets:
                    ticker = market.get("ticker", "")
                    if not ticker:
                        continue
                    all_tickers.append(ticker)

                    # Fetch candles
                    try:
                        time.sleep(self.sleep_sec)
                        candles = self.client.get_market_candlesticks_raw(
                            ticker,
                            period_interval=self.period_interval,
                            start_ts=start_ts, end_ts=now_ts,
                            series_ticker=series,
                        )
                    except Exception as exc:
                        logger.debug("Candle fetch failed for %s: %s", ticker, exc)
                        continue

                    if not candles:
                        continue

                    # Convert to trades and build features
                    trades_rows = _candles_to_trades(candles)
                    if len(trades_rows) < 5:
                        continue

                    close_time = None
                    ct_raw = market.get("close_time") or market.get("expected_expiration_time")
                    if ct_raw:
                        try:
                            close_time = datetime.fromisoformat(str(ct_raw).replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            pass

                    try:
                        trades_df = pl.DataFrame(trades_rows)
                        feat_df = build_kalshi_features(
                            trades_df, ticker=ticker, period="1h",
                            close_time=close_time,
                            feature_groups=["probability_calibration", "volume_activity", "time_decay"],
                        )
                    except Exception as exc:
                        logger.debug("Feature build failed for %s: %s", ticker, exc)
                        continue

                    if feat_df.is_empty():
                        continue

                    # Run signals
                    result = self._score_market(
                        ticker, feat_df, families, close_time,
                    )
                    if result:
                        results.append(result)

        # Enrich with news context
        if all_tickers:
            cal = EconomicNewsCalendar(all_tickers)
            now = datetime.now(tz=timezone.utc)
            for i, r in enumerate(results):
                ctx = cal.label_market_move(r.ticker, now)
                results[i] = ScanResult(
                    ticker=r.ticker, yes_price=r.yes_price,
                    days_to_close=r.days_to_close,
                    signal_scores=r.signal_scores,
                    strongest_signal=r.strongest_signal,
                    recommended_side=r.recommended_side,
                    confidence=r.confidence,
                    news_context=ctx,
                    kelly_fraction=r.kelly_fraction,
                )

        results.sort(key=lambda r: r.confidence, reverse=True)
        return results

    @staticmethod
    def _score_market(
        ticker: str,
        feat_df: pl.DataFrame,
        families: dict[str, Any],
        close_time: datetime | None,
    ) -> ScanResult | None:
        import pandas as pd
        pdf = feat_df.to_pandas()
        if pdf.empty:
            return None

        last_row = pdf.iloc[-1]
        yes_price = float(last_row.get("close", 50))
        days_to_close = int(last_row.get("days_to_close", 0)) if pd.notna(last_row.get("days_to_close")) else 0

        signal_scores: dict[str, float] = {}
        for name, family in families.items():
            try:
                sig_df = family.build_signal_frame(pdf)
                if sig_df is not None and not sig_df.empty:
                    last_sig = sig_df.iloc[-1]
                    score = float(last_sig.get("signal_value", 0))
                    if pd.notna(score):
                        signal_scores[name] = score
            except Exception:
                continue

        if not signal_scores:
            return None

        # Universe filter: skip markets too far out or near resolution
        if days_to_close is not None and days_to_close > 60:
            return None
        if yes_price < 10 or yes_price > 90:
            return None

        # Find strongest signal
        strongest = max(signal_scores, key=lambda k: abs(signal_scores[k]))
        strongest_score = signal_scores[strongest]
        # Normalize to 0-1 using a wider range (scores can be -10 to +10)
        # Use sigmoid-like mapping: abs(score) of 1.0 → ~0.46, 2.0 → ~0.73, 3.0 → ~0.86
        raw_abs = abs(strongest_score)
        confidence = raw_abs / (raw_abs + 2.0)  # hyperbolic normalization
        recommended_side = "YES" if strongest_score > 0 else "NO"
        kelly = min(0.02, confidence * 0.05)  # max 2% per trade

        return ScanResult(
            ticker=ticker,
            yes_price=yes_price,
            days_to_close=days_to_close,
            signal_scores=signal_scores,
            strongest_signal=strongest,
            recommended_side=recommended_side,
            confidence=round(confidence, 3),
            news_context="unscheduled",  # enriched later
            kelly_fraction=round(kelly, 4),
        )
