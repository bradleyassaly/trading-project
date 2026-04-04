"""Tests for Kalshi live candle collector."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from trading_platform.kalshi.live_candle_collector import (
    KalshiLiveCandleCollector,
    LiveCandleResult,
)


class FakeLiveCandleClient:
    """Minimal fake client for testing the live candle collector."""

    def __init__(
        self,
        events_by_series: dict[str, list[dict]] | None = None,
        markets_by_event: dict[str, list[dict]] | None = None,
        candles_by_ticker: dict[str, list[dict]] | None = None,
    ) -> None:
        self._events = events_by_series or {}
        self._markets = markets_by_event or {}
        self._candles = candles_by_ticker or {}

    def get_events_raw(self, **kw: Any) -> tuple[list[dict], str | None]:
        st = kw.get("series_ticker", "")
        return self._events.get(st, []), None

    def get_markets_raw(self, **kw: Any) -> tuple[list[dict], str | None]:
        et = kw.get("event_ticker", "")
        return self._markets.get(et, []), None

    def get_market_candlesticks_raw(self, ticker: str, **kw: Any) -> list[dict]:
        return list(self._candles.get(ticker, []))


def _candle(ts: int, price: float = 0.55) -> dict:
    """Produce a candle matching the real Kalshi live API format."""
    return {
        "end_period_ts": ts,
        "price": {
            "close_dollars": f"{price:.4f}",
            "open_dollars": f"{price - 0.01:.4f}",
            "high_dollars": f"{price + 0.02:.4f}",
            "low_dollars": f"{price - 0.02:.4f}",
            "mean_dollars": f"{price:.4f}",
            "previous_dollars": f"{price:.4f}",
        },
        "yes_bid": {"close_dollars": f"{price - 0.01:.4f}"},
        "yes_ask": {"close_dollars": f"{price + 0.01:.4f}"},
        "volume_fp": "5.00",
        "open_interest_fp": "100.00",
    }


class TestLiveCandleCollector:
    def test_happy_path(self, tmp_path: Path) -> None:
        base_ts = 1700000000
        candles = [_candle(base_ts + i * 3600) for i in range(20)]

        client = FakeLiveCandleClient(
            events_by_series={"KXFED": [{"event_ticker": "KXFED-27APR"}]},
            markets_by_event={"KXFED-27APR": [{"ticker": "KXFED-27APR-T4.25"}]},
            candles_by_ticker={"KXFED-27APR-T4.25": candles},
        )
        collector = KalshiLiveCandleCollector(client, series_tickers=["KXFED"])
        result = collector.run_once(tmp_path, lookback_days=30)

        assert result.series_scanned == 1
        assert result.events_found == 1
        assert result.markets_found == 1
        assert result.markets_with_candles == 1
        assert result.candles_fetched == 20

        # Raw candles saved
        assert (tmp_path / "candles" / "KXFED-27APR-T4.25.json").exists()

        # Feature parquet written
        assert result.feature_files_written == 1
        assert (tmp_path / "features" / "KXFED-27APR-T4.25.parquet").exists()

    def test_no_events(self, tmp_path: Path) -> None:
        client = FakeLiveCandleClient(events_by_series={})
        collector = KalshiLiveCandleCollector(client, series_tickers=["KXCPI"])
        result = collector.run_once(tmp_path)

        assert result.events_found == 0
        assert result.markets_found == 0

    def test_no_candles(self, tmp_path: Path) -> None:
        client = FakeLiveCandleClient(
            events_by_series={"KXCPI": [{"event_ticker": "KXCPI-26MAY"}]},
            markets_by_event={"KXCPI-26MAY": [{"ticker": "KXCPI-26MAY-T0.3"}]},
            candles_by_ticker={},
        )
        collector = KalshiLiveCandleCollector(client, series_tickers=["KXCPI"])
        result = collector.run_once(tmp_path)

        assert result.markets_found == 1
        assert result.markets_with_candles == 0
        assert result.feature_files_written == 0

    def test_multiple_series(self, tmp_path: Path) -> None:
        base_ts = 1700000000
        candles = [_candle(base_ts + i * 3600) for i in range(15)]

        client = FakeLiveCandleClient(
            events_by_series={
                "KXCPI": [{"event_ticker": "KXCPI-EV1"}],
                "KXFED": [{"event_ticker": "KXFED-EV1"}],
            },
            markets_by_event={
                "KXCPI-EV1": [{"ticker": "KXCPI-MKT1"}],
                "KXFED-EV1": [{"ticker": "KXFED-MKT1"}],
            },
            candles_by_ticker={
                "KXCPI-MKT1": candles,
                "KXFED-MKT1": candles,
            },
        )
        collector = KalshiLiveCandleCollector(
            client, series_tickers=["KXCPI", "KXFED"],
        )
        result = collector.run_once(tmp_path)

        assert result.series_scanned == 2
        assert result.feature_files_written == 2
