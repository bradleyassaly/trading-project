"""Tests for Kalshi market scanner."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest

from trading_platform.kalshi.market_scanner import KalshiMarketScanner, ScanResult


class FakeScanClient:
    def __init__(
        self,
        events: list[dict] | None = None,
        markets: list[dict] | None = None,
        candles: list[dict] | None = None,
    ) -> None:
        self._events = events or []
        self._markets = markets or []
        self._candles = candles or []

    def get_events_raw(self, **kw: Any) -> tuple[list[dict], str | None]:
        return self._events, None

    def get_markets_raw(self, **kw: Any) -> tuple[list[dict], str | None]:
        return self._markets, None

    def get_market_candlesticks_raw(self, ticker: str, **kw: Any) -> list[dict]:
        return list(self._candles)


def _candle(ts: int, price: float = 0.55) -> dict:
    from datetime import datetime as dt, timezone as tz
    iso = dt.fromtimestamp(ts, tz=tz.utc).isoformat()
    return {
        "end_period_ts": ts,
        "price": {"close_dollars": f"{price:.4f}", "open_dollars": f"{price:.4f}",
                  "high_dollars": f"{price:.4f}", "low_dollars": f"{price:.4f}"},
        "yes_bid": {"close_dollars": f"{price - 0.01:.4f}"},
        "yes_ask": {"close_dollars": f"{price + 0.01:.4f}"},
        "volume_fp": "5.00",
    }


class TestKalshiMarketScanner:
    def test_scan_returns_results(self) -> None:
        import time as _time
        now = int(_time.time())
        # Candles from last 30 hours, close in 30 days
        candles = [_candle(now - (30 - i) * 3600, 0.50 + i * 0.005) for i in range(30)]
        from datetime import datetime as _dt, timedelta, timezone as _tz
        close_iso = (_dt.now(tz=_tz.utc) + timedelta(days=30)).isoformat()

        client = FakeScanClient(
            events=[{"event_ticker": "KXFED-27APR"}],
            markets=[{"ticker": "KXFED-27APR-T4.25", "close_time": close_iso}],
            candles=candles,
        )
        scanner = KalshiMarketScanner(client, sleep_sec=0)
        results = scanner.scan(["KXFED"], lookback_days=30)

        assert len(results) >= 1
        r = results[0]
        assert r.ticker == "KXFED-27APR-T4.25"
        assert isinstance(r.signal_scores, dict)
        assert r.strongest_signal != ""
        assert r.recommended_side in ("YES", "NO")
        assert 0 <= r.confidence <= 1
        assert r.kelly_fraction <= 0.05

    def test_scan_empty_series(self) -> None:
        client = FakeScanClient(events=[], markets=[], candles=[])
        scanner = KalshiMarketScanner(client, sleep_sec=0)
        results = scanner.scan(["KXCPI"])
        assert results == []

    def test_scan_no_candles_skipped(self) -> None:
        client = FakeScanClient(
            events=[{"event_ticker": "KXCPI-26MAY"}],
            markets=[{"ticker": "KXCPI-26MAY-T0.3"}],
            candles=[],
        )
        scanner = KalshiMarketScanner(client, sleep_sec=0)
        results = scanner.scan(["KXCPI"])
        assert results == []

    def test_scan_result_dataclass(self) -> None:
        r = ScanResult(
            ticker="T1", yes_price=55.0, days_to_close=10,
            signal_scores={"cal_drift": 1.2}, strongest_signal="cal_drift",
            recommended_side="YES", confidence=0.4,
            news_context="unscheduled", kelly_fraction=0.04,
        )
        assert r.ticker == "T1"
        assert r.kelly_fraction == 0.04
