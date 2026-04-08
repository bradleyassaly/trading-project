"""
Tests for the pmxt-backed MarketDataService.

Mocks the pmxt exchange object so the tests run without Node.js, the
sidecar, or any network access. Verifies the pure-function transforms
(velocity / imbalance / baseline) and the fallback path through
fusion_score when pmxt is unavailable.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from trading_platform.polymarket import fusion_score
from trading_platform.polymarket.market_data_service import (
    MarketDataService,
    _EMPTY_BASELINE,
    _EMPTY_BOOK,
    _EMPTY_VELOCITY,
)


# ── Test fixtures ────────────────────────────────────────────────────────────


def _bootstrap_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "wi.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE outcome_id_cache (
            condition_id TEXT NOT NULL,
            market_slug TEXT,
            direction TEXT NOT NULL,
            outcome_id TEXT,
            cached_at INTEGER NOT NULL,
            PRIMARY KEY (condition_id, direction)
        );
        CREATE TABLE pmxt_health (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_call_at INTEGER,
            last_success_at INTEGER,
            total_calls INTEGER DEFAULT 0,
            total_errors INTEGER DEFAULT 0,
            cache_hits INTEGER DEFAULT 0,
            cache_misses INTEGER DEFAULT 0,
            last_error TEXT
        );
        INSERT INTO pmxt_health (id) VALUES (1);
        """
    )
    conn.commit()
    conn.close()
    return db_path


@dataclass
class _Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class _Level:
    price: float
    size: float


@dataclass
class _Book:
    bids: list[_Level]
    asks: list[_Level]


@dataclass
class _Outcome:
    label: str
    outcome_id: str
    price: float = 0.5


@dataclass
class _Market:
    question: str
    outcomes: list[_Outcome]


class _FakeExchange:
    """Minimal stand-in for pmxt.Polymarket()."""

    def __init__(self, *, candles=None, book=None, market=None, raise_on=None):
        self.candles = candles or []
        self.book = book
        self.market = market
        self.raise_on: set[str] = set(raise_on or [])

    def fetch_market(self, *args, **kwargs):
        if "fetch_market" in self.raise_on:
            raise RuntimeError("simulated fetch_market failure")
        return self.market

    def fetch_ohlcv(self, *args, **kwargs):
        if "fetch_ohlcv" in self.raise_on:
            raise RuntimeError("simulated ohlcv failure")
        return self.candles

    def fetch_order_book(self, *args, **kwargs):
        if "fetch_order_book" in self.raise_on:
            raise RuntimeError("simulated book failure")
        return self.book


def _make_candles(prices: list[float], volumes: list[float] | None = None,
                  start_ts: int = 1_700_000_000, step: int = 3600) -> list[_Candle]:
    vols = volumes or [100.0] * len(prices)
    candles: list[_Candle] = []
    for i, (p, v) in enumerate(zip(prices, vols)):
        candles.append(_Candle(
            timestamp=start_ts + i * step,
            open=p, high=p * 1.01, low=p * 0.99, close=p, volume=v,
        ))
    return candles


# ── Pure-function math ──────────────────────────────────────────────────────


class TestVelocityFromCandles:
    def test_empty_candles(self):
        out = MarketDataService._velocity_from_candles([])
        assert out["available"] is False
        assert out["candles"] == []

    def test_velocity_basic(self):
        # Flat 0.5 then jump to 0.6 in last hour
        prices = [0.50] * 23 + [0.60]
        vols = [10.0] * 23 + [50.0]
        out = MarketDataService._velocity_from_candles(_make_candles(prices, vols))
        assert out["current_price"] == 0.60
        assert out["price_1h_ago"] == 0.50
        assert out["velocity_1h"] == pytest.approx(0.20, abs=1e-4)
        assert out["volume_1h"] == 50.0
        # 24h volume = 23*10 + 50 = 280, hourly avg = 280/24 ≈ 11.67, ratio ≈ 4.29
        assert out["volume_ratio"] == pytest.approx(50.0 / (280.0 / 24.0), abs=1e-3)
        assert out["available"] is True

    def test_volume_ratio_null_when_zero_volume(self):
        out = MarketDataService._velocity_from_candles(
            _make_candles([0.5, 0.5, 0.5], [0, 0, 0])
        )
        assert out["volume_ratio"] is None


class TestImbalanceFromBook:
    def test_empty_book(self):
        out = MarketDataService._imbalance_from_book(_Book(bids=[], asks=[]))
        assert out["available"] is False

    def test_balanced_book(self):
        book = _Book(
            bids=[_Level(0.50, 100), _Level(0.49, 100), _Level(0.48, 100)],
            asks=[_Level(0.51, 100), _Level(0.52, 100), _Level(0.53, 100)],
        )
        out = MarketDataService._imbalance_from_book(book)
        assert out["available"] is True
        assert out["best_bid"] == 0.50
        assert out["best_ask"] == 0.51
        assert out["spread"] == pytest.approx(0.01, abs=1e-4)
        # Roughly balanced — imbalance close to 0 (bids slightly heavier
        # since they're at slightly lower prices)
        assert -0.1 <= out["imbalance"] <= 0.1

    def test_buy_pressure(self):
        book = _Book(
            bids=[_Level(0.50, 1000), _Level(0.49, 1000), _Level(0.48, 1000)],
            asks=[_Level(0.51, 100)],
        )
        out = MarketDataService._imbalance_from_book(book)
        # Bid depth dominates
        assert out["imbalance"] > 0.5

    def test_dict_format_book(self):
        book = {"bids": [{"price": 0.5, "size": 100}], "asks": [{"price": 0.51, "size": 100}]}
        out = MarketDataService._imbalance_from_book(book)
        assert out["available"] is True
        assert out["best_bid"] == 0.50


class TestBaselineFromCandles:
    def test_empty_returns_default(self):
        out = MarketDataService._baseline_from_candles([], 1_700_000_000)
        assert out == _EMPTY_BASELINE

    def test_whale_moves_market(self):
        # 5-min candles: flat 0.40 for an hour, then jumps to 0.55 at entry
        ts0 = 1_700_000_000
        candles = []
        for i in range(13):
            ts = ts0 + i * 300
            p = 0.40 if i < 12 else 0.55
            candles.append(_Candle(timestamp=ts, open=p, high=p, low=p, close=p, volume=10))
        out = MarketDataService._baseline_from_candles(candles, ts0 + 12 * 300)
        assert out["available"] is True
        assert out["price_before"] == pytest.approx(0.40)
        assert out["price_at_entry"] == pytest.approx(0.55)
        assert out["whale_impact"] == pytest.approx(0.15, abs=1e-4)
        assert out["info_already_priced"] is False

    def test_info_already_priced_in(self):
        # Price runs 0.40 → 0.65 in first 6 buckets (30 min) and PLATEAUS
        # at 0.65 from bucket 6 onward. The whale enters at bucket 12, so
        # the 30-min-before snapshot is bucket 6 (0.65) → no impact.
        ts0 = 1_700_000_000
        prices = [0.40, 0.45, 0.50, 0.55, 0.60, 0.62, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65]
        candles = [
            _Candle(timestamp=ts0 + i * 300, open=p, high=p, low=p, close=p, volume=10)
            for i, p in enumerate(prices)
        ]
        # Whale enters at index 12
        out = MarketDataService._baseline_from_candles(candles, ts0 + 12 * 300)
        assert out["whale_impact"] == pytest.approx(0.0, abs=1e-4)
        assert out["info_already_priced"] is True


class TestMarketQualityScore:
    def test_score_in_unit_interval(self):
        velocity = {
            "current_price": 0.5, "volume_ratio": 2.0, "available": True,
        }
        book = {"spread_pct": 0.01, "available": True}
        baseline = dict(_EMPTY_BASELINE)
        score = MarketDataService._market_quality_score(velocity, book, baseline)
        assert score is not None
        assert 0.0 <= score <= 1.0

    def test_score_high_for_quality_market(self):
        velocity = {"current_price": 0.5, "volume_ratio": 4.0, "available": True}
        book = {"spread_pct": 0.01, "available": True}
        baseline = {"available": True, "info_already_priced": False, "whale_impact": 0.01}
        score = MarketDataService._market_quality_score(velocity, book, baseline)
        assert score is not None
        assert score >= 0.85

    def test_score_low_when_info_priced(self):
        # Worst-case market: wide spread + dead volume + extreme price + whale late.
        # All four components contribute their lowest values.
        velocity = {"current_price": 0.97, "volume_ratio": 0.3, "available": True}
        book = {"spread_pct": 0.15, "available": True}
        baseline = {"available": True, "info_already_priced": True, "whale_impact": 0.0}
        score = MarketDataService._market_quality_score(velocity, book, baseline)
        assert score is not None
        assert score <= 0.35

    def test_score_none_when_no_components(self):
        score = MarketDataService._market_quality_score(
            dict(_EMPTY_VELOCITY), dict(_EMPTY_BOOK), dict(_EMPTY_BASELINE)
        )
        assert score is None


# ── Service class with mocked exchange ─────────────────────────────────────


class TestServiceMockedExchange:
    def test_fallback_when_pmxt_unavailable(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        # Simulate pmxt missing
        svc._exchange_failed = True
        out = svc.get_price_velocity("any_token")
        assert out == _EMPTY_VELOCITY

    def test_get_price_velocity_with_mock(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        svc._exchange = _FakeExchange(
            candles=_make_candles([0.45] * 20 + [0.50, 0.50, 0.50, 0.55])
        )
        out = svc.get_price_velocity("token_x")
        assert out["available"] is True
        assert out["current_price"] == 0.55
        assert out["velocity_1h"] is not None

    def test_get_price_velocity_handles_exception(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        svc._exchange = _FakeExchange(raise_on={"fetch_ohlcv"})
        out = svc.get_price_velocity("token_x")
        assert out == _EMPTY_VELOCITY

    def test_get_order_book_with_mock(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        svc._exchange = _FakeExchange(
            book=_Book(
                bids=[_Level(0.50, 1000)],
                asks=[_Level(0.51, 100)],
            )
        )
        out = svc.get_order_book_imbalance("token_x")
        assert out["available"] is True
        assert out["imbalance"] > 0.5

    def test_outcome_id_cache_hit(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        # Pre-populate the cache
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            """INSERT INTO outcome_id_cache
               (condition_id, market_slug, direction, outcome_id, cached_at)
               VALUES ('0xabc', 'slug', 'YES', 'cached_outcome', ?)""",
            (int(time.time()),),
        )
        conn.commit()
        conn.close()

        svc = MarketDataService(str(db_path))
        # Sentinel: any pmxt call would fail, but cache should short-circuit
        svc._exchange_failed = True
        out = svc.resolve_outcome_id(condition_id="0xabc", direction="YES")
        assert out == "cached_outcome"
        # Cache hit should be recorded
        h = svc.get_health()
        assert h["cache_hits"] >= 1

    def test_resolve_outcome_id_writes_cache(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        svc._exchange = _FakeExchange(
            market=_Market(
                question="Test",
                outcomes=[
                    _Outcome(label="Yes", outcome_id="yes_id_123"),
                    _Outcome(label="No", outcome_id="no_id_456"),
                ],
            )
        )
        out = svc.resolve_outcome_id(condition_id="0xnewcid", direction="YES")
        assert out == "yes_id_123"
        # Verify it's now in the SQLite cache
        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT outcome_id FROM outcome_id_cache WHERE condition_id='0xnewcid' AND direction='YES'"
        ).fetchone()
        conn.close()
        assert row is not None and row[0] == "yes_id_123"

    def test_get_market_microstructure_combines_all(self, tmp_path: Path):
        db_path = _bootstrap_db(tmp_path)
        svc = MarketDataService(str(db_path))
        svc._exchange = _FakeExchange(
            candles=_make_candles([0.50] * 24, [10.0] * 24),
            book=_Book(
                bids=[_Level(0.49, 500)],
                asks=[_Level(0.51, 500)],
            ),
        )
        out = svc.get_market_microstructure(outcome_id="token_x")
        assert out["available"] is True
        assert out["velocity"]["available"] is True
        assert out["order_book"]["available"] is True
        assert out["market_quality_score"] is not None


# ── Fusion score fallback path ──────────────────────────────────────────────


class TestFusionFallback:
    def test_compute_fusion_uses_basic_when_no_microstructure(self):
        result = fusion_score.compute_fusion(
            wallet_wr=0.65, wallet_tier="tier1h",
            trade_size_usd=2000, wallet_avg_bet_usd=2000,
            market_volume_usd=150_000, current_price=0.55,
            days_since_last_trade=0.5,
            minutes_since_whale_entry=10, convergence_count=1,
        )
        assert result.score > 0
        assert result.decision in ("auto", "half", "skip")

    def test_compute_fusion_uses_enhanced_when_microstructure_present(self):
        microstructure = {
            "available": True,
            "velocity": {"volume_ratio": 4.0, "available": True},
            "order_book": {"spread_pct": 0.01, "available": True},
            "baseline": {
                "available": True, "info_already_priced": False, "whale_impact": 0.01,
            },
        }
        result = fusion_score.compute_fusion(
            wallet_wr=0.65, wallet_tier="tier1h",
            trade_size_usd=2000, wallet_avg_bet_usd=2000,
            market_volume_usd=None, current_price=0.55,
            days_since_last_trade=None,
            minutes_since_whale_entry=10, convergence_count=1,
            microstructure=microstructure,
        )
        # Enhanced market signal should yield a meaningful score
        assert result.market_signal > 0.5
        assert result.score > 0

    def test_enhanced_market_signal_falls_back_when_microstructure_unavailable(self):
        out = fusion_score.market_signal_enhanced(None)
        assert out is None
        out = fusion_score.market_signal_enhanced({"available": False})
        assert out is None

    def test_enhanced_market_signal_penalizes_priced_in_info(self):
        microstructure = {
            "available": True,
            "velocity": {"volume_ratio": 0.4, "available": True},
            "order_book": {"spread_pct": 0.12, "available": True},
            "baseline": {
                "available": True, "info_already_priced": True, "whale_impact": 0.0,
            },
        }
        score = fusion_score.market_signal_enhanced(microstructure)
        assert score is not None
        assert score < 0.4
