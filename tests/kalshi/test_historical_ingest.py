"""Tests for the Kalshi historical ingest pipeline."""
from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from trading_platform.kalshi.historical_ingest import (
    HistoricalIngestConfig,
    HistoricalIngestPipeline,
    _parse_trade_row,
    _result_to_price,
    _trades_to_dataframe,
)


# ── _result_to_price ──────────────────────────────────────────────────────────

def test_result_to_price_yes():
    assert _result_to_price("yes") == pytest.approx(100.0)


def test_result_to_price_no():
    assert _result_to_price("no") == pytest.approx(0.0)


def test_result_to_price_none():
    assert _result_to_price(None) is None


def test_result_to_price_unknown():
    assert _result_to_price("pending") is None
    assert _result_to_price("voided") is None


# ── _parse_trade_row ──────────────────────────────────────────────────────────

def test_parse_trade_row_basic():
    raw = {
        "trade_id": "trade-001",
        "ticker": "FED-MAR-YES",
        "taker_side": "yes",
        "yes_price_dollars": 0.35,
        "no_price_dollars": 0.65,
        "count": 10,
        "created_time": "2025-06-15T14:30:00Z",
    }
    row = _parse_trade_row(raw)
    assert row["trade_id"] == "trade-001"
    assert row["yes_price"] == pytest.approx(0.35)
    assert row["count"] == 10
    assert isinstance(row["traded_at"], datetime)


def test_parse_trade_row_missing_fields():
    raw = {}
    row = _parse_trade_row(raw)
    assert row["trade_id"] == ""
    assert row["yes_price"] is None
    assert row["count"] == 0
    assert row["traded_at"] is None


def test_parse_trade_row_fallback_price_fields():
    raw = {
        "trade_id": "t1",
        "yes_price": 0.70,  # non-dollar key
        "count": 5,
        "created_time": "2025-01-01T00:00:00Z",
    }
    row = _parse_trade_row(raw)
    assert row["yes_price"] == pytest.approx(0.70)


# ── _trades_to_dataframe ──────────────────────────────────────────────────────

def test_trades_to_dataframe_empty():
    df = _trades_to_dataframe([])
    assert df.is_empty()
    assert "yes_price" in df.columns
    assert "traded_at" in df.columns


def test_trades_to_dataframe_with_rows():
    raw_trades = [
        {
            "trade_id": f"t{i}",
            "ticker": "TEST",
            "taker_side": "yes",
            "yes_price_dollars": 0.40 + i * 0.01,
            "no_price_dollars": 0.60 - i * 0.01,
            "count": 5,
            "created_time": f"2025-06-{i+1:02d}T10:00:00Z",
        }
        for i in range(10)
    ]
    df = _trades_to_dataframe(raw_trades)
    assert len(df) == 10
    assert "yes_price" in df.columns
    assert "traded_at" in df.columns
    # Prices should be in [0,1] range (not yet scaled to 0-100)
    assert float(df["yes_price"].min()) >= 0.0


# ── HistoricalIngestPipeline (mocked client) ──────────────────────────────────

def _make_mock_client(n_markets: int = 3, n_trades_per_market: int = 20) -> MagicMock:
    """Build a mock KalshiClient that returns synthetic historical data."""
    now = datetime.now(UTC)
    client = MagicMock()

    markets = [
        {
            "ticker": f"TEST-MARKET-{i:03d}",
            "title": f"Will event {i} happen?",
            "series_ticker": f"TEST-SERIES",
            "result": "yes" if i % 2 == 0 else "no",
            "close_time": (now - timedelta(days=i + 1)).isoformat(),
            "status": "settled",
        }
        for i in range(n_markets)
    ]

    # get_all_historical_markets returns a flat list
    client.get_all_historical_markets.return_value = markets

    # get_all_historical_trades returns a list of trade dicts per ticker
    def _make_trades(ticker, **kwargs):
        return [
            {
                "trade_id": f"{ticker}-trade-{j}",
                "ticker": ticker,
                "taker_side": "yes",
                "yes_price_dollars": 0.40 + j * 0.01,
                "no_price_dollars": 0.60 - j * 0.01,
                "count": 1,
                "created_time": (now - timedelta(days=365, hours=j)).isoformat(),
            }
            for j in range(n_trades_per_market)
        ]

    client.get_all_historical_trades.side_effect = lambda ticker, **kw: _make_trades(ticker)

    return client


class TestHistoricalIngestPipeline:

    def test_run_produces_output_files(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            lookback_days=365,
            feature_period="1d",
            min_trades=5,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
        )

        client = _make_mock_client(n_markets=3, n_trades_per_market=15)
        pipeline = HistoricalIngestPipeline(client, config)
        result = pipeline.run()

        assert result.markets_downloaded == 3
        assert result.markets_with_trades == 3
        assert result.markets_skipped_no_trades == 0
        assert result.total_trades == 3 * 15
        assert result.resolution_count == 3

        # Verify output files exist
        manifest = json.loads(Path(config.manifest_path).read_text())
        assert manifest["markets_downloaded"] == 3
        assert manifest["total_trades"] == 3 * 15

        # Resolution CSV
        import csv
        with open(config.resolution_csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 3
        for row in rows:
            assert row["resolution_price"] in ("100.0", "0.0")

    def test_run_skips_market_with_too_few_trades(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            min_trades=10,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
        )

        # 2 trades < min_trades=10 → should be skipped
        client = _make_mock_client(n_markets=2, n_trades_per_market=2)
        pipeline = HistoricalIngestPipeline(client, config)
        result = pipeline.run()

        assert result.markets_skipped_no_trades == 2
        assert result.markets_with_trades == 0

    def test_run_writes_raw_market_json(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            min_trades=5,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
        )

        client = _make_mock_client(n_markets=2, n_trades_per_market=10)
        pipeline = HistoricalIngestPipeline(client, config)
        pipeline.run()

        markets_dir = Path(config.raw_markets_dir)
        json_files = list(markets_dir.glob("*.json"))
        assert len(json_files) == 2

    def test_run_handles_trade_fetch_failure_gracefully(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            min_trades=5,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
        )

        client = _make_mock_client(n_markets=3, n_trades_per_market=10)
        # Make one market's trade fetch fail
        original_side_effect = client.get_all_historical_trades.side_effect

        call_count = [0]

        def _sometimes_fail(ticker, **kw):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("Simulated API failure")
            return original_side_effect(ticker, **kw)

        client.get_all_historical_trades.side_effect = _sometimes_fail

        # Should not raise
        result = pipeline = HistoricalIngestPipeline(client, config)
        result = pipeline.run()

        assert result.markets_failed == 1
        assert result.markets_with_trades == 2

    def test_run_ticker_filter(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            min_trades=5,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
            ticker_filter=["TEST-MARKET-000"],
        )

        client = _make_mock_client(n_markets=5, n_trades_per_market=10)
        pipeline = HistoricalIngestPipeline(client, config)
        result = pipeline.run()

        # Only 1 market matches the filter
        assert result.markets_downloaded == 1

    def test_resolution_encoding_yes_no(self, tmp_path):
        config = HistoricalIngestConfig(
            raw_markets_dir=str(tmp_path / "raw/markets"),
            raw_trades_dir=str(tmp_path / "raw/trades"),
            trades_parquet_dir=str(tmp_path / "trades"),
            features_dir=str(tmp_path / "features"),
            resolution_csv_path=str(tmp_path / "raw/resolution.csv"),
            manifest_path=str(tmp_path / "raw/manifest.json"),
            min_trades=5,
            request_sleep_sec=0.0,
            run_base_rate=False,
            run_metaculus=False,
        )

        client = MagicMock()
        client.get_all_historical_markets.return_value = [
            {
                "ticker": "YES-MARKET",
                "title": "Test",
                "series_ticker": "TEST",
                "result": "yes",
                "close_time": "2025-12-01T00:00:00Z",
            },
            {
                "ticker": "NO-MARKET",
                "title": "Test 2",
                "series_ticker": "TEST",
                "result": "no",
                "close_time": "2025-12-01T00:00:00Z",
            },
        ]
        client.get_all_historical_trades.return_value = [
            {
                "trade_id": f"t{j}",
                "ticker": "X",
                "taker_side": "yes",
                "yes_price_dollars": 0.50,
                "no_price_dollars": 0.50,
                "count": 1,
                "created_time": f"2025-06-{j+1:02d}T10:00:00Z",
            }
            for j in range(10)
        ]

        pipeline = HistoricalIngestPipeline(client, config)
        result = pipeline.run()

        import csv
        with open(config.resolution_csv_path, newline="") as f:
            rows = {r["ticker"]: float(r["resolution_price"]) for r in csv.DictReader(f)}

        assert rows["YES-MARKET"] == pytest.approx(100.0)
        assert rows["NO-MARKET"] == pytest.approx(0.0)
