"""Tests for Polymarket live collector: SQLite storage, message parsing, hourly export."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from trading_platform.polymarket.live_db import LiveTickStore
from trading_platform.polymarket.live_collector import (
    LiveMarketInfo,
    PolymarketLiveCollector,
    PolymarketLiveCollectorConfig,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _info(market_id: str = "mkt-1", token_id: str = "tok-1") -> LiveMarketInfo:
    return LiveMarketInfo(
        market_id=market_id,
        question=f"Will {market_id} happen?",
        yes_token_id=token_id,
        volume=10000.0,
    )


# ── LiveTickStore ────────────────────────────────────────────────────────────


class TestLiveTickStore:
    def test_creates_db_and_tables(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        # ticks table should exist
        row = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ticks'"
        ).fetchone()
        assert row is not None
        store.close()

    def test_wal_mode_enabled(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        store.close()

    def test_insert_and_retrieve_tick(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.insert_tick(
            asset_id="tok-1",
            market_id="mkt-1",
            price=0.65,
            timestamp="2026-04-03T12:00:00+00:00",
            msg_type="last_trade_price",
        )
        ticks = store.get_ticks_for_hour(
            "mkt-1",
            "2026-04-03T12:00:00+00:00",
            "2026-04-03T13:00:00+00:00",
        )
        assert len(ticks) == 1
        assert ticks[0]["price"] == 0.65
        store.close()

    def test_insert_batch(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        rows = [
            ("tok-1", "mkt-1", 0.60, None, None, "2026-04-03T12:01:00+00:00", "price_change"),
            ("tok-1", "mkt-1", 0.61, "BUY", 10.0, "2026-04-03T12:02:00+00:00", "price_change"),
            ("tok-1", "mkt-1", 0.62, None, None, "2026-04-03T12:03:00+00:00", "last_trade_price"),
        ]
        count = store.insert_ticks_batch(rows)
        assert count == 3
        store.close()

    def test_get_ticks_for_hour_filters_by_msg_type(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.60,
            timestamp="2026-04-03T12:01:00+00:00", msg_type="price_change",
        )
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.65,
            timestamp="2026-04-03T12:02:00+00:00", msg_type="last_trade_price",
        )
        # Default msg_type filter: last_trade_price only
        ticks = store.get_ticks_for_hour(
            "mkt-1",
            "2026-04-03T12:00:00+00:00",
            "2026-04-03T13:00:00+00:00",
        )
        assert len(ticks) == 1
        assert ticks[0]["price"] == 0.65

        # All msg types
        all_ticks = store.get_ticks_for_hour(
            "mkt-1",
            "2026-04-03T12:00:00+00:00",
            "2026-04-03T13:00:00+00:00",
            msg_type=None,
        )
        assert len(all_ticks) == 2
        store.close()

    def test_get_latest_prices(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.50,
            timestamp="2026-04-03T12:00:00+00:00", msg_type="last_trade_price",
        )
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.70,
            timestamp="2026-04-03T12:05:00+00:00", msg_type="last_trade_price",
        )
        store.insert_tick(
            asset_id="tok-2", market_id="mkt-2", price=0.30,
            timestamp="2026-04-03T12:03:00+00:00", msg_type="last_trade_price",
        )
        latest = store.get_latest_prices()
        assert latest["mkt-1"]["price"] == 0.70
        assert latest["mkt-2"]["price"] == 0.30
        store.close()

    def test_get_latest_prices_excludes_price_change(self, tmp_path: Path) -> None:
        """price_change ticks should not appear in latest prices."""
        store = LiveTickStore(tmp_path / "test.db")
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.50,
            timestamp="2026-04-03T12:00:00+00:00", msg_type="last_trade_price",
        )
        # A later price_change tick with a different price
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.01,
            timestamp="2026-04-03T12:10:00+00:00", msg_type="price_change",
        )
        latest = store.get_latest_prices()
        assert latest["mkt-1"]["price"] == 0.50  # not 0.01
        store.close()

    def test_upsert_and_get_market_info(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.upsert_market_info("mkt-1", "Will X happen?", 5000.0, "tok-1", "2026-06-01T00:00:00Z")
        store.upsert_market_info("mkt-2", "Will Y happen?", 3000.0, "tok-2")
        info = store.get_market_info()
        assert info["mkt-1"]["question"] == "Will X happen?"
        assert info["mkt-1"]["end_date_iso"] == "2026-06-01T00:00:00Z"
        assert info["mkt-2"]["volume"] == 3000.0
        assert info["mkt-2"]["end_date_iso"] is None
        store.close()

    def test_upsert_markets_batch(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.upsert_markets_batch([
            ("m1", "Q1?", 100.0, "t1", "2026-05-01"),
            ("m2", "Q2?", 200.0, "t2", None),
        ])
        info = store.get_market_info()
        assert len(info) == 2
        assert info["m1"]["end_date_iso"] == "2026-05-01"
        store.close()

    def test_get_tick_counts(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        for i in range(5):
            store.insert_tick(
                asset_id="tok-1", market_id="mkt-1", price=0.5,
                timestamp=f"2026-04-03T12:0{i}:00+00:00", msg_type="last_trade_price",
            )
        store.insert_tick(
            asset_id="tok-2", market_id="mkt-2", price=0.3,
            timestamp="2026-04-03T12:00:00+00:00", msg_type="last_trade_price",
        )
        counts = store.get_tick_counts()
        assert counts["mkt-1"] == 5
        assert counts["mkt-2"] == 1
        store.close()

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        deep_path = tmp_path / "a" / "b" / "c" / "test.db"
        store = LiveTickStore(deep_path)
        assert deep_path.exists()
        store.close()

    def test_get_ticks_for_market(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        for i in range(10):
            store.insert_tick(
                asset_id="tok-1", market_id="mkt-1", price=0.50 + i * 0.01,
                timestamp=f"2026-04-03T12:{i:02d}:00+00:00", msg_type="last_trade_price",
            )
        ticks = store.get_ticks_for_market("mkt-1", limit=5)
        assert len(ticks) == 5
        # Should be in ascending order (most recent 5)
        assert ticks[0]["price"] < ticks[-1]["price"]
        assert ticks[0]["price"] == pytest.approx(0.55)
        store.close()

    def test_get_ticks_for_market_ascending_order(self, tmp_path: Path) -> None:
        store = LiveTickStore(tmp_path / "test.db")
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.40,
            timestamp="2026-04-03T12:00:00+00:00", msg_type="last_trade_price",
        )
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.60,
            timestamp="2026-04-03T12:01:00+00:00", msg_type="last_trade_price",
        )
        ticks = store.get_ticks_for_market("mkt-1")
        assert ticks[0]["price"] == 0.40
        assert ticks[1]["price"] == 0.60
        store.close()


# ── Message Handling ─────────────────────────────────────────────────────────


class TestMessageHandling:
    def _make_collector(self, tmp_path: Path, markets: list[LiveMarketInfo] | None = None):
        markets = markets or [_info()]
        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(tmp_path / "bars"),
        )
        collector = PolymarketLiveCollector(config, markets)
        collector._store = LiveTickStore(config.db_path)
        return collector

    def test_handle_last_trade_price(self, tmp_path: Path) -> None:
        collector = self._make_collector(tmp_path)
        msg = {
            "event_type": "last_trade_price",
            "asset_id": "tok-1",
            "price": "0.72",
            "timestamp": "2026-04-03T14:00:00Z",
        }
        collector._handle_last_trade_price(msg)
        assert collector.state.ticks_stored == 1
        assert collector.state.latest_prices["mkt-1"] == 0.72
        collector._store.close()

    def test_handle_price_change(self, tmp_path: Path) -> None:
        collector = self._make_collector(tmp_path)
        msg = {
            "event_type": "price_change",
            "asset_id": "tok-1",
            "timestamp": "2026-04-03T14:00:00Z",
            "changes": [
                {"price": "0.65", "side": "BUY", "size": "10"},
                {"price": "0.64", "side": "SELL", "size": "5"},
            ],
        }
        collector._handle_price_change(msg)
        assert collector.state.ticks_stored == 2
        # price_change should NOT update latest_prices
        assert "mkt-1" not in collector.state.latest_prices
        collector._store.close()

    def test_handle_book_snapshot(self, tmp_path: Path) -> None:
        collector = self._make_collector(tmp_path)
        msg = {
            "event_type": "book",
            "asset_id": "tok-1",
            "timestamp": "1729084877448",
            "bids": [{"price": "0.60", "size": "100"}, {"price": "0.59", "size": "50"}],
            "asks": [{"price": "0.62", "size": "80"}],
        }
        collector._handle_book(msg)
        assert collector.state.ticks_stored == 1
        # midpoint of 0.60 and 0.62 = 0.61
        assert collector.state.latest_prices["mkt-1"] == pytest.approx(0.61)
        # Verify orderbook data stored in DB
        row = collector._store._conn.execute(
            "SELECT best_bid, best_ask, spread FROM ticks WHERE market_id='mkt-1'"
        ).fetchone()
        assert row[0] == pytest.approx(0.60)  # best_bid
        assert row[1] == pytest.approx(0.62)  # best_ask
        assert row[2] == pytest.approx(0.02)  # spread
        collector._store.close()

    def test_message_loop_handles_array_wrapped_messages(self, tmp_path: Path) -> None:
        """WS sends messages as JSON arrays like [{"event_type": ...}]."""
        collector = self._make_collector(tmp_path)

        # Simulate _message_loop processing by calling the dispatch logic directly
        # The actual _message_loop is async; test the handler pathway instead.
        # An array with two events: one price_change, one last_trade_price
        array_msg = [
            {
                "event_type": "price_change",
                "asset_id": "tok-1",
                "timestamp": "2026-04-03T14:00:00Z",
                "changes": [{"price": "0.60", "side": "BUY", "size": "5"}],
            },
            {
                "event_type": "last_trade_price",
                "asset_id": "tok-1",
                "price": "0.61",
                "timestamp": "2026-04-03T14:00:01Z",
            },
        ]
        # Process each item like _message_loop does after parsing the array
        for msg in array_msg:
            msg_type = msg.get("event_type", "")
            if msg_type == "price_change":
                collector._handle_price_change(msg)
            elif msg_type == "last_trade_price":
                collector._handle_last_trade_price(msg)

        assert collector.state.ticks_stored == 2
        collector._store.close()

    def test_unknown_asset_id_ignored(self, tmp_path: Path) -> None:
        collector = self._make_collector(tmp_path)
        msg = {
            "event_type": "last_trade_price",
            "asset_id": "unknown-token",
            "price": "0.50",
            "timestamp": "2026-04-03T14:00:00Z",
        }
        collector._handle_last_trade_price(msg)
        assert collector.state.ticks_stored == 0
        collector._store.close()


# ── Hourly Export ────────────────────────────────────────────────────────────


class TestHourlyExport:
    def test_export_creates_parquet(self, tmp_path: Path) -> None:
        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(tmp_path / "bars"),
        )
        info = _info()
        collector = PolymarketLiveCollector(config, [info])
        collector._store = LiveTickStore(config.db_path)

        # Insert ticks for the previous hour
        now = datetime.now(tz=timezone.utc)
        hour_end = now.replace(minute=0, second=0, microsecond=0)
        hour_start = hour_end - timedelta(hours=1)

        for i in range(5):
            ts = (hour_start + timedelta(minutes=i * 10)).isoformat()
            collector._store.insert_tick(
                asset_id="tok-1", market_id="mkt-1",
                price=0.50 + i * 0.02, timestamp=ts,
                msg_type="last_trade_price",
            )

        asyncio.run(collector._export_hourly_bars())

        parquet_path = tmp_path / "bars" / "mkt-1.parquet"
        assert parquet_path.exists()
        df = pd.read_parquet(parquet_path)
        assert len(df) == 1
        assert df.iloc[0]["open"] == 0.50
        assert df.iloc[0]["high"] == 0.58
        assert df.iloc[0]["low"] == 0.50
        assert df.iloc[0]["close"] == 0.58
        assert df.iloc[0]["volume"] == 5.0
        collector._store.close()

    def test_export_merges_with_existing(self, tmp_path: Path) -> None:
        bars_dir = tmp_path / "bars"
        bars_dir.mkdir()
        path = bars_dir / "mkt-1.parquet"

        # Create an existing bar
        existing = pd.DataFrame([{
            "timestamp": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
            "symbol": "mkt-1",
            "open": 0.40, "high": 0.45, "low": 0.38, "close": 0.43,
            "volume": 10.0, "dollar_volume": 430.0,
        }])
        existing.to_parquet(path, index=False)

        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(bars_dir),
        )
        collector = PolymarketLiveCollector(config, [_info()])
        collector._store = LiveTickStore(config.db_path)

        now = datetime.now(tz=timezone.utc)
        hour_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        collector._store.insert_tick(
            asset_id="tok-1", market_id="mkt-1",
            price=0.55, timestamp=hour_start.isoformat(),
            msg_type="last_trade_price",
        )

        asyncio.run(collector._export_hourly_bars())

        df = pd.read_parquet(path)
        assert len(df) == 2  # existing + new bar
        collector._store.close()

    def test_export_empty_hour_no_crash(self, tmp_path: Path) -> None:
        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(tmp_path / "bars"),
        )
        collector = PolymarketLiveCollector(config, [_info()])
        collector._store = LiveTickStore(config.db_path)

        # No ticks — should not crash or create files
        asyncio.run(collector._export_hourly_bars())

        bars_dir = tmp_path / "bars"
        parquet_files = list(bars_dir.glob("*.parquet")) if bars_dir.exists() else []
        assert len(parquet_files) == 0
        collector._store.close()


# ── WebSocket Subscribe ──────────────────────────────────────────────────────


class TestWebSocketSubscribe:
    def test_subscribe_sends_correct_message(self, tmp_path: Path) -> None:
        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(tmp_path / "bars"),
        )
        markets = [_info("m1", "tok-A"), _info("m2", "tok-B")]
        collector = PolymarketLiveCollector(config, markets)

        mock_ws = AsyncMock()
        with patch(
            "trading_platform.polymarket.live_collector.ws_connect",
            new_callable=AsyncMock,
            return_value=mock_ws,
        ):
            asyncio.run(collector._connect_and_subscribe())

        # Verify the subscribe message
        sent = mock_ws.send.call_args[0][0]
        payload = json.loads(sent)
        assert payload["type"] == "market"
        assert set(payload["assets_ids"]) == {"tok-A", "tok-B"}


# ── Config ───────────────────────────────────────────────────────────────────


class TestConfig:
    def test_default_config_values(self) -> None:
        cfg = PolymarketLiveCollectorConfig()
        assert "wss://" in cfg.ws_url
        assert cfg.reconnect_delay == 2.0
        assert cfg.export_interval_sec == 3600.0
        assert cfg.heartbeat_interval_sec == 300.0

    def test_live_market_info(self) -> None:
        info = _info("abc", "tok-123")
        assert info.market_id == "abc"
        assert info.yes_token_id == "tok-123"

    def test_write_stats(self, tmp_path: Path) -> None:
        config = PolymarketLiveCollectorConfig(
            db_path=str(tmp_path / "test.db"),
            hourly_bars_dir=str(tmp_path / "bars"),
            stats_path=str(tmp_path / "stats" / "stats.json"),
        )
        collector = PolymarketLiveCollector(config, [_info()])
        collector._store = LiveTickStore(config.db_path)
        collector.state.started_at = datetime.now(tz=timezone.utc)
        collector.state.ticks_stored = 42
        collector.state.latest_prices["mkt-1"] = 0.65
        collector._write_stats()

        stats_path = tmp_path / "stats" / "stats.json"
        assert stats_path.exists()
        stats = json.loads(stats_path.read_text())
        assert stats["total_ticks"] == 42
        assert stats["markets_subscribed"] == 1
        assert stats["markets_active"] == 1
        collector._store.close()


# ── FastAPI Endpoint ─────────────────────────────────────────────────────────


class TestPolymarketLiveEndpoint:
    def test_no_db_returns_unavailable(self, tmp_path: Path) -> None:
        from trading_platform.api import artifact_reader as reader
        original = reader.DATA_ROOT
        reader.DATA_ROOT = tmp_path
        try:
            result = reader.read_polymarket_live_markets()
            assert result["available"] is False
            assert result["count"] == 0
        finally:
            reader.DATA_ROOT = original

    def test_with_data_returns_markets(self, tmp_path: Path) -> None:
        from trading_platform.api import artifact_reader as reader
        # Create a live DB with some ticks and market metadata
        db_dir = tmp_path / "polymarket" / "live"
        db_dir.mkdir(parents=True)
        store = LiveTickStore(db_dir / "prices.db")
        store.upsert_market_info("mkt-1", "Will X happen?", 5000.0, "tok-1", "2026-06-01T00:00:00Z")
        store.insert_tick(
            asset_id="tok-1", market_id="mkt-1", price=0.65,
            timestamp="2026-04-03T12:00:00+00:00", msg_type="last_trade_price",
        )
        store.close()

        original = reader.DATA_ROOT
        reader.DATA_ROOT = tmp_path
        try:
            result = reader.read_polymarket_live_markets()
            assert result["available"] is True
            assert result["count"] == 1
            m = result["data"][0]
            assert m["market_id"] == "mkt-1"
            assert m["question"] == "Will X happen?"
            assert m["end_date_iso"] == "2026-06-01T00:00:00Z"
            assert m["yes_price"] == 65.0
            assert m["tick_count"] >= 1
            assert m["live"] is True
        finally:
            reader.DATA_ROOT = original

    def test_market_ticks_no_db(self, tmp_path: Path) -> None:
        from trading_platform.api import artifact_reader as reader
        original = reader.DATA_ROOT
        reader.DATA_ROOT = tmp_path
        try:
            result = reader.read_polymarket_market_ticks("nonexistent")
            assert result["available"] is False
        finally:
            reader.DATA_ROOT = original

    def test_market_ticks_with_data(self, tmp_path: Path) -> None:
        from trading_platform.api import artifact_reader as reader
        db_dir = tmp_path / "polymarket" / "live"
        db_dir.mkdir(parents=True)
        store = LiveTickStore(db_dir / "prices.db")
        store.upsert_market_info("mkt-1", "Will X happen?", 5000.0, "tok-1", "2026-06-01")
        for i in range(10):
            store.insert_tick(
                asset_id="tok-1", market_id="mkt-1", price=0.50 + i * 0.01,
                timestamp=f"2026-04-03T12:{i:02d}:00+00:00", msg_type="last_trade_price",
            )
        store.close()

        original = reader.DATA_ROOT
        reader.DATA_ROOT = tmp_path
        try:
            result = reader.read_polymarket_market_ticks("mkt-1")
            assert result["available"] is True
            assert result["question"] == "Will X happen?"
            assert len(result["ticks"]) == 10
            # Prices should be in 0-100 scale
            assert result["ticks"][0]["price"] == 50.0
            assert result["ticks"][-1]["price"] == 59.0
            # Stats
            assert result["stats"]["min"] == 50.0
            assert result["stats"]["max"] == 59.0
            assert result["stats"]["tick_count"] == 10
        finally:
            reader.DATA_ROOT = original

    def test_market_ticks_no_ticks(self, tmp_path: Path) -> None:
        from trading_platform.api import artifact_reader as reader
        db_dir = tmp_path / "polymarket" / "live"
        db_dir.mkdir(parents=True)
        store = LiveTickStore(db_dir / "prices.db")
        store.upsert_market_info("mkt-1", "Q?", 1000.0, "tok-1")
        store.close()

        original = reader.DATA_ROOT
        reader.DATA_ROOT = tmp_path
        try:
            result = reader.read_polymarket_market_ticks("mkt-1")
            assert result["available"] is False
        finally:
            reader.DATA_ROOT = original
