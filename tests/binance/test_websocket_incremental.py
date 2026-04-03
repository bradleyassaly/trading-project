from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.binance.models import BinanceWebsocketIngestConfig
from trading_platform.binance.websocket import BinanceWebsocketIngestService, parse_binance_websocket_message


def _combined(payload: dict[str, object], stream: str) -> str:
    return json.dumps({"stream": stream, "data": payload})


def test_parse_binance_websocket_message_supports_kline_agg_trade_and_book_ticker() -> None:
    kline = parse_binance_websocket_message(
        json.loads(
            _combined(
                {
                    "e": "kline",
                    "E": 1704067200000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": 1704067200000,
                        "T": 1704067259999,
                        "i": "1m",
                        "o": "100",
                        "c": "101",
                        "h": "102",
                        "l": "99",
                        "v": "2",
                        "n": 2,
                        "x": False,
                        "q": "200",
                        "V": "1",
                        "Q": "100",
                    },
                },
                "btcusdt@kline_1m",
            )
        )
    )
    trade = parse_binance_websocket_message(
        json.loads(
            _combined(
                {"e": "aggTrade", "E": 1704067200001, "s": "BTCUSDT", "a": 1, "p": "100", "q": "0.2", "f": 1, "l": 2, "T": 1704067200001, "m": False},
                "btcusdt@aggTrade",
            )
        )
    )
    book = parse_binance_websocket_message(
        json.loads(_combined({"u": 10, "s": "BTCUSDT", "b": "100", "B": "1", "a": "100.1", "A": "1.2"}, "btcusdt@bookTicker"))
    )

    assert kline["stream_family"] == "kline"
    assert trade["stream_family"] == "agg_trade"
    assert book["stream_family"] == "book_ticker"
    assert book["ordering_key"] == 10


class _FakeWebSocket:
    def __init__(self, messages: list[str], *, fail_after: bool = False) -> None:
        self._messages = list(messages)
        self._fail_after = fail_after

    async def recv(self) -> str:
        if self._messages:
            return self._messages.pop(0)
        if self._fail_after:
            raise RuntimeError("disconnect")
        raise asyncio.TimeoutError()


class _FakeConnection:
    def __init__(self, websocket: _FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> _FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


def test_websocket_ingest_reconnects_and_dedupes(tmp_path: Path, monkeypatch) -> None:
    messages_round_one = [
        _combined(
            {"e": "aggTrade", "E": 1704067200001, "s": "BTCUSDT", "a": 1, "p": "100", "q": "0.2", "f": 1, "l": 2, "T": 1704067200001, "m": False},
            "btcusdt@aggTrade",
        )
    ]
    messages_round_two = [
        _combined(
            {"e": "aggTrade", "E": 1704067200001, "s": "BTCUSDT", "a": 1, "p": "100", "q": "0.2", "f": 1, "l": 2, "T": 1704067200001, "m": False},
            "btcusdt@aggTrade",
        ),
        _combined(
            {"e": "aggTrade", "E": 1704067200002, "s": "BTCUSDT", "a": 2, "p": "100.1", "q": "0.3", "f": 3, "l": 4, "T": 1704067200002, "m": True},
            "btcusdt@aggTrade",
        ),
    ]
    connections = iter(
        [
            _FakeConnection(_FakeWebSocket(messages_round_one, fail_after=True)),
            _FakeConnection(_FakeWebSocket(messages_round_two)),
        ]
    )

    def fake_connect(*args, **kwargs):  # noqa: ANN001, ANN002
        return next(connections)

    async def _no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr("trading_platform.binance.websocket.asyncio.sleep", _no_sleep)
    config = BinanceWebsocketIngestConfig(
        enabled=True,
        symbols=("BTCUSDT",),
        intervals=("1m",),
        stream_families=("agg_trade",),
        max_messages=3,
        max_reconnect_attempts=2,
        reconnect_backoff_base_sec=0.0,
        reconnect_backoff_max_sec=0.0,
        raw_incremental_root=str(tmp_path / "raw"),
        normalized_incremental_root=str(tmp_path / "normalized"),
        checkpoint_path=str(tmp_path / "raw" / "checkpoint.json"),
        summary_path=str(tmp_path / "raw" / "summary.json"),
        projection_output_root=str(tmp_path / "projections"),
    )
    result = BinanceWebsocketIngestService(config, connect_factory=fake_connect).run()

    assert result.reconnect_count >= 1
    assert result.duplicates_dropped == 1
    assert result.messages_written == 2
    frame = pd.read_parquet(tmp_path / "normalized" / "agg_trades" / "BTCUSDT.parquet")
    assert frame["aggregate_trade_id"].tolist() == [1, 2]
    assert Path(result.projection_summary_path).exists()


def test_websocket_ingest_checkpoint_allows_restart_without_duplicate_append(tmp_path: Path, monkeypatch) -> None:
    message = _combined(
        {"e": "aggTrade", "E": 1704067200001, "s": "BTCUSDT", "a": 5, "p": "100", "q": "0.2", "f": 1, "l": 2, "T": 1704067200001, "m": False},
        "btcusdt@aggTrade",
    )

    def fake_connect(*args, **kwargs):  # noqa: ANN001, ANN002
        return _FakeConnection(_FakeWebSocket([message]))

    async def _no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr("trading_platform.binance.websocket.asyncio.sleep", _no_sleep)
    config = BinanceWebsocketIngestConfig(
        enabled=True,
        symbols=("BTCUSDT",),
        intervals=("1m",),
        stream_families=("agg_trade",),
        max_messages=1,
        max_reconnect_attempts=0,
        raw_incremental_root=str(tmp_path / "raw"),
        normalized_incremental_root=str(tmp_path / "normalized"),
        checkpoint_path=str(tmp_path / "raw" / "checkpoint.json"),
        summary_path=str(tmp_path / "raw" / "summary.json"),
        projection_output_root=str(tmp_path / "projections"),
    )
    first = BinanceWebsocketIngestService(config, connect_factory=fake_connect).run()
    second = BinanceWebsocketIngestService(config, connect_factory=fake_connect).run()

    assert first.messages_written == 1
    assert second.duplicates_dropped == 1
    frame = pd.read_parquet(tmp_path / "normalized" / "agg_trades" / "BTCUSDT.parquet")
    assert frame["aggregate_trade_id"].tolist() == [5]
