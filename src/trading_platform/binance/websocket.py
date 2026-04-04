from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, Awaitable, Callable

import pandas as pd
import websockets

from trading_platform.binance.models import (
    BinanceProjectionConfig,
    BinanceWebsocketIngestConfig,
    BinanceWebsocketIngestResult,
)
from trading_platform.binance.projection import project_binance_market_data

logger = logging.getLogger(__name__)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")) or {})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")


def _merge_frame(
    path: Path,
    frame: pd.DataFrame,
    *,
    dedupe_keys: list[str],
    sort_keys: list[str],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    combined = pd.concat([existing, frame], ignore_index=True) if not existing.empty else frame.copy()
    available_sort = [column for column in sort_keys if column in combined.columns]
    if available_sort:
        combined = combined.sort_values(available_sort)
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    if available_sort:
        combined = combined.sort_values(available_sort)
    combined.to_parquet(path, index=False)
    return int(len(frame.index))


def parse_binance_websocket_message(message: dict[str, Any]) -> dict[str, Any]:
    envelope = dict(message)
    if "data" in envelope and "stream" in envelope:
        stream = str(envelope["stream"])
        data = dict(envelope["data"] or {})
    else:
        data = envelope
        stream = str(data.get("stream") or "")
    event_type = str(data.get("e") or "")
    if not event_type and {"u", "s", "b", "B", "a", "A"}.issubset(data.keys()):
        event_type = "bookTicker"
    if event_type == "kline":
        kline = dict(data["k"])
        return {
            "stream": stream,
            "stream_family": "kline",
            "symbol": str(data["s"]).upper(),
            "interval": str(kline["i"]),
            "event_time": pd.to_datetime(int(data["E"]), unit="ms", utc=True),
            "dedupe_key": f"{data['s']}|{kline['i']}|{kline['t']}|{data['E']}",
            "ordering_key": int(data["E"]),
            "payload": data,
        }
    if event_type == "aggTrade":
        return {
            "stream": stream,
            "stream_family": "agg_trade",
            "symbol": str(data["s"]).upper(),
            "interval": None,
            "event_time": pd.to_datetime(int(data["E"]), unit="ms", utc=True),
            "dedupe_key": f"{data['s']}|{data['a']}",
            "ordering_key": int(data["a"]),
            "payload": data,
        }
    if event_type == "bookTicker":
        update_id = int(data.get("u") or data.get("U") or 0)
        event_time_ms = int(data.get("E") or update_id or 0)
        return {
            "stream": stream,
            "stream_family": "book_ticker",
            "symbol": str(data["s"]).upper(),
            "interval": None,
            "event_time": pd.to_datetime(event_time_ms, unit="ms", utc=True) if event_time_ms else pd.Timestamp.now(tz=UTC),
            "dedupe_key": f"{data['s']}|{update_id}",
            "ordering_key": update_id,
            "payload": data,
        }
    raise ValueError(f"Unsupported Binance websocket event type: {event_type}")


def _normalized_frame(parsed: dict[str, Any], *, raw_artifact_path: str, ingested_at: str) -> tuple[str, pd.DataFrame]:
    family = parsed["stream_family"]
    payload = parsed["payload"]
    if family == "kline":
        kline = payload["k"]
        frame = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime(int(kline["t"]), unit="ms", utc=True),
                    "close_timestamp": pd.to_datetime(int(kline["T"]), unit="ms", utc=True),
                    "event_time": pd.to_datetime(int(payload["E"]), unit="ms", utc=True),
                    "ingested_at": pd.to_datetime(ingested_at, utc=True),
                    "symbol": str(payload["s"]).upper(),
                    "interval": str(kline["i"]),
                    "open": float(kline["o"]),
                    "high": float(kline["h"]),
                    "low": float(kline["l"]),
                    "close": float(kline["c"]),
                    "volume": float(kline["v"]),
                    "quote_volume": float(kline["q"]),
                    "trade_count": int(kline["n"]),
                    "taker_buy_base_volume": float(kline["V"]),
                    "taker_buy_quote_volume": float(kline["Q"]),
                    "is_final": bool(kline["x"]),
                    "provider": "binance",
                    "source": "binance_websocket",
                    "asset_class": "crypto",
                    "schema_version": "binance_crypto_klines_v1",
                    "raw_artifact_path": raw_artifact_path,
                    "dedupe_key": parsed["dedupe_key"],
                }
            ]
        )
        return "kline", frame
    if family == "agg_trade":
        frame = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime(int(payload["T"]), unit="ms", utc=True),
                    "event_time": pd.to_datetime(int(payload["E"]), unit="ms", utc=True),
                    "ingested_at": pd.to_datetime(ingested_at, utc=True),
                    "symbol": str(payload["s"]).upper(),
                    "aggregate_trade_id": int(payload["a"]),
                    "first_trade_id": int(payload["f"]),
                    "last_trade_id": int(payload["l"]),
                    "price": float(payload["p"]),
                    "quantity": float(payload["q"]),
                    "is_buyer_maker": bool(payload["m"]),
                    "provider": "binance",
                    "source": "binance_websocket",
                    "asset_class": "crypto",
                    "schema_version": "binance_crypto_agg_trades_v1",
                    "raw_artifact_path": raw_artifact_path,
                    "dedupe_key": parsed["dedupe_key"],
                }
            ]
        )
        return "agg_trade", frame
    frame = pd.DataFrame(
        [
            {
                "timestamp": parsed["event_time"],
                "event_time": parsed["event_time"],
                "ingested_at": pd.to_datetime(ingested_at, utc=True),
                "symbol": str(payload["s"]).upper(),
                "bid_price": float(payload["b"]),
                "bid_quantity": float(payload["B"]),
                "ask_price": float(payload["a"]),
                "ask_quantity": float(payload["A"]),
                "update_id": int(payload.get("u") or payload.get("U") or 0),
                "provider": "binance",
                "source": "binance_websocket",
                "asset_class": "crypto",
                "schema_version": "binance_crypto_book_ticker_v1",
                "raw_artifact_path": raw_artifact_path,
                "dedupe_key": parsed["dedupe_key"],
            }
        ]
    )
    return "book_ticker", frame


@dataclass
class _WebsocketRuntimeState:
    messages_processed: int = 0
    messages_written: int = 0
    duplicates_dropped: int = 0
    reconnect_count: int = 0
    warnings: list[str] = None  # type: ignore[assignment]
    failures: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []
        if self.failures is None:
            self.failures = []


class BinanceWebsocketIngestService:
    def __init__(
        self,
        config: BinanceWebsocketIngestConfig,
        *,
        connect_factory: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        self.config = config
        self.connect_factory = connect_factory or websockets.connect
        self.raw_root = Path(config.raw_incremental_root)
        self.normalized_root = Path(config.normalized_incremental_root)
        self.checkpoint_path = Path(config.checkpoint_path)
        self.summary_path = Path(config.summary_path)
        self._runtime = _WebsocketRuntimeState()
        self._last_projection_summary_path: str | None = None

    def _load_checkpoint(self) -> dict[str, Any]:
        checkpoint = _load_json(self.checkpoint_path)
        checkpoint.setdefault("streams", {})
        checkpoint.setdefault("message_counts", {})
        return checkpoint

    def _stream_key(self, parsed: dict[str, Any]) -> str:
        if parsed["stream_family"] == "kline":
            return f"kline:{parsed['symbol']}:{parsed['interval']}"
        return f"{parsed['stream_family']}:{parsed['symbol']}"

    def _message_is_duplicate(self, checkpoint: dict[str, Any], parsed: dict[str, Any]) -> bool:
        stream_state = checkpoint["streams"].setdefault(self._stream_key(parsed), {})
        ordering_key = int(parsed["ordering_key"])
        last_ordering_key = int(stream_state.get("last_ordering_key", -1))
        if ordering_key < last_ordering_key:
            return True
        if ordering_key == last_ordering_key and stream_state.get("last_dedupe_key") == parsed["dedupe_key"]:
            return True
        stream_state["last_ordering_key"] = ordering_key
        stream_state["last_dedupe_key"] = parsed["dedupe_key"]
        return False

    def _raw_jsonl_path(self, parsed: dict[str, Any]) -> Path:
        event_day = parsed["event_time"].strftime("%Y-%m-%d")
        family = parsed["stream_family"]
        symbol = parsed["symbol"]
        if family == "kline":
            return self.raw_root / family / symbol / str(parsed["interval"]) / f"{event_day}.jsonl"
        return self.raw_root / family / symbol / f"{event_day}.jsonl"

    def _normalized_parquet_path(self, family: str, frame: pd.DataFrame) -> Path:
        symbol = str(frame.loc[0, "symbol"]).upper()
        if family == "kline":
            return self.normalized_root / "klines" / symbol / f"{frame.loc[0, 'interval']}.parquet"
        if family == "agg_trade":
            return self.normalized_root / "agg_trades" / f"{symbol}.parquet"
        return self.normalized_root / "book_ticker" / f"{symbol}.parquet"

    def _persist_message(self, checkpoint: dict[str, Any], parsed: dict[str, Any]) -> None:
        ingest_ts = datetime.now(UTC).isoformat()
        raw_path = self._raw_jsonl_path(parsed)
        envelope = {
            "stream": parsed["stream"],
            "stream_family": parsed["stream_family"],
            "symbol": parsed["symbol"],
            "interval": parsed["interval"],
            "event_time": parsed["event_time"].isoformat(),
            "received_at": ingest_ts,
            "dedupe_key": parsed["dedupe_key"],
            "payload": parsed["payload"],
        }
        _append_jsonl(raw_path, envelope)
        family, frame = _normalized_frame(parsed, raw_artifact_path=str(raw_path), ingested_at=ingest_ts)
        output_path = self._normalized_parquet_path(family, frame)
        if family == "kline":
            _merge_frame(output_path, frame, dedupe_keys=["symbol", "interval", "timestamp"], sort_keys=["timestamp", "event_time", "ingested_at"])
        elif family == "agg_trade":
            _merge_frame(output_path, frame, dedupe_keys=["symbol", "aggregate_trade_id"], sort_keys=["aggregate_trade_id", "event_time", "ingested_at"])
        else:
            _merge_frame(output_path, frame, dedupe_keys=["symbol", "dedupe_key"], sort_keys=["event_time", "ingested_at"])
        stream_state = checkpoint["streams"].setdefault(self._stream_key(parsed), {})
        stream_state["last_event_time"] = parsed["event_time"].isoformat()
        counts = checkpoint["message_counts"]
        counts[self._stream_key(parsed)] = int(counts.get(self._stream_key(parsed), 0)) + 1
        self._runtime.messages_written += 1
        _write_json(self.checkpoint_path, checkpoint)

    def _build_url(self) -> str:
        stream_names = self.config.stream_names()
        if self.config.combined_stream:
            return f"{self.config.base_url}/stream?streams={'/'.join(stream_names)}"
        if len(stream_names) != 1:
            raise ValueError("Non-combined Binance websocket mode supports exactly one stream in this milestone")
        return f"{self.config.base_url}/ws/{stream_names[0]}"

    async def _consume_once(self, websocket: Any, checkpoint: dict[str, Any], deadline: float | None) -> bool:
        while True:
            if self.config.max_messages is not None and self._runtime.messages_processed >= self.config.max_messages:
                return False
            if deadline is not None and monotonic() >= deadline:
                return False
            timeout = self.config.receive_timeout_sec
            if deadline is not None:
                timeout = min(timeout, max(deadline - monotonic(), 0.0))
            if timeout <= 0:
                return False
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            parsed = parse_binance_websocket_message(json.loads(raw_message))
            self._runtime.messages_processed += 1
            if self._message_is_duplicate(checkpoint, parsed):
                self._runtime.duplicates_dropped += 1
                continue
            self._persist_message(checkpoint, parsed)

    def _finalize(self) -> BinanceWebsocketIngestResult:
        projection_summary_path: str | None = None
        checkpoint = self._load_checkpoint()
        latest_event_times = [
            str(stream_state.get("last_event_time"))
            for stream_state in (checkpoint.get("streams") or {}).values()
            if stream_state.get("last_event_time")
        ]
        latest_event_time = max(latest_event_times) if latest_event_times else None
        if self.config.refresh_projection_after_ingest:
            projection_result = project_binance_market_data(
                BinanceProjectionConfig(
                    historical_normalized_root=str(self.normalized_root.parent),
                    incremental_normalized_root=str(self.normalized_root),
                    output_root=self.config.projection_output_root,
                    summary_path=str(Path(self.config.projection_output_root) / "projection_summary.json"),
                )
            )
            self._last_projection_summary_path = projection_result.summary_path
            projection_summary_path = projection_result.summary_path
        summary = {
            "symbols": list(self.config.symbols),
            "stream_families": list(self.config.stream_families),
            "intervals": list(self.config.intervals),
            "messages_processed": self._runtime.messages_processed,
            "messages_written": self._runtime.messages_written,
            "duplicates_dropped": self._runtime.duplicates_dropped,
            "reconnect_count": self._runtime.reconnect_count,
            "warnings": list(self._runtime.warnings),
            "failures": list(self._runtime.failures),
            "checkpoint_path": str(self.checkpoint_path),
            "raw_incremental_root": str(self.raw_root),
            "normalized_incremental_root": str(self.normalized_root),
            "projection_summary_path": projection_summary_path,
            "stream_count": len(self.config.stream_names()),
            "latest_event_time": latest_event_time,
            "stream_state": checkpoint.get("streams", {}),
        }
        _write_json(self.summary_path, summary)
        return BinanceWebsocketIngestResult(
            summary_path=str(self.summary_path),
            checkpoint_path=str(self.checkpoint_path),
            raw_incremental_root=str(self.raw_root),
            normalized_incremental_root=str(self.normalized_root),
            messages_processed=self._runtime.messages_processed,
            messages_written=self._runtime.messages_written,
            duplicates_dropped=self._runtime.duplicates_dropped,
            reconnect_count=self._runtime.reconnect_count,
            warnings=list(self._runtime.warnings),
            failures=list(self._runtime.failures),
            projection_summary_path=projection_summary_path,
        )

    async def run_async(self) -> BinanceWebsocketIngestResult:
        checkpoint = self._load_checkpoint()
        url = self._build_url()
        deadline = monotonic() + self.config.max_runtime_seconds if self.config.max_runtime_seconds is not None else None
        attempt = 0
        while True:
            if self.config.max_messages is not None and self._runtime.messages_processed >= self.config.max_messages:
                break
            if deadline is not None and monotonic() >= deadline:
                break
            try:
                async with self.connect_factory(
                    url,
                    ping_interval=self.config.ping_interval_sec,
                    ping_timeout=self.config.ping_timeout_sec,
                ) as websocket:
                    attempt = 0
                    should_continue = await self._consume_once(websocket, checkpoint, deadline)
                    if not should_continue:
                        break
            except asyncio.TimeoutError:
                warning = "Binance websocket receive timeout; reconnecting"
                logger.warning(warning)
                self._runtime.warnings.append(warning)
            except Exception as exc:
                self._runtime.failures.append({"error_type": type(exc).__name__, "error": str(exc)})
                logger.warning("Binance websocket ingest error: %s", exc)
            attempt += 1
            self._runtime.reconnect_count += 1
            if attempt > self.config.max_reconnect_attempts:
                break
            backoff = min(
                self.config.reconnect_backoff_base_sec * (2 ** max(attempt - 1, 0)),
                self.config.reconnect_backoff_max_sec,
            )
            await asyncio.sleep(backoff)

        return self._finalize()

    def run(self) -> BinanceWebsocketIngestResult:
        try:
            return asyncio.run(self.run_async())
        except KeyboardInterrupt:
            warning = "Binance websocket ingest interrupted by operator"
            logger.warning(warning)
            self._runtime.warnings.append(warning)
            return self._finalize()
