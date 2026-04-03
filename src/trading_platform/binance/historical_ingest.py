from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.binance.client import BinanceClient
from trading_platform.binance.models import (
    BinanceHistoricalIngestConfig,
    BinanceHistoricalIngestSummary,
    BinanceNormalizeConfig,
)
from trading_platform.binance.normalize import normalize_binance_artifacts

logger = logging.getLogger(__name__)

_INTERVAL_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def _parse_datetime_to_ms(value: str | None, *, default_now: bool = False) -> int | None:
    if value is None:
        if default_now:
            return int(datetime.now(UTC).timestamp() * 1000)
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")) or {})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


@dataclass(frozen=True)
class BinanceHistoricalIngestResult:
    summary_path: str
    checkpoint_path: str
    raw_root: str
    normalized_root: str
    exchange_info_path: str
    request_count: int
    retry_count: int
    pages_fetched: int
    raw_artifacts_written: int
    kline_rows_fetched: int
    agg_trade_rows_fetched: int
    book_ticker_snapshots_fetched: int
    normalization_summary_path: str | None = None


class BinanceHistoricalIngestPipeline:
    def __init__(self, client: BinanceClient, config: BinanceHistoricalIngestConfig) -> None:
        self.client = client
        self.config = config
        self.raw_root = Path(config.raw_root)
        self.normalized_root = Path(config.normalized_root)
        self.checkpoint_path = Path(config.checkpoint_path)
        self.summary_path = Path(config.summary_path)
        self.exchange_info_path = Path(config.exchange_info_path)
        self.start_ms = _parse_datetime_to_ms(config.start)
        self.end_ms = _parse_datetime_to_ms(config.end, default_now=True)
        if self.start_ms is None or self.end_ms is None:
            raise ValueError("Historical ingest requires a bounded start and end time")
        if self.start_ms >= self.end_ms:
            raise ValueError("Binance historical ingest start must be before end")

    def _load_checkpoint(self) -> dict[str, Any]:
        checkpoint = _load_json(self.checkpoint_path)
        checkpoint.setdefault("symbols", {})
        return checkpoint

    def _write_checkpoint(self, checkpoint: dict[str, Any]) -> None:
        _write_json(self.checkpoint_path, checkpoint)

    def _symbol_state(self, checkpoint: dict[str, Any], symbol: str) -> dict[str, Any]:
        symbols = checkpoint.setdefault("symbols", {})
        symbol_state = dict(symbols.get(symbol) or {})
        symbol_state.setdefault("klines", {})
        symbol_state.setdefault("agg_trades", {})
        symbol_state.setdefault("book_ticker", {})
        symbols[symbol] = symbol_state
        return symbol_state

    def _write_raw_batch(self, root: Path, filename: str, payload: Any) -> tuple[Path, bool]:
        root.mkdir(parents=True, exist_ok=True)
        path = root / filename
        created = False
        if not path.exists():
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            created = True
        return path, created

    def _validate_symbols(self, exchange_info: dict[str, Any]) -> tuple[list[str], list[str]]:
        valid_symbols = {str(entry.get("symbol", "")).upper(): entry for entry in exchange_info.get("symbols", [])}
        validated: list[str] = []
        skipped: list[str] = []
        for symbol in self.config.symbols:
            info = valid_symbols.get(symbol)
            if not info or info.get("status") != "TRADING":
                skipped.append(symbol)
                continue
            validated.append(symbol)
        return validated, skipped

    def _fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        checkpoint: dict[str, Any],
        summary: BinanceHistoricalIngestSummary,
    ) -> None:
        interval_ms = _INTERVAL_TO_MS.get(interval)
        if interval_ms is None:
            raise ValueError(f"Unsupported Binance interval for this milestone: {interval}")
        symbol_state = self._symbol_state(checkpoint, symbol)
        kline_state = dict(symbol_state["klines"].get(interval) or {})
        next_start_ms = int(kline_state.get("next_start_ms", self.start_ms))
        while next_start_ms < self.end_ms:
            batch = self.client.get_klines(
                symbol=symbol,
                interval=interval,
                start_time_ms=next_start_ms,
                end_time_ms=self.end_ms,
                limit=self.config.kline_limit,
            )
            summary.pages_fetched += 1
            if not batch:
                break
            filename = f"{int(batch[0][0])}_{int(batch[-1][0])}.json"
            path, created = self._write_raw_batch(self.raw_root / "klines" / symbol / interval, filename, batch)
            if created:
                summary.raw_artifacts_written += 1
            summary.kline_rows_fetched += len(batch)
            per_symbol = summary.per_symbol.setdefault(symbol, {"intervals": {}, "agg_trades": {}, "book_ticker": {}})
            interval_payload = per_symbol["intervals"].setdefault(interval, {"rows": 0, "files": []})
            interval_payload["rows"] += len(batch)
            interval_payload["files"].append(str(path))
            last_open_time_ms = int(batch[-1][0])
            next_start_ms = last_open_time_ms + interval_ms
            symbol_state["klines"][interval] = {"next_start_ms": next_start_ms}
            self._write_checkpoint(checkpoint)
            if len(batch) < self.config.kline_limit:
                break

    def _fetch_agg_trades(
        self,
        *,
        symbol: str,
        checkpoint: dict[str, Any],
        summary: BinanceHistoricalIngestSummary,
    ) -> None:
        symbol_state = self._symbol_state(checkpoint, symbol)
        agg_state = dict(symbol_state.get("agg_trades") or {})
        next_from_id = agg_state.get("next_from_id")
        first_request = next_from_id is None
        while True:
            batch = self.client.get_agg_trades(
                symbol=symbol,
                start_time_ms=self.start_ms if first_request else None,
                end_time_ms=self.end_ms if first_request else None,
                from_id=int(next_from_id) if next_from_id is not None else None,
                limit=self.config.agg_trade_limit,
            )
            first_request = False
            summary.pages_fetched += 1
            if not batch:
                break
            filtered = [row for row in batch if int(row["T"]) <= self.end_ms]
            if not filtered:
                break
            filename = f"{int(filtered[0]['a'])}_{int(filtered[-1]['a'])}.json"
            path, created = self._write_raw_batch(self.raw_root / "agg_trades" / symbol, filename, filtered)
            if created:
                summary.raw_artifacts_written += 1
            summary.agg_trade_rows_fetched += len(filtered)
            per_symbol = summary.per_symbol.setdefault(symbol, {"intervals": {}, "agg_trades": {}, "book_ticker": {}})
            trades_payload = per_symbol.get("agg_trades") or {"rows": 0, "files": []}
            per_symbol["agg_trades"] = trades_payload
            trades_payload["rows"] += len(filtered)
            trades_payload["files"].append(str(path))
            next_from_id = int(filtered[-1]["a"]) + 1
            symbol_state["agg_trades"] = {"next_from_id": next_from_id}
            self._write_checkpoint(checkpoint)
            if len(filtered) < self.config.agg_trade_limit or int(filtered[-1]["T"]) >= self.end_ms:
                break

    def _fetch_book_ticker(
        self,
        *,
        symbol: str,
        checkpoint: dict[str, Any],
        summary: BinanceHistoricalIngestSummary,
    ) -> None:
        symbol_state = self._symbol_state(checkpoint, symbol)
        book_state = dict(symbol_state.get("book_ticker") or {})
        if book_state.get("captured"):
            return
        payload = self.client.get_book_ticker(symbol=symbol)
        payload["_snapshot_timestamp"] = datetime.now(UTC).isoformat()
        filename = f"{payload['_snapshot_timestamp'].replace(':', '').replace('-', '')}.json"
        path, created = self._write_raw_batch(self.raw_root / "book_ticker" / symbol, filename, payload)
        summary.pages_fetched += 1
        if created:
            summary.raw_artifacts_written += 1
        summary.book_ticker_snapshots_fetched += 1
        per_symbol = summary.per_symbol.setdefault(symbol, {"intervals": {}, "agg_trades": {}, "book_ticker": {}})
        per_symbol["book_ticker"] = {"files": [str(path)], "rows": 1}
        symbol_state["book_ticker"] = {"captured": True}
        self._write_checkpoint(checkpoint)

    def run(self) -> BinanceHistoricalIngestResult:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.normalized_root.mkdir(parents=True, exist_ok=True)
        checkpoint = self._load_checkpoint()
        exchange_info = self.client.get_exchange_info()
        _write_json(self.exchange_info_path, exchange_info)
        validated_symbols, skipped_symbols = self._validate_symbols(exchange_info)
        summary = BinanceHistoricalIngestSummary(
            symbols_requested=list(self.config.symbols),
            symbols_validated=validated_symbols,
            intervals=list(self.config.intervals),
            start=self.config.start,
            end=self.config.end,
            exchange_info_path=str(self.exchange_info_path),
            raw_root=str(self.raw_root),
            normalized_root=str(self.normalized_root),
            skipped_symbols=skipped_symbols,
            rate_limits=list(exchange_info.get("rateLimits") or []),
        )

        for symbol in validated_symbols:
            try:
                for interval in self.config.intervals:
                    self._fetch_klines(symbol=symbol, interval=interval, checkpoint=checkpoint, summary=summary)
                self._fetch_agg_trades(symbol=symbol, checkpoint=checkpoint, summary=summary)
                if self.config.capture_book_ticker:
                    self._fetch_book_ticker(symbol=symbol, checkpoint=checkpoint, summary=summary)
            except Exception as exc:
                logger.exception("Binance ingest failed for %s", symbol)
                summary.failures.append({"symbol": symbol, "error_type": type(exc).__name__, "error": str(exc)})

        summary.request_count = self.client.stats.request_count
        summary.retry_count = self.client.stats.retry_count

        if self.config.normalize_after_ingest:
            normalize_result = normalize_binance_artifacts(BinanceNormalizeConfig.from_ingest_config(self.config))
            summary.normalization_summary_path = normalize_result.summary_path

        _write_json(self.summary_path, summary.to_dict())
        return BinanceHistoricalIngestResult(
            summary_path=str(self.summary_path),
            checkpoint_path=str(self.checkpoint_path),
            raw_root=str(self.raw_root),
            normalized_root=str(self.normalized_root),
            exchange_info_path=str(self.exchange_info_path),
            request_count=summary.request_count,
            retry_count=summary.retry_count,
            pages_fetched=summary.pages_fetched,
            raw_artifacts_written=summary.raw_artifacts_written,
            kline_rows_fetched=summary.kline_rows_fetched,
            agg_trade_rows_fetched=summary.agg_trade_rows_fetched,
            book_ticker_snapshots_fetched=summary.book_ticker_snapshots_fetched,
            normalization_summary_path=summary.normalization_summary_path,
        )
