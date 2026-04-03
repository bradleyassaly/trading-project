from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.historical_ingest import BinanceHistoricalIngestPipeline
from trading_platform.binance.models import BinanceHistoricalIngestConfig


class FakeBinanceClient:
    def __init__(self) -> None:
        self.stats = type("Stats", (), {"request_count": 0, "retry_count": 0})()
        self.kline_calls: list[dict[str, object]] = []
        self.agg_trade_calls: list[dict[str, object]] = []
        self.book_ticker_calls: list[str] = []

    def get_exchange_info(self) -> dict[str, object]:
        self.stats.request_count += 1
        return {
            "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "limit": 6000}],
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING"},
                {"symbol": "ETHUSDT", "status": "TRADING"},
            ],
        }

    def get_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int,
    ) -> list[list[object]]:
        self.stats.request_count += 1
        self.kline_calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
                "limit": limit,
            }
        )
        minute = 60_000
        if len(self.kline_calls) == 1:
            return [
                [start_time_ms, "100.0", "101.0", "99.0", "100.5", "2.0", start_time_ms + minute - 1, "200.0", 3, "1.0", "100.0"],
                [start_time_ms + minute, "100.5", "102.0", "100.0", "101.5", "3.0", start_time_ms + (2 * minute) - 1, "300.0", 4, "1.5", "150.0"],
            ]
        return []

    def get_agg_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        from_id: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, object]]:
        self.stats.request_count += 1
        self.agg_trade_calls.append(
            {
                "symbol": symbol,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
                "from_id": from_id,
                "limit": limit,
            }
        )
        if len(self.agg_trade_calls) == 1:
            return [
                {"a": 10, "p": "100.1", "q": "0.2", "f": 1, "l": 2, "T": start_time_ms, "m": False, "M": True},
                {"a": 11, "p": "100.2", "q": "0.3", "f": 3, "l": 4, "T": start_time_ms + 1_000, "m": True, "M": True},
            ]
        return []

    def get_book_ticker(self, *, symbol: str) -> dict[str, object]:
        self.stats.request_count += 1
        self.book_ticker_calls.append(symbol)
        return {"symbol": symbol, "bidPrice": "100.0", "bidQty": "1.0", "askPrice": "100.1", "askQty": "1.2"}


def _config(tmp_path: Path, **overrides: object) -> BinanceHistoricalIngestConfig:
    config = BinanceHistoricalIngestConfig(
        symbols=("BTCUSDT",),
        intervals=("1m",),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:05:00Z",
        raw_root=str(tmp_path / "raw"),
        normalized_root=str(tmp_path / "normalized"),
        checkpoint_path=str(tmp_path / "raw" / "ingest_checkpoint.json"),
        summary_path=str(tmp_path / "raw" / "ingest_summary.json"),
        exchange_info_path=str(tmp_path / "raw" / "exchange_info.json"),
    )
    return BinanceHistoricalIngestConfig(**{**config.__dict__, **overrides})


def test_historical_ingest_writes_raw_and_normalized_artifacts(tmp_path: Path) -> None:
    client = FakeBinanceClient()
    result = BinanceHistoricalIngestPipeline(client, _config(tmp_path)).run()

    assert result.pages_fetched == 3
    assert result.kline_rows_fetched == 2
    assert result.agg_trade_rows_fetched == 2
    assert result.book_ticker_snapshots_fetched == 1
    assert Path(result.exchange_info_path).exists()
    assert Path(result.summary_path).exists()
    assert (tmp_path / "normalized" / "klines" / "BTCUSDT" / "1m.parquet").exists()
    assert (tmp_path / "normalized" / "agg_trades" / "BTCUSDT.parquet").exists()
    assert (tmp_path / "normalized" / "book_ticker" / "BTCUSDT.parquet").exists()

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["symbols_validated"] == ["BTCUSDT"]
    assert summary["pages_fetched"] == 3
    assert summary["rate_limits"][0]["limit"] == 6000


def test_historical_ingest_uses_checkpoint_to_resume(tmp_path: Path) -> None:
    client = FakeBinanceClient()
    checkpoint_path = tmp_path / "raw" / "ingest_checkpoint.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps({"symbols": {"BTCUSDT": {"klines": {"1m": {"next_start_ms": 1704067320000}}}}}),
        encoding="utf-8",
    )

    BinanceHistoricalIngestPipeline(client, _config(tmp_path)).run()

    assert client.kline_calls[0]["start_time_ms"] == 1704067320000


def test_historical_ingest_skips_invalid_symbols(tmp_path: Path) -> None:
    client = FakeBinanceClient()
    config = _config(tmp_path, symbols=("BTCUSDT", "XRPUSDT"))
    result = BinanceHistoricalIngestPipeline(client, config).run()

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["symbols_validated"] == ["BTCUSDT"]
    assert summary["skipped_symbols"] == ["XRPUSDT"]


def test_historical_ingest_normalized_kline_output_has_expected_columns(tmp_path: Path) -> None:
    client = FakeBinanceClient()
    BinanceHistoricalIngestPipeline(client, _config(tmp_path)).run()

    frame = pd.read_parquet(tmp_path / "normalized" / "klines" / "BTCUSDT" / "1m.parquet")
    assert set(
        [
            "timestamp",
            "close_timestamp",
            "symbol",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "provider",
            "source",
            "asset_class",
            "schema_version",
            "raw_artifact_path",
        ]
    ).issubset(frame.columns)
    assert frame["symbol"].tolist() == ["BTCUSDT", "BTCUSDT"]
