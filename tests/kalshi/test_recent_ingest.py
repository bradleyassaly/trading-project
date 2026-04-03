from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from trading_platform.kalshi.recent_ingest import RecentIngestConfig, RecentIngestPipeline


def _make_config(tmp_path: Path, **overrides: object) -> RecentIngestConfig:
    base = RecentIngestConfig(
        raw_markets_dir=str(tmp_path / "data/kalshi/raw/markets"),
        raw_trades_dir=str(tmp_path / "data/kalshi/raw/trades"),
        raw_candles_dir=str(tmp_path / "data/kalshi/raw/candles"),
        trades_parquet_dir=str(tmp_path / "data/kalshi/normalized/trades"),
        normalized_candles_dir=str(tmp_path / "data/kalshi/normalized/candles"),
        normalized_markets_path=str(tmp_path / "data/kalshi/normalized/markets.parquet"),
        features_dir=str(tmp_path / "data/kalshi/features/real"),
        resolution_csv_path=str(tmp_path / "data/kalshi/normalized/resolution.csv"),
        legacy_resolution_csv_path=str(tmp_path / "data/kalshi/resolution.csv"),
        manifest_path=str(tmp_path / "data/kalshi/raw/recent_ingest_manifest.json"),
        checkpoint_path=str(tmp_path / "data/kalshi/raw/recent_ingest_checkpoint.json"),
        checkpoint_backup_path=str(tmp_path / "data/kalshi/raw/recent_ingest_checkpoint.bak.json"),
        summary_path=str(tmp_path / "data/kalshi/raw/recent_ingest_summary.json"),
        status_artifacts_root=str(tmp_path / "artifacts/kalshi_ingest"),
        lookback_days=30,
        min_trades=2,
        request_sleep_sec=0.0,
        authenticated_request_sleep_sec=0.0,
        run_base_rate=False,
        run_metaculus=False,
        resume=False,
        resume_mode="fresh",
        recent_ingest_statuses=["settled"],
        recent_ingest_categories=["Economics"],
        recent_ingest_limit=50,
        preferred_categories=["Economics"],
        economics_series=["KXINFL", "KXFED", "KXGDP", "KXJOBS", "KXPCE", "KXCPI"],
        politics_series=["KXPRESAPPROVAL", "KXSENATE"],
        exclude_market_type_patterns=["CROSSCATEGORY", "SPORTSMULTIGAME", "EXTENDED"],
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def _market(
    ticker: str,
    *,
    close_time: datetime | None = None,
    status: str = "settled",
    category: str = "Economics",
    result: str = "yes",
    time_field_name: str = "close_time",
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "title": f"{ticker} title",
        "series_ticker": "KXINFL",
        "event_ticker": "KXINFL-2026",
        "status": status,
        "category": category,
        "result": result,
        **({time_field_name: close_time.isoformat()} if close_time is not None else {}),
        **({"expiration_time": close_time.isoformat()} if close_time is not None and time_field_name != "expiration_time" else {}),
        **({"settlement_time": (close_time + timedelta(hours=1)).isoformat()} if close_time is not None else {}),
        "yes_bid_dollars": 0.55,
        "yes_ask_dollars": 0.57,
        "no_bid_dollars": 0.43,
        "no_ask_dollars": 0.45,
        "last_price_dollars": 0.56,
        "volume": 12,
        "open_interest": 7,
        "liquidity_dollars": 1000,
    }


def _trade(ticker: str, trade_id: str, created_time: datetime, *, yes_price: float = 0.55) -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "ticker": ticker,
        "taker_side": "yes",
        "yes_price_dollars": yes_price,
        "no_price_dollars": 1.0 - yes_price,
        "count_fp": 1.0,
        "created_time": created_time.isoformat(),
    }


def _candles() -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        {
            "end_period_ts": (start + timedelta(hours=1)).isoformat(),
            "open_price_dollars": 0.45,
            "high_price_dollars": 0.55,
            "low_price_dollars": 0.40,
            "close_price_dollars": 0.50,
            "count": 3,
        },
        {
            "end_period_ts": (start + timedelta(hours=2)).isoformat(),
            "open_price_dollars": 0.50,
            "high_price_dollars": 0.58,
            "low_price_dollars": 0.48,
            "close_price_dollars": 0.56,
            "count": 5,
        },
    ]


def _market_batch(prefix: str, count: int, *, start_days_ago: int = 1) -> list[dict[str, object]]:
    now = datetime.now(UTC)
    return [
        _market(f"{prefix}-{index + 1}", close_time=now - timedelta(days=start_days_ago + index))
        for index in range(count)
    ]


class FakeRecentClient:
    def __init__(
        self,
        *,
        live_pages: dict[tuple[str | None, str | None, str | None, str | None], list[tuple[list[dict[str, object]], str | None]]],
        historical_markets: dict[str, dict[str, object]] | None = None,
        live_trades: dict[str, list[dict[str, object]]] | None = None,
        historical_trades: dict[str, list[dict[str, object]]] | None = None,
        live_candles: dict[str, list[dict[str, object]]] | None = None,
        historical_candles: dict[str, list[dict[str, object]]] | None = None,
    ) -> None:
        self.live_pages = {key: list(value) for key, value in live_pages.items()}
        self.historical_markets = historical_markets or {}
        self.live_trades = live_trades or {}
        self.historical_trades = historical_trades or {}
        self.live_candles = live_candles or {}
        self.historical_candles = historical_candles or {}
        self.market_calls: list[dict[str, object]] = []
        self.historical_market_calls: list[str] = []

    def get_historical_cutoff(self) -> dict[str, str]:
        cutoff = datetime.now(UTC) - timedelta(days=7)
        return {
            "market_settled_ts": cutoff.isoformat(),
            "trades_created_ts": cutoff.isoformat(),
            "orders_updated_ts": cutoff.isoformat(),
        }

    def get_markets_raw(
        self,
        *,
        status: str | None = None,
        category: str | None = None,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        tickers: list[str] | None = None,
    ) -> tuple[list[dict[str, object]], str | None]:
        self.market_calls.append(
            {
                "status": status,
                "category": category,
                "series_ticker": series_ticker,
                "event_ticker": event_ticker,
                "limit": limit,
                "cursor": cursor,
                "tickers": tickers,
            }
        )
        key = (status, category, series_ticker, event_ticker)
        pages = self.live_pages.get(key, [])
        return pages.pop(0) if pages else ([], None)

    def get_historical_market(self, ticker: str) -> dict[str, object]:
        self.historical_market_calls.append(ticker)
        return dict(self.historical_markets[ticker])

    def get_all_historical_trades(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.historical_trades.get(ticker, []))

    def get_all_trades_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.live_trades.get(ticker, []))

    def get_historical_market_candlesticks_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.historical_candles.get(ticker, []))

    def get_market_candlesticks_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.live_candles.get(ticker, []))


def test_recent_ingest_live_filters_pagination_and_normalized_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    market_one = _market("ECO-RECENT-1", close_time=now - timedelta(days=2))
    market_two = _market("ECO-RECENT-2", close_time=now - timedelta(days=1), result="no")
    client = FakeRecentClient(
        live_pages={
            ("settled", "Economics", None, None): [
                ([market_one], "cursor-2"),
                ([market_two], None),
            ]
        },
        live_trades={
            "ECO-RECENT-1": [_trade("ECO-RECENT-1", "t1", now - timedelta(days=2)), _trade("ECO-RECENT-1", "t2", now - timedelta(days=2, hours=-1))],
            "ECO-RECENT-2": [_trade("ECO-RECENT-2", "t3", now - timedelta(days=1), yes_price=0.42), _trade("ECO-RECENT-2", "t4", now - timedelta(days=1, hours=-1), yes_price=0.41)],
        },
        historical_trades={"ECO-RECENT-1": [], "ECO-RECENT-2": []},
        live_candles={"ECO-RECENT-1": _candles(), "ECO-RECENT-2": _candles()},
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    result = RecentIngestPipeline(client, _make_config(tmp_path)).run()

    assert result.markets_downloaded == 2
    assert result.markets_with_trades == 2
    assert len(client.market_calls) == 2
    assert client.market_calls[0]["status"] == "settled"
    assert client.market_calls[0]["category"] == "Economics"

    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet").sort("ticker")
    assert markets.get_column("ticker").to_list() == ["ECO-RECENT-1", "ECO-RECENT-2"]
    assert markets.get_column("source_endpoint").to_list() == ["/markets", "/markets"]
    assert markets.get_column("source_mode").to_list() == ["live_recent_filtered", "live_recent_filtered"]
    assert "settlement_time" in markets.columns
    assert "yes_bid" in markets.columns
    assert "open_interest" in markets.columns

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert summary["recent_ingest"]["statuses"] == ["settled"]
    assert summary["recent_ingest"]["categories"] == ["Economics"]
    assert summary["page_diagnostics_summary"]["pages_fetched"] == 2


def test_recent_ingest_corrects_mismatched_category_series_combination(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    market = _market("ECO-CONFLICT-1", close_time=now - timedelta(days=1))
    client = FakeRecentClient(
        live_pages={("settled", "Economics", "KXINFL", None): [([market], None)]},
        live_trades={"ECO-CONFLICT-1": []},
        historical_trades={"ECO-CONFLICT-1": []},
        live_candles={"ECO-CONFLICT-1": []},
    )

    config = _make_config(
        tmp_path,
        recent_ingest_categories=["Politics"],
        recent_ingest_series_tickers=["KXINFL"],
        preferred_categories=["Politics"],
    )
    result = RecentIngestPipeline(client, config).run()

    assert result.markets_downloaded == 1
    assert client.market_calls[0]["category"] == "Economics"
    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert summary["recent_ingest"]["filter_resolution"]["filter_conflicts"]
    assert summary["recent_ingest"]["filter_resolution"]["category_ignored_for_series_count"] == 1


def test_recent_ingest_keeps_valid_category_series_combination(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    market = _market("ECO-VALID-1", close_time=now - timedelta(days=1))
    client = FakeRecentClient(
        live_pages={("settled", "Economics", "KXINFL", None): [([market], None)]},
        live_trades={"ECO-VALID-1": []},
        historical_trades={"ECO-VALID-1": []},
        live_candles={"ECO-VALID-1": []},
    )

    config = _make_config(
        tmp_path,
        recent_ingest_categories=["Economics"],
        recent_ingest_series_tickers=["KXINFL"],
        preferred_categories=["Economics"],
    )
    RecentIngestPipeline(client, config).run()

    assert client.market_calls[0]["category"] == "Economics"
    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert summary["recent_ingest"]["filter_resolution"]["filter_conflicts"] == []


def test_recent_ingest_zero_results_reports_filter_conflict_diagnostics(tmp_path: Path) -> None:
    client = FakeRecentClient(live_pages={("settled", "Economics", "KXINFL", None): [([], None)]})
    config = _make_config(
        tmp_path,
        recent_ingest_categories=["Politics"],
        recent_ingest_series_tickers=["KXINFL"],
        preferred_categories=["Politics"],
    )

    result = RecentIngestPipeline(client, config).run()

    assert result.markets_downloaded == 0
    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert summary["recent_ingest"]["zero_results_due_to_filter_conflicts"] is True


def test_recent_ingest_deduplicates_tickers_and_is_safe_to_rerun(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    dup_market = _market("ECO-DUP-1", close_time=now - timedelta(days=2))
    other_market = _market("ECO-DUP-2", close_time=now - timedelta(days=1))

    def _build_client() -> FakeRecentClient:
        return FakeRecentClient(
            live_pages={
                ("settled", "Economics", None, None): [
                    ([dup_market], "cursor-next"),
                    ([dup_market, other_market], None),
                ]
            },
            live_trades={
                "ECO-DUP-1": [_trade("ECO-DUP-1", "t1", now - timedelta(days=2)), _trade("ECO-DUP-1", "t2", now - timedelta(days=2, hours=-1))],
                "ECO-DUP-2": [_trade("ECO-DUP-2", "t3", now - timedelta(days=1)), _trade("ECO-DUP-2", "t4", now - timedelta(days=1, hours=-1))],
            },
            historical_trades={"ECO-DUP-1": [], "ECO-DUP-2": []},
            live_candles={"ECO-DUP-1": _candles(), "ECO-DUP-2": _candles()},
        )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path)
    first_result = RecentIngestPipeline(_build_client(), config).run()
    second_result = RecentIngestPipeline(_build_client(), config).run()

    assert first_result.markets_downloaded == 3
    assert second_result.markets_downloaded == 3
    assert len(list((tmp_path / "data/kalshi/raw/markets").glob("*.json"))) == 2

    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    assert sorted(markets.get_column("ticker").to_list()) == ["ECO-DUP-1", "ECO-DUP-2"]


def test_recent_ingest_supports_direct_historical_ticker_lookup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    old_market = _market("ECO-OLD-1", close_time=now - timedelta(days=5))
    client = FakeRecentClient(
        live_pages={},
        historical_markets={"ECO-OLD-1": old_market},
        historical_trades={
            "ECO-OLD-1": [_trade("ECO-OLD-1", "t1", now - timedelta(days=4)), _trade("ECO-OLD-1", "t2", now - timedelta(days=3), yes_price=0.47)],
        },
        historical_candles={"ECO-OLD-1": _candles()},
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [48.0], "symbol": [ticker]}),
    )

    config = _make_config(
        tmp_path,
        recent_ingest_enabled=False,
        recent_ingest_categories=[],
        preferred_categories=[],
        direct_historical_tickers=["ECO-OLD-1"],
    )
    result = RecentIngestPipeline(client, config).run()

    assert result.markets_downloaded == 1
    assert client.historical_market_calls == ["ECO-OLD-1"]

    raw_market = json.loads((tmp_path / "data/kalshi/raw/markets" / "ECO-OLD-1.json").read_text(encoding="utf-8"))
    assert raw_market["source_mode"] == "direct_historical_ticker"
    assert raw_market["source_endpoint"] == "/historical/markets/ECO-OLD-1"

    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    row = markets.row(0, named=True)
    assert row["ticker"] == "ECO-OLD-1"
    assert row["source_mode"] == "direct_historical_ticker"


def test_recent_ingest_keeps_valid_market_with_no_trades_or_candles(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    market = _market("ECO-NOTRADES-1", close_time=now - timedelta(days=2))
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([market], None)]},
        live_trades={"ECO-NOTRADES-1": []},
        historical_trades={"ECO-NOTRADES-1": []},
        live_candles={"ECO-NOTRADES-1": []},
    )

    result = RecentIngestPipeline(client, _make_config(tmp_path)).run()

    assert result.markets_downloaded == 1
    assert result.markets_with_trades == 0
    assert result.normalized_markets_written == 1

    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    assert markets.get_column("ticker").to_list() == ["ECO-NOTRADES-1"]
    assert json.loads((tmp_path / "data/kalshi/raw/trades" / "ECO-NOTRADES-1.json").read_text(encoding="utf-8")) == []
    assert json.loads((tmp_path / "data/kalshi/raw/candles" / "ECO-NOTRADES-1.json").read_text(encoding="utf-8")) == []
    assert pl.read_parquet(tmp_path / "data/kalshi/normalized/trades" / "ECO-NOTRADES-1.parquet").is_empty()
    assert pl.read_parquet(tmp_path / "data/kalshi/normalized/candles" / "ECO-NOTRADES-1.parquet").is_empty()


def test_recent_ingest_keeps_market_with_no_time_fields(tmp_path: Path) -> None:
    market = _market("ECO-NOTIME-1", close_time=None)
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([market], None)]},
        live_trades={"ECO-NOTIME-1": []},
        historical_trades={"ECO-NOTIME-1": []},
        live_candles={"ECO-NOTIME-1": []},
    )

    result = RecentIngestPipeline(client, _make_config(tmp_path)).run()

    assert result.markets_downloaded == 1
    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    row = markets.row(0, named=True)
    assert row["ticker"] == "ECO-NOTIME-1"
    assert row["status"] == "settled"
    assert row["close_time"] is None


def test_recent_ingest_maps_alternative_time_field_names(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    market = _market("ECO-ALT-TIME-1", close_time=now - timedelta(days=1), time_field_name="end_date")
    market.pop("expiration_time", None)
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([market], None)]},
        live_trades={"ECO-ALT-TIME-1": []},
        historical_trades={"ECO-ALT-TIME-1": []},
        live_candles={"ECO-ALT-TIME-1": []},
    )

    RecentIngestPipeline(client, _make_config(tmp_path)).run()

    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    row = markets.row(0, named=True)
    assert row["ticker"] == "ECO-ALT-TIME-1"
    assert str(row["close_time"]).startswith((now - timedelta(days=1)).date().isoformat())


def test_recent_ingest_mixed_dataset_writes_all_valid_markets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    traded_market = _market("ECO-MIX-1", close_time=now - timedelta(days=2))
    pricing_only_market = _market("ECO-MIX-2", close_time=None, result="no")
    alt_time_market = _market("ECO-MIX-3", close_time=now - timedelta(days=1), result="yes", time_field_name="expiration_ts")
    alt_time_market.pop("expiration_time", None)
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([traded_market, pricing_only_market, alt_time_market], None)]},
        live_trades={
            "ECO-MIX-1": [_trade("ECO-MIX-1", "t1", now - timedelta(days=2)), _trade("ECO-MIX-1", "t2", now - timedelta(days=2, hours=-1))]
        },
        historical_trades={"ECO-MIX-1": [], "ECO-MIX-2": [], "ECO-MIX-3": []},
        live_candles={"ECO-MIX-1": _candles(), "ECO-MIX-2": [], "ECO-MIX-3": []},
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    result = RecentIngestPipeline(client, _make_config(tmp_path)).run()

    assert result.markets_downloaded == 3
    assert result.markets_with_trades == 1
    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet").sort("ticker")
    assert markets.get_column("ticker").to_list() == ["ECO-MIX-1", "ECO-MIX-2", "ECO-MIX-3"]
    assert (tmp_path / "data/kalshi/normalized/trades" / "ECO-MIX-2.parquet").exists()


def test_recent_ingest_limit_caps_total_fetched_records_and_logs_stop_reason(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeRecentClient(
        live_pages={
            ("settled", "Economics", None, None): [
                (_market_batch("ECO-LIMIT5", 8), "cursor-next"),
            ]
        },
        live_trades={},
        historical_trades={},
        live_candles={},
    )

    caplog.set_level("INFO", logger="trading_platform.kalshi.recent_ingest")
    RecentIngestPipeline(
        client,
        _make_config(tmp_path, recent_ingest_limit=5, market_page_size=10),
    ).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert len(client.market_calls) == 1
    assert client.market_calls[0]["limit"] == 5
    assert summary["page_diagnostics_summary"]["total_markets_fetched"] == 5
    assert summary["page_diagnostics_summary"]["pages_fetched"] == 1
    assert summary["page_diagnostics_summary"]["pagination_stop_reason"] == "recent_limit_reached"
    assert "page=1 records=5 total_fetched=5 stop_reason=recent_limit_reached" in caplog.text


def test_recent_ingest_limit_stops_after_total_records_across_multiple_pages(tmp_path: Path) -> None:
    client = FakeRecentClient(
        live_pages={
            ("settled", "Economics", None, None): [
                (_market_batch("ECO-LIMIT20A", 7, start_days_ago=1), "cursor-2"),
                (_market_batch("ECO-LIMIT20B", 7, start_days_ago=8), "cursor-3"),
                (_market_batch("ECO-LIMIT20C", 7, start_days_ago=15), "cursor-4"),
                (_market_batch("ECO-LIMIT20D", 7, start_days_ago=22), None),
            ]
        },
        live_trades={},
        historical_trades={},
        live_candles={},
    )

    RecentIngestPipeline(
        client,
        _make_config(tmp_path, recent_ingest_limit=20, market_page_size=7),
    ).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    assert [call["limit"] for call in client.market_calls] == [7, 7, 6]
    assert summary["page_diagnostics_summary"]["total_markets_fetched"] == 20
    assert summary["page_diagnostics_summary"]["pages_fetched"] == 3
    assert summary["page_diagnostics_summary"]["pagination_stop_reason"] == "recent_limit_reached"
    assert markets.height == 20


def test_recent_ingest_excludes_low_volume_and_market_type_markets(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    now = datetime.now(UTC)
    valid_market = _market("ECO-HIGHSIGNAL-1", close_time=now - timedelta(days=1))
    valid_market["volume"] = 250
    low_volume_market = _market("ECO-LOWVOL-1", close_time=now - timedelta(days=2))
    low_volume_market["volume"] = 25
    market_type_market = _market("ECO-CROSSCATEGORY-1", close_time=now - timedelta(days=3))
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([valid_market, low_volume_market, market_type_market], None)]},
        live_trades={
            "ECO-HIGHSIGNAL-1": [_trade("ECO-HIGHSIGNAL-1", "t1", now - timedelta(days=1)), _trade("ECO-HIGHSIGNAL-1", "t2", now - timedelta(days=1, hours=-1))]
        },
        historical_trades={"ECO-HIGHSIGNAL-1": []},
        live_candles={"ECO-HIGHSIGNAL-1": _candles()},
    )

    caplog.set_level("INFO", logger="trading_platform.kalshi.recent_ingest")
    result = RecentIngestPipeline(client, _make_config(tmp_path, min_volume=100)).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet")
    assert result.markets_downloaded == 3
    assert markets.get_column("ticker").to_list() == ["ECO-HIGHSIGNAL-1"]
    assert summary["filter_diagnostics"]["excluded_by_min_volume"] == 1
    assert summary["filter_diagnostics"]["excluded_by_market_type"] == 1
    assert summary["recent_ingest"]["market_type_filter_enabled"] is True
    assert "Excluding Kalshi recent market ECO-LOWVOL-1: reason=min_volume volume=25.00" in caplog.text
    assert "Excluding Kalshi recent market ECO-CROSSCATEGORY-1: reason=market_type:CROSSCATEGORY volume=12.00" in caplog.text


def test_recent_ingest_disable_market_type_filter_override_keeps_type_filtered_markets(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    valid_market = _market("ECO-HIGHSIGNAL-1", close_time=now - timedelta(days=1))
    valid_market["volume"] = 250
    market_type_market = _market("ECO-CROSSCATEGORY-1", close_time=now - timedelta(days=2))
    market_type_market["volume"] = 250
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([valid_market, market_type_market], None)]},
        live_trades={
            "ECO-HIGHSIGNAL-1": [_trade("ECO-HIGHSIGNAL-1", "t1", now - timedelta(days=1)), _trade("ECO-HIGHSIGNAL-1", "t2", now - timedelta(days=1, hours=-1))],
            "ECO-CROSSCATEGORY-1": [_trade("ECO-CROSSCATEGORY-1", "t3", now - timedelta(days=2)), _trade("ECO-CROSSCATEGORY-1", "t4", now - timedelta(days=2, hours=-1))],
        },
        historical_trades={"ECO-HIGHSIGNAL-1": [], "ECO-CROSSCATEGORY-1": []},
        live_candles={"ECO-HIGHSIGNAL-1": _candles(), "ECO-CROSSCATEGORY-1": _candles()},
    )

    result = RecentIngestPipeline(client, _make_config(tmp_path, min_volume=100, disable_market_type_filter=True)).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    markets = pl.read_parquet(tmp_path / "data/kalshi/normalized/markets.parquet").sort("ticker")
    assert result.markets_downloaded == 2
    assert markets.get_column("ticker").to_list() == ["ECO-CROSSCATEGORY-1", "ECO-HIGHSIGNAL-1"]
    assert summary["filter_diagnostics"]["excluded_by_market_type"] == 0
    assert summary["recent_ingest"]["market_type_filter_enabled"] is False


def test_recent_ingest_warns_when_all_markets_excluded_by_market_type(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    now = datetime.now(UTC)
    first_market = _market("ECO-CROSSCATEGORY-1", close_time=now - timedelta(days=1))
    first_market["volume"] = 250
    second_market = _market("ECO-SPORTSMULTIGAME-1", close_time=now - timedelta(days=2))
    second_market["volume"] = 250
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([first_market, second_market], None)]},
        live_trades={},
        historical_trades={},
        live_candles={},
    )

    caplog.set_level("INFO")
    result = RecentIngestPipeline(client, _make_config(tmp_path, min_volume=100)).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert result.markets_downloaded == 2
    assert summary["filter_diagnostics"]["excluded_by_market_type"] == 2
    assert "market-type filter removed all fetched markets" in caplog.text


def test_recent_ingest_skips_feature_generation_below_min_trades(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    market = _market("ECO-LOWTRADES-1", close_time=now - timedelta(days=1))
    client = FakeRecentClient(
        live_pages={("settled", "Economics", None, None): [([market], None)]},
        live_trades={"ECO-LOWTRADES-1": [_trade("ECO-LOWTRADES-1", "t1", now - timedelta(days=1))]},
        historical_trades={"ECO-LOWTRADES-1": []},
        live_candles={"ECO-LOWTRADES-1": _candles()},
    )
    feature_calls: list[str] = []

    def _unexpected_feature_call(*_, ticker, **__):
        feature_calls.append(ticker)
        return pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]})

    monkeypatch.setattr("trading_platform.kalshi.features.build_kalshi_features", _unexpected_feature_call)

    result = RecentIngestPipeline(client, _make_config(tmp_path, min_trades=2)).run()

    summary = json.loads((tmp_path / "data/kalshi/raw/recent_ingest_summary.json").read_text(encoding="utf-8"))
    assert result.markets_downloaded == 1
    assert result.markets_with_trades == 1
    assert result.feature_files_written == 0
    assert feature_calls == []
    assert (tmp_path / "data/kalshi/normalized/trades" / "ECO-LOWTRADES-1.parquet").exists()
    assert not (tmp_path / "data/kalshi/features/real" / "ECO-LOWTRADES-1.parquet").exists()
    assert any(item["stage"] == "feature_min_trades" for item in summary["skipped_or_failed"])
