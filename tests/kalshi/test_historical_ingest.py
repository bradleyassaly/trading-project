from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from trading_platform.kalshi.historical_ingest import (
    HistoricalIngestConfig,
    HistoricalIngestPipeline,
    _parse_trade_row,
    _result_to_price,
    _safe_volume,
    _trades_to_dataframe,
)


def _make_config(tmp_path: Path, **overrides: object) -> HistoricalIngestConfig:
    base = HistoricalIngestConfig(
        raw_markets_dir=str(tmp_path / "data/kalshi/raw/markets"),
        raw_trades_dir=str(tmp_path / "data/kalshi/raw/trades"),
        raw_candles_dir=str(tmp_path / "data/kalshi/raw/candles"),
        trades_parquet_dir=str(tmp_path / "data/kalshi/normalized/trades"),
        normalized_candles_dir=str(tmp_path / "data/kalshi/normalized/candles"),
        normalized_markets_path=str(tmp_path / "data/kalshi/normalized/markets.parquet"),
        features_dir=str(tmp_path / "data/kalshi/features/real"),
        resolution_csv_path=str(tmp_path / "data/kalshi/normalized/resolution.csv"),
        legacy_resolution_csv_path=str(tmp_path / "data/kalshi/resolution.csv"),
        manifest_path=str(tmp_path / "data/kalshi/raw/ingest_manifest.json"),
        checkpoint_path=str(tmp_path / "data/kalshi/raw/ingest_checkpoint.json"),
        summary_path=str(tmp_path / "data/kalshi/raw/ingest_summary.json"),
        status_artifacts_root=str(tmp_path / "artifacts/kalshi_ingest"),
        lookback_days=30,
        min_trades=2,
        request_sleep_sec=0.0,
        run_base_rate=False,
        run_metaculus=False,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def _market(
    ticker: str,
    *,
    close_time: datetime,
    result: str = "yes",
    source_tier: str | None = None,
) -> dict[str, object]:
    payload = {
        "ticker": ticker,
        "title": f"{ticker} title",
        "series_ticker": "SERIES",
        "event_ticker": "EVENT",
        "status": "settled",
        "category": "economics",
        "result": result,
        "close_time": close_time.isoformat(),
    }
    if source_tier is not None:
        payload["source_tier"] = source_tier
    return payload


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
    rows: list[dict[str, object]] = []
    for idx in range(2):
        end = start + timedelta(hours=idx + 1)
        rows.append(
            {
                "end_period_ts": end.isoformat(),
                "open_price_dollars": 0.45 + idx * 0.01,
                "high_price_dollars": 0.55 + idx * 0.01,
                "low_price_dollars": 0.40 + idx * 0.01,
                "close_price_dollars": 0.50 + idx * 0.01,
                "count": 3 + idx,
            }
        )
    return rows


class FakeKalshiClient:
    def __init__(self, *, historical_markets: list[tuple[list[dict[str, object]], str | None]], live_markets: list[tuple[list[dict[str, object]], str | None]], historical_trades: dict[str, list[dict[str, object]]], live_trades: dict[str, list[dict[str, object]]], historical_candles: dict[str, list[dict[str, object]]], live_candles: dict[str, list[dict[str, object]]], cutoff: dict[str, str]):
        self.historical_markets_pages = list(historical_markets)
        self.live_markets_pages = list(live_markets)
        self.historical_trades = historical_trades
        self.live_trades = live_trades
        self.historical_candles = historical_candles
        self.live_candles = live_candles
        self.cutoff = cutoff
        self.trade_calls: list[tuple[str, str]] = []
        self.live_market_calls = 0

    def get_historical_cutoff(self) -> dict[str, str]:
        return self.cutoff

    def get_historical_markets(self, **_: object) -> tuple[list[dict[str, object]], str | None]:
        return self.historical_markets_pages.pop(0) if self.historical_markets_pages else ([], None)

    def get_markets_raw(self, **_: object) -> tuple[list[dict[str, object]], str | None]:
        self.live_market_calls += 1
        return self.live_markets_pages.pop(0) if self.live_markets_pages else ([], None)

    def get_all_historical_trades(self, ticker: str, **_: object) -> list[dict[str, object]]:
        self.trade_calls.append(("historical", ticker))
        return list(self.historical_trades.get(ticker, []))

    def get_all_trades_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        self.trade_calls.append(("live", ticker))
        return list(self.live_trades.get(ticker, []))

    def get_historical_market_candlesticks_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.historical_candles.get(ticker, []))

    def get_market_candlesticks_raw(self, ticker: str, **_: object) -> list[dict[str, object]]:
        return list(self.live_candles.get(ticker, []))


def test_result_to_price_yes_no() -> None:
    assert _result_to_price("yes") == pytest.approx(100.0)
    assert _result_to_price("no") == pytest.approx(0.0)
    assert _result_to_price("pending") is None


def test_parse_trade_row_prefers_count_fp() -> None:
    row = _parse_trade_row(
        {
            "trade_id": "t-1",
            "ticker": "KX-1",
            "yes_price_dollars": 0.61,
            "count_fp": 2.5,
            "count": 1,
            "created_time": "2026-01-01T00:00:00Z",
        }
    )
    assert row["count"] == pytest.approx(2.5)
    assert row["yes_price"] == pytest.approx(0.61)


def test_trades_to_dataframe_empty_schema() -> None:
    frame = _trades_to_dataframe([])
    assert frame.is_empty()
    assert frame.columns == ["trade_id", "ticker", "side", "yes_price", "no_price", "count", "traded_at"]


def test_run_writes_real_raw_normalized_and_feature_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=7)
    historical_market = _market("REAL-HIST-001", close_time=now - timedelta(days=20), source_tier="historical")
    live_market = _market("REAL-LIVE-001", close_time=now - timedelta(days=2), source_tier="live", result="no")
    synth_market = _market("SYNTH-000", close_time=now - timedelta(days=15), source_tier="historical")

    client = FakeKalshiClient(
        historical_markets=[([historical_market, synth_market], None)],
        live_markets=[([live_market], None)],
        historical_trades={
            "REAL-HIST-001": [_trade("REAL-HIST-001", "h1", now - timedelta(days=18)), _trade("REAL-HIST-001", "h2", now - timedelta(days=17), yes_price=0.57)],
            "REAL-LIVE-001": [_trade("REAL-LIVE-001", "l1", now - timedelta(days=3)), _trade("REAL-LIVE-001", "l2", now - timedelta(days=2, hours=1), yes_price=0.44)],
            "SYNTH-000": [_trade("SYNTH-000", "s1", now - timedelta(days=15)), _trade("SYNTH-000", "s2", now - timedelta(days=14))],
        },
        live_trades={
            "REAL-HIST-001": [],
            "REAL-LIVE-001": [_trade("REAL-LIVE-001", "l3", now - timedelta(days=1), yes_price=0.43)],
        },
        historical_candles={"REAL-HIST-001": _candles()},
        live_candles={"REAL-LIVE-001": _candles()},
        cutoff={
            "market_settled_ts": cutoff.isoformat(),
            "trades_created_ts": cutoff.isoformat(),
            "orders_updated_ts": cutoff.isoformat(),
        },
    )

    def _fake_build_features(*_: object, ticker: str, **__: object) -> pl.DataFrame:
        return pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]})

    monkeypatch.setattr("trading_platform.kalshi.features.build_kalshi_features", _fake_build_features)

    config = _make_config(tmp_path)
    result = HistoricalIngestPipeline(client, config).run()

    assert result.markets_downloaded == 3
    assert result.markets_with_trades == 2
    assert result.total_trades >= 4
    assert result.total_candlesticks == 4
    assert result.normalized_markets_written == 2
    assert Path(config.normalized_markets_path).exists()
    assert Path(config.resolution_csv_path).exists()
    assert Path(config.legacy_resolution_csv_path).exists()
    assert Path(config.summary_path).exists()
    assert Path(config.manifest_path).exists()
    assert (Path(config.features_dir) / "REAL-HIST-001.parquet").exists()
    assert (Path(config.features_dir) / "REAL-LIVE-001.parquet").exists()
    assert not (Path(config.features_dir) / "SYNTH-000.parquet").exists()
    assert (Path(config.raw_trades_dir) / "REAL-HIST-001.json").exists()
    assert (Path(config.raw_candles_dir) / "REAL-LIVE-001.json").exists()
    assert (Path(config.trades_parquet_dir) / "REAL-HIST-001.parquet").exists()
    assert (Path(config.normalized_candles_dir) / "REAL-LIVE-001.parquet").exists()

    summary = json.loads(Path(config.summary_path).read_text(encoding="utf-8"))
    assert Path(summary["output_layout"]["features_dir"]).parts[-4:] == ("data", "kalshi", "features", "real")
    assert summary["resolution_count"] == 2
    assert "skipped_or_failed" in summary

    normalized_markets = pl.read_parquet(config.normalized_markets_path)
    assert sorted(normalized_markets.get_column("ticker").to_list()) == ["REAL-HIST-001", "REAL-LIVE-001"]

    assert result.status_artifact_path is not None
    assert result.run_summary_artifact_path is not None
    status_payload = json.loads(result.status_artifact_path.read_text(encoding="utf-8"))
    run_summary_payload = json.loads(result.run_summary_artifact_path.read_text(encoding="utf-8"))
    assert status_payload["overall_status"] == "completed"
    assert status_payload["current_stage"] == "final_summary"
    assert status_payload["pages_seen"] >= 1
    assert status_payload["retained_markets_started"] >= 2
    assert any(stage["stage_name"] == "market_universe_fetch" and stage["status"] == "completed" for stage in status_payload["stages"])
    assert run_summary_payload["run_summary"]["status_artifact_path"] == str(result.status_artifact_path)
    assert run_summary_payload["run_summary"]["stop_reason"] in {"cursor_exhausted", "aged_out_pages"}


def test_status_artifact_updates_during_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    market = _market("FED-STATUS-001", close_time=now - timedelta(days=2), source_tier="live")
    market["volume"] = 500
    client = FakeKalshiClient(
        historical_markets=[([], None)],
        live_markets=[([market], None)],
        historical_trades={"FED-STATUS-001": []},
        live_trades={"FED-STATUS-001": [_trade("FED-STATUS-001", "t1", now - timedelta(days=2)), _trade("FED-STATUS-001", "t2", now - timedelta(days=2, hours=-1))]},
        historical_candles={},
        live_candles={"FED-STATUS-001": _candles()},
        cutoff={
            "market_settled_ts": (now - timedelta(days=5)).isoformat(),
            "trades_created_ts": (now - timedelta(days=5)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=5)).isoformat(),
        },
    )
    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )
    observed_statuses: list[dict[str, object]] = []
    from trading_platform.ingest.status import IngestStatusTracker
    original_snapshot = IngestStatusTracker.snapshot

    def _wrapped_snapshot(self):
        payload = original_snapshot(self)
        observed_statuses.append(payload)
        return payload

    monkeypatch.setattr(IngestStatusTracker, "snapshot", _wrapped_snapshot)
    result = HistoricalIngestPipeline(client, _make_config(tmp_path, min_trades=1)).run()
    assert result.status_artifact_path is not None and result.status_artifact_path.exists()
    assert any(payload["current_stage"] == "market_universe_fetch" for payload in observed_statuses)
    assert any(payload["current_stage"] == "retained_market_processing" for payload in observed_statuses)
    assert any(payload["current_stage"] == "final_summary" for payload in observed_statuses)


def test_run_resumes_processed_tickers_from_checkpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    market = _market("REAL-HIST-001", close_time=now - timedelta(days=12), source_tier="historical")
    client = FakeKalshiClient(
        historical_markets=[([market], None)],
        live_markets=[([], None)],
        historical_trades={"REAL-HIST-001": [_trade("REAL-HIST-001", "h1", now - timedelta(days=11)), _trade("REAL-HIST-001", "h2", now - timedelta(days=10))]},
        live_trades={},
        historical_candles={"REAL-HIST-001": _candles()},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=7)).isoformat(),
            "trades_created_ts": (now - timedelta(days=7)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=7)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [50.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path)
    pipeline = HistoricalIngestPipeline(client, config)
    pipeline.run()
    first_call_count = len(client.trade_calls)

    pipeline.run()
    assert len(client.trade_calls) == first_call_count


def test_fetch_market_trades_combines_historical_and_live_by_cutoff(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=5)
    market = _market("REAL-LIVE-001", close_time=now - timedelta(days=2), source_tier="live")
    client = FakeKalshiClient(
        historical_markets=[],
        live_markets=[],
        historical_trades={"REAL-LIVE-001": [_trade("REAL-LIVE-001", "old", now - timedelta(days=8))]},
        live_trades={"REAL-LIVE-001": [_trade("REAL-LIVE-001", "new", now - timedelta(days=1))]},
        historical_candles={},
        live_candles={},
        cutoff={
            "market_settled_ts": cutoff.isoformat(),
            "trades_created_ts": cutoff.isoformat(),
            "orders_updated_ts": cutoff.isoformat(),
        },
    )
    pipeline = HistoricalIngestPipeline(client, _make_config(tmp_path))

    rows = pipeline._fetch_market_trades(market, {"trades_created_ts": cutoff, "market_settled_ts": cutoff, "orders_updated_ts": cutoff})

    assert [row["trade_id"] for row in rows] == ["old", "new"]
    assert ("historical", "REAL-LIVE-001") in client.trade_calls
    assert ("live", "REAL-LIVE-001") in client.trade_calls


# ── Market filter tests ───────────────────────────────────────────────────────


def _minimal_pipeline(tmp_path: Path, **filter_kwargs: object) -> HistoricalIngestPipeline:
    """Return a pipeline configured with custom filter settings and all others disabled."""
    cfg = _make_config(tmp_path, **filter_kwargs)
    # FakeKalshiClient not needed — we call _apply_market_filters directly
    return HistoricalIngestPipeline(object(), cfg)


def _mkt(ticker: str, *, series: str | None = None, event: str | None = None,
         category: str = "economics", volume: float | None = None) -> dict:
    m: dict = {"ticker": ticker, "category": category}
    if series is not None:
        m["series_ticker"] = series
    if event is not None:
        m["event_ticker"] = event
    if volume is not None:
        m["volume"] = volume
    return m


# _safe_volume ----------------------------------------------------------------

def test_safe_volume_missing_returns_zero():
    assert _safe_volume({}) == 0.0


def test_safe_volume_numeric():
    assert _safe_volume({"volume": 250}) == pytest.approx(250.0)


def test_safe_volume_string_numeric():
    assert _safe_volume({"volume": "1500"}) == pytest.approx(1500.0)


def test_safe_volume_non_numeric_returns_zero():
    assert _safe_volume({"volume": "n/a"}) == 0.0


# _apply_market_filters — preferred_categories --------------------------------

def test_filter_preferred_categories_keeps_matching(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, preferred_categories=["Economics", "Politics"])
    markets = [
        _mkt("ECON-1", category="economics"),
        _mkt("POL-1",  category="Politics"),
        _mkt("SPORT-1", category="Sports"),
    ]
    result = pipeline._apply_market_filters(markets)
    tickers = [m["ticker"] for m in result]
    assert "ECON-1" in tickers
    assert "POL-1" in tickers
    assert "SPORT-1" not in tickers


def test_filter_preferred_categories_case_insensitive(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, preferred_categories=["ECONOMICS"])
    markets = [_mkt("ECON-1", category="economics"), _mkt("SPORT-1", category="Sports")]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "ECON-1"


def test_filter_preferred_categories_empty_allows_all(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, preferred_categories=[])
    markets = [_mkt("A"), _mkt("B", category="Sports")]
    assert len(pipeline._apply_market_filters(markets)) == 2


# _apply_market_filters — excluded_series_patterns ----------------------------

def test_filter_excluded_series_removes_matching_series(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, excluded_series_patterns=["KXBTC", "KXETH"])
    markets = [
        _mkt("KXBTC-25MAR-T95000", series="KXBTC"),
        _mkt("KXETH-25APR-T2000",  series="KXETH"),
        _mkt("FED-25MAR",          series="KXFED"),
    ]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "FED-25MAR"


def test_filter_excluded_series_case_insensitive(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, excluded_series_patterns=["kxbtc"])
    markets = [_mkt("KXBTC-001", series="KXBTC"), _mkt("FED-001", series="KXFED")]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "FED-001"


def test_filter_excluded_series_falls_back_to_ticker_when_no_series(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, excluded_series_patterns=["KXNFL"])
    markets = [
        {"ticker": "KXNFL-WEEK10-TDB", "category": "sports"},   # no series_ticker key
        {"ticker": "CPI-2025-MAR", "category": "economics"},
    ]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "CPI-2025-MAR"


def test_filter_excluded_series_empty_allows_all(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, excluded_series_patterns=[])
    markets = [_mkt("KXBTC-001", series="KXBTC"), _mkt("FED-001")]
    assert len(pipeline._apply_market_filters(markets)) == 2


# _apply_market_filters — min_volume ------------------------------------------

def test_filter_min_volume_removes_low_volume(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, min_volume=100.0)
    markets = [
        _mkt("LIQUID",   volume=500.0),
        _mkt("ILLIQUID", volume=50.0),
        _mkt("ZERO",     volume=0.0),
        _mkt("NONE"),                  # no volume key → treated as 0
    ]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "LIQUID"


def test_filter_min_volume_exactly_at_threshold_passes(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, min_volume=100.0)
    markets = [_mkt("EXACT", volume=100.0), _mkt("BELOW", volume=99.9)]
    result = pipeline._apply_market_filters(markets)
    assert len(result) == 1
    assert result[0]["ticker"] == "EXACT"


def test_filter_min_volume_zero_disabled(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, min_volume=0.0)
    markets = [_mkt("A", volume=0.0), _mkt("B")]
    assert len(pipeline._apply_market_filters(markets)) == 2


# _apply_market_filters — max_markets_per_event -------------------------------

def test_filter_max_markets_per_event_removes_bracket_events(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, max_markets_per_event=3)
    # Event with 5 markets → bracket; event with 2 → legitimate
    bracket = [_mkt(f"BTC-{i}", event="KXBTC-MAR25") for i in range(5)]
    legit   = [_mkt(f"FED-{i}", event="FOMC-MAR25")  for i in range(2)]
    markets = bracket + legit
    result = pipeline._apply_market_filters(markets)
    tickers = [m["ticker"] for m in result]
    assert all(t.startswith("FED-") for t in tickers)
    assert len(tickers) == 2


def test_filter_max_markets_per_event_exact_boundary(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, max_markets_per_event=3)
    # Exactly 3 markets for an event → kept (not strictly greater)
    markets = [_mkt(f"M-{i}", event="EVT") for i in range(3)]
    assert len(pipeline._apply_market_filters(markets)) == 3


def test_filter_max_markets_per_event_zero_disabled(tmp_path: Path):
    pipeline = _minimal_pipeline(tmp_path, max_markets_per_event=0)
    markets = [_mkt(f"BTC-{i}", event="KXBTC-MAR25") for i in range(100)]
    assert len(pipeline._apply_market_filters(markets)) == 100


# _apply_market_filters — combined --------------------------------------------

def test_filter_combined_applies_all_rules(tmp_path: Path):
    pipeline = _minimal_pipeline(
        tmp_path,
        preferred_categories=["Economics"],
        excluded_series_patterns=["KXBTC"],
        min_volume=50.0,
        max_markets_per_event=2,
    )
    markets = [
        _mkt("ECON-A", series="KXFED", event="FOMC", category="economics", volume=200.0),
        _mkt("ECON-B", series="KXFED", event="FOMC", category="economics", volume=200.0),
        _mkt("BTC-1",  series="KXBTC", event="KXBTC-MAR", category="economics", volume=9999.0),
        _mkt("SPORT",  series="KXSPT", event="NBA", category="Sports", volume=9999.0),
        _mkt("POOR",   series="KXPOOR", event="POOR-EVT", category="economics", volume=10.0),
    ]
    result = pipeline._apply_market_filters(markets)
    tickers = [m["ticker"] for m in result]
    assert "ECON-A" in tickers
    assert "ECON-B" in tickers
    assert "BTC-1"  not in tickers   # excluded_series_patterns
    assert "SPORT"  not in tickers   # wrong category
    assert "POOR"   not in tickers   # below min_volume


# _apply_market_filters — summary log -----------------------------------------

def test_filter_logs_summary(tmp_path: Path, caplog):
    import logging
    pipeline = _minimal_pipeline(tmp_path, min_volume=100.0)
    markets = [_mkt("LIQUID", volume=500.0), _mkt("DRY", volume=0.0)]
    with caplog.at_level(logging.INFO, logger="trading_platform.kalshi.historical_ingest"):
        pipeline._apply_market_filters(markets)
    summary_lines = [r.message for r in caplog.records if "Market filter summary" in r.message]
    assert summary_lines, "Expected a 'Market filter summary' log line"
    assert "Found 2 total markets" in summary_lines[0]
    assert "filtering to 1" in summary_lines[0]


# integration: filters respected in full pipeline run -------------------------

def test_run_respects_excluded_series_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Markets whose series matches an exclusion pattern must not have trades fetched."""
    now = datetime.now(UTC)
    btc_market = _market("KXBTC-25MAR-T95000", close_time=now - timedelta(days=5), source_tier="historical")
    btc_market["series_ticker"] = "KXBTC"
    btc_market["volume"] = 9999
    fed_market = _market("FED-25MAR", close_time=now - timedelta(days=5), source_tier="historical")
    fed_market["series_ticker"] = "KXFED"
    fed_market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([btc_market, fed_market], None)],
        live_markets=[([], None)],
        historical_trades={
            "KXBTC-25MAR-T95000": [_trade("KXBTC-25MAR-T95000", "b1", now - timedelta(days=4)),
                                    _trade("KXBTC-25MAR-T95000", "b2", now - timedelta(days=3))],
            "FED-25MAR":          [_trade("FED-25MAR", "f1", now - timedelta(days=4)),
                                   _trade("FED-25MAR", "f2", now - timedelta(days=3))],
        },
        live_trades={},
        historical_candles={"FED-25MAR": _candles()},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path, excluded_series_patterns=["KXBTC"], min_trades=1)
    result = HistoricalIngestPipeline(client, config).run()

    # Both markets downloaded and written to indexes
    assert result.markets_downloaded == 2
    # But only FED market processed
    assert result.markets_with_trades == 1
    assert result.feature_files_written == 1
    assert (Path(config.features_dir) / "FED-25MAR.parquet").exists()
    assert not (Path(config.features_dir) / "KXBTC-25MAR-T95000.parquet").exists()
    assert not (Path(config.raw_markets_dir) / "KXBTC-25MAR-T95000.json").exists()
    assert (Path(config.raw_markets_dir) / "FED-25MAR.json").exists()
    # BTC trades should never have been fetched
    btc_calls = [ticker for _, ticker in client.trade_calls if "KXBTC" in ticker]
    assert not btc_calls


def test_run_respects_min_volume_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Markets below min_volume must be skipped even if they have trades."""
    now = datetime.now(UTC)
    liquid_market = _market("LIQUID-001", close_time=now - timedelta(days=5), source_tier="historical")
    liquid_market["volume"] = 500
    dry_market = _market("DRY-001", close_time=now - timedelta(days=5), source_tier="historical")
    dry_market["volume"] = 10

    client = FakeKalshiClient(
        historical_markets=[([liquid_market, dry_market], None)],
        live_markets=[([], None)],
        historical_trades={
            "LIQUID-001": [_trade("LIQUID-001", "l1", now - timedelta(days=4)),
                           _trade("LIQUID-001", "l2", now - timedelta(days=3))],
            "DRY-001":    [_trade("DRY-001", "d1", now - timedelta(days=4)),
                           _trade("DRY-001", "d2", now - timedelta(days=3))],
        },
        live_trades={},
        historical_candles={"LIQUID-001": _candles()},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path, min_volume=100.0, min_trades=1)
    result = HistoricalIngestPipeline(client, config).run()

    assert result.markets_downloaded == 2
    assert result.markets_with_trades == 1
    assert (Path(config.features_dir) / "LIQUID-001.parquet").exists()
    assert not (Path(config.features_dir) / "DRY-001.parquet").exists()
    assert not (Path(config.raw_markets_dir) / "DRY-001.json").exists()
    dry_calls = [ticker for _, ticker in client.trade_calls if "DRY" in ticker]
    assert not dry_calls


def test_run_summary_includes_filter_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The JSON summary must include markets_after_filters and filter_config."""
    now = datetime.now(UTC)
    market = _market("FED-001", close_time=now - timedelta(days=5), source_tier="historical")
    market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([market], None)],
        live_markets=[([], None)],
        historical_trades={"FED-001": [_trade("FED-001", "t1", now - timedelta(days=4)),
                                        _trade("FED-001", "t2", now - timedelta(days=3))]},
        live_trades={},
        historical_candles={"FED-001": _candles()},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path, min_volume=100.0, min_trades=1)
    HistoricalIngestPipeline(client, config).run()

    summary = json.loads(Path(config.summary_path).read_text(encoding="utf-8"))
    assert "markets_after_filters" in summary
    assert "markets_excluded_by_filters" in summary
    assert "filter_config" in summary
    assert "filter_diagnostics" in summary
    assert summary["markets_after_filters"] == 1
    assert summary["markets_excluded_by_filters"] == 0
    assert summary["filter_config"]["min_volume"] == pytest.approx(100.0)
    assert summary["filter_diagnostics"]["retained_markets"] == 1
    assert summary["filter_diagnostics"]["excluded_by_min_volume"] == 0


def test_run_exits_cleanly_when_no_markets_match_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    sports_market = _market("KXNBA-001", close_time=now - timedelta(days=5), source_tier="historical")
    sports_market["series_ticker"] = "KXNBA"
    sports_market["category"] = "sports"
    sports_market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([sports_market], None)],
        live_markets=[([], None)],
        historical_trades={"KXNBA-001": [_trade("KXNBA-001", "t1", now - timedelta(days=4))]},
        live_trades={},
        historical_candles={},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(
        tmp_path,
        preferred_categories=["Economics"],
        excluded_series_patterns=["KXNBA"],
        min_trades=1,
    )
    result = HistoricalIngestPipeline(client, config).run()

    assert result.markets_downloaded == 1
    assert result.markets_with_trades == 0
    assert result.total_trades == 0
    assert not list(Path(config.raw_markets_dir).glob("*.json"))
    summary = json.loads(Path(config.summary_path).read_text(encoding="utf-8"))
    assert summary["filter_diagnostics"]["total_markets_before_filters"] == 1
    assert summary["filter_diagnostics"]["retained_markets"] == 0
    assert summary["filter_diagnostics"]["excluded_by_category"] == 1 or summary["filter_diagnostics"]["excluded_by_series_pattern"] == 1
    assert result.status_artifact_path is not None
    status_payload = json.loads(result.status_artifact_path.read_text(encoding="utf-8"))
    assert status_payload["overall_status"] == "completed"
    assert status_payload["retained_markets_started"] == 0
    assert status_payload["pages_without_retained_markets"] >= 1
    assert any(stage["stage_name"] == "retained_market_processing" and stage["status"] == "completed" for stage in status_payload["stages"])


def test_run_filters_each_page_before_writing_raw_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    bad_market_page_1 = _market("KXNBA-001", close_time=now - timedelta(days=5), source_tier="historical")
    bad_market_page_1["series_ticker"] = "KXNBA"
    bad_market_page_1["category"] = "sports"
    bad_market_page_1["volume"] = 500
    good_market_page_1 = _market("FED-001", close_time=now - timedelta(days=5), source_tier="historical")
    good_market_page_1["series_ticker"] = "KXFED"
    good_market_page_1["volume"] = 500
    bad_market_page_2 = _market("LOWVOL-001", close_time=now - timedelta(days=4), source_tier="historical")
    bad_market_page_2["series_ticker"] = "KXLOW"
    bad_market_page_2["volume"] = 5
    good_market_page_2 = _market("CPI-001", close_time=now - timedelta(days=4), source_tier="historical")
    good_market_page_2["series_ticker"] = "KXCPI"
    good_market_page_2["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([bad_market_page_1, good_market_page_1], "cursor-2"), ([bad_market_page_2, good_market_page_2], None)],
        live_markets=[([], None)],
        historical_trades={
            "FED-001": [_trade("FED-001", "f1", now - timedelta(days=4)), _trade("FED-001", "f2", now - timedelta(days=3))],
            "CPI-001": [_trade("CPI-001", "c1", now - timedelta(days=4)), _trade("CPI-001", "c2", now - timedelta(days=3))],
        },
        live_trades={},
        historical_candles={"FED-001": _candles(), "CPI-001": _candles()},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(
        tmp_path,
        preferred_categories=["Economics"],
        excluded_series_patterns=["KXNBA"],
        min_volume=100.0,
        min_trades=1,
    )
    HistoricalIngestPipeline(client, config).run()

    raw_market_files = sorted(path.name for path in Path(config.raw_markets_dir).glob("*.json"))
    assert raw_market_files == ["CPI-001.json", "FED-001.json"]
    summary = json.loads(Path(config.summary_path).read_text(encoding="utf-8"))
    assert summary["filter_diagnostics"]["total_markets_before_filters"] == 4
    assert summary["filter_diagnostics"]["retained_markets"] == 2
    assert summary["filter_diagnostics"]["excluded_by_category"] == 1
    assert summary["filter_diagnostics"]["excluded_by_min_volume"] == 1


def test_live_market_payload_fields_filter_correctly(tmp_path: Path) -> None:
    pipeline = _minimal_pipeline(
        tmp_path,
        preferred_categories=["Economics"],
        excluded_series_patterns=["KXNBA"],
        min_volume=100.0,
    )
    live_markets = [
        {
            "ticker": "ECON-OK-001",
            "event_ticker": "EVENT-1",
            "series_ticker": "KXFED",
            "category": "Economics",
            "volume": 500,
            "status": "settled",
            "close_time": "2026-01-01T00:00:00Z",
        },
        {
            "ticker": "SPORT-BAD-001",
            "event_ticker": "EVENT-2",
            "series_ticker": "KXNBA",
            "category": "Sports",
            "volume": 500,
            "status": "settled",
            "close_time": "2026-01-01T00:00:00Z",
        },
    ]
    retained, diagnostics = pipeline._filter_markets_for_ingest_page(live_markets)
    assert [market["ticker"] for market in retained] == ["ECON-OK-001"]
    assert diagnostics["discarded_by_reason"]["category"] == 1 or diagnostics["discarded_by_reason"]["series_pattern"] == 1


def test_processing_begins_after_first_retained_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    first_market = _market("FED-FAST-001", close_time=now - timedelta(days=2), source_tier="live")
    first_market["volume"] = 500
    second_market = _market("CPI-LATER-001", close_time=now - timedelta(days=1), source_tier="live")
    second_market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([], None)],
        live_markets=[([first_market], "cursor-2"), ([second_market], None)],
        historical_trades={
            "FED-FAST-001": [],
            "CPI-LATER-001": [],
        },
        live_trades={
            "FED-FAST-001": [_trade("FED-FAST-001", "f1", now - timedelta(days=2)), _trade("FED-FAST-001", "f2", now - timedelta(days=2, hours=-1))],
            "CPI-LATER-001": [_trade("CPI-LATER-001", "c1", now - timedelta(days=1)), _trade("CPI-LATER-001", "c2", now - timedelta(days=1, hours=-1))],
        },
        historical_candles={},
        live_candles={"FED-FAST-001": _candles(), "CPI-LATER-001": _candles()},
        cutoff={
            "market_settled_ts": (now - timedelta(days=3)).isoformat(),
            "trades_created_ts": (now - timedelta(days=3)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=3)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    processing_order: list[str] = []
    original_process = HistoricalIngestPipeline._process_market_artifacts

    def _wrapped_process(self, market, **kwargs):
        processing_order.append(str(market["ticker"]))
        return original_process(self, market, **kwargs)

    monkeypatch.setattr(HistoricalIngestPipeline, "_process_market_artifacts", _wrapped_process)

    config = _make_config(tmp_path, min_trades=1)
    result = HistoricalIngestPipeline(client, config).run()

    assert result.markets_with_trades == 2
    assert processing_order[0] == "FED-FAST-001"
    assert (Path(config.trades_parquet_dir) / "FED-FAST-001.parquet").exists()


def test_live_bridge_stops_after_pages_fall_outside_lookback_window(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    in_range_market = _market("FED-IN-RANGE-001", close_time=now - timedelta(days=2), source_tier="live")
    in_range_market["volume"] = 500
    old_market = _market("FED-OLD-001", close_time=now - timedelta(days=120), source_tier="live")
    old_market["volume"] = 500
    very_old_market = _market("FED-VERY-OLD-001", close_time=now - timedelta(days=150), source_tier="live")
    very_old_market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([], None)],
        live_markets=[([in_range_market], "cursor-2"), ([old_market], "cursor-3"), ([very_old_market], None)],
        historical_trades={"FED-IN-RANGE-001": []},
        live_trades={"FED-IN-RANGE-001": [_trade("FED-IN-RANGE-001", "f1", now - timedelta(days=2)), _trade("FED-IN-RANGE-001", "f2", now - timedelta(days=2, hours=-1))]},
        historical_candles={},
        live_candles={"FED-IN-RANGE-001": _candles()},
        cutoff={
            "market_settled_ts": (now - timedelta(days=10)).isoformat(),
            "trades_created_ts": (now - timedelta(days=10)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=10)).isoformat(),
        },
    )

    monkeypatch.setattr(
        "trading_platform.kalshi.features.build_kalshi_features",
        lambda *_, ticker, **__: pl.DataFrame({"timestamp": [now], "close": [55.0], "symbol": [ticker]}),
    )

    config = _make_config(tmp_path, lookback_days=30, min_trades=1)
    result = HistoricalIngestPipeline(client, config).run()

    assert client.live_market_calls == 2
    assert not (Path(config.raw_markets_dir) / "FED-OLD-001.json").exists()
    assert result.run_summary_artifact_path is not None
    run_summary_payload = json.loads(result.run_summary_artifact_path.read_text(encoding="utf-8"))
    assert run_summary_payload["stop_reason"] == "aged_out_pages"
    assert run_summary_payload["run_summary"]["first_retained_processing_milestone"]["ticker"] == "FED-IN-RANGE-001"


def test_live_bridge_fail_fast_on_retained_market_explosion_without_processing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    retained_page_1 = [_market(f"FED-KEEP-{idx:03d}", close_time=now - timedelta(days=2), source_tier="live") for idx in range(3)]
    retained_page_2 = [_market(f"CPI-KEEP-{idx:03d}", close_time=now - timedelta(days=1), source_tier="live") for idx in range(3)]
    for market in retained_page_1 + retained_page_2:
        market["volume"] = 500

    client = FakeKalshiClient(
        historical_markets=[([], None)],
        live_markets=[(retained_page_1, "cursor-2"), (retained_page_2, None)],
        historical_trades={},
        live_trades={},
        historical_candles={},
        live_candles={},
        cutoff={
            "market_settled_ts": (now - timedelta(days=5)).isoformat(),
            "trades_created_ts": (now - timedelta(days=5)).isoformat(),
            "orders_updated_ts": (now - timedelta(days=5)).isoformat(),
        },
    )

    monkeypatch.setattr(
        HistoricalIngestPipeline,
        "_process_market_artifacts",
        lambda self, market, **kwargs: None,
    )

    config = _make_config(
        tmp_path,
        min_trades=1,
        max_raw_markets_without_processing=5,
    )

    with pytest.raises(RuntimeError, match="retained raw-market attempts exceeded 5"):
        HistoricalIngestPipeline(client, config).run()

    run_dirs = sorted(Path(config.status_artifacts_root).glob("*"))
    assert run_dirs
    run_summary_path = run_dirs[-1] / "ingest_run_summary.json"
    assert run_summary_path.exists()
    run_summary_payload = json.loads(run_summary_path.read_text(encoding="utf-8"))
    assert run_summary_payload["overall_status"] == "failed"
    assert run_summary_payload["fail_fast_triggered"] is True
    assert run_summary_payload["fail_fast_reason"] == "retained_without_processing"
    assert run_summary_payload["run_summary"]["stop_reason"] == "fail_fast_retained_without_processing"
