from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from trading_platform.kalshi.validation import (
    FAIL,
    PASS,
    WARNING,
    KalshiDataValidationConfig,
    KalshiValidationThresholds,
    run_kalshi_data_validation,
)


def _write_dataset(
    tmp_path: Path,
    *,
    market_rows: list[dict],
    trade_tickers: list[str],
    candle_tickers: list[str],
    filter_diagnostics: dict | None = None,
) -> KalshiDataValidationConfig:
    normalized_dir = tmp_path / "data" / "kalshi" / "normalized"
    trades_dir = normalized_dir / "trades"
    candles_dir = normalized_dir / "candles"
    raw_dir = tmp_path / "data" / "kalshi" / "raw"
    validation_dir = tmp_path / "data" / "kalshi" / "validation"
    features_dir = tmp_path / "data" / "kalshi" / "features" / "real"

    trades_dir.mkdir(parents=True, exist_ok=True)
    candles_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    validation_dir.mkdir(parents=True, exist_ok=True)
    features_dir.mkdir(parents=True, exist_ok=True)

    markets_path = normalized_dir / "markets.parquet"
    pl.DataFrame(market_rows).write_parquet(markets_path)

    resolution_rows = [
        {
            "ticker": row["ticker"],
            "resolution_price": 100.0 if row.get("result") == "yes" else 0.0,
            "result": row.get("result"),
            "close_time": row.get("close_time"),
            "source_tier": row.get("source_tier", "historical"),
        }
        for row in market_rows
        if row.get("result") is not None
    ]
    pl.DataFrame(resolution_rows).write_csv(normalized_dir / "resolution.csv")

    for ticker in trade_tickers:
        pl.DataFrame(
            {
                "trade_id": [f"{ticker}-t1", f"{ticker}-t2"],
                "ticker": [ticker, ticker],
                "traded_at": ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"],
                "yes_price": [55.0, 56.0],
                "count": [1.0, 2.0],
            }
        ).write_parquet(trades_dir / f"{ticker}.parquet")

    for ticker in candle_tickers:
        pl.DataFrame(
            {
                "timestamp": ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"],
                "open": [50.0, 51.0],
                "high": [55.0, 56.0],
                "low": [49.0, 50.0],
                "close": [54.0, 55.0],
                "volume": [10.0, 12.0],
            }
        ).write_parquet(candles_dir / f"{ticker}.parquet")

    ingest_summary = {
        "markets_downloaded": len(market_rows) + 3,
        "markets_after_filters": len(market_rows),
        "markets_excluded_by_filters": 3,
        "filter_config": {
            "preferred_categories": ["Economics"],
            "excluded_series_patterns": ["KXBTC"],
            "min_volume": 100.0,
            "max_markets_per_event": 5,
        },
        "filter_diagnostics": filter_diagnostics
        or {
            "total_markets_before_filters": len(market_rows) + 3,
            "retained_markets": len(market_rows),
            "excluded_markets_total": 3,
            "excluded_by_category": 1,
            "excluded_by_series_pattern": 1,
            "excluded_by_min_volume": 1,
            "excluded_by_bracket": 0,
            "effective_filter_config": {
                "preferred_categories": ["Economics"],
                "excluded_series_patterns": ["KXBTC"],
                "min_volume": 100.0,
                "max_markets_per_event": 5,
            },
        },
        "output_layout": {
            "features_dir": str(features_dir),
            "normalized_markets_path": str(markets_path),
            "normalized_trades_dir": str(trades_dir),
            "normalized_candles_dir": str(candles_dir),
        },
    }
    (raw_dir / "ingest_summary.json").write_text(json.dumps(ingest_summary), encoding="utf-8")
    (raw_dir / "ingest_manifest.json").write_text(json.dumps(ingest_summary), encoding="utf-8")
    (raw_dir / "ingest_checkpoint.json").write_text(
        json.dumps({"processed_tickers": sorted({row["ticker"] for row in market_rows})}),
        encoding="utf-8",
    )

    return KalshiDataValidationConfig(
        normalized_markets_path=str(markets_path),
        normalized_trades_path=str(trades_dir),
        normalized_candles_path=str(candles_dir),
        resolution_csv_path=str(normalized_dir / "resolution.csv"),
        ingest_summary_path=str(raw_dir / "ingest_summary.json"),
        ingest_manifest_path=str(raw_dir / "ingest_manifest.json"),
        ingest_checkpoint_path=str(raw_dir / "ingest_checkpoint.json"),
        features_dir=str(features_dir),
        output_dir=str(validation_dir),
    )


def _market_rows(*tickers: str) -> list[dict]:
    rows = []
    for index, ticker in enumerate(tickers, start=1):
        rows.append(
            {
                "ticker": ticker,
                "market_id": f"m-{index}",
                "title": f"{ticker} title",
                "category": "Economics",
                "status": "settled",
                "close_time": f"2026-01-0{index}T00:00:00Z",
                "result": "yes" if index % 2 else "no",
                "source_tier": "historical",
            }
        )
    return rows


def test_validation_healthy_dataset_passes_and_writes_artifacts(tmp_path: Path) -> None:
    config = _write_dataset(
        tmp_path,
        market_rows=_market_rows("REAL-001", "REAL-002"),
        trade_tickers=["REAL-001", "REAL-002"],
        candle_tickers=["REAL-001", "REAL-002"],
    )

    result = run_kalshi_data_validation(config)

    assert result.status == PASS
    assert result.passed is True
    assert result.artifacts.summary_path.exists()
    assert result.artifacts.details_path.exists()
    assert result.artifacts.report_path.exists()
    report = result.artifacts.report_path.read_text(encoding="utf-8")
    assert "Kalshi Data Validation Report" in report
    assert "Overall status: PASS" in report


def test_validation_duplicate_markets_trigger_warning_and_failure(tmp_path: Path) -> None:
    rows = _market_rows("REAL-001", "REAL-002")
    rows.append(
        {
            "ticker": "REAL-001",
            "market_id": "m-duplicate",
            "title": "duplicate",
            "category": "Economics",
            "status": "settled",
            "close_time": "2026-01-03T00:00:00Z",
            "result": "yes",
            "source_tier": "historical",
        }
    )
    config = _write_dataset(
        tmp_path,
        market_rows=rows,
        trade_tickers=["REAL-001", "REAL-002"],
        candle_tickers=["REAL-001", "REAL-002"],
    )

    warning_result = run_kalshi_data_validation(
        KalshiDataValidationConfig(
            **{**config.__dict__, "thresholds": KalshiValidationThresholds(max_duplicate_ticker_fail_rate=0.75)}
        )
    )
    fail_result = run_kalshi_data_validation(
        KalshiDataValidationConfig(
            **{**config.__dict__, "thresholds": KalshiValidationThresholds(max_duplicate_ticker_fail_rate=0.25)}
        )
    )

    assert warning_result.status == WARNING
    assert fail_result.status == FAIL


def test_validation_trade_coverage_trigger_warning_and_failure(tmp_path: Path) -> None:
    config = _write_dataset(
        tmp_path,
        market_rows=_market_rows("REAL-001", "REAL-002", "REAL-003", "REAL-004"),
        trade_tickers=["REAL-001", "REAL-002"],
        candle_tickers=["REAL-001", "REAL-002", "REAL-003", "REAL-004"],
    )

    warning_result = run_kalshi_data_validation(
        KalshiDataValidationConfig(
            **{
                **config.__dict__,
                "thresholds": KalshiValidationThresholds(
                    min_trade_coverage_warn_pct=0.60,
                    min_trade_coverage_fail_pct=0.40,
                ),
            }
        )
    )
    fail_result = run_kalshi_data_validation(
        KalshiDataValidationConfig(
            **{
                **config.__dict__,
                "thresholds": KalshiValidationThresholds(
                    min_trade_coverage_warn_pct=0.80,
                    min_trade_coverage_fail_pct=0.75,
                ),
            }
        )
    )

    assert warning_result.status == WARNING
    assert fail_result.status == FAIL


def test_validation_detects_synthetic_markers_in_real_dataset(tmp_path: Path) -> None:
    config = _write_dataset(
        tmp_path,
        market_rows=_market_rows("SYNTH-001", "REAL-002"),
        trade_tickers=["SYNTH-001", "REAL-002"],
        candle_tickers=["SYNTH-001", "REAL-002"],
    )

    result = run_kalshi_data_validation(config)

    assert result.status == FAIL
    assert "synthetic_markers_detected" in {
        finding["code"] for finding in result.details_payload["findings"]
    }


def test_validation_reports_filter_breakdown(tmp_path: Path) -> None:
    config = _write_dataset(
        tmp_path,
        market_rows=_market_rows("REAL-001", "REAL-002"),
        trade_tickers=["REAL-001", "REAL-002"],
        candle_tickers=["REAL-001", "REAL-002"],
        filter_diagnostics={
            "total_markets_before_filters": 10,
            "retained_markets": 2,
            "excluded_markets_total": 8,
            "excluded_by_category": 3,
            "excluded_by_series_pattern": 2,
            "excluded_by_min_volume": 1,
            "excluded_by_bracket": 2,
            "effective_filter_config": {"preferred_categories": ["Economics"]},
        },
    )

    result = run_kalshi_data_validation(config)

    diagnostics = result.summary_payload["filter_diagnostics"]
    assert diagnostics["total_markets_before_filters"] == 10
    assert diagnostics["retained_markets"] == 2
    assert diagnostics["excluded_by_category"] == 3
    assert diagnostics["excluded_by_series_pattern"] == 2
    assert diagnostics["excluded_by_min_volume"] == 1
    assert diagnostics["excluded_by_bracket"] == 2


def test_validation_allows_recent_market_only_dataset_with_zero_trades(tmp_path: Path) -> None:
    rows = _market_rows("REAL-RECENT-001", "REAL-RECENT-002")
    for row in rows:
        row["status"] = "settled"
        row["source_mode"] = "live_recent_filtered"
        row["expiration_time"] = row["close_time"]
        row["yes_bid"] = 55.0
        row["yes_ask"] = 57.0
    config = _write_dataset(
        tmp_path,
        market_rows=rows,
        trade_tickers=[],
        candle_tickers=[],
        filter_diagnostics={
            "total_markets_before_filters": 4,
            "retained_markets": 2,
            "excluded_markets_total": 2,
            "excluded_by_category": 1,
            "excluded_by_series_pattern": 0,
            "excluded_by_series": 0,
            "excluded_by_min_volume": 0,
            "excluded_by_bracket": 0,
            "excluded_missing_core_fields": 0,
            "excluded_by_lookback": 1,
            "excluded_no_trade_data": 0,
            "effective_filter_config": {"preferred_categories": ["Economics"]},
        },
    )

    result = run_kalshi_data_validation(config)

    assert result.status == PASS
    assert result.summary_payload["recent_market_only_dataset"] is True
    finding_by_code = {finding["code"]: finding for finding in result.details_payload["findings"]}
    assert finding_by_code["trade_coverage"]["severity"] == PASS
    assert finding_by_code["candle_coverage"]["severity"] == PASS
    assert result.summary_payload["coverage"]["trade_pct"] == 0.0
    assert result.summary_payload["coverage"]["market_core_fields_pct"] == 1.0


def test_validation_allows_recent_markets_without_time_fields(tmp_path: Path) -> None:
    rows = _market_rows("REAL-NOTIME-001", "REAL-NOTIME-002")
    for row in rows:
        row["source_mode"] = "live_recent_filtered"
        row.pop("close_time", None)
    config = _write_dataset(
        tmp_path,
        market_rows=rows,
        trade_tickers=[],
        candle_tickers=[],
    )

    result = run_kalshi_data_validation(config)

    assert result.status == PASS
    assert result.summary_payload["coverage"]["market_core_fields_pct"] == 1.0


def test_validation_uses_alternative_market_time_keys_for_date_range(tmp_path: Path) -> None:
    rows = _market_rows("REAL-ALTDATE-001")
    rows[0].pop("close_time", None)
    rows[0]["end_date"] = "2026-01-09T00:00:00Z"
    rows[0]["source_mode"] = "live_recent_filtered"
    config = _write_dataset(
        tmp_path,
        market_rows=rows,
        trade_tickers=[],
        candle_tickers=[],
    )

    result = run_kalshi_data_validation(config)

    assert result.details_payload["date_ranges"]["markets_close_time"]["start"] == "2026-01-09T00:00:00+00:00"
