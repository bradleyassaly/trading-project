from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.ingestion.validation import (
    MarketDataValidationReport,
    validate_market_data_frame,
    write_market_data_validation_report,
)


def _valid_market_data_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "symbol": ["AAPL", "AAPL"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000.0, 1100.0],
            "timeframe": ["1d", "1d"],
            "provider": ["yahoo", "yahoo"],
            "asset_class": ["equity", "equity"],
            "schema_version": ["market_data_v1", "market_data_v1"],
        }
    )


def test_validate_market_data_frame_accepts_valid_input() -> None:
    report = validate_market_data_frame(
        _valid_market_data_frame(),
        symbol="AAPL",
        timeframe="1d",
        provider="yahoo",
        asset_class="equity",
    )

    assert report.passed is True
    assert report.issue_count == 0


def test_validate_market_data_frame_reports_duplicate_and_non_monotonic_rows() -> None:
    frame = pd.concat([_valid_market_data_frame().iloc[[1, 0]], _valid_market_data_frame().iloc[[0]]], ignore_index=True)

    report = validate_market_data_frame(frame)

    assert report.passed is False
    assert {issue.rule for issue in report.issues} >= {
        "duplicate_timestamp_symbol",
        "non_monotonic_timestamps",
    }


def test_validate_market_data_frame_reports_missing_columns_and_nulls() -> None:
    frame = _valid_market_data_frame().drop(columns=["provider"]).copy()
    report = validate_market_data_frame(frame)

    assert report.passed is False
    assert report.issues[0].rule == "missing_required_columns"


def test_write_market_data_validation_report_round_trips(tmp_path: Path) -> None:
    report = validate_market_data_frame(_valid_market_data_frame())

    output_path = write_market_data_validation_report(
        output_path=tmp_path / "validation.json",
        report=report,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert MarketDataValidationReport.from_dict(payload) == report
