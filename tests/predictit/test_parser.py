"""Tests for PredictIt historical CSV parser."""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.predictit.parser import PredictItParser


# ── Helpers ──────────────────────────────────────────────────────────────────


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    if not fieldnames:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _daily_rows(
    contract_id: str = "12345",
    contract_name: str = "Will X happen?",
    market_name: str = "Test Market",
    n: int = 20,
    start_price: float = 0.50,
    price_step: float = 0.01,
) -> list[dict]:
    """Generate N days of PredictIt-style CSV rows."""
    rows = []
    for i in range(n):
        price = start_price + i * price_step
        rows.append({
            "ContractID": contract_id,
            "Date": f"{1 + (i // 28):02d}/{1 + (i % 28):02d}/2024",
            "ContractName": contract_name,
            "MarketName": market_name,
            "OpenSharePrice": f"{price:.2f}",
            "CloseSharePrice": f"{price + 0.005:.2f}",
            "LowSharePrice": f"{price - 0.01:.2f}",
            "HighSharePrice": f"{price + 0.02:.2f}",
            "Volume": str(100 + i * 10),
        })
    return rows


# ── Tests ────────────────────────────────────────────────────────────────────


class TestPredictItParser:
    def test_happy_path(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, _daily_rows(n=20))

        result = PredictItParser().parse(csv_path, tmp_path / "out")

        assert result.rows_loaded == 20
        assert result.contracts_found == 1
        assert result.contracts_processed == 1
        assert result.feature_files_written == 1

        parquet = tmp_path / "out" / "features" / "12345.parquet"
        assert parquet.exists()
        df = pd.read_parquet(parquet)
        assert len(df) > 0
        assert "close" in df.columns

        res_path = tmp_path / "out" / "resolution.csv"
        assert res_path.exists()

    def test_multiple_contracts(self, tmp_path: Path) -> None:
        rows = _daily_rows("c1", n=15) + _daily_rows("c2", n=15)
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, rows)

        result = PredictItParser().parse(csv_path, tmp_path / "out")

        assert result.contracts_found == 2
        assert result.contracts_processed == 2
        assert result.feature_files_written == 2

    def test_skips_few_bars(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, _daily_rows(n=5))

        result = PredictItParser().parse(csv_path, tmp_path / "out", min_bars=10)

        assert result.contracts_skipped_few_bars == 1
        assert result.contracts_processed == 0

    def test_limit_flag(self, tmp_path: Path) -> None:
        rows = _daily_rows("c1", n=15) + _daily_rows("c2", n=15) + _daily_rows("c3", n=15)
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, rows)

        result = PredictItParser().parse(csv_path, tmp_path / "out", limit=2)

        assert result.contracts_processed <= 2

    def test_missing_csv_returns_empty(self, tmp_path: Path) -> None:
        result = PredictItParser().parse(tmp_path / "missing.csv", tmp_path / "out")
        assert result.rows_loaded == 0
        assert result.contracts_processed == 0

    def test_resolution_yes(self, tmp_path: Path) -> None:
        """Contract closing at 0.95 should resolve YES (100)."""
        rows = _daily_rows(n=20, start_price=0.90)
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, rows)

        PredictItParser().parse(csv_path, tmp_path / "out")

        res_path = tmp_path / "out" / "resolution.csv"
        with res_path.open() as f:
            res_rows = list(csv.DictReader(f))
        assert float(res_rows[0]["resolution_price"]) == 100.0

    def test_resolution_no(self, tmp_path: Path) -> None:
        """Contract closing at <=0.10 should resolve NO (0)."""
        rows = _daily_rows(n=20, start_price=0.02, price_step=0.001)
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, rows)

        PredictItParser().parse(csv_path, tmp_path / "out")

        res_path = tmp_path / "out" / "resolution.csv"
        with res_path.open() as f:
            res_rows = list(csv.DictReader(f))
        assert float(res_rows[0]["resolution_price"]) == 0.0

    def test_date_range_populated(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "data.csv"
        _write_csv(csv_path, _daily_rows(n=20))

        result = PredictItParser().parse(csv_path, tmp_path / "out")

        assert result.date_range_start is not None
        assert result.date_range_end is not None
