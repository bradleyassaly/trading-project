"""Tests for Polymarket blockchain trade ingestion."""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.blockchain_ingest import PolymarketBlockchainIngest


def _write_trades_csv(path: Path, trades: list[dict]) -> None:
    fieldnames = ["block_number", "timestamp", "tx_hash", "wallet", "token_id",
                   "side", "tokens", "price", "total_usdc"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)


def _trade(
    token_id: str = "tok-1",
    price: float = 0.65,
    usdc: float = 100.0,
    hour_offset: int = 0,
) -> dict:
    return {
        "block_number": str(1000000 + hour_offset),
        "timestamp": f"2026-04-01T{hour_offset:02d}:00:00+00:00",
        "tx_hash": f"0xabc{hour_offset}",
        "wallet": "0xwallet1",
        "token_id": token_id,
        "side": "BUY",
        "tokens": "10.0",
        "price": str(price),
        "total_usdc": str(usdc),
    }


class TestBlockchainIngest:
    def test_happy_path(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "trades.csv"
        trades = [_trade(hour_offset=i, price=0.50 + i * 0.02) for i in range(20)]
        _write_trades_csv(csv_path, trades)

        result = PolymarketBlockchainIngest().run(csv_path, tmp_path / "out")

        assert result.rows_loaded == 20
        assert result.markets_processed >= 1
        assert result.feature_files_written >= 1

    def test_multiple_token_ids(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "trades.csv"
        trades = (
            [_trade(token_id="tok-A", hour_offset=i) for i in range(15)] +
            [_trade(token_id="tok-B", hour_offset=i, price=0.30) for i in range(15)]
        )
        _write_trades_csv(csv_path, trades)

        result = PolymarketBlockchainIngest().run(csv_path, tmp_path / "out")

        assert result.token_ids_found == 2
        assert result.markets_processed == 2

    def test_skips_few_trades(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "trades.csv"
        trades = [_trade(hour_offset=i) for i in range(5)]
        _write_trades_csv(csv_path, trades)

        result = PolymarketBlockchainIngest().run(csv_path, tmp_path / "out", min_trades=10)

        assert result.markets_skipped_few_trades == 1
        assert result.markets_processed == 0

    def test_limit_flag(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "trades.csv"
        trades = (
            [_trade(token_id="tok-A", hour_offset=i) for i in range(15)] +
            [_trade(token_id="tok-B", hour_offset=i) for i in range(15)] +
            [_trade(token_id="tok-C", hour_offset=i) for i in range(15)]
        )
        _write_trades_csv(csv_path, trades)

        result = PolymarketBlockchainIngest().run(csv_path, tmp_path / "out", limit=2)

        assert result.markets_processed <= 2

    def test_missing_csv(self, tmp_path: Path) -> None:
        result = PolymarketBlockchainIngest().run(tmp_path / "missing.csv", tmp_path / "out")
        assert result.rows_loaded == 0

    def test_resolution_csv_written(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "trades.csv"
        trades = [_trade(hour_offset=i, price=0.50) for i in range(15)]
        _write_trades_csv(csv_path, trades)

        PolymarketBlockchainIngest().run(csv_path, tmp_path / "out")

        res_path = tmp_path / "out" / "resolution.csv"
        assert res_path.exists()

    def test_metadata_from_db(self, tmp_path: Path) -> None:
        """When metadata DB exists, token_id maps to market_id."""
        from trading_platform.polymarket.live_db import LiveTickStore

        db_path = tmp_path / "meta.db"
        store = LiveTickStore(db_path)
        store.upsert_market_info("mkt-real", "Will X?", 5000.0, "tok-1", None)
        store.close()

        csv_path = tmp_path / "trades.csv"
        trades = [_trade(token_id="tok-1", hour_offset=i) for i in range(15)]
        _write_trades_csv(csv_path, trades)

        result = PolymarketBlockchainIngest().run(
            csv_path, tmp_path / "out", metadata_db_path=db_path,
        )

        assert result.markets_processed == 1
        parquet = tmp_path / "out" / "features" / "mkt-real.parquet"
        assert parquet.exists()
