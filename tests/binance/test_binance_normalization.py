from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.binance.models import BinanceNormalizeConfig
from trading_platform.binance.normalize import normalize_binance_artifacts


def test_normalize_binance_artifacts_builds_expected_outputs(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    kline_dir = raw_root / "klines" / "BTCUSDT" / "1m"
    agg_trade_dir = raw_root / "agg_trades" / "BTCUSDT"
    book_ticker_dir = raw_root / "book_ticker" / "BTCUSDT"
    kline_dir.mkdir(parents=True)
    agg_trade_dir.mkdir(parents=True)
    book_ticker_dir.mkdir(parents=True)
    (kline_dir / "a.json").write_text(
        json.dumps(
            [
                [1_704_067_200_000, "100", "101", "99", "100.5", "2", 1_704_067_259_999, "200", 3, "1", "100"],
            ]
        ),
        encoding="utf-8",
    )
    (agg_trade_dir / "a.json").write_text(
        json.dumps(
            [{"a": 1, "p": "100.1", "q": "0.2", "f": 1, "l": 2, "T": 1_704_067_200_000, "m": False, "M": True}]
        ),
        encoding="utf-8",
    )
    (book_ticker_dir / "a.json").write_text(
        json.dumps(
            {
                "_snapshot_timestamp": "2024-01-01T00:00:00+00:00",
                "symbol": "BTCUSDT",
                "bidPrice": "100.0",
                "bidQty": "1.0",
                "askPrice": "100.1",
                "askQty": "1.2",
            }
        ),
        encoding="utf-8",
    )

    result = normalize_binance_artifacts(
        BinanceNormalizeConfig(
            raw_root=str(raw_root),
            normalized_root=str(tmp_path / "normalized"),
            summary_path=str(tmp_path / "normalized" / "summary.json"),
        )
    )

    assert result.kline_files_written == 1
    assert result.agg_trade_files_written == 1
    assert result.book_ticker_files_written == 1
    frame = pd.read_parquet(tmp_path / "normalized" / "klines" / "BTCUSDT" / "1m.parquet")
    assert frame.loc[0, "schema_version"] == "binance_crypto_klines_v1"
    assert frame.loc[0, "raw_artifact_path"].endswith("a.json")
