from __future__ import annotations

import json
import pandas as pd

from trading_platform.data.ingest import ingest_symbol


class FakeBarDataProvider:
    @property
    def provider_name(self) -> str:
        return "yahoo"

    def fetch_bars(
        self,
        symbol: str,
        start: str,
        end: str | None = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, 102.0],
                "Volume": [1000, 1100],
            },
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )


def test_ingest_symbol_writes_normalized_parquet(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.data.ingest.RAW_DATA_DIR", tmp_path / "raw")
    monkeypatch.setattr(
        "trading_platform.data.ingest.NORMALIZED_DATA_DIR",
        tmp_path / "normalized",
    )

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "normalized").mkdir(parents=True, exist_ok=True)

    out_path = ingest_symbol(
        symbol="AAPL",
        start="2024-01-01",
        provider=FakeBarDataProvider(),
    )

    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert list(df.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "timeframe",
        "provider",
        "asset_class",
        "schema_version",
    ]
    assert df["symbol"].tolist() == ["AAPL", "AAPL"]


def test_ingest_symbol_writes_raw_snapshot(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.data.ingest.RAW_DATA_DIR", tmp_path / "raw")
    monkeypatch.setattr(
        "trading_platform.data.ingest.NORMALIZED_DATA_DIR",
        tmp_path / "normalized",
    )

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "normalized").mkdir(parents=True, exist_ok=True)

    ingest_symbol(
        symbol="MSFT",
        start="2024-01-01",
        provider=FakeBarDataProvider(),
    )

    raw_path = tmp_path / "raw" / "MSFT.parquet"
    norm_path = tmp_path / "normalized" / "MSFT.parquet"

    assert raw_path.exists()
    assert norm_path.exists()


def test_ingest_symbol_writes_unified_market_data_artifacts(tmp_path, monkeypatch) -> None:
    market_data_root = tmp_path / "market_data"
    monkeypatch.setattr("trading_platform.data.ingest.RAW_DATA_DIR", tmp_path / "raw")
    monkeypatch.setattr("trading_platform.data.ingest.NORMALIZED_DATA_DIR", tmp_path / "normalized")
    monkeypatch.setattr("trading_platform.ingestion.framework.MARKET_DATA_ARTIFACTS_DIR", market_data_root)

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "normalized").mkdir(parents=True, exist_ok=True)

    ingest_symbol(
        symbol="MSFT",
        start="2024-01-01",
        provider=FakeBarDataProvider(),
    )

    unified_root = market_data_root / "equity" / "yahoo" / "1d"
    manifest_path = unified_root / "MSFT.manifest.json"
    dataset_path = unified_root / "MSFT.parquet"

    assert manifest_path.exists()
    assert dataset_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "market_data_v1"
    assert payload["provider"] == "yahoo"
    assert payload["asset_class"] == "equity"
