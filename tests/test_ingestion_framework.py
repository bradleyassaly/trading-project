from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.ingestion.contracts import (
    CANONICAL_MARKET_DATA_COLUMNS,
    MarketDataArtifactManifest,
)
from trading_platform.ingestion.framework import (
    CryptoIntradayIngestionScaffoldAdapter,
    YahooEquityDailyIngestionAdapter,
    build_market_data_artifact_paths,
    normalize_market_data_frame,
    write_market_data_artifacts,
)


class FakeYahooProvider:
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


def test_yahoo_equity_daily_adapter_normalizes_to_canonical_market_data() -> None:
    adapter = YahooEquityDailyIngestionAdapter(provider=FakeYahooProvider())

    raw = adapter.fetch_raw_bars(symbol="AAPL", start="2024-01-01", timeframe="1d")
    out = adapter.normalize_raw_bars(raw_frame=raw, symbol="AAPL", timeframe="1d")

    assert list(out.columns) == CANONICAL_MARKET_DATA_COLUMNS
    assert out["provider"].tolist() == ["yahoo", "yahoo"]
    assert out["asset_class"].tolist() == ["equity", "equity"]
    assert out["timeframe"].tolist() == ["1d", "1d"]


def test_crypto_scaffold_adapter_normalizes_intraday_rows() -> None:
    adapter = CryptoIntradayIngestionScaffoldAdapter()
    raw = pd.DataFrame(
        {
            "timestamp": ["2025-01-01T09:30:00", "2025-01-01T09:31:00"],
            "open": [45000.0, 45010.0],
            "high": [45020.0, 45030.0],
            "low": [44990.0, 45000.0],
            "close": [45010.0, 45020.0],
            "volume": [1.2, 1.3],
        }
    )

    out = adapter.normalize_raw_bars(raw_frame=raw, symbol="BTCUSD", timeframe="1m")

    assert list(out.columns) == CANONICAL_MARKET_DATA_COLUMNS
    assert out["symbol"].tolist() == ["BTCUSD", "BTCUSD"]
    assert out["asset_class"].tolist() == ["crypto", "crypto"]
    assert out["timeframe"].tolist() == ["1m", "1m"]


def test_write_market_data_artifacts_emits_versioned_manifest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.ingestion.framework.MARKET_DATA_ARTIFACTS_DIR",
        tmp_path / "market_data",
    )
    normalized = normalize_market_data_frame(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "symbol": ["AAPL", "AAPL"],
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1000.0, 1100.0],
            }
        ),
        symbol="AAPL",
        timeframe="1d",
        provider="yahoo",
        asset_class="equity",
    )
    raw = pd.DataFrame({"Open": [100.0], "Close": [101.0]})

    manifest = write_market_data_artifacts(
        raw_frame=raw,
        normalized_frame=normalized,
        symbol="AAPL",
        provider="yahoo",
        asset_class="equity",
        timeframe="1d",
        metadata={"note": "test"},
    )

    manifest_path = Path(manifest.manifest_path)
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert MarketDataArtifactManifest.from_dict(payload) == manifest
    assert payload["schema_version"] == "market_data_v1"
    assert payload["row_count"] == 2
    assert Path(payload["validation_report_path"]).exists()
    assert payload["metadata"] == {"note": "test"}


def test_build_market_data_artifact_paths_separates_intraday_datasets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.ingestion.framework.MARKET_DATA_ARTIFACTS_DIR",
        tmp_path / "market_data",
    )

    daily_paths = build_market_data_artifact_paths(
        symbol="AAPL",
        provider="yahoo",
        asset_class="equity",
        timeframe="1d",
    )
    intraday_paths = build_market_data_artifact_paths(
        symbol="AAPL",
        provider="crypto_scaffold",
        asset_class="crypto",
        timeframe="1m",
    )

    assert daily_paths["normalized_path"].name == "AAPL.parquet"
    assert intraday_paths["normalized_path"].name == "AAPL__1m.parquet"
