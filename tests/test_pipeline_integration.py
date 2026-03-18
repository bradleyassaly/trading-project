from __future__ import annotations

import pandas as pd

from trading_platform.config.models import FeatureConfig, IngestConfig


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
        n = 250
        dates = pd.date_range("2024-01-01", periods=n, freq="D")
        return pd.DataFrame(
            {
                "Open": [100.0 + i for i in range(n)],
                "High": [101.0 + i for i in range(n)],
                "Low": [99.0 + i for i in range(n)],
                "Close": [100.5 + i for i in range(n)],
                "Volume": [1000 + i for i in range(n)],
            },
            index=dates,
        )


def test_research_prep_pipeline_end_to_end(tmp_path, monkeypatch) -> None:
    raw_dir = tmp_path / "raw"
    normalized_dir = tmp_path / "normalized"
    features_dir = tmp_path / "features"

    for path in [raw_dir, normalized_dir, features_dir]:
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("trading_platform.data.ingest.RAW_DATA_DIR", raw_dir)
    monkeypatch.setattr("trading_platform.data.ingest.NORMALIZED_DATA_DIR", normalized_dir)
    monkeypatch.setattr("trading_platform.features.build.NORMALIZED_DATA_DIR", normalized_dir)
    monkeypatch.setattr("trading_platform.features.build.FEATURES_DIR", features_dir)

    from trading_platform.services.pipeline_service import run_research_prep_pipeline

    ingest_config = IngestConfig(
        symbol="AAPL",
        start="2024-01-01",
        interval="1d",
    )
    feature_config = FeatureConfig(
        symbol="AAPL",
        feature_groups=None,
    )

    out = run_research_prep_pipeline(
        ingest_config=ingest_config,
        feature_config=feature_config,
        provider=FakeBarDataProvider(),
    )

    assert out["normalized_path"].exists()
    assert out["features_path"].exists()

    feature_df = pd.read_parquet(out["features_path"])
    assert "sma_20" in feature_df.columns
    assert "mom_20" in feature_df.columns
    assert "vol_20" in feature_df.columns