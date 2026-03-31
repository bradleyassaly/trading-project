from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.features.store import FeatureStoreArtifact, LocalFeatureStore


def _write_feature_frame(path: Path) -> None:
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "symbol": ["AAPL"] * 3,
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000.0, 1100.0, 1200.0],
            "sma_20": [None, None, 101.5],
            "mom_20": [None, None, 0.02],
        }
    ).to_parquet(path, index=False)


def test_local_feature_store_writes_and_reads_artifact(tmp_path: Path) -> None:
    source = tmp_path / "features.parquet"
    _write_feature_frame(source)
    store = LocalFeatureStore(tmp_path / "feature_store")

    artifact = store.write_from_parquet(
        source_path=source,
        symbol="AAPL",
        timeframe="1d",
        feature_groups=["momentum", "trend"],
        metadata={"source": "test"},
    )

    loaded = store.read_artifact(
        symbol="AAPL",
        timeframe="1d",
        feature_groups=["trend", "momentum"],
    )
    frame = store.read_frame(
        symbol="AAPL",
        timeframe="1d",
        feature_groups=["trend", "momentum"],
    )

    assert loaded == artifact
    assert frame["symbol"].tolist() == ["AAPL", "AAPL", "AAPL"]
    assert artifact.feature_set_id == "momentum__trend"
    assert artifact.metadata == {"source": "test"}
    assert "sma_20" in artifact.feature_columns


def test_feature_store_artifact_round_trips() -> None:
    artifact = FeatureStoreArtifact(
        symbol="AAPL",
        timeframe="1d",
        feature_set_id="default",
        feature_groups=["trend"],
        row_count=10,
        data_path="data.parquet",
        manifest_path="manifest.json",
        start_timestamp="2024-01-01T00:00:00",
        end_timestamp="2024-01-10T00:00:00",
        feature_columns=["sma_20"],
        metadata={"source": "test"},
    )

    assert FeatureStoreArtifact.from_dict(artifact.to_dict()) == artifact
