from __future__ import annotations

import pytest

from trading_platform.features.build import build_features

import pandas as pd


def test_build_features_from_normalized_bars(tmp_path, monkeypatch) -> None:
    normalized_dir = tmp_path / "normalized"
    features_dir = tmp_path / "features"

    normalized_dir.mkdir(parents=True, exist_ok=True)
    features_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "trading_platform.features.build.NORMALIZED_DATA_DIR",
        normalized_dir,
    )
    monkeypatch.setattr(
        "trading_platform.features.build.FEATURES_DIR",
        features_dir,
    )

    dates = pd.date_range("2024-01-01", periods=250, freq="D")
    df = pd.DataFrame(
        {
            "timestamp": dates,
            "symbol": ["AAPL"] * len(dates),
            "open": [100.0 + i for i in range(len(dates))],
            "high": [101.0 + i for i in range(len(dates))],
            "low": [99.0 + i for i in range(len(dates))],
            "close": [100.5 + i for i in range(len(dates))],
            "volume": [1000 + i for i in range(len(dates))],
        }
    )

    input_path = normalized_dir / "AAPL.parquet"
    df.to_parquet(input_path, index=False)

    from trading_platform.features.build import build_features

    out_path = build_features("AAPL")

    out_df = pd.read_parquet(out_path)

    assert (features_dir / "AAPL.parquet").exists()
    assert "sma_20" in out_df.columns
    assert "sma_50" in out_df.columns
    assert "mom_20" in out_df.columns
    assert "vol_20" in out_df.columns
    assert "vol_ratio_20" in out_df.columns



def test_build_features_raises_when_normalized_file_missing(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "trading_platform.features.build.NORMALIZED_DATA_DIR",
        tmp_path / "normalized",
    )
    monkeypatch.setattr(
        "trading_platform.features.build.FEATURES_DIR",
        tmp_path / "features",
    )

    (tmp_path / "normalized").mkdir(parents=True, exist_ok=True)
    (tmp_path / "features").mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError, match="Normalized data file not found"):
        build_features("AAPL")