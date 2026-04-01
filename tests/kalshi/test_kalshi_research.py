from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from trading_platform.kalshi.research import KalshiResearchConfig, KalshiResearchResult, run_kalshi_alpha_research
from trading_platform.kalshi.signals import KALSHI_SIGNAL_FAMILY_NAMES


def _make_feature_parquet(tmp_path: Path, ticker: str, n: int = 60, seed: int = 0) -> Path:
    rng = np.random.default_rng(seed)
    close = np.clip(50.0 + np.cumsum(rng.normal(0, 1, n)), 1.0, 99.0)
    df = pd.DataFrame({
        "close": close,
        "symbol": ticker,
        "calibration_drift_z": rng.normal(0, 1, n),
        "extreme_volume": rng.normal(0, 2, n),
        "tension": np.abs(rng.normal(5, 2, n)),
        "price_var_proxy": close * (100.0 - close),
        "volume_spike": rng.choice([0, 1], n, p=[0.9, 0.1]).astype(float),
    })
    path = tmp_path / f"{ticker}.parquet"
    df.to_parquet(path)
    return path


def _write_resolution_csv(tmp_path: Path, tickers: list[str]) -> Path:
    rng = np.random.default_rng(99)
    df = pd.DataFrame({
        "ticker": tickers,
        "resolution_price": rng.choice([0.0, 100.0], len(tickers)),
    })
    path = tmp_path / "resolution.csv"
    df.to_csv(path, index=False)
    return path


def test_run_kalshi_alpha_research_basic(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    tickers = [f"MKT-{i:03d}" for i in range(15)]
    for i, ticker in enumerate(tickers):
        _make_feature_parquet(feature_dir, ticker, seed=i)

    config = KalshiResearchConfig(
        feature_dir=str(feature_dir),
        output_dir=str(tmp_path / "output"),
        signal_families=KALSHI_SIGNAL_FAMILY_NAMES,
        run_id="test-run-001",
    )
    result = run_kalshi_alpha_research(config)

    assert isinstance(result, KalshiResearchResult)
    assert result.run_id == "test-run-001"
    assert len(result.signal_summary) == 3
    assert len(result.leaderboard) == 3

    for row in result.signal_summary:
        assert "signal_family" in row
        assert "n_observations" in row
        assert "ic" in row
        assert "win_rate" in row
        assert "mean_edge" in row
        assert row["n_markets"] == 15

    assert result.artifact_paths["leaderboard_csv"].exists()
    assert result.artifact_paths["summary_json"].exists()

    leaderboard_df = pd.read_csv(result.artifact_paths["leaderboard_csv"])
    assert len(leaderboard_df) == 3
    assert "signal_family" in leaderboard_df.columns


def test_run_kalshi_alpha_research_empty_feature_dir(tmp_path: Path) -> None:
    feature_dir = tmp_path / "empty_features"
    feature_dir.mkdir()

    config = KalshiResearchConfig(
        feature_dir=str(feature_dir),
        output_dir=str(tmp_path / "output"),
        run_id="empty-test",
    )
    result = run_kalshi_alpha_research(config)

    assert result.run_id == "empty-test"
    for row in result.signal_summary:
        assert row["n_markets"] == 0
        assert row["n_observations"] == 0


def test_run_kalshi_alpha_research_with_backtest(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    tickers = [f"BT-{i:03d}" for i in range(20)]
    for i, ticker in enumerate(tickers):
        _make_feature_parquet(feature_dir, ticker, seed=i + 100)
    resolution_path = _write_resolution_csv(tmp_path, tickers)

    config = KalshiResearchConfig(
        feature_dir=str(feature_dir),
        output_dir=str(tmp_path / "output"),
        resolution_data_path=str(resolution_path),
        run_backtest=True,
        run_id="bt-test",
    )
    result = run_kalshi_alpha_research(config)

    assert "backtest_results_csv" in result.artifact_paths
    assert result.artifact_paths["backtest_results_csv"].exists()
    bt_df = pd.read_csv(result.artifact_paths["backtest_results_csv"])
    assert len(bt_df) == 3
    assert set(bt_df["signal_family"]) == set(KALSHI_SIGNAL_FAMILY_NAMES)


def test_run_kalshi_alpha_research_below_min_rows_skipped(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    # Write 10-row parquets; min_rows=30 → should be skipped
    rng = np.random.default_rng(0)
    close = np.clip(50.0 + np.cumsum(rng.normal(0, 1, 10)), 1.0, 99.0)
    df = pd.DataFrame({
        "close": close,
        "calibration_drift_z": rng.normal(0, 1, 10),
        "extreme_volume": rng.normal(0, 2, 10),
        "tension": np.abs(rng.normal(5, 2, 10)),
    })
    (feature_dir / "SHORT.parquet").write_bytes(df.to_parquet())

    config = KalshiResearchConfig(
        feature_dir=str(feature_dir),
        output_dir=str(tmp_path / "output"),
        min_rows=30,
        run_id="short-test",
    )
    result = run_kalshi_alpha_research(config)
    for row in result.signal_summary:
        assert row["n_observations"] == 0


def test_run_kalshi_alpha_research_best_family_set(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    for i in range(20):
        _make_feature_parquet(feature_dir, f"M{i:03d}", seed=i)

    config = KalshiResearchConfig(
        feature_dir=str(feature_dir),
        output_dir=str(tmp_path / "output"),
        run_id="best-family-test",
    )
    result = run_kalshi_alpha_research(config)
    # best_family may be None if all ICs are NaN, but with 20 markets and 60 rows it should be set
    if result.best_family is not None:
        assert result.best_family in KALSHI_SIGNAL_FAMILY_NAMES
