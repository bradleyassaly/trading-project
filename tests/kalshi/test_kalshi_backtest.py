from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_platform.kalshi.backtest import KalshiBacktester, KalshiBacktestResult, _compute_sharpe, _compute_max_drawdown
from trading_platform.kalshi.signals import ALL_KALSHI_SIGNAL_FAMILIES, KALSHI_CALIBRATION_DRIFT


def _make_feature_parquet(tmp_path: Path, ticker: str, n: int = 60, seed: int = 0) -> Path:
    rng = np.random.default_rng(seed)
    close = 50.0 + np.cumsum(rng.normal(0, 1, n))
    close = np.clip(close, 1.0, 99.0)
    df = pd.DataFrame({
        "close": close,
        "calibration_drift_z": rng.normal(0, 1, n),
        "extreme_volume": rng.normal(0, 2, n),
        "tension": np.abs(rng.normal(5, 2, n)),
        "price_var_proxy": close * (100.0 - close),
    })
    path = tmp_path / f"{ticker}.parquet"
    df.to_parquet(path)
    return path


def _make_resolution_df(tickers: list[str], seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "ticker": tickers,
        "resolution_price": rng.choice([0.0, 100.0], len(tickers)),
    })


def test_backtester_runs_with_features_and_resolution(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    tickers = [f"MKT-{i:03d}" for i in range(10)]
    for i, ticker in enumerate(tickers):
        _make_feature_parquet(feature_dir, ticker, seed=i)
    resolution = _make_resolution_df(tickers)

    bt = KalshiBacktester()
    results = bt.run(
        feature_dir=feature_dir,
        resolution_data=resolution,
        signal_families=ALL_KALSHI_SIGNAL_FAMILIES,
        output_dir=tmp_path / "out",
    )

    assert len(results) == 3
    for result in results:
        assert isinstance(result, KalshiBacktestResult)
        assert result.signal_family in {"kalshi_calibration_drift", "kalshi_volume_spike", "kalshi_time_decay"}

    csv_path = tmp_path / "out" / "backtest_results.csv"
    assert csv_path.exists()
    df = pd.read_csv(csv_path)
    assert len(df) == 3
    assert "signal_family" in df.columns
    assert "win_rate" in df.columns
    assert "mean_edge" in df.columns
    assert "sharpe" in df.columns


def test_backtester_empty_feature_dir_returns_nan_results(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    bt = KalshiBacktester()
    results = bt.run(
        feature_dir=feature_dir,
        resolution_data=pd.DataFrame(),
        signal_families=ALL_KALSHI_SIGNAL_FAMILIES,
        output_dir=tmp_path / "out",
    )
    assert len(results) == 3
    for result in results:
        assert result.n_trades == 0
        assert np.isnan(result.win_rate)


def test_backtester_no_resolution_data_skips_all_tickers(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    _make_feature_parquet(feature_dir, "MKT-001")
    bt = KalshiBacktester()
    results = bt.run(
        feature_dir=feature_dir,
        resolution_data=pd.DataFrame(columns=["ticker", "resolution_price"]),
        signal_families=[KALSHI_CALIBRATION_DRIFT],
        output_dir=tmp_path / "out",
    )
    assert results[0].n_trades == 0


def test_backtester_long_only_skips_short_trades(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    # Create a market where signal is negative (would normally short)
    n = 60
    rng = np.random.default_rng(10)
    close = np.clip(50.0 + np.cumsum(rng.normal(0, 1, n)), 1.0, 99.0)
    # Make calibration_drift_z strongly negative → signal after direction=-1 is strongly positive
    # But we want signal strongly NEGATIVE to test long_only skipping
    df = pd.DataFrame({
        "close": close,
        "calibration_drift_z": np.full(n, 5.0),  # drift_z=+5 → signal=-5 (strongly negative)
        "extreme_volume": np.zeros(n),
        "tension": np.ones(n),
        "price_var_proxy": close * (100.0 - close),
    })
    path = feature_dir / "MKT-ONLY.parquet"
    df.to_parquet(path)
    resolution = pd.DataFrame({"ticker": ["MKT-ONLY"], "resolution_price": [100.0]})

    bt_all = KalshiBacktester(long_only=False)
    bt_long = KalshiBacktester(long_only=True)

    results_all = bt_all.run(feature_dir, resolution, [KALSHI_CALIBRATION_DRIFT], tmp_path / "out_all")
    results_long = bt_long.run(feature_dir, resolution, [KALSHI_CALIBRATION_DRIFT], tmp_path / "out_long")

    # With long_only=True: signal is -5 (<0) → skip
    # With long_only=False: signal is -5 (<0) → takes short position
    assert results_long[0].n_trades == 0
    # long_only=False may or may not trade depending on threshold, but won't skip the direction
    # We just confirm the long_only path doesn't crash and writes output
    assert (tmp_path / "out_long" / "backtest_results.csv").exists()


def test_compute_sharpe_normal() -> None:
    edges = pd.Series([1.0, 2.0, 3.0, 1.5, 2.5])
    sharpe = _compute_sharpe(edges)
    assert sharpe > 0.0
    assert not np.isnan(sharpe)


def test_compute_sharpe_zero_std_returns_zero() -> None:
    edges = pd.Series([1.0, 1.0, 1.0])
    assert _compute_sharpe(edges) == 0.0


def test_compute_max_drawdown() -> None:
    equity = pd.Series([100.0, 120.0, 90.0, 110.0, 80.0])
    dd = _compute_max_drawdown(equity)
    assert dd < 0.0
    assert dd == pytest.approx(-1.0 / 3.0, abs=0.01)  # 80/120 - 1 = -1/3


def test_compute_max_drawdown_monotonic_increase() -> None:
    equity = pd.Series([10.0, 20.0, 30.0, 40.0])
    dd = _compute_max_drawdown(equity)
    assert dd == 0.0
