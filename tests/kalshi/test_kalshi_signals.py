from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trading_platform.kalshi.signals import (
    ALL_KALSHI_SIGNAL_FAMILIES,
    KALSHI_CALIBRATION_DRIFT,
    KALSHI_SIGNAL_FAMILY_NAMES,
    KALSHI_TIME_DECAY,
    KALSHI_VOLUME_SPIKE,
    KalshiSignalFamily,
    compute_kalshi_signal,
    get_kalshi_signal_family,
)


def _make_feature_df(n: int = 50) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    close = 50.0 + np.cumsum(rng.normal(0, 1, n))
    close = np.clip(close, 1.0, 99.0)
    df = pd.DataFrame({
        "close": close,
        "calibration_drift_z": rng.normal(0, 1, n),
        "volume_spike": rng.choice([0, 1], n, p=[0.9, 0.1]),
        "extreme_volume": rng.normal(0, 2, n),
        "tension": np.abs(rng.normal(5, 2, n)),
        "price_var_proxy": close * (100.0 - close),
    })
    return df


def test_all_signal_families_defined() -> None:
    assert len(ALL_KALSHI_SIGNAL_FAMILIES) == 3
    names = {f.name for f in ALL_KALSHI_SIGNAL_FAMILIES}
    assert names == set(KALSHI_SIGNAL_FAMILY_NAMES)


def test_get_kalshi_signal_family_valid() -> None:
    for name in KALSHI_SIGNAL_FAMILY_NAMES:
        family = get_kalshi_signal_family(name)
        assert family.name == name


def test_get_kalshi_signal_family_invalid_raises() -> None:
    with pytest.raises(ValueError, match="Unknown Kalshi signal family"):
        get_kalshi_signal_family("nonexistent_signal")


def test_compute_signal_calibration_drift() -> None:
    df = _make_feature_df()
    signal = compute_kalshi_signal(df, KALSHI_CALIBRATION_DRIFT)
    assert isinstance(signal, pd.Series)
    assert len(signal) == len(df)
    assert signal.notna().any()
    # direction=-1 → signal should be negated calibration_drift_z
    expected = -df["calibration_drift_z"]
    pd.testing.assert_series_equal(signal.dropna(), expected.dropna(), check_names=False)


def test_compute_signal_volume_spike() -> None:
    df = _make_feature_df()
    signal = compute_kalshi_signal(df, KALSHI_VOLUME_SPIKE)
    assert isinstance(signal, pd.Series)
    assert signal.notna().any()
    # Uses extreme_volume (direction=+1)
    pd.testing.assert_series_equal(signal.dropna(), df["extreme_volume"].dropna(), check_names=False)


def test_compute_signal_time_decay() -> None:
    df = _make_feature_df()
    signal = compute_kalshi_signal(df, KALSHI_TIME_DECAY)
    assert isinstance(signal, pd.Series)
    assert signal.notna().any()
    # direction=-1 → negated tension
    expected = -df["tension"]
    pd.testing.assert_series_equal(signal.dropna(), expected.dropna(), check_names=False)


def test_signal_missing_feature_returns_nan_series() -> None:
    df = pd.DataFrame({"close": [50.0, 51.0, 52.0]})
    signal = compute_kalshi_signal(df, KALSHI_CALIBRATION_DRIFT)
    assert signal.isna().all()


def test_signal_falls_back_to_alt_column() -> None:
    # volume_spike alt_feature_cols = ("volume_spike",)
    # extreme_volume is the primary; if missing, falls back to volume_spike
    df = pd.DataFrame({"volume_spike": [0.0, 1.0, 1.0, 0.0]})
    signal = compute_kalshi_signal(df, KALSHI_VOLUME_SPIKE)
    assert signal.notna().any()
    pd.testing.assert_series_equal(signal, df["volume_spike"].astype(float), check_names=False)


def test_signal_family_direction_applied_correctly() -> None:
    family_neg = KalshiSignalFamily(name="test_neg", feature_col="x", direction=-1)
    family_pos = KalshiSignalFamily(name="test_pos", feature_col="x", direction=1)
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    neg_signal = compute_kalshi_signal(df, family_neg)
    pos_signal = compute_kalshi_signal(df, family_pos)
    pd.testing.assert_series_equal(neg_signal, -df["x"], check_names=False)
    pd.testing.assert_series_equal(pos_signal, df["x"], check_names=False)
