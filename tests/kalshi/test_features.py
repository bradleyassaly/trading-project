"""Tests for the Kalshi feature generator."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from trading_platform.kalshi.features import (
    KALSHI_FEATURE_GROUPS,
    _add_probability_calibration,
    _add_time_decay,
    _add_volume_activity,
    build_kalshi_features,
    resample_trades_to_bars,
    write_feature_parquet,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_trades(n: int = 100, base_price: float = 0.65) -> pl.DataFrame:
    """Generate synthetic trades with yes_price in [0,1] float range."""
    import random

    random.seed(42)
    base = datetime(2024, 1, 1, 0, 0, 0)
    records = []
    price = base_price
    for i in range(n):
        price = max(0.02, min(0.98, price + random.gauss(0, 0.01)))
        records.append({
            "traded_at": base + timedelta(minutes=i * 10),
            "yes_price": round(price, 4),
            "count": random.randint(1, 50),
            "ticker": "TEST-24",
            "side": "yes",
        })
    return pl.from_dicts(records).with_columns(
        pl.col("traded_at").cast(pl.Datetime)
    )


@pytest.fixture()
def trades() -> pl.DataFrame:
    return _make_trades(120)


@pytest.fixture()
def bars(trades) -> pl.DataFrame:
    return resample_trades_to_bars(trades, period="1h")


# ── resample_trades_to_bars ───────────────────────────────────────────────────

def test_resample_produces_ohlcv_columns(bars):
    assert {"timestamp", "open", "high", "low", "close", "volume", "dollar_volume"}.issubset(set(bars.columns))


def test_resample_close_in_0_100_range(bars):
    assert bars["close"].min() >= 0.0
    assert bars["close"].max() <= 100.0


def test_resample_high_gte_low(bars):
    assert (bars["high"] >= bars["low"]).all()


def test_resample_volume_positive(bars):
    assert (bars["volume"] > 0).all()


def test_resample_empty_input():
    empty = pl.DataFrame(schema={"traded_at": pl.Datetime, "yes_price": pl.Float64, "count": pl.Int64})
    result = resample_trades_to_bars(empty, period="1h")
    assert result.is_empty()


def test_resample_normalises_sub_one_prices():
    """yes_price given as 0-1 float should be multiplied to 0-100."""
    df = pl.DataFrame({
        "traded_at": [datetime(2024, 1, 1, 0, i) for i in range(5)],
        "yes_price": [0.60, 0.61, 0.62, 0.63, 0.64],
        "count": [10, 10, 10, 10, 10],
    })
    bars = resample_trades_to_bars(df, period="1h")
    assert bars["close"].max() > 1.0  # confirms scaling to 0-100


# ── Probability calibration ───────────────────────────────────────────────────

def test_calibration_columns_present(bars):
    df = _add_probability_calibration(bars)
    assert "log_odds" in df.columns
    assert "calibration_drift" in df.columns
    assert "calibration_drift_z" in df.columns


def test_log_odds_finite_for_mid_range():
    """log_odds must be finite for prices far from 0 and 100."""
    df = pl.DataFrame({
        "close": [30.0, 50.0, 70.0],
        "volume": [10.0, 10.0, 10.0],
    })
    result = _add_probability_calibration(df)
    finite = result["log_odds"].drop_nulls()
    assert len(finite) == 3
    assert all(abs(v) < 100 for v in finite.to_list())


def test_log_odds_clipped_at_extremes():
    """Prices at 0 or 100 should not produce ±inf (clipping applied)."""
    df = pl.DataFrame({
        "close": [0.0, 0.1, 99.9, 100.0],
        "volume": [1.0, 1.0, 1.0, 1.0],
    })
    result = _add_probability_calibration(df)
    assert result["log_odds"].is_finite().all()


# ── Volume activity ───────────────────────────────────────────────────────────

def test_volume_activity_columns_present(bars):
    df = bars.with_columns(bars["volume"].rolling_mean(20).alias("vol_avg_20"))
    df = _add_volume_activity(df)
    assert "volume_z" in df.columns
    assert "volume_spike" in df.columns
    assert "extreme_volume" in df.columns


def test_volume_spike_is_binary(bars):
    df = bars.with_columns(bars["volume"].rolling_mean(20).alias("vol_avg_20"))
    df = _add_volume_activity(df)
    spikes = df["volume_spike"].drop_nulls()
    assert set(spikes.to_list()).issubset({0, 1})


# ── Time decay ────────────────────────────────────────────────────────────────

def test_time_decay_with_close_time(bars):
    future = datetime(2025, 1, 1)
    df = bars.with_columns(bars["close"].pct_change().rolling_std(10).alias("vol_10"))
    result = _add_time_decay(df, close_time=future)
    assert "days_to_close" in result.columns
    assert "tension" in result.columns
    assert "time_norm_vol_10" in result.columns
    days = result["days_to_close"].drop_nulls()
    assert (days > 0).all()


def test_time_decay_null_when_no_close_time(bars):
    df = bars.with_columns(bars["close"].pct_change().rolling_std(10).alias("vol_10"))
    result = _add_time_decay(df, close_time=None)
    assert result["days_to_close"].is_null().all()
    assert result["tension"].is_null().all()


def test_price_var_proxy_peaks_at_50():
    """price_var_proxy = close × (100 − close) should be max at 50."""
    df = pl.DataFrame({"close": [10.0, 50.0, 90.0], "volume": [1.0, 1.0, 1.0]})
    df = df.with_columns((pl.col("close").pct_change().rolling_std(10)).alias("vol_10"))
    result = _add_time_decay(df, close_time=None)
    proxy = result["price_var_proxy"].to_list()
    assert proxy[1] > proxy[0]  # 50 > 10 distance from extreme
    assert proxy[1] > proxy[2]  # 50 > 90 distance from extreme


# ── build_kalshi_features (integration) ──────────────────────────────────────

def test_build_kalshi_features_all_groups(trades):
    df = build_kalshi_features(trades, ticker="TEST-24")
    assert "symbol" in df.columns
    assert df["symbol"][0] == "TEST-24"

    expected_cols = {
        # canonical
        "timestamp", "open", "high", "low", "close", "volume", "dollar_volume",
        # equity-style
        "mom_5", "mom_20", "vol_10", "vol_20", "sma_20", "vol_avg_20", "vol_ratio_20",
        # prediction-market-specific
        "log_odds", "calibration_drift", "calibration_drift_z",
        "volume_z", "volume_spike", "extreme_volume",
        "price_var_proxy", "days_to_close", "tension", "time_norm_vol_10",
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_build_kalshi_features_subset_groups(trades):
    df = build_kalshi_features(
        trades,
        ticker="T",
        feature_groups=["probability_calibration"],
    )
    assert "log_odds" in df.columns
    assert "mom_5" not in df.columns  # momentum not requested


def test_build_kalshi_features_empty_raises(trades):
    empty = trades.filter(pl.lit(False))
    with pytest.raises(ValueError, match="No bars"):
        build_kalshi_features(empty, ticker="NONE")


def test_build_kalshi_features_compatible_with_canonical(trades):
    """Output must satisfy normalize_research_frame without errors."""
    from trading_platform.data.canonical import normalize_research_frame

    df = build_kalshi_features(trades, ticker="TEST-24")
    pdf = df.to_pandas()
    normalized = normalize_research_frame(pdf, symbol="TEST-24")
    assert "close" in normalized.columns
    assert "timestamp" in normalized.columns
    assert normalized["close"].notna().any()


# ── write_feature_parquet ─────────────────────────────────────────────────────

def test_write_feature_parquet_roundtrip(trades, tmp_path):
    df = build_kalshi_features(trades, ticker="TEST-24")
    path = write_feature_parquet(df, tmp_path, "TEST-24")
    assert path.exists()

    loaded = pl.read_parquet(path)
    assert loaded.shape == df.shape
    assert set(loaded.columns) == set(df.columns)
