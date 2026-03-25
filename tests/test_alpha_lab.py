from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.composite import (
    build_composite_scores,
    normalize_signal_by_date,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.lifecycle import (
    SignalLifecycleConfig,
    build_dynamic_signal_weights,
)
from trading_platform.research.alpha_lab.regime import (
    RegimeConfig,
    build_regime_aware_signal_weights,
    build_regime_labels_by_date,
)
from trading_platform.research.alpha_lab.composite_portfolio import (
    CompositePortfolioConfig,
    apply_liquidity_filters,
    build_composite_portfolio_weights,
    build_liquidity_panel,
    build_long_short_quantile_weights,
    estimate_capacity,
    run_stress_tests,
    run_composite_portfolio_backtest,
)
from trading_platform.research.alpha_lab.metrics import (
    evaluate_cross_sectional_signal,
    compute_turnover,
    evaluate_signal,
)
from trading_platform.research.alpha_lab.promotion import (
    DEFAULT_PROMOTION_THRESHOLDS,
    apply_promotion_rules,
)
from trading_platform.research.alpha_lab.runner import (
    _add_equity_context_features,
    _load_symbol_feature_data,
    run_alpha_research,
)
from trading_platform.research.alpha_lab.signals import build_candidate_grid, build_signal


def test_load_symbol_feature_data_preserves_existing_timestamp_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert "timestamp" in result.columns
    assert result["timestamp"].is_monotonic_increasing
    assert result["symbol"].tolist() == ["AAPL", "AAPL", "AAPL"]
    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_load_symbol_feature_data_normalizes_lowercase_date_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert "timestamp" in result.columns
    assert result["timestamp"].iloc[0] == pd.Timestamp("2024-01-01")


def test_load_symbol_feature_data_normalizes_capitalized_date_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert "timestamp" in result.columns
    assert result["timestamp"].iloc[-1] == pd.Timestamp("2024-01-03")


def test_load_symbol_feature_data_normalizes_datetime_index(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0],
        },
        index=pd.date_range("2024-01-01", periods=3, freq="D"),
    ).to_parquet(feature_dir / "AAPL.parquet")

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert "timestamp" in result.columns
    assert result["timestamp"].tolist() == list(pd.date_range("2024-01-01", periods=3, freq="D"))


def test_load_symbol_feature_data_normalizes_capitalized_close_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "Close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_load_symbol_feature_data_normalizes_adj_close_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "adj_close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_load_symbol_feature_data_normalizes_adj_close_with_space(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "Adj Close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_load_symbol_feature_data_normalizes_adjusted_close_column(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "adjusted_close": [100.0, 101.0, 102.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_load_symbol_feature_data_raises_for_missing_date_like_field(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0],
            "volume": [1, 2, 3],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    with pytest.raises(ValueError, match="timestamp"):
        _load_symbol_feature_data(feature_dir, "AAPL")


def test_load_symbol_feature_data_raises_for_missing_close_like_field(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "open": [99.0, 100.0, 101.0],
        }
    ).to_parquet(feature_dir / "AAPL.parquet", index=False)

    with pytest.raises(ValueError, match="close"):
        _load_symbol_feature_data(feature_dir, "AAPL")


def test_add_forward_return_labels_aligns_1d_and_5d_correctly() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="D"),
            "close": [100.0, 110.0, 121.0, 133.1, 146.41, 161.051],
        }
    )

    result = add_forward_return_labels(df, horizons=[1, 5])

    assert result.loc[0, "fwd_return_1d"] == pytest.approx(0.10)
    assert result.loc[1, "fwd_return_1d"] == pytest.approx(0.10)
    assert result.loc[0, "fwd_return_5d"] == pytest.approx(0.61051)
    assert pd.isna(result.loc[5, "fwd_return_1d"])
    assert pd.isna(result.loc[1, "fwd_return_5d"])


def test_build_signal_momentum_returns_expected_values() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 110.0, 121.0, 133.1],
        }
    )

    signal = build_signal(df, signal_family="momentum", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx(0.10)
    assert signal.iloc[2] == pytest.approx(0.10)
    assert signal.iloc[3] == pytest.approx(0.10)


def test_build_signal_short_term_reversal_negates_momentum() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 110.0, 121.0, 133.1],
        }
    )

    momentum = build_signal(df, signal_family="momentum", lookback=1)
    reversal = build_signal(df, signal_family="short_term_reversal", lookback=1)

    pd.testing.assert_series_equal(reversal, -momentum)


def test_build_signal_equity_context_momentum_uses_relative_vol_and_breadth_features() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0],
            "relative_return_1": [None, 0.02, 0.03, 0.04],
            "breadth_impulse_1": [0.0, 0.10, -0.10, 0.20],
            "realized_vol_20": [0.0, 0.02, 0.00, 0.04],
            "volume_ratio_20": [1.0, 1.2, 2.0, 0.25],
        }
    )

    signal = build_signal(df, signal_family="equity_context_momentum", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx(1.32)
    assert signal.iloc[2] == pytest.approx(0.0405)
    assert signal.iloc[3] == pytest.approx(0.6)


def test_build_signal_volatility_adjusted_momentum_alias_matches_existing_family() -> None:
    df = pd.DataFrame({"close": [100.0, 102.0, 104.0, 108.0, 110.0]})

    legacy = build_signal(df, signal_family="vol_adjusted_momentum", lookback=2)
    aliased = build_signal(df, signal_family="volatility_adjusted_momentum", lookback=2)

    pd.testing.assert_series_equal(legacy, aliased)


def test_build_signal_short_horizon_mean_reversion_negates_return_zscore() -> None:
    df = pd.DataFrame({"close": [100.0, 104.0, 103.0, 101.0, 99.0, 100.0]})

    signal = build_signal(df, signal_family="short_horizon_mean_reversion", lookback=3)

    assert signal.notna().sum() > 0
    assert signal.iloc[4] > 0.0


def test_build_signal_momentum_acceleration_compares_fast_and_slow_returns() -> None:
    df = pd.DataFrame({"close": [100.0, 100.0, 100.0, 90.0, 95.0, 110.0]})

    signal = build_signal(df, signal_family="momentum_acceleration", lookback=4)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[5] > 0.0


def test_build_signal_cross_sectional_relative_strength_prefers_relative_return_feature() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0],
            "relative_return_1": [None, 0.02, 0.01, 0.03],
        }
    )

    signal = build_signal(df, signal_family="cross_sectional_relative_strength", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx(0.02)
    assert signal.iloc[3] == pytest.approx(0.03)


def test_build_signal_cross_sectional_momentum_alias_matches_relative_strength() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0],
            "relative_return_1": [None, 0.02, 0.01, 0.03],
        }
    )

    relative_strength = build_signal(df, signal_family="cross_sectional_relative_strength", lookback=1)
    aliased = build_signal(df, signal_family="cross_sectional_momentum", lookback=1)

    pd.testing.assert_series_equal(relative_strength, aliased)


def test_build_candidate_grid_broad_v1_expands_variants_with_stable_names() -> None:
    candidates = build_candidate_grid(
        signal_family="cross_sectional_momentum",
        lookbacks=[5, 20],
        horizons=[1, 5],
        candidate_grid_preset="broad_v1",
    )

    assert len(candidates) == 24
    assert candidates[0].signal_variant == "base"
    assert {candidate.signal_variant for candidate in candidates} >= {
        "base",
        "relative_strength_focus",
        "breadth_confirmed",
        "volatility_filtered",
    }


def test_build_signal_cross_sectional_momentum_variants_change_signal_profile() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 103.0, 104.0, 106.0],
            "relative_return_2": [None, None, 0.03, 0.01, 0.04],
            "breadth_impulse_2": [0.0, 0.0, 0.10, -0.05, 0.20],
            "realized_vol_20": [0.15, 0.18, 0.25, 0.35, 0.20],
            "volume_ratio_20": [1.0, 1.1, 1.4, 0.9, 1.8],
        }
    )

    base = build_signal(df, signal_family="cross_sectional_momentum", lookback=2)
    breadth_confirmed = build_signal(
        df,
        signal_family="cross_sectional_momentum",
        lookback=2,
        signal_variant="breadth_confirmed",
        variant_params={"market_relative_weight": 1.1, "breadth_weight": 0.4},
    )

    assert breadth_confirmed.notna().sum() > 0
    assert breadth_confirmed.iloc[4] != pytest.approx(base.iloc[4])


def test_build_signal_cross_sectional_momentum_composite_preset_uses_relative_context_and_flow() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 103.0, 104.0, 106.0],
            "relative_return_2": [None, None, 0.03, 0.01, 0.04],
            "cross_sectional_relative_rank_2": [0.0, 0.0, 0.2, -0.1, 0.4],
            "trend_slope_2": [0.0, 0.0, 0.3, 0.1, 0.25],
            "trend_persistence_2": [0.0, 0.0, 0.5, 0.0, 0.5],
            "breadth_impulse_2": [0.0, 0.0, 0.10, -0.05, 0.20],
            "market_trend_strength_2": [0.0, 0.0, 0.15, -0.05, 0.10],
            "realized_vol_2": [0.2, 0.2, 0.25, 0.35, 0.20],
            "cross_sectional_vol_rank_2": [0.0, 0.0, -0.2, -0.4, 0.1],
            "flow_confirmation_2": [0.0, 0.0, 0.10, -0.05, 0.30],
        }
    )

    legacy = build_signal(df, signal_family="cross_sectional_momentum", lookback=2)
    composite = build_signal(
        df,
        signal_family="cross_sectional_momentum",
        lookback=2,
        signal_composition_preset="composite_v1",
    )

    assert composite.notna().sum() > 0
    assert composite.iloc[4] != pytest.approx(legacy.iloc[4])
    assert composite.iloc[4] > composite.iloc[3]


def test_add_equity_context_features_derives_richer_signal_inputs() -> None:
    timestamps = pd.date_range("2024-01-01", periods=8, freq="D")
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": ["AAPL"] * len(timestamps),
                "close": [100.0, 101.0, 102.0, 104.0, 105.0, 107.0, 108.0, 110.0],
                "volume": [1000, 1100, 1200, 1400, 1350, 1500, 1600, 1700],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": ["MSFT"] * len(timestamps),
                "close": [100.0, 100.5, 101.0, 101.5, 102.0, 102.2, 102.4, 102.6],
                "volume": [1000, 980, 990, 995, 1005, 1010, 1015, 1020],
            }
        ),
    }

    enriched = _add_equity_context_features(symbol_data, lookbacks=[2, 3], include_volume=True)
    aapl = enriched["AAPL"]

    assert {
        "return_2",
        "trend_slope_2",
        "trend_persistence_2",
        "breakout_distance_2",
        "breakout_percentile_2",
        "reversal_intensity_2",
        "cross_sectional_relative_rank_2",
        "cross_sectional_vol_rank_2",
        "market_trend_strength_2",
        "market_dispersion_2",
        "dollar_volume_ratio_2",
        "flow_confirmation_2",
    } <= set(aapl.columns)
    assert aapl["cross_sectional_relative_rank_2"].notna().sum() > 0
    assert aapl["flow_confirmation_2"].notna().sum() > 0


def test_build_signal_breakout_continuation_rewards_new_highs() -> None:
    df = pd.DataFrame({"close": [100.0, 101.0, 102.0, 101.0, 104.0, 106.0]})

    signal = build_signal(df, signal_family="breakout_continuation", lookback=3)

    assert signal.notna().sum() > 0
    assert signal.iloc[4] > 0.0
    assert signal.iloc[5] > 0.0


def test_build_signal_benchmark_relative_rotation_uses_relative_and_breadth_context() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0],
            "relative_return_1": [None, 0.02, 0.01, 0.03],
            "breadth_impulse_1": [0.0, 0.20, -0.10, 0.10],
            "realized_vol_20": [1.0, 0.5, 1.0, 2.0],
        }
    )

    signal = build_signal(df, signal_family="benchmark_relative_rotation", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx(0.048)
    assert signal.iloc[2] == pytest.approx(0.009)
    assert signal.iloc[3] == pytest.approx(0.0165)


def test_build_signal_regime_conditioned_momentum_suppresses_risk_off_periods() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 102.0, 104.0, 103.0],
            "market_return_1": [None, 0.01, -0.02, 0.01],
            "breadth_impulse_1": [0.0, 0.20, -0.10, 0.10],
        }
    )

    signal = build_signal(df, signal_family="regime_conditioned_momentum", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx((102.0 / 100.0 - 1.0) * 1.2)
    assert signal.iloc[2] == pytest.approx((104.0 / 102.0 - 1.0) * 0.25 * 0.9)
    assert signal.iloc[3] == pytest.approx((103.0 / 104.0 - 1.0) * 1.1)


def test_build_signal_volatility_dispersion_selection_prefers_low_vol_relative_strength() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 104.0, 105.0, 107.0],
            "relative_return_1": [None, 0.02, 0.01, 0.03, 0.025, 0.03],
            "realized_vol_20": [0.20, 0.20, 0.40, 0.35, 0.15, 0.10],
            "dispersion_regime": [
                "low_dispersion",
                "low_dispersion",
                "high_dispersion",
                "high_dispersion",
                "low_dispersion",
                "high_dispersion",
            ],
        }
    )

    signal = build_signal(df, signal_family="volatility_dispersion_selection", lookback=2)

    assert signal.notna().sum() > 0
    assert signal.iloc[5] > signal.iloc[2]


def test_build_signal_sector_relative_momentum_uses_sector_baseline_when_available() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 102.0, 104.0, 106.0],
            "sector_return_1": [None, 0.01, 0.03, 0.01],
        }
    )

    signal = build_signal(df, signal_family="sector_relative_momentum", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx((102.0 / 100.0 - 1.0) - 0.01)
    assert signal.iloc[2] == pytest.approx((104.0 / 102.0 - 1.0) - 0.03)


def test_build_signal_liquidity_flow_tilt_uses_volume_and_dollar_flow_confirmation() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 104.0],
            "relative_return_1": [None, 0.01, 0.02, 0.03],
            "volume_ratio_20": [1.0, 0.8, 1.5, 2.0],
            "dollar_volume": [100.0, 110.0, 180.0, 240.0],
            "avg_dollar_volume_20": [100.0, 100.0, 120.0, 150.0],
        }
    )

    signal = build_signal(df, signal_family="liquidity_flow_tilt", lookback=1)

    assert signal.iloc[0] == pytest.approx(0.0)
    assert signal.iloc[3] > signal.iloc[1]
    assert signal.iloc[3] > signal.iloc[2]


def test_build_signal_volatility_adjusted_reversal_is_inverse_of_vol_adjusted_momentum_direction() -> None:
    df = pd.DataFrame({"close": [100.0, 103.0, 101.0, 99.0, 98.0]})

    reversal = build_signal(df, signal_family="volatility_adjusted_reversal", lookback=2)
    momentum = build_signal(df, signal_family="vol_adjusted_momentum", lookback=2)

    assert reversal.notna().sum() > 0
    assert reversal.iloc[4] == pytest.approx(-momentum.iloc[4])


def test_build_signal_volume_shock_momentum_scales_momentum_by_volume_ratio() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 104.0],
            "volume_ratio_20": [1.0, 0.5, 2.0, 1.5],
        }
    )

    signal = build_signal(df, signal_family="volume_shock_momentum", lookback=1)

    assert pd.isna(signal.iloc[0])
    assert signal.iloc[1] == pytest.approx(0.005)
    assert signal.iloc[2] == pytest.approx((102.0 / 101.0 - 1.0) * 2.0)


def test_evaluate_signal_returns_expected_ic_on_perfect_rank_order() -> None:
    signal = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    forward_return = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])

    metrics = evaluate_signal(signal, forward_return)

    assert metrics["n_obs"] == 5
    assert metrics["pearson_ic"] == pytest.approx(1.0)
    assert metrics["spearman_ic"] == pytest.approx(1.0)
    assert metrics["hit_rate"] == pytest.approx(1.0)
    assert metrics["turnover"] > 0.0


def test_compute_turnover_zero_for_constant_signal() -> None:
    signal = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0])

    turnover = compute_turnover(signal)

    assert turnover == pytest.approx(0.0)


def test_evaluate_cross_sectional_signal_ranks_symbols_per_date() -> None:
    panel = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": ["AAPL", "MSFT", "NVDA", "AAPL", "MSFT", "NVDA"],
            "signal": [1.0, 2.0, 3.0, 3.0, 2.0, 1.0],
            "forward_return": [0.01, 0.02, 0.03, 0.03, 0.02, 0.01],
        }
    )

    metrics = evaluate_cross_sectional_signal(panel)

    assert metrics["dates_evaluated"] == 2
    assert metrics["symbols_evaluated"] == 3
    assert metrics["pearson_ic"] == pytest.approx(1.0)
    assert metrics["spearman_ic"] == pytest.approx(1.0)
    assert metrics["long_short_spread"] == pytest.approx(0.02)
    assert metrics["quantile_spread"] == pytest.approx(metrics["long_short_spread"])
    assert metrics["turnover"] > 0.0


def test_evaluate_cross_sectional_signal_uses_rank_buckets_per_date() -> None:
    panel = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": [
                "AAPL",
                "MSFT",
                "NVDA",
                "AMZN",
                "META",
                "AAPL",
                "MSFT",
                "NVDA",
                "AMZN",
                "META",
            ],
            "signal": [1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 4.0, 3.0, 2.0, 1.0],
            "forward_return": [0.00, 0.01, 0.02, 0.03, 0.04, 0.04, 0.03, 0.02, 0.01, 0.00],
        }
    )

    metrics = evaluate_cross_sectional_signal(
        panel,
        top_quantile=0.4,
        bottom_quantile=0.4,
    )

    assert metrics["dates_evaluated"] == 2
    assert metrics["n_obs"] == pytest.approx(10.0)
    assert metrics["long_short_spread"] == pytest.approx(0.03)
    assert metrics["quantile_spread"] == pytest.approx(0.03)


def test_normalize_signal_by_date_centers_ranks_within_each_date() -> None:
    panel = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": ["AAPL", "MSFT", "NVDA", "AAPL", "MSFT"],
            "signal": [1.0, 2.0, 3.0, 10.0, 20.0],
        }
    )

    normalized = normalize_signal_by_date(panel)

    first_day = normalized.loc[normalized["timestamp"] == pd.Timestamp("2024-01-01")]
    second_day = normalized.loc[normalized["timestamp"] == pd.Timestamp("2024-01-02")]

    assert first_day["normalized_signal"].tolist() == pytest.approx(
        [-0.3333333333, 0.3333333333, 1.0]
    )
    assert second_day["normalized_signal"].tolist() == pytest.approx([0.0, 1.0])


def test_select_low_redundancy_signals_filters_highly_correlated_candidates() -> None:
    promoted_signals_df = pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "mean_spearman_ic": 0.08,
                "mean_long_short_spread": 0.03,
            },
            {
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "mean_spearman_ic": 0.07,
                "mean_long_short_spread": 0.025,
            },
        ]
    )
    redundancy_df = pd.DataFrame(
        [
            {
                "signal_family_a": "momentum",
                "lookback_a": 5,
                "horizon_a": 1,
                "signal_family_b": "momentum",
                "lookback_b": 10,
                "horizon_b": 1,
                "score_corr": 0.95,
                "performance_corr": 0.90,
                "rank_ic_corr": 0.85,
            }
        ]
    )

    selected, excluded = select_low_redundancy_signals(
        promoted_signals_df,
        redundancy_df,
        horizon=1,
        redundancy_corr_threshold=0.8,
    )

    assert len(selected) == 1
    assert int(selected.loc[0, "lookback"]) == 5
    assert len(excluded) == 1
    assert excluded[0]["candidate_id"].endswith("|10|1")


def test_build_composite_scores_combines_available_components() -> None:
    selected_signals_df = pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "mean_spearman_ic": 0.08,
                "mean_long_short_spread": 0.03,
            },
            {
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "mean_spearman_ic": 0.04,
                "mean_long_short_spread": 0.02,
            },
        ]
    )
    score_panel_by_candidate = {
        ("momentum", 5, 1): pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
                "symbol": ["AAPL", "MSFT"],
                "signal": [1.0, 3.0],
            }
        ),
        ("momentum", 10, 1): pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
                "symbol": ["AAPL", "MSFT"],
                "signal": [2.0, 4.0],
            }
        ),
    }

    composite_scores = build_composite_scores(
        selected_signals_df,
        score_panel_by_candidate=score_panel_by_candidate,
        weighting_scheme="equal",
        quality_metric="mean_spearman_ic",
    )

    assert len(composite_scores) == 2
    assert set(composite_scores["symbol"]) == {"AAPL", "MSFT"}
    assert composite_scores["component_count"].tolist() == [2, 2]
    assert composite_scores.loc[composite_scores["symbol"] == "AAPL", "composite_score"].iloc[0] == pytest.approx(0.0)
    assert composite_scores.loc[composite_scores["symbol"] == "MSFT", "composite_score"].iloc[0] == pytest.approx(1.0)


def test_build_dynamic_signal_weights_downweights_and_deactivates() -> None:
    selected_signals_df = pd.DataFrame(
        [
            {"candidate_id": "momentum|5|1", "signal_family": "momentum", "lookback": 5, "horizon": 1},
            {"candidate_id": "momentum|10|1", "signal_family": "momentum", "lookback": 10, "horizon": 1},
        ]
    )
    daily_metrics_by_candidate = {
        ("momentum", 5, 1): pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="D"),
                "spearman_ic": [0.06, 0.05, 0.04, 0.03, 0.02],
            }
        ),
        ("momentum", 10, 1): pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="D"),
                "spearman_ic": [0.03, 0.00, -0.01, -0.03, -0.15],
            }
        ),
    }

    dynamic_weights, active_signals, deactivated_signals, lifecycle_report = build_dynamic_signal_weights(
        selected_signals_df,
        daily_metrics_by_candidate=daily_metrics_by_candidate,
        horizon=1,
        config=SignalLifecycleConfig(
            min_history=2,
            recent_quality_window=3,
            downweight_mean_rank_ic=0.02,
            deactivate_mean_rank_ic=-0.01,
        ),
    )

    assert not dynamic_weights.empty
    assert not active_signals.empty
    assert not deactivated_signals.empty
    latest_equal = dynamic_weights.loc[
        (dynamic_weights["timestamp"] == pd.Timestamp("2024-01-05"))
        & (dynamic_weights["weighting_scheme"] == "equal")
    ].sort_values("candidate_id")
    assert latest_equal["candidate_id"].tolist() == ["momentum|10|1", "momentum|5|1"]
    assert latest_equal["signal_weight"].tolist() == pytest.approx([0.0, 1.0])
    assert "weight_concentration_summary" in set(lifecycle_report["report_type"])


def test_build_dynamic_signal_weights_uses_only_prior_dates() -> None:
    selected_signals_df = pd.DataFrame(
        [
            {"candidate_id": "momentum|5|1", "signal_family": "momentum", "lookback": 5, "horizon": 1},
        ]
    )
    daily_metrics_by_candidate = {
        ("momentum", 5, 1): pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=4, freq="D"),
                "spearman_ic": [0.01, 0.01, 0.01, 1.00],
            }
        ),
    }

    dynamic_weights, _, _, _ = build_dynamic_signal_weights(
        selected_signals_df,
        daily_metrics_by_candidate=daily_metrics_by_candidate,
        horizon=1,
        config=SignalLifecycleConfig(
            min_history=1,
            recent_quality_window=2,
        ),
    )

    decision_row = dynamic_weights.loc[
        (dynamic_weights["timestamp"] == pd.Timestamp("2024-01-04"))
        & (dynamic_weights["weighting_scheme"] == "recent_quality")
    ].iloc[0]
    assert decision_row["recent_mean_rank_ic"] == pytest.approx(0.01)


def test_build_composite_scores_uses_dynamic_weights_by_date() -> None:
    selected_signals_df = pd.DataFrame(
        [
            {"candidate_id": "momentum|5|1", "signal_family": "momentum", "lookback": 5, "horizon": 1, "mean_spearman_ic": 0.05},
            {"candidate_id": "momentum|10|1", "signal_family": "momentum", "lookback": 10, "horizon": 1, "mean_spearman_ic": 0.04},
        ]
    )
    score_panel_by_candidate = {
        ("momentum", 5, 1): pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")],
                "symbol": ["AAPL", "MSFT"],
                "signal": [1.0, 3.0],
            }
        ),
        ("momentum", 10, 1): pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")],
                "symbol": ["AAPL", "MSFT"],
                "signal": [3.0, 1.0],
            }
        ),
    }
    dynamic_signal_weights_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")],
            "candidate_id": ["momentum|5|1", "momentum|10|1"],
            "signal_weight": [1.0, 0.0],
            "weighting_scheme": ["equal", "equal"],
        }
    )

    composite_scores = build_composite_scores(
        selected_signals_df,
        score_panel_by_candidate=score_panel_by_candidate,
        weighting_scheme="equal",
        quality_metric="mean_spearman_ic",
        dynamic_signal_weights_df=dynamic_signal_weights_df,
    )

    assert composite_scores.loc[composite_scores["symbol"] == "AAPL", "composite_score"].iloc[0] == pytest.approx(0.0)
    assert composite_scores.loc[composite_scores["symbol"] == "MSFT", "composite_score"].iloc[0] == pytest.approx(1.0)


def test_build_dynamic_signal_weights_handles_empty_inputs() -> None:
    dynamic_weights, active_signals, deactivated_signals, lifecycle_report = build_dynamic_signal_weights(
        pd.DataFrame(columns=["candidate_id", "signal_family", "lookback", "horizon"]),
        daily_metrics_by_candidate={},
        horizon=1,
    )

    assert dynamic_weights.empty
    assert active_signals.empty
    assert deactivated_signals.empty
    assert lifecycle_report.empty


def test_build_regime_labels_by_date_classifies_simple_regimes() -> None:
    asset_returns = pd.DataFrame(
        {
            "AAPL": [0.01, 0.01, -0.02, -0.03, 0.04, 0.05],
            "MSFT": [0.01, 0.00, -0.01, -0.04, 0.03, 0.06],
            "NVDA": [0.02, 0.01, -0.03, -0.05, 0.05, 0.08],
        },
        index=pd.date_range("2024-01-01", periods=6, freq="D"),
    )

    labels = build_regime_labels_by_date(
        asset_returns,
        config=RegimeConfig(
            enabled=True,
            volatility_window=2,
            trend_window=2,
            dispersion_window=2,
            min_history=2,
        ),
    )

    assert not labels.empty
    assert "regime_key" in labels.columns
    assert set(labels["volatility_regime"]) <= {"high_vol", "low_vol"}
    assert set(labels["trend_regime"]) <= {"uptrend", "downtrend"}
    assert set(labels["dispersion_regime"]) <= {"high_dispersion", "low_dispersion"}


def test_build_regime_aware_signal_weights_changes_by_regime_without_lookahead() -> None:
    base_dynamic_weights_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-03")],
            "candidate_id": ["momentum|5|1", "momentum|10|1"] * 3,
            "signal_family": ["momentum", "momentum"] * 3,
            "lookback": [5, 10] * 3,
            "horizon": [1, 1] * 3,
            "weighting_scheme": ["stability_decay", "stability_decay"] * 3,
            "lifecycle_status": ["active", "active"] * 3,
            "lifecycle_reason": ["none", "none"] * 3,
            "signal_weight": [0.5, 0.5] * 3,
        }
    )
    daily_metrics_by_candidate = {
        ("momentum", 5, 1): pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "spearman_ic": [0.05, 0.06, 0.07],
                "long_short_spread": [0.02, 0.03, 0.03],
            }
        ),
        ("momentum", 10, 1): pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "spearman_ic": [0.02, -0.06, -0.03],
                "long_short_spread": [0.01, -0.01, -0.01],
            }
        ),
    }
    regime_labels_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "regime_key": ["high_vol|downtrend|high_dispersion"] * 3,
            "volatility_regime": ["high_vol"] * 3,
            "trend_regime": ["downtrend"] * 3,
            "dispersion_regime": ["high_dispersion"] * 3,
        }
    )

    regime_weights, report = build_regime_aware_signal_weights(
        base_dynamic_weights_df,
        daily_metrics_by_candidate=daily_metrics_by_candidate,
        regime_labels_df=regime_labels_df,
        horizon=1,
        config=RegimeConfig(
            enabled=True,
            min_history=1,
            underweight_mean_rank_ic=0.01,
            exclude_mean_rank_ic=-0.01,
        ),
    )

    day_two = regime_weights.loc[regime_weights["timestamp"] == pd.Timestamp("2024-01-02")].sort_values("candidate_id")
    day_three = regime_weights.loc[regime_weights["timestamp"] == pd.Timestamp("2024-01-03")].sort_values("candidate_id")
    assert day_two["signal_weight"].sum() == pytest.approx(1.0)
    assert day_two["signal_weight"].iloc[1] > day_two["signal_weight"].iloc[0]
    assert day_three["signal_weight"].tolist() == pytest.approx([0.0, 1.0])
    assert day_two.loc[day_two["candidate_id"] == "momentum|10|1", "regime_mean_rank_ic"].iloc[0] == pytest.approx(0.02)
    assert "regime_frequency_summary" in set(report["report_type"])


def test_build_regime_aware_signal_weights_handles_empty_inputs() -> None:
    regime_weights, report = build_regime_aware_signal_weights(
        pd.DataFrame(),
        daily_metrics_by_candidate={},
        regime_labels_df=pd.DataFrame(),
        horizon=1,
    )

    assert regime_weights.empty
    assert report.empty


def test_build_composite_portfolio_weights_selects_top_n_and_normalizes_weights() -> None:
    composite_scores_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01")] * 3,
            "symbol": ["AAPL", "MSFT", "NVDA"],
            "horizon": [1, 1, 1],
            "weighting_scheme": ["equal"] * 3,
            "composite_score": [0.1, 0.3, 0.2],
            "component_count": [2, 2, 2],
            "selected_signal_count": [2, 2, 2],
        }
    )

    weights_df = build_composite_portfolio_weights(
        composite_scores_df,
        config=CompositePortfolioConfig(modes=("long_only_top_n",), top_n=2),
    )

    assert set(weights_df["symbol"]) == {"MSFT", "NVDA"}
    assert weights_df["weight"].sum() == pytest.approx(1.0)
    assert (weights_df["weight"] > 0).all()


def test_build_long_short_quantile_weights_balances_long_and_short_legs() -> None:
    scores_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01")] * 4,
            "symbol": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "score": [0.4, 0.3, 0.2, 0.1],
        }
    )

    weights_df = build_long_short_quantile_weights(
        scores_df,
        long_quantile=0.25,
        short_quantile=0.25,
        gross_target=1.0,
        net_target=0.0,
    )

    assert set(weights_df["symbol"]) == {"AAPL", "AMZN"}
    assert weights_df["weight"].abs().sum() == pytest.approx(1.0)
    assert weights_df["weight"].sum() == pytest.approx(0.0)


def test_composite_portfolio_backtest_turnover_and_cost_reduce_returns() -> None:
    composite_scores_df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "horizon": [1, 1, 1, 1],
            "weighting_scheme": ["equal"] * 4,
            "composite_score": [1.0, 0.0, 0.0, 1.0],
            "component_count": [1, 1, 1, 1],
            "selected_signal_count": [1, 1, 1, 1],
        }
    )
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [100.0, 110.0, 121.0],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [100.0, 90.0, 81.0],
            }
        ),
    }

    no_cost_returns, _, _, _ = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(
            modes=("long_only_top_n",),
            top_n=1,
            commission=0.0,
        ),
    )
    cost_returns, _, _, _ = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(
            modes=("long_only_top_n",),
            top_n=1,
            commission=0.01,
        ),
    )

    assert (no_cost_returns["turnover"] >= 0.0).all()
    assert cost_returns["portfolio_return_net"].sum() < no_cost_returns["portfolio_return_net"].sum()


def test_apply_liquidity_filters_excludes_illiquid_names() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "symbol": ["AAPL", "MSFT"],
            "weight": [0.5, 0.5],
            "horizon": [1, 1],
            "weighting_scheme": ["equal", "equal"],
            "portfolio_mode": ["long_only_top_n", "long_only_top_n"],
        }
    )
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-01")],
                "close": [10.0],
                "volume": [1_000.0],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-01")],
                "close": [2.0],
                "volume": [10.0],
            }
        ),
    }

    filtered_weights, exclusions_df, low_liquidity_fraction = apply_liquidity_filters(
        weights_df,
        liquidity_panel=build_liquidity_panel(symbol_data),
        config=CompositePortfolioConfig(
            min_price=5.0,
            min_volume=100.0,
        ),
    )

    assert filtered_weights["symbol"].tolist() == ["AAPL"]
    assert filtered_weights["weight"].iloc[0] == pytest.approx(0.5)
    assert exclusions_df["symbol"].tolist() == ["MSFT"]
    assert "min_price" in exclusions_df["reason"].iloc[0]
    assert "min_volume" in exclusions_df["reason"].iloc[0]
    assert low_liquidity_fraction["excluded_weight_fraction"].iloc[0] == pytest.approx(0.5)


def test_estimate_capacity_reduces_with_tighter_participation() -> None:
    weights_df = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "symbol": ["AAPL", "MSFT"],
            "weight": [0.5, 0.5],
            "horizon": [1, 1],
            "weighting_scheme": ["equal", "equal"],
            "portfolio_mode": ["long_only_top_n", "long_only_top_n"],
        }
    )
    liquidity_panel = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "symbol": ["AAPL", "MSFT"],
            "close": [10.0, 20.0],
            "volume": [100.0, 100.0],
            "dollar_volume": [1_000.0, 2_000.0],
            "avg_dollar_volume": [1_000.0, 2_000.0],
        }
    )

    loose_capacity = estimate_capacity(
        weights_df,
        liquidity_panel=liquidity_panel,
        config=CompositePortfolioConfig(max_adv_participation=0.10),
    )
    tight_capacity = estimate_capacity(
        weights_df,
        liquidity_panel=liquidity_panel,
        config=CompositePortfolioConfig(max_adv_participation=0.02),
    )

    assert tight_capacity["capacity_multiple"].iloc[0] < loose_capacity["capacity_multiple"].iloc[0]


def test_composite_portfolio_backtest_uses_next_bar_execution_timing() -> None:
    composite_scores_df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "horizon": [1, 1, 1, 1],
            "weighting_scheme": ["equal"] * 4,
            "composite_score": [1.0, 0.0, 0.0, 1.0],
            "component_count": [1, 1, 1, 1],
            "selected_signal_count": [1, 1, 1, 1],
        }
    )
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [100.0, 110.0, 99.0],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [100.0, 100.0, 120.0],
            }
        ),
    }

    portfolio_returns_df, _, portfolio_weights_df, _ = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(
            modes=("long_only_top_n",),
            top_n=1,
            commission=0.0,
        ),
    )

    first_return = portfolio_returns_df.sort_values("timestamp").iloc[0]
    second_weight_date = portfolio_weights_df.sort_values("timestamp")["timestamp"].iloc[1]

    assert first_return["portfolio_return_net"] == pytest.approx(0.0)
    assert second_weight_date == pd.Timestamp("2024-01-02")


def test_run_stress_tests_reduces_performance_vs_baseline() -> None:
    composite_scores_df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-03"),
            ],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT", "AAPL", "MSFT"],
            "horizon": [1, 1, 1, 1, 1, 1],
            "weighting_scheme": ["equal"] * 6,
            "composite_score": [1.0, 0.0, 1.0, 0.0, 1.0, 0.0],
            "component_count": [1] * 6,
            "selected_signal_count": [1] * 6,
        }
    )
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=4, freq="D"),
                "close": [100.0, 110.0, 121.0, 133.1],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=4, freq="D"),
                "close": [100.0, 90.0, 81.0, 72.9],
            }
        ),
    }

    stress_results = run_stress_tests(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(modes=("long_only_top_n",), top_n=1),
    )

    baseline = stress_results.loc[stress_results["stress_test"] == "baseline"]
    shuffled = stress_results.loc[stress_results["stress_test"] == "shuffle_by_date"]
    lagged = stress_results.loc[stress_results["stress_test"] == "lag_plus_one"]

    assert not baseline.empty
    assert not shuffled.empty
    assert not lagged.empty
    assert baseline["portfolio_total_return"].max() >= shuffled["portfolio_total_return"].max()
    assert baseline["portfolio_total_return"].max() >= lagged["portfolio_total_return"].max()


def test_composite_portfolio_backtest_slippage_reduces_returns() -> None:
    composite_scores_df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-02"),
            ],
            "symbol": ["AAPL", "MSFT", "AAPL", "MSFT"],
            "horizon": [1, 1, 1, 1],
            "weighting_scheme": ["equal"] * 4,
            "composite_score": [1.0, 0.0, 0.0, 1.0],
            "component_count": [1, 1, 1, 1],
            "selected_signal_count": [1, 1, 1, 1],
        }
    )
    symbol_data = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [10.0, 11.0, 12.0],
                "volume": [5.0, 5.0, 5.0],
            }
        ),
        "MSFT": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [10.0, 9.0, 8.0],
                "volume": [5.0, 5.0, 5.0],
            }
        ),
    }

    _, base_metrics, _, _ = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(
            modes=("long_only_top_n",),
            top_n=1,
            commission=0.0,
            slippage_bps_per_turnover=0.0,
            slippage_bps_per_adv=0.0,
        ),
    )
    _, _, _, diagnostics = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=CompositePortfolioConfig(
            modes=("long_only_top_n",),
            top_n=1,
            commission=0.0,
            slippage_bps_per_turnover=50.0,
            slippage_bps_per_adv=500.0,
        ),
    )

    liquidity_filtered_metrics = diagnostics["liquidity_filtered_portfolio_metrics"]
    assert not liquidity_filtered_metrics.empty
    assert (
        liquidity_filtered_metrics["portfolio_total_return"].iloc[0]
        < base_metrics["portfolio_total_return"].iloc[0]
    )
    slippage_costs = diagnostics["estimated_slippage_costs"]
    assert not slippage_costs.empty
    assert slippage_costs["estimated_slippage_cost"].sum() > 0.0


def test_run_alpha_research_writes_leaderboard_and_fold_results(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    aapl = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=30, freq="D"),
            "symbol": ["AAPL"] * 30,
            "close": [100.0 + i for i in range(30)],
        }
    )
    msft = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=30, freq="D"),
            "symbol": ["MSFT"] * 30,
            "close": [200.0 + 2 * i for i in range(30)],
        }
    )

    aapl.to_parquet(feature_dir / "AAPL.parquet", index=False)
    msft.to_parquet(feature_dir / "MSFT.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 5],
        horizons=[1, 5],
        min_rows=10,
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir=output_dir,
        train_size=10,
        test_size=5,
        step_size=5,
    )

    leaderboard_path = Path(result["leaderboard_path"])
    fold_results_path = Path(result["fold_results_path"])
    promoted_signals_path = Path(result["promoted_signals_path"])
    redundancy_report_path = Path(result["redundancy_report_path"])
    redundancy_path = Path(result["redundancy_path"])
    composite_scores_path = Path(result["composite_scores_path"])
    composite_leaderboard_path = Path(result["composite_leaderboard_path"])
    dynamic_signal_weights_path = Path(result["dynamic_signal_weights_path"])
    active_signals_by_date_path = Path(result["active_signals_by_date_path"])
    deactivated_signals_path = Path(result["deactivated_signals_path"])
    signal_lifecycle_report_path = Path(result["signal_lifecycle_report_path"])
    regime_labels_by_date_path = Path(result["regime_labels_by_date_path"])
    signal_performance_by_regime_path = Path(result["signal_performance_by_regime_path"])
    signal_performance_by_sub_universe_path = Path(result["signal_performance_by_sub_universe_path"])
    signal_performance_by_benchmark_context_path = Path(result["signal_performance_by_benchmark_context_path"])
    signal_family_summary_path = Path(result["signal_family_summary_path"])
    regime_aware_signal_weights_path = Path(result["regime_aware_signal_weights_path"])
    regime_selection_report_path = Path(result["regime_selection_report_path"])
    composite_diagnostics_path = Path(result["composite_diagnostics_path"])
    portfolio_returns_path = Path(result["portfolio_returns_path"])
    portfolio_metrics_path = Path(result["portfolio_metrics_path"])
    portfolio_weights_path = Path(result["portfolio_weights_path"])
    portfolio_diagnostics_path = Path(result["portfolio_diagnostics_path"])
    robustness_report_path = Path(result["robustness_report_path"])
    regime_performance_path = Path(result["regime_performance_path"])
    stress_test_results_path = Path(result["stress_test_results_path"])
    implementability_report_path = Path(result["implementability_report_path"])
    liquidity_filtered_metrics_path = Path(result["liquidity_filtered_portfolio_metrics_path"])
    capacity_scenarios_path = Path(result["capacity_scenarios_path"])

    assert leaderboard_path.exists()
    assert fold_results_path.exists()
    assert promoted_signals_path.exists()
    assert redundancy_report_path.exists()
    assert redundancy_path.exists()
    assert composite_scores_path.exists()
    assert composite_leaderboard_path.exists()
    assert dynamic_signal_weights_path.exists()
    assert active_signals_by_date_path.exists()
    assert deactivated_signals_path.exists()
    assert signal_lifecycle_report_path.exists()
    assert regime_labels_by_date_path.exists()
    assert signal_performance_by_regime_path.exists()
    assert signal_performance_by_sub_universe_path.exists()
    assert signal_performance_by_benchmark_context_path.exists()
    assert signal_family_summary_path.exists()
    assert regime_aware_signal_weights_path.exists()
    assert regime_selection_report_path.exists()
    assert composite_diagnostics_path.exists()
    assert portfolio_returns_path.exists()
    assert portfolio_metrics_path.exists()
    assert portfolio_weights_path.exists()
    assert portfolio_diagnostics_path.exists()
    assert robustness_report_path.exists()
    assert regime_performance_path.exists()
    assert stress_test_results_path.exists()
    assert implementability_report_path.exists()
    assert liquidity_filtered_metrics_path.exists()
    assert capacity_scenarios_path.exists()
    assert (output_dir / "leaderboard.parquet").exists()
    assert (output_dir / "fold_results.parquet").exists()
    assert (output_dir / "promoted_signals.parquet").exists()
    assert (output_dir / "redundancy_report.parquet").exists()
    assert (output_dir / "redundancy_diagnostics.parquet").exists()
    assert (output_dir / "composite_scores.parquet").exists()
    assert (output_dir / "composite_leaderboard.parquet").exists()
    assert (output_dir / "dynamic_signal_weights.parquet").exists()
    assert (output_dir / "active_signals_by_date.parquet").exists()
    assert (output_dir / "deactivated_signals.parquet").exists()
    assert (output_dir / "signal_lifecycle_report.parquet").exists()
    assert (output_dir / "regime_labels_by_date.parquet").exists()
    assert (output_dir / "signal_performance_by_regime.parquet").exists()
    assert (output_dir / "signal_performance_by_sub_universe.parquet").exists()
    assert (output_dir / "signal_performance_by_benchmark_context.parquet").exists()
    assert (output_dir / "signal_family_summary.parquet").exists()
    assert (output_dir / "regime_aware_signal_weights.parquet").exists()
    assert (output_dir / "regime_selection_report.parquet").exists()
    assert (output_dir / "composite_diagnostics.json").exists()
    assert (output_dir / "portfolio_returns.parquet").exists()
    assert (output_dir / "portfolio_metrics.parquet").exists()
    assert (output_dir / "portfolio_weights.parquet").exists()
    assert (output_dir / "portfolio_diagnostics.json").exists()
    assert (output_dir / "robustness_report.parquet").exists()
    assert (output_dir / "regime_performance.parquet").exists()
    assert (output_dir / "stress_test_results.parquet").exists()
    assert (output_dir / "implementability_report.parquet").exists()
    assert (output_dir / "liquidity_filtered_portfolio_metrics.parquet").exists()
    assert (output_dir / "capacity_scenarios.parquet").exists()
    assert (output_dir / "signal_diagnostics.json").exists()

    leaderboard_df = pd.read_csv(leaderboard_path)
    fold_results_df = pd.read_csv(fold_results_path)
    promoted_signals_df = pd.read_csv(promoted_signals_path)
    redundancy_df = pd.read_csv(redundancy_path)
    composite_scores_df = pd.read_csv(composite_scores_path)
    composite_leaderboard_df = pd.read_csv(composite_leaderboard_path)
    dynamic_signal_weights_df = pd.read_csv(dynamic_signal_weights_path)
    active_signals_by_date_df = pd.read_csv(active_signals_by_date_path)
    deactivated_signals_df = pd.read_csv(deactivated_signals_path)
    signal_lifecycle_report_df = pd.read_csv(signal_lifecycle_report_path)
    regime_labels_by_date_df = pd.read_csv(regime_labels_by_date_path)
    signal_performance_by_regime_df = pd.read_csv(signal_performance_by_regime_path)
    signal_performance_by_sub_universe_df = pd.read_csv(signal_performance_by_sub_universe_path)
    signal_performance_by_benchmark_context_df = pd.read_csv(signal_performance_by_benchmark_context_path)
    regime_aware_signal_weights_df = pd.read_csv(regime_aware_signal_weights_path)
    regime_selection_report_df = pd.read_csv(regime_selection_report_path)
    portfolio_returns_df = pd.read_csv(portfolio_returns_path)
    portfolio_metrics_df = pd.read_csv(portfolio_metrics_path)
    portfolio_weights_df = pd.read_csv(portfolio_weights_path)
    robustness_report_df = pd.read_csv(robustness_report_path)
    regime_performance_df = pd.read_csv(regime_performance_path)
    stress_test_results_df = pd.read_csv(stress_test_results_path)
    implementability_report_df = pd.read_csv(implementability_report_path)
    liquidity_filtered_metrics_df = pd.read_csv(liquidity_filtered_metrics_path)
    capacity_scenarios_df = pd.read_csv(capacity_scenarios_path)

    assert not leaderboard_df.empty
    assert not fold_results_df.empty
    assert promoted_signals_df.empty
    assert composite_scores_df.empty
    assert composite_leaderboard_df.empty
    assert dynamic_signal_weights_df.empty
    assert active_signals_by_date_df.empty
    assert deactivated_signals_df.empty
    assert signal_lifecycle_report_df.empty
    assert not regime_labels_by_date_df.empty
    assert signal_performance_by_regime_df.empty
    assert signal_performance_by_sub_universe_df.empty
    assert signal_performance_by_benchmark_context_df.empty
    assert regime_aware_signal_weights_df.empty
    assert regime_selection_report_df.empty
    assert portfolio_returns_df.empty
    assert portfolio_metrics_df.empty
    assert portfolio_weights_df.empty
    assert robustness_report_df.empty
    assert regime_performance_df.empty
    assert stress_test_results_df.empty
    assert implementability_report_df.empty
    assert liquidity_filtered_metrics_df.empty
    assert capacity_scenarios_df.empty
    assert "performance_corr" in redundancy_df.columns

    assert "signal_family" in leaderboard_df.columns
    assert "lookback" in leaderboard_df.columns
    assert "horizon" in leaderboard_df.columns
    assert "mean_spearman_ic" in leaderboard_df.columns
    assert "mean_long_short_spread" in leaderboard_df.columns
    assert "worst_fold_spearman_ic" in leaderboard_df.columns
    assert "rejection_reason" in leaderboard_df.columns
    assert "promotion_status" in leaderboard_df.columns

    assert "symbols_evaluated" in fold_results_df.columns
    assert "dates_evaluated" in fold_results_df.columns
    assert "pearson_ic" in fold_results_df.columns
    assert "spearman_ic" in fold_results_df.columns
    assert "long_short_spread" in fold_results_df.columns
    assert fold_results_df["symbols_evaluated"].max() == pytest.approx(2.0)


def test_run_alpha_research_supports_multiple_signal_families_in_one_run(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    fundamentals_path = tmp_path / "daily_fundamental_features.parquet"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=120, freq="D")
    for symbol, drift in {"AAPL": 0.004, "MSFT": 0.002, "NVDA": 0.006}.items():
        closes = [100.0]
        for _ in range(len(timestamps) - 1):
            closes.append(closes[-1] * (1.0 + drift))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
                "volume": [1_000_000 + idx * 5_000 for idx in range(len(timestamps))],
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    fundamentals_frames: list[pd.DataFrame] = []
    for symbol, raw_value, rank_pct, zscore in (
        ("AAPL", 0.08, 2.0 / 3.0, 0.0),
        ("MSFT", 0.04, 1.0 / 3.0, -1.0),
        ("NVDA", 0.12, 1.0, 1.0),
    ):
        fundamentals_frames.append(
            pd.DataFrame(
                {
                    "timestamp": timestamps,
                    "symbol": [symbol] * len(timestamps),
                    "sector": ["Technology"] * len(timestamps),
                    "industry": ["Software"] * len(timestamps),
                    "earnings_yield": [raw_value] * len(timestamps),
                    "book_to_market": [raw_value * 0.8] * len(timestamps),
                    "sales_to_price": [raw_value * 0.6] * len(timestamps),
                    "free_cash_flow_yield": [raw_value * 0.7] * len(timestamps),
                    "fundamental_value_score": [raw_value] * len(timestamps),
                    "sector_neutral_value_score": [zscore] * len(timestamps),
                    "earnings_yield_rank_pct": [rank_pct] * len(timestamps),
                    "earnings_yield_zscore": [zscore] * len(timestamps),
                    "book_to_market_rank_pct": [rank_pct] * len(timestamps),
                    "book_to_market_zscore": [zscore] * len(timestamps),
                    "sales_to_price_rank_pct": [rank_pct] * len(timestamps),
                    "sales_to_price_zscore": [zscore] * len(timestamps),
                    "free_cash_flow_yield_rank_pct": [rank_pct] * len(timestamps),
                    "free_cash_flow_yield_zscore": [zscore] * len(timestamps),
                    "fundamental_value_score_rank_pct": [rank_pct] * len(timestamps),
                    "fundamental_value_score_zscore": [zscore] * len(timestamps),
                    "sector_neutral_value_score_rank_pct": [rank_pct] * len(timestamps),
                    "sector_neutral_value_score_zscore": [zscore] * len(timestamps),
                    "days_since_available": list(range(len(timestamps))),
                }
            )
        )
    pd.concat(fundamentals_frames, ignore_index=True).to_parquet(fundamentals_path, index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        signal_families=["momentum", "fundamental_value"],
        lookbacks=[5],
        horizons=[1],
        min_rows=30,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=40,
        test_size=20,
        step_size=20,
        fundamentals_enabled=True,
        fundamentals_daily_features_path=fundamentals_path,
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    signal_family_summary_df = pd.read_csv(result["signal_family_summary_path"])
    research_registry_df = pd.read_csv(result["research_registry_path"])
    promotion_candidates_df = pd.read_csv(result["promotion_candidates_path"])
    diagnostics = json.loads(Path(result["signal_diagnostics_path"]).read_text(encoding="utf-8"))

    assert set(leaderboard_df["signal_family"]) == {"momentum", "fundamental_value"}
    assert set(signal_family_summary_df["signal_family"]) == {"fundamental_value", "momentum"}
    assert set(research_registry_df["signal_family"]) == {"momentum", "fundamental_value"}
    assert set(promotion_candidates_df.columns) >= {"candidate_id", "signal_family", "promotion_status"}
    assert diagnostics["signal_families"] == ["momentum", "fundamental_value"]
    assert diagnostics["generated_candidate_count_by_family"]["momentum"] > 0
    assert diagnostics["generated_candidate_count_by_family"]["fundamental_value"] > 0


def test_run_alpha_research_broad_candidate_grid_emits_variant_identity(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=60, freq="D")
    for symbol, base_price, daily_return, volume_bias in (
        ("AAPL", 100.0, 0.006, 1.1),
        ("MSFT", 120.0, 0.004, 0.9),
        ("NVDA", 90.0, 0.009, 1.4),
    ):
        closes = [base_price]
        for _ in range(len(timestamps) - 1):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
                "volume": [1_000_000 * volume_bias * (1.0 + 0.01 * idx) for idx in range(len(timestamps))],
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="cross_sectional_momentum",
        lookbacks=[5, 10],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        candidate_grid_preset="broad_v1",
        signal_composition_preset="composite_v1",
        max_variants_per_family=4,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        equity_context_enabled=True,
        equity_context_include_volume=True,
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    fold_results_df = pd.read_csv(result["fold_results_path"])
    diagnostics = json.loads(Path(result["signal_diagnostics_path"]).read_text(encoding="utf-8"))

    assert {"candidate_id", "candidate_name", "signal_variant", "variant_parameters_json"} <= set(
        leaderboard_df.columns
    )
    assert {"candidate_id", "candidate_name", "signal_variant"} <= set(fold_results_df.columns)
    assert leaderboard_df["signal_variant"].nunique() == 4
    assert len(leaderboard_df) == 8
    assert diagnostics["candidate_grid_preset"] == "broad_v1"
    assert diagnostics["signal_composition_preset"] == "composite_v1"
    assert diagnostics["signal_composition"]["preset"] == "composite_v1"
    assert diagnostics["generated_candidate_count"] == 8
    signal_family_summary_df = pd.read_csv(result["signal_family_summary_path"])
    assert "composite_candidate_count" in signal_family_summary_df.columns
    assert signal_family_summary_df.loc[0, "candidate_count"] == 8


def test_run_alpha_research_emits_sub_universe_and_benchmark_context_artifacts(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    daily_returns = {
        "AAPL": 0.010,
        "MSFT": 0.015,
        "NVDA": 0.020,
        "AMD": 0.018,
    }
    sub_universe_map = {
        "AAPL": "mega_cap",
        "MSFT": "mega_cap",
        "NVDA": "growth_leaders",
        "AMD": "growth_leaders",
    }

    for symbol, daily_return in daily_returns.items():
        closes = [100.0]
        for _ in range(79):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
                "sub_universe_id": [sub_universe_map[symbol]] * len(timestamps),
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA", "AMD"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 2],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        regime_aware_enabled=True,
        regime_min_history=1,
        equity_context_enabled=True,
    )

    signal_performance_by_sub_universe_df = pd.read_csv(
        result["signal_performance_by_sub_universe_path"]
    )
    signal_performance_by_benchmark_context_df = pd.read_csv(
        result["signal_performance_by_benchmark_context_path"]
    )

    assert not signal_performance_by_sub_universe_df.empty
    assert not signal_performance_by_benchmark_context_df.empty
    assert {"candidate_id", "sub_universe_id", "sample_size", "coverage_ratio"} <= set(
        signal_performance_by_sub_universe_df.columns
    )
    assert {
        "candidate_id",
        "benchmark_context_label",
        "mean_relative_return",
        "mean_market_return",
        "context_source",
        "context_status",
    } <= set(signal_performance_by_benchmark_context_df.columns)
    assert set(signal_performance_by_sub_universe_df["sub_universe_id"]) == {
        "mega_cap",
        "growth_leaders",
    }
    assert set(signal_performance_by_sub_universe_df["context_status"]) == {"confirmed"}
    assert set(signal_performance_by_benchmark_context_df["context_status"]) == {"confirmed"}
    assert set(signal_performance_by_benchmark_context_df["context_source"]) <= {
        "benchmark_context_label_1",
        "benchmark_context_label_2",
    }
    assert signal_performance_by_benchmark_context_df["benchmark_context_label"].str.len().gt(0).all()
    assert signal_performance_by_benchmark_context_df["coverage_ratio"].gt(0.0).all()


def test_run_alpha_research_persists_context_feature_labels_from_metadata_and_equity_context(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    metadata_dir = tmp_path / "metadata"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"symbol": "AAPL", "sub_universe_id": "liquid_trend"},
            {"symbol": "MSFT", "sub_universe_id": "liquid_trend"},
            {"symbol": "NVDA", "sub_universe_id": "growth_leaders"},
            {"symbol": "AMD", "sub_universe_id": "growth_leaders"},
        ]
    ).to_csv(metadata_dir / "sub_universe_snapshot.csv", index=False)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    daily_returns = {
        "AAPL": 0.010,
        "MSFT": 0.015,
        "NVDA": 0.020,
        "AMD": 0.018,
    }
    for symbol, daily_return in daily_returns.items():
        closes = [100.0]
        for _ in range(79):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA", "AMD"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 2],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        equity_context_enabled=True,
    )

    context_features_dir = Path(result["context_features_dir"])
    coverage_payload = json.loads(
        Path(result["research_context_coverage_path"]).read_text(encoding="utf-8")
    )
    context_frame = pd.read_parquet(context_features_dir / "AAPL.parquet")

    assert context_features_dir.exists()
    assert result["research_context_coverage_path"]
    assert context_frame["sub_universe_id"].dropna().eq("liquid_trend").all()
    assert context_frame["sub_universe_context_source"].dropna().eq("context_artifact").all()
    assert context_frame["benchmark_context_label_1"].notna().any()
    assert context_frame["benchmark_context_label_2"].notna().any()
    assert context_frame["benchmark_context_source_1"].dropna().eq("equity_context_features").all()
    assert coverage_payload["symbols_with_sub_universe_labels"] == 4
    assert coverage_payload["symbols_with_derived_benchmark_labels"] == 4


def test_run_alpha_research_prefers_explicit_benchmark_context_labels_when_present(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    explicit_labels = {
        "AAPL": "explicit_risk_on",
        "MSFT": "explicit_risk_on",
        "NVDA": "explicit_risk_off",
        "AMD": "explicit_risk_off",
    }
    daily_returns = {
        "AAPL": 0.010,
        "MSFT": 0.015,
        "NVDA": 0.020,
        "AMD": 0.018,
    }
    for symbol, daily_return in daily_returns.items():
        closes = [100.0]
        for _ in range(79):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
                "benchmark_context_label_1": [explicit_labels[symbol]] * len(timestamps),
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA", "AMD"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        equity_context_enabled=True,
    )

    signal_performance_by_benchmark_context_df = pd.read_csv(
        result["signal_performance_by_benchmark_context_path"]
    )
    context_frame = pd.read_parquet(Path(result["context_features_dir"]) / "AAPL.parquet")

    assert not signal_performance_by_benchmark_context_df.empty
    assert set(signal_performance_by_benchmark_context_df["context_status"]) == {"confirmed"}
    assert set(signal_performance_by_benchmark_context_df["context_source"]) == {
        "benchmark_context_label_1"
    }
    assert set(signal_performance_by_benchmark_context_df["benchmark_context_label"]) == {
        "explicit_risk_on",
        "explicit_risk_off",
    }
    assert context_frame["benchmark_context_label_1"].dropna().eq("explicit_risk_on").all()
    assert context_frame["benchmark_context_source_1"].dropna().eq("benchmark_context_label_1").all()


def test_add_forward_return_labels_does_not_use_current_bar_as_future_return() -> None:
    df = pd.DataFrame({"close": [100.0, 110.0, 121.0]})
    result = add_forward_return_labels(df, horizons=[1])

    assert result.loc[0, "fwd_return_1d"] != 0.0
    assert result.loc[0, "fwd_return_1d"] == pytest.approx(0.10)


def test_apply_promotion_rules_adds_expected_rejection_reasons() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "horizon": 1,
                "symbols_tested": 3.0,
                "folds_tested": 3,
                "mean_dates_evaluated": 10.0,
                "mean_spearman_ic": 0.05,
                "mean_turnover": 0.2,
                "worst_fold_spearman_ic": 0.01,
                "total_obs": 300.0,
            },
            {
                "signal_family": "momentum",
                "lookback": 10,
                "horizon": 1,
                "symbols_tested": 1.0,
                "folds_tested": 1,
                "mean_dates_evaluated": 2.0,
                "mean_spearman_ic": 0.0,
                "mean_turnover": 0.9,
                "worst_fold_spearman_ic": -0.2,
                "total_obs": 20.0,
            },
        ]
    )

    result = apply_promotion_rules(leaderboard_df)

    assert result.loc[0, "promotion_status"] == "promote"
    assert result.loc[0, "rejection_reason"] == "none"
    assert result.loc[1, "promotion_status"] == "reject"
    assert "low_mean_rank_ic" in result.loc[1, "rejection_reason"]
    assert "insufficient_symbols" in result.loc[1, "rejection_reason"]
    assert "insufficient_folds" in result.loc[1, "rejection_reason"]
    assert "insufficient_dates" in result.loc[1, "rejection_reason"]
    assert "insufficient_observations" in result.loc[1, "rejection_reason"]
    assert "high_turnover" in result.loc[1, "rejection_reason"]
    assert "weak_worst_fold_rank_ic" in result.loc[1, "rejection_reason"]


def test_run_alpha_research_adds_promotion_and_redundancy_outputs(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    daily_returns = {
        "AAPL": 0.010,
        "MSFT": 0.015,
        "NVDA": 0.020,
    }

    for symbol, daily_return in daily_returns.items():
        closes = [100.0]
        for _ in range(79):
            closes.append(closes[-1] * (1.0 + daily_return))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 2],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        regime_aware_enabled=True,
        regime_min_history=1,
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    promoted_signals_df = pd.read_csv(result["promoted_signals_path"])
    redundancy_report_df = pd.read_csv(result["redundancy_report_path"])
    redundancy_df = pd.read_csv(result["redundancy_path"])
    composite_scores_df = pd.read_csv(result["composite_scores_path"])
    composite_leaderboard_df = pd.read_csv(result["composite_leaderboard_path"])
    signal_family_summary_df = pd.read_csv(result["signal_family_summary_path"])
    dynamic_signal_weights_df = pd.read_csv(result["dynamic_signal_weights_path"])
    active_signals_by_date_df = pd.read_csv(result["active_signals_by_date_path"])
    deactivated_signals_df = pd.read_csv(result["deactivated_signals_path"])
    signal_lifecycle_report_df = pd.read_csv(result["signal_lifecycle_report_path"])
    regime_labels_by_date_df = pd.read_csv(result["regime_labels_by_date_path"])
    signal_performance_by_regime_df = pd.read_csv(result["signal_performance_by_regime_path"])
    signal_performance_by_sub_universe_df = pd.read_csv(
        result["signal_performance_by_sub_universe_path"]
    )
    signal_performance_by_benchmark_context_df = pd.read_csv(
        result["signal_performance_by_benchmark_context_path"]
    )
    regime_aware_signal_weights_df = pd.read_csv(result["regime_aware_signal_weights_path"])
    regime_selection_report_df = pd.read_csv(result["regime_selection_report_path"])
    portfolio_returns_df = pd.read_csv(result["portfolio_returns_path"])
    portfolio_metrics_df = pd.read_csv(result["portfolio_metrics_path"])
    portfolio_weights_df = pd.read_csv(result["portfolio_weights_path"])
    robustness_report_df = pd.read_csv(result["robustness_report_path"])
    regime_performance_df = pd.read_csv(result["regime_performance_path"])
    stress_test_results_df = pd.read_csv(result["stress_test_results_path"])
    implementability_report_df = pd.read_csv(result["implementability_report_path"])
    liquidity_filtered_metrics_df = pd.read_csv(result["liquidity_filtered_portfolio_metrics_path"])
    capacity_scenarios_df = pd.read_csv(result["capacity_scenarios_path"])

    assert set(leaderboard_df["promotion_status"]) == {"promote"}
    assert set(leaderboard_df["rejection_reason"]) == {"none"}
    assert len(promoted_signals_df) == 2
    assert set(promoted_signals_df["promotion_status"]) == {"promote"}
    assert len(redundancy_report_df) == 1
    assert len(redundancy_df) == 1
    assert not composite_scores_df.empty
    assert not composite_leaderboard_df.empty
    assert not signal_family_summary_df.empty
    assert not dynamic_signal_weights_df.empty
    assert not active_signals_by_date_df.empty
    assert not signal_lifecycle_report_df.empty
    assert not regime_labels_by_date_df.empty
    assert not signal_performance_by_regime_df.empty
    assert signal_performance_by_sub_universe_df.empty
    assert signal_performance_by_benchmark_context_df.empty
    assert not regime_aware_signal_weights_df.empty
    assert not regime_selection_report_df.empty
    assert not portfolio_returns_df.empty
    assert not portfolio_metrics_df.empty
    assert not portfolio_weights_df.empty
    assert not robustness_report_df.empty
    assert not regime_performance_df.empty
    assert not stress_test_results_df.empty
    assert not implementability_report_df.empty
    assert not liquidity_filtered_metrics_df.empty
    assert not capacity_scenarios_df.empty
    assert set(composite_scores_df["weighting_scheme"]) == {"equal", "recent_quality", "stability_decay", "regime_aware"}
    assert set(dynamic_signal_weights_df["weighting_scheme"]) == {"equal", "recent_quality", "stability_decay"}
    assert set(regime_aware_signal_weights_df["weighting_scheme"]) == {"regime_aware"}
    assert set(signal_lifecycle_report_df["report_type"]) == {"signal_summary", "weight_concentration_summary"}
    assert {"signal_regime_weight_summary", "regime_frequency_summary"} <= set(regime_selection_report_df["report_type"])
    assert deactivated_signals_df.empty or "lifecycle_reason" in deactivated_signals_df.columns
    assert set(portfolio_weights_df["portfolio_mode"]) == {
        "long_only_top_n",
        "long_short_quantile",
    }
    assert "portfolio_max_drawdown_duration" in robustness_report_df.columns
    assert set(stress_test_results_df["stress_test"]) == {
        "baseline",
        "shuffle_by_date",
        "lag_plus_one",
    }
    assert "return_drag" in implementability_report_df.columns
    assert "mean_capacity_multiple" in implementability_report_df.columns
    assert "excluded_weight_fraction" in liquidity_filtered_metrics_df.columns
    assert set(capacity_scenarios_df["scenario"]) == {"tight", "base", "loose"}
    assert redundancy_df.loc[0, "score_corr"] > 0.999
    assert redundancy_df.loc[0, "performance_corr"] > 0.999
    assert redundancy_df.loc[0, "overlap_dates"] > 0


def test_run_alpha_research_handles_empty_outputs_and_edge_case_rejections(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "alpha_outputs"
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)

    result = run_alpha_research(
        symbols=["MISSING"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[5],
        horizons=[1],
        min_rows=20,
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir=output_dir,
        train_size=20,
        test_size=5,
        step_size=5,
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    promoted_signals_df = pd.read_csv(result["promoted_signals_path"])
    redundancy_report_df = pd.read_csv(result["redundancy_report_path"])
    redundancy_df = pd.read_csv(result["redundancy_path"])
    composite_scores_df = pd.read_csv(result["composite_scores_path"])
    composite_leaderboard_df = pd.read_csv(result["composite_leaderboard_path"])
    dynamic_signal_weights_df = pd.read_csv(result["dynamic_signal_weights_path"])
    active_signals_by_date_df = pd.read_csv(result["active_signals_by_date_path"])
    deactivated_signals_df = pd.read_csv(result["deactivated_signals_path"])
    signal_lifecycle_report_df = pd.read_csv(result["signal_lifecycle_report_path"])
    regime_labels_by_date_df = pd.read_csv(result["regime_labels_by_date_path"])
    signal_performance_by_regime_df = pd.read_csv(result["signal_performance_by_regime_path"])
    signal_performance_by_sub_universe_df = pd.read_csv(
        result["signal_performance_by_sub_universe_path"]
    )
    signal_performance_by_benchmark_context_df = pd.read_csv(
        result["signal_performance_by_benchmark_context_path"]
    )
    regime_aware_signal_weights_df = pd.read_csv(result["regime_aware_signal_weights_path"])
    regime_selection_report_df = pd.read_csv(result["regime_selection_report_path"])
    portfolio_returns_df = pd.read_csv(result["portfolio_returns_path"])
    portfolio_metrics_df = pd.read_csv(result["portfolio_metrics_path"])
    portfolio_weights_df = pd.read_csv(result["portfolio_weights_path"])
    robustness_report_df = pd.read_csv(result["robustness_report_path"])
    regime_performance_df = pd.read_csv(result["regime_performance_path"])
    stress_test_results_df = pd.read_csv(result["stress_test_results_path"])
    implementability_report_df = pd.read_csv(result["implementability_report_path"])
    liquidity_filtered_metrics_df = pd.read_csv(result["liquidity_filtered_portfolio_metrics_path"])
    capacity_scenarios_df = pd.read_csv(result["capacity_scenarios_path"])

    assert leaderboard_df.empty
    assert promoted_signals_df.empty
    assert redundancy_report_df.empty
    assert redundancy_df.empty
    assert composite_scores_df.empty
    assert composite_leaderboard_df.empty
    assert dynamic_signal_weights_df.empty
    assert active_signals_by_date_df.empty
    assert deactivated_signals_df.empty
    assert signal_lifecycle_report_df.empty
    assert regime_labels_by_date_df.empty
    assert signal_performance_by_regime_df.empty
    assert signal_performance_by_sub_universe_df.empty
    assert signal_performance_by_benchmark_context_df.empty
    assert regime_aware_signal_weights_df.empty
    assert regime_selection_report_df.empty
    assert portfolio_returns_df.empty
    assert portfolio_metrics_df.empty
    assert portfolio_weights_df.empty
    assert robustness_report_df.empty
    assert regime_performance_df.empty
    assert stress_test_results_df.empty
    assert implementability_report_df.empty
    assert liquidity_filtered_metrics_df.empty
    assert capacity_scenarios_df.empty
    assert "rejection_reason" in leaderboard_df.columns
    assert "promotion_status" in leaderboard_df.columns
    assert "score_corr" in redundancy_df.columns
    assert DEFAULT_PROMOTION_THRESHOLDS.min_worst_fold_spearman_ic == pytest.approx(-0.10)
