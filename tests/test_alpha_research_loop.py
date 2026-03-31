from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.alpha_lab.automation import (
    AutomatedAlphaResearchConfig,
    ResearchResourceAllocationConfig,
    SignalSearchSpace,
    _load_symbol_feature_data,
    _resolve_symbols,
    build_research_resource_allocation_plan,
    build_fallback_usage_report,
    build_feature_availability_report,
    build_near_miss_signals_report,
    build_promotion_threshold_diagnostics,
    build_signal_family_summary,
    build_skipped_candidates_report,
    build_top_rejected_signals_report,
    generate_candidate_configs,
    load_research_registry,
    run_automated_alpha_research_loop,
    select_untested_candidates,
)
from trading_platform.research.alpha_lab.generation import (
    CROSS_SECTIONAL_TRANSFORMS,
    SignalGenerationConfig,
    apply_cross_sectional_transform,
    build_generated_signal,
    generate_candidate_signals,
)


def _write_feature_file(
    feature_dir: Path,
    symbol: str,
    *,
    base_return: float,
) -> None:
    timestamps = pd.date_range("2024-01-01", periods=160, freq="D")
    closes = [100.0]
    for day_index in range(1, len(timestamps)):
        drift = base_return + 0.00005 * day_index
        closes.append(closes[-1] * (1.0 + drift))
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": symbol,
            "close": closes,
        }
    )
    frame["mom_20"] = frame["close"].pct_change(20)
    frame["vol_20"] = frame["close"].pct_change().rolling(20).std()
    frame["sector_momentum_20"] = 0.5 * frame["mom_20"]
    frame["volume"] = 1_000_000 + base_return * 100_000
    frame["dist_sma_200"] = 0.0
    frame["vol_ratio_20"] = 1.0 + base_return
    frame.to_parquet(feature_dir / f"{symbol}.parquet", index=False)


def _write_loader_feature_file(
    feature_dir: Path,
    symbol: str,
    payload: pd.DataFrame,
    *,
    index: bool = False,
) -> None:
    feature_dir.mkdir(parents=True, exist_ok=True)
    payload.to_parquet(feature_dir / f"{symbol}.parquet", index=index)


def test_generate_candidate_signals_are_unique_across_families_and_parameters() -> None:
    candidates = generate_candidate_signals(
        SignalGenerationConfig(
            signal_families=(
                "momentum",
                "mean_reversion",
                "volatility",
                "market_residual_momentum",
                "residual_momentum",
                "sector_relative_momentum",
                "cross_sectional_rank_momentum",
                "cross_sectional_zscore_momentum",
                "vol_adjusted_momentum",
                "vol_adjusted_reversal",
                "breakout_distance",
                "extreme_move",
                "volume_surprise",
                "interaction_momentum_volatility",
                "interaction_reversal_volume_spike",
                "feature_combo",
            ),
            lookbacks=(5, 10),
            vol_windows=(10,),
            combo_thresholds=(0.5, 1.0),
            combo_pairs=(("mom_20", "vol_20"),),
            horizons=(1, 5),
        ),
        feature_columns=["mom_20", "vol_20", "sector_momentum_20", "volume"],
    )

    assert not candidates.empty
    assert candidates["candidate_id"].nunique() == len(candidates)
    assert {"candidate_name", "variant_id", "signal_variant", "variant_parameters_json", "config_json"} <= set(candidates.columns)
    assert set(candidates["signal_variant"]) == {"base"}
    assert {
        "momentum",
        "mean_reversion",
        "volatility",
        "market_residual_momentum",
        "residual_momentum",
        "sector_relative_momentum",
        "cross_sectional_rank_momentum",
        "cross_sectional_zscore_momentum",
        "vol_adjusted_momentum",
        "vol_adjusted_reversal",
        "breakout_distance",
        "extreme_move",
        "volume_surprise",
        "interaction_momentum_volatility",
        "interaction_reversal_volume_spike",
        "feature_combo",
    } <= set(candidates["signal_name"])


def test_generate_candidate_signals_supports_variant_sweeps_and_traceability() -> None:
    candidates = generate_candidate_signals(
        SignalGenerationConfig(
            signal_families=("momentum", "volume_surprise"),
            lookbacks=(5,),
            vol_windows=(10,),
            horizons=(1,),
            candidate_grid_preset="broad_v1",
            max_variants_per_family=3,
        ),
        feature_columns=["volume"],
    )

    momentum_variants = candidates.loc[candidates["signal_family"] == "momentum", "signal_variant"].tolist()
    volume_variants = candidates.loc[candidates["signal_family"] == "volume_surprise", "signal_variant"].tolist()

    assert momentum_variants == ["base", "smoothed", "risk_scaled"]
    assert volume_variants == ["base", "zscored", "clipped"]
    assert candidates["candidate_id"].nunique() == len(candidates)
    assert candidates["variant_id"].tolist() == candidates["signal_variant"].tolist()
    sample_config = json.loads(candidates.loc[candidates["signal_variant"] == "risk_scaled", "config_json"].iloc[0])
    assert sample_config["variant_id"] == "risk_scaled"
    assert sample_config["parameters"] == {"lookback": 5}
    sample_variant = json.loads(candidates.loc[candidates["signal_variant"] == "smoothed", "variant_parameters_json"].iloc[0])
    assert sample_variant["smoothing_window"] == 5


def test_generate_candidate_configs_from_search_spaces_supports_named_variants() -> None:
    candidates = generate_candidate_configs(
        (
            SignalSearchSpace(
                signal_family="momentum",
                lookbacks=(5,),
                horizons=(1,),
                candidate_grid_preset="broad_v1",
                max_variants_per_family=3,
            ),
        ),
    )

    assert candidates["candidate_id"].nunique() == len(candidates)
    assert candidates["signal_variant"].tolist() == ["base", "fast_trend", "relative_strength_focus"]
    assert "candidate_name" in candidates.columns
    assert "variant_parameters_json" in candidates.columns
    assert json.loads(candidates.loc[candidates["signal_variant"] == "fast_trend", "variant_parameters_json"].iloc[0])[
        "trend_weight"
    ] == pytest.approx(1.2)



def test_resolve_symbols_uses_named_universe_registry() -> None:
    result = _resolve_symbols(None, "magnificent7")

    assert "AAPL" in result
    assert "MSFT" in result
    assert len(result) >= 7


def test_resolve_symbols_preserves_explicit_symbols() -> None:
    result = _resolve_symbols(["msft", "AAPL", "MSFT"], None)

    assert result == ["MSFT", "AAPL"]


def test_resolve_symbols_unknown_universe_raises_clean_error() -> None:
    with pytest.raises(KeyError, match="not_a_real_universe"):
        _resolve_symbols(None, "not_a_real_universe")


def test_automation_loader_preserves_existing_timestamp_and_close_columns(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_loader_feature_file(
        feature_dir,
        "AAPL",
        pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
                "close": [100.0, 101.0, 102.0],
            }
        ),
    )

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["timestamp"].iloc[0] == pd.Timestamp("2024-01-01")
    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])
    assert result["symbol"].tolist() == ["AAPL", "AAPL", "AAPL"]


@pytest.mark.parametrize(
    ("frame", "index"),
    [
        (
            pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", periods=3, freq="D"),
                    "Close": [100.0, 101.0, 102.0],
                }
            ),
            False,
        ),
        (
            pd.DataFrame(
                {
                    "Date": pd.date_range("2024-01-01", periods=3, freq="D"),
                    "adj_close": [100.0, 101.0, 102.0],
                }
            ),
            False,
        ),
        (
            pd.DataFrame(
                {
                    "Adj Close": [100.0, 101.0, 102.0],
                },
                index=pd.date_range("2024-01-01", periods=3, freq="D"),
            ),
            True,
        ),
    ],
)
def test_automation_loader_normalizes_supported_timestamp_and_close_variants(
    tmp_path: Path,
    frame: pd.DataFrame,
    index: bool,
) -> None:
    feature_dir = tmp_path / "features"
    _write_loader_feature_file(feature_dir, "AAPL", frame, index=index)

    result = _load_symbol_feature_data(feature_dir, "AAPL")

    assert result["timestamp"].tolist() == list(pd.date_range("2024-01-01", periods=3, freq="D"))
    assert result["close"].tolist() == pytest.approx([100.0, 101.0, 102.0])


def test_automation_loader_raises_clean_error_for_invalid_schema(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    _write_loader_feature_file(
        feature_dir,
        "AAPL",
        pd.DataFrame(
            {
                "open": [99.0, 100.0, 101.0],
                "volume": [1, 2, 3],
            }
        ),
    )

    with pytest.raises(ValueError, match="timestamp"):
        _load_symbol_feature_data(feature_dir, "AAPL")


def test_build_top_rejected_signals_report_includes_threshold_distances() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "candidate_id": "momentum|horizon=1|lookback=5",
                "mean_spearman_ic": 0.018,
                "folds_tested": 3,
                "mean_dates_evaluated": 5.0,
                "mean_turnover": 0.30,
                "worst_fold_spearman_ic": -0.05,
                "total_obs": 150.0,
                "symbols_tested": 3.0,
                "mean_long_short_spread": 0.02,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic",
            },
            {
                "candidate_id": "momentum|horizon=1|lookback=10",
                "mean_spearman_ic": 0.03,
                "folds_tested": 3,
                "mean_dates_evaluated": 5.0,
                "mean_turnover": 0.90,
                "worst_fold_spearman_ic": -0.05,
                "total_obs": 150.0,
                "symbols_tested": 3.0,
                "mean_long_short_spread": 0.01,
                "promotion_status": "reject",
                "rejection_reason": "high_turnover",
            },
        ]
    )

    result = build_top_rejected_signals_report(leaderboard_df, universe="sp500")

    assert result["candidate_id"].tolist()[0] == "momentum|horizon=1|lookback=10"
    assert set(result["universe"]) == {"sp500"}
    assert "distance_mean_rank_ic" in result.columns
    assert "distance_turnover_headroom" in result.columns
    assert result.loc[result["candidate_id"] == "momentum|horizon=1|lookback=5", "distance_mean_rank_ic"].iloc[0] < 0.0
    assert result.loc[result["candidate_id"] == "momentum|horizon=1|lookback=10", "distance_turnover_headroom"].iloc[0] < 0.0


def test_build_near_miss_signals_report_prioritizes_smallest_threshold_gaps() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "candidate_id": "signal_a",
                "mean_spearman_ic": 0.019,
                "folds_tested": 2,
                "mean_dates_evaluated": 3.0,
                "mean_turnover": 0.70,
                "worst_fold_spearman_ic": -0.08,
                "total_obs": 110.0,
                "symbols_tested": 2.0,
                "mean_long_short_spread": 0.01,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic",
            },
            {
                "candidate_id": "signal_b",
                "mean_spearman_ic": 0.010,
                "folds_tested": 1,
                "mean_dates_evaluated": 2.0,
                "mean_turnover": 0.95,
                "worst_fold_spearman_ic": -0.20,
                "total_obs": 20.0,
                "symbols_tested": 1.0,
                "mean_long_short_spread": 0.00,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic;insufficient_folds;high_turnover",
            },
        ]
    )

    result = build_near_miss_signals_report(leaderboard_df, universe="sp500")

    assert result["candidate_id"].tolist()[0] == "signal_a"
    assert result.loc[result["candidate_id"] == "signal_a", "failing_threshold_count"].iloc[0] == 1
    assert result.loc[result["candidate_id"] == "signal_b", "failing_threshold_count"].iloc[0] > 1
    assert result.loc[result["candidate_id"] == "signal_a", "promotion_gap_score"].iloc[0] < result.loc[
        result["candidate_id"] == "signal_b",
        "promotion_gap_score",
    ].iloc[0]


def test_build_promotion_threshold_diagnostics_summarizes_candidate_pool() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "candidate_id": "promoted_signal",
                "mean_spearman_ic": 0.03,
                "folds_tested": 3,
                "mean_dates_evaluated": 5.0,
                "mean_turnover": 0.30,
                "worst_fold_spearman_ic": 0.00,
                "total_obs": 150.0,
                "symbols_tested": 3.0,
                "promotion_status": "promote",
                "rejection_reason": "none",
            },
            {
                "candidate_id": "rejected_signal",
                "mean_spearman_ic": 0.015,
                "folds_tested": 1,
                "mean_dates_evaluated": 2.0,
                "mean_turnover": 0.90,
                "worst_fold_spearman_ic": -0.20,
                "total_obs": 20.0,
                "symbols_tested": 1.0,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic;high_turnover;insufficient_folds",
            },
        ]
    )

    result = build_promotion_threshold_diagnostics(leaderboard_df, universe="nasdaq100")

    assert result["universe"] == "nasdaq100"
    assert result["candidate_pool_summary"]["total_candidates"] == 2
    assert result["candidate_pool_summary"]["promoted_candidates"] == 1
    assert result["candidate_pool_summary"]["rejected_candidates"] == 1
    assert result["rejection_reason_counts"]["high_turnover"] == 1
    assert "mean" in result["mean_rank_ic_distribution"]
    assert "mean" in result["turnover_distribution"]


def test_build_signal_family_summary_groups_candidates_and_rejections() -> None:
    leaderboard_df = pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "mean_spearman_ic": 0.03,
                "promotion_status": "promote",
                "rejection_reason": "none",
            },
            {
                "signal_family": "momentum",
                "mean_spearman_ic": 0.01,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic;high_turnover",
            },
            {
                "signal_family": "volume_surprise",
                "mean_spearman_ic": -0.01,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic",
            },
        ]
    )

    result = build_signal_family_summary(leaderboard_df, universe="sp500")

    momentum_row = result.loc[result["signal_family"] == "momentum"].iloc[0]
    assert momentum_row["candidate_count"] == 2
    assert momentum_row["promotion_count"] == 1
    assert "high_turnover" in momentum_row["rejection_reason_counts"]


def test_build_feature_availability_and_skip_reports_capture_missing_inputs() -> None:
    symbol_data = {
        "AAPL": pd.DataFrame(columns=["timestamp", "symbol", "close", "volume"]),
        "MSFT": pd.DataFrame(columns=["timestamp", "symbol", "close"]),
    }
    generation_config = SignalGenerationConfig(
        signal_families=("market_residual_momentum", "sector_relative_momentum", "volume_surprise", "feature_combo"),
        lookbacks=(5,),
        vol_windows=(10,),
        combo_thresholds=(0.5,),
        combo_pairs=(("mom_20", "vol_20"),),
        horizons=(1,),
    )

    availability = build_feature_availability_report(
        symbol_data,
        generation_config=generation_config,
        universe="custom",
    )
    skipped = build_skipped_candidates_report(
        generation_config,
        feature_columns={"close", "volume"},
        universe="custom",
    )

    assert availability.loc[0, "symbols_total"] == 2
    assert availability.loc[0, "volume_availability_rate"] == pytest.approx(0.5)
    assert skipped["skipped_candidate_count"].sum() > 0
    assert "sector_relative_momentum" in set(skipped["signal_family"])


def test_build_fallback_usage_report_counts_proxy_usage() -> None:
    symbol_data = {
        "AAPL": pd.DataFrame(columns=["timestamp", "symbol", "close", "dollar_volume"]),
        "MSFT": pd.DataFrame(columns=["timestamp", "symbol", "close"]),
    }
    generation_config = SignalGenerationConfig(
        signal_families=("market_residual_momentum", "volume_surprise", "interaction_reversal_volume_spike"),
        lookbacks=(5,),
        vol_windows=(10,),
        horizons=(1,),
    )

    result = build_fallback_usage_report(
        symbol_data,
        generation_config=generation_config,
        universe="custom",
    )

    assert "trailing_mean_proxy" in set(result["fallback_type"])
    assert "dollar_volume_proxy" in set(result["fallback_type"])


def test_build_generated_signal_supports_new_cross_sectional_feature_families() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 102.0, 101.0, 105.0, 107.0, 108.0],
            "volume": [100.0, 110.0, 90.0, 140.0, 160.0, 150.0],
            "market_return_3": [0.00, 0.01, 0.00, 0.02, 0.01, 0.03],
            "sector_mean_return_3": [0.00, 0.01, 0.00, 0.02, 0.01, 0.03],
            "sector_momentum_3": [0.00, 0.01, 0.00, 0.02, 0.01, 0.03],
        }
    )

    market_residual = build_generated_signal(
        df,
        pd.Series({"signal_name": "market_residual_momentum", "lookback": 3}),
    )
    residual = build_generated_signal(
        df,
        pd.Series({"signal_name": "residual_momentum", "lookback": 3}),
    )
    sector_relative = build_generated_signal(
        df,
        pd.Series({"signal_name": "sector_relative_momentum", "lookback": 3}),
    )
    vol_adjusted_reversal = build_generated_signal(
        df,
        pd.Series({"signal_name": "vol_adjusted_reversal", "lookback": 3}),
    )
    breakout = build_generated_signal(
        df,
        pd.Series({"signal_name": "breakout_distance", "window": 3}),
    )
    volume_surprise = build_generated_signal(
        df,
        pd.Series({"signal_name": "volume_surprise", "window": 3}),
    )
    vol_adjusted_momentum = build_generated_signal(
        df,
        pd.Series({"signal_name": "vol_adjusted_momentum", "lookback": 3}),
    )
    extreme_move = build_generated_signal(
        df,
        pd.Series({"signal_name": "extreme_move", "window": 3}),
    )
    interaction_momentum_volatility = build_generated_signal(
        df,
        pd.Series({"signal_name": "interaction_momentum_volatility", "lookback": 3}),
    )
    interaction_reversal_volume_spike = build_generated_signal(
        df,
        pd.Series({"signal_name": "interaction_reversal_volume_spike", "lookback": 3}),
    )

    assert market_residual.notna().sum() > 0
    assert residual.notna().sum() > 0
    assert sector_relative.notna().sum() > 0
    assert vol_adjusted_momentum.notna().sum() > 0
    assert vol_adjusted_reversal.notna().sum() > 0
    assert breakout.notna().sum() > 0
    assert extreme_move.notna().sum() > 0
    assert volume_surprise.notna().sum() > 0
    assert interaction_momentum_volatility.notna().sum() > 0
    assert interaction_reversal_volume_spike.notna().sum() > 0


def test_build_generated_signal_applies_variant_transform_parameters() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 102.0, 104.0, 103.0, 107.0, 111.0, 115.0],
            "volume": [100.0, 110.0, 115.0, 120.0, 180.0, 175.0, 170.0],
        }
    )

    base_signal = build_generated_signal(
        df,
        pd.Series({"signal_name": "momentum", "lookback": 2, "variant_parameters_json": "{}"}),
    )
    smoothed_signal = build_generated_signal(
        df,
        pd.Series(
            {
                "signal_name": "momentum",
                "lookback": 2,
                "variant_parameters_json": json.dumps({"smoothing_window": 2}, sort_keys=True),
            }
        ),
    )
    zscored_signal = build_generated_signal(
        df,
        pd.Series(
            {
                "signal_name": "volume_surprise",
                "window": 3,
                "variant_parameters_json": json.dumps({"zscore_window": 3}, sort_keys=True),
            }
        ),
    )

    assert not base_signal.equals(smoothed_signal)
    assert smoothed_signal.notna().sum() > 0
    assert zscored_signal.notna().sum() > 0


def test_apply_cross_sectional_transform_normalizes_by_date() -> None:
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

    ranked = apply_cross_sectional_transform(panel, method=CROSS_SECTIONAL_TRANSFORMS["cross_sectional_rank_momentum"])
    zscored = apply_cross_sectional_transform(panel, method=CROSS_SECTIONAL_TRANSFORMS["cross_sectional_zscore_momentum"])

    assert ranked.loc[ranked["timestamp"] == pd.Timestamp("2024-01-01"), "signal"].tolist() == [1 / 3, 2 / 3, 1.0]
    day_two_zscores = zscored.loc[zscored["timestamp"] == pd.Timestamp("2024-01-02"), "signal"]
    assert day_two_zscores.mean() == pytest.approx(0.0)


def test_generated_features_do_not_use_lookahead_information() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
            "volume": [100.0, 100.0, 100.0, 100.0, 500.0],
        }
    )

    baseline = build_generated_signal(
        df,
        pd.Series({"signal_name": "volume_surprise", "window": 3}),
    )

    shocked = df.copy()
    shocked.loc[4, "volume"] = 5_000.0
    shocked_signal = build_generated_signal(
        shocked,
        pd.Series({"signal_name": "volume_surprise", "window": 3}),
    )

    assert baseline.iloc[:4].tolist() == pytest.approx(shocked_signal.iloc[:4].tolist(), nan_ok=True)


def test_select_untested_candidates_skips_completed_registry_entries() -> None:
    candidates = generate_candidate_signals(
        SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5, 10),
            horizons=(1,),
        )
    )
    registry = pd.DataFrame(
        [
            {
                "candidate_id": candidates.iloc[0]["candidate_id"],
                "evaluation_status": "completed",
            }
        ]
    )

    pending = select_untested_candidates(candidates, registry)

    assert pending["candidate_id"].tolist() == [candidates.iloc[1]["candidate_id"]]


def test_build_research_resource_allocation_plan_prioritizes_families_and_caps_variants() -> None:
    candidates = pd.DataFrame(
        [
            {"candidate_id": "mom_a", "candidate_name": "mom_a", "signal_family": "momentum", "variant_id": "base", "signal_variant": "base"},
            {"candidate_id": "mom_b", "candidate_name": "mom_b", "signal_family": "momentum", "variant_id": "fast", "signal_variant": "fast"},
            {"candidate_id": "rev_a", "candidate_name": "rev_a", "signal_family": "mean_reversion", "variant_id": "base", "signal_variant": "base"},
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "candidate_id": "hist_mom",
                "signal_family": "momentum",
                "evaluation_status": "completed",
                "promotion_status": "promote",
                "mean_spearman_ic": 0.03,
            },
            {
                "candidate_id": "hist_rev",
                "signal_family": "mean_reversion",
                "evaluation_status": "completed",
                "promotion_status": "reject",
                "mean_spearman_ic": -0.01,
            },
        ]
    )

    allocation = build_research_resource_allocation_plan(
        candidates,
        registry,
        config=ResearchResourceAllocationConfig(
            max_candidates_per_iteration=2,
            max_variants_per_family=1,
            prioritize_by_family_promise=True,
        ),
    )

    assert allocation.loc[allocation["candidate_id"] == "mom_a", "allocation_status"].iloc[0] == "selected"
    assert allocation.loc[allocation["candidate_id"] == "mom_b", "pruning_reason"].iloc[0] == "family_variant_cap"
    assert allocation.loc[allocation["candidate_id"] == "rev_a", "allocation_status"].iloc[0] == "selected"
    assert allocation.loc[allocation["candidate_id"] == "mom_a", "family_priority_score"].iloc[0] > allocation.loc[
        allocation["candidate_id"] == "rev_a",
        "family_priority_score",
    ].iloc[0]


def test_run_automated_alpha_research_loop_updates_registry_and_artifacts(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum", "mean_reversion", "feature_combo"),
            lookbacks=(5, 20),
            combo_thresholds=(0.5,),
            combo_pairs=(("mom_20", "vol_20"),),
            horizons=(1,),
            candidate_grid_preset="broad_v1",
            max_variants_per_family=2,
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
        resource_allocation=ResearchResourceAllocationConfig(
            max_candidates_per_iteration=4,
            max_variants_per_family=1,
            prioritize_by_family_promise=True,
        ),
    )

    first_result = run_automated_alpha_research_loop(config=config)
    first_schedule_payload = json.loads((output_dir / "research_schedule.json").read_text(encoding="utf-8"))
    second_result = run_automated_alpha_research_loop(config=config)

    registry_df = pd.read_csv(first_result["registry_path"])
    signal_registry_df = pd.read_csv(first_result["signal_registry_path"])
    history_df = pd.read_csv(first_result["history_path"])
    promoted_df = pd.read_csv(first_result["promoted_signals_path"])
    rejected_df = pd.read_csv(first_result["rejected_signals_path"])
    top_rejected_df = pd.read_csv(first_result["top_rejected_signals_path"])
    near_miss_df = pd.read_csv(first_result["near_miss_signals_path"])
    signal_family_summary_df = pd.read_csv(first_result["signal_family_summary_path"])
    feature_availability_report_df = pd.read_csv(first_result["feature_availability_report_path"])
    skipped_candidates_report_df = pd.read_csv(first_result["skipped_candidates_report_path"])
    fallback_usage_report_df = pd.read_csv(first_result["fallback_usage_report_path"])
    promotion_threshold_diagnostics = json.loads(
        Path(first_result["promotion_threshold_diagnostics_path"]).read_text(encoding="utf-8")
    )
    candidate_grid_df = pd.read_csv(first_result["candidate_grid_path"])
    candidate_grid_manifest = json.loads(Path(first_result["candidate_grid_manifest_path"]).read_text(encoding="utf-8"))
    allocation_plan_df = pd.read_csv(first_result["candidate_allocation_path"])
    allocation_summary = json.loads(Path(first_result["allocation_summary_path"]).read_text(encoding="utf-8"))
    deferred_df = pd.read_csv(first_result["deferred_candidates_path"])
    run_summary = json.loads(Path(first_result["run_summary_path"]).read_text(encoding="utf-8"))
    composite_inputs = json.loads((output_dir / "composite_inputs.json").read_text(encoding="utf-8"))

    assert not registry_df.empty
    assert registry_df.equals(signal_registry_df)
    assert {
        "signal_name",
        "parameters_json",
        "promotion_status",
        "rejection_reason",
        "candidate_name",
        "variant_id",
        "signal_variant",
        "variant_parameters_json",
        "config_json",
    } <= set(registry_df.columns)
    assert len(history_df) == len(registry_df)
    assert len(promoted_df) + len(rejected_df) == len(registry_df)
    assert len(top_rejected_df) == len(rejected_df)
    assert len(near_miss_df) == len(rejected_df)
    assert not signal_family_summary_df.empty
    assert not feature_availability_report_df.empty
    assert skipped_candidates_report_df.empty
    assert {"candidate_id", "universe", "rejection_reason", "distance_mean_rank_ic"} <= set(top_rejected_df.columns)
    assert {"candidate_id", "failing_threshold_count", "promotion_gap_score"} <= set(near_miss_df.columns)
    assert {"signal_family", "candidate_count", "variant_count", "promotion_count"} <= set(signal_family_summary_df.columns)
    assert {"benchmark_data_availability_rate", "volume_availability_rate"} <= set(feature_availability_report_df.columns)
    assert {"fallback_type", "usage_count"} <= set(fallback_usage_report_df.columns)
    assert set(top_rejected_df["universe"]) == {"custom"}
    assert promotion_threshold_diagnostics["candidate_pool_summary"]["total_candidates"] == len(registry_df)
    assert "rejection_reason_counts" in promotion_threshold_diagnostics
    assert candidate_grid_manifest["candidate_count"] == len(candidate_grid_df)
    assert "momentum" in candidate_grid_manifest["family_variant_counts"]
    assert len(allocation_plan_df) == run_summary["candidates_pending"]
    assert allocation_summary["selected_count"] == run_summary["candidates_evaluated"]
    assert allocation_summary["deferred_count"] == len(deferred_df)
    assert {"allocation_score", "allocation_status", "pruning_reason"} <= set(allocation_plan_df.columns)
    assert run_summary["candidates_generated"] == len(candidate_grid_df)
    assert run_summary["registry_rows"] == len(registry_df)
    assert run_summary["artifact_paths"]["promoted_signals_path"] == first_result["promoted_signals_path"]
    assert Path(first_result["experiment_registry_path"]).exists()
    assert "horizons" in composite_inputs
    assert first_schedule_payload["candidates_generated"] == len(candidate_grid_df)
    assert first_schedule_payload["candidates_evaluated"] <= len(registry_df)
    assert first_result["iterations_completed"] == 1

    second_schedule = json.loads(Path(second_result["schedule_path"]).read_text(encoding="utf-8"))
    assert second_schedule["candidates_pending"] == len(allocation_plan_df)
    assert second_schedule["candidates_deferred"] == len(deferred_df)
    assert second_result["iterations_completed"] == 1


def test_registry_updates_incrementally_without_retesting_existing_candidates(
    tmp_path: Path,
) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    first_config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
    )
    second_config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum", "volatility"),
            lookbacks=(5,),
            vol_windows=(10,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
    )

    first_result = run_automated_alpha_research_loop(config=first_config)
    first_registry_df = pd.read_csv(first_result["registry_path"])
    second_result = run_automated_alpha_research_loop(config=second_config)

    registry_df = load_research_registry(Path(second_result["registry_path"]))
    history_df = pd.read_csv(second_result["history_path"])

    assert len(registry_df) == 2
    assert history_df["candidate_id"].nunique() == 2
    assert history_df["candidate_id"].value_counts().max() == 1
    assert len(first_registry_df) == 1


def test_run_automated_alpha_research_loop_stops_after_max_iterations(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
        force=True,
        stale_after_days=0,
        max_iterations=2,
    )

    result = run_automated_alpha_research_loop(config=config)
    history_df = pd.read_csv(result["history_path"])

    assert result["iterations_completed"] == 2
    assert len(history_df) == 2
    assert (output_dir / "leaderboard.csv").exists()


def test_run_automated_alpha_research_loop_defaults_to_one_iteration(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_loop"
    feature_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_return in {"AAPL": 0.004, "MSFT": 0.007, "NVDA": 0.011}.items():
        _write_feature_file(feature_dir, symbol, base_return=base_return)

    config = AutomatedAlphaResearchConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_dir=feature_dir,
        output_dir=output_dir,
        generation_config=SignalGenerationConfig(
            signal_families=("momentum",),
            lookbacks=(5,),
            horizons=(1,),
        ),
        min_rows=60,
        top_quantile=0.34,
        bottom_quantile=0.34,
        train_size=60,
        test_size=20,
        step_size=20,
        schedule_frequency="manual",
        force=True,
    )

    result = run_automated_alpha_research_loop(config=config)
    history_df = pd.read_csv(result["history_path"])

    assert result["iterations_completed"] == 1
    assert len(history_df) == 1


def test_build_top_rejected_signals_report_handles_no_promoted_or_empty_cases() -> None:
    empty_result = build_top_rejected_signals_report(pd.DataFrame(), universe=None)
    empty_near_miss = build_near_miss_signals_report(pd.DataFrame(), universe=None)
    empty_diagnostics = build_promotion_threshold_diagnostics(pd.DataFrame(), universe=None)
    empty_signal_family_summary = build_signal_family_summary(pd.DataFrame(), universe=None)
    empty_feature_availability = build_feature_availability_report({}, generation_config=None, universe=None)
    empty_skipped = build_skipped_candidates_report(None, feature_columns=set(), universe=None)
    empty_fallback = build_fallback_usage_report({}, generation_config=None, universe=None)
    assert empty_result.empty
    assert empty_near_miss.empty
    assert empty_diagnostics["candidate_pool_summary"]["total_candidates"] == 0
    assert empty_signal_family_summary.empty
    assert empty_feature_availability.loc[0, "symbols_total"] == 0
    assert empty_skipped.empty
    assert empty_fallback.empty

    rejected_only = pd.DataFrame(
        [
            {
                "candidate_id": "volume_surprise|horizon=1|window=10",
                "mean_spearman_ic": -0.01,
                "folds_tested": 1,
                "mean_dates_evaluated": 2.0,
                "mean_turnover": 0.8,
                "worst_fold_spearman_ic": -0.2,
                "total_obs": 20.0,
                "symbols_tested": 1.0,
                "mean_long_short_spread": -0.01,
                "promotion_status": "reject",
                "rejection_reason": "low_mean_rank_ic;insufficient_folds",
            }
        ]
    )

    rejected_result = build_top_rejected_signals_report(rejected_only, universe=None)
    near_miss_result = build_near_miss_signals_report(rejected_only, universe=None)

    assert len(rejected_result) == 1
    assert rejected_result.loc[0, "universe"] == "custom"
    assert len(near_miss_result) == 1
