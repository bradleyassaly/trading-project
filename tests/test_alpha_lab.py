from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.composite import (
    build_composite_scores,
    normalize_signal_by_date,
    select_low_redundancy_signals,
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
from trading_platform.research.alpha_lab.runner import run_alpha_research
from trading_platform.research.alpha_lab.signals import build_signal


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
    composite_diagnostics_path = Path(result["composite_diagnostics_path"])

    assert leaderboard_path.exists()
    assert fold_results_path.exists()
    assert promoted_signals_path.exists()
    assert redundancy_report_path.exists()
    assert redundancy_path.exists()
    assert composite_scores_path.exists()
    assert composite_leaderboard_path.exists()
    assert composite_diagnostics_path.exists()
    assert (output_dir / "leaderboard.parquet").exists()
    assert (output_dir / "fold_results.parquet").exists()
    assert (output_dir / "promoted_signals.parquet").exists()
    assert (output_dir / "redundancy_report.parquet").exists()
    assert (output_dir / "redundancy_diagnostics.parquet").exists()
    assert (output_dir / "composite_scores.parquet").exists()
    assert (output_dir / "composite_leaderboard.parquet").exists()
    assert (output_dir / "composite_diagnostics.json").exists()
    assert (output_dir / "signal_diagnostics.json").exists()

    leaderboard_df = pd.read_csv(leaderboard_path)
    fold_results_df = pd.read_csv(fold_results_path)
    promoted_signals_df = pd.read_csv(promoted_signals_path)
    redundancy_df = pd.read_csv(redundancy_path)
    composite_scores_df = pd.read_csv(composite_scores_path)
    composite_leaderboard_df = pd.read_csv(composite_leaderboard_path)

    assert not leaderboard_df.empty
    assert not fold_results_df.empty
    assert promoted_signals_df.empty
    assert composite_scores_df.empty
    assert composite_leaderboard_df.empty
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
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    promoted_signals_df = pd.read_csv(result["promoted_signals_path"])
    redundancy_report_df = pd.read_csv(result["redundancy_report_path"])
    redundancy_df = pd.read_csv(result["redundancy_path"])
    composite_scores_df = pd.read_csv(result["composite_scores_path"])
    composite_leaderboard_df = pd.read_csv(result["composite_leaderboard_path"])

    assert set(leaderboard_df["promotion_status"]) == {"promote"}
    assert set(leaderboard_df["rejection_reason"]) == {"none"}
    assert len(promoted_signals_df) == 2
    assert set(promoted_signals_df["promotion_status"]) == {"promote"}
    assert len(redundancy_report_df) == 1
    assert len(redundancy_df) == 1
    assert not composite_scores_df.empty
    assert not composite_leaderboard_df.empty
    assert set(composite_scores_df["weighting_scheme"]) == {"equal", "quality"}
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

    assert leaderboard_df.empty
    assert promoted_signals_df.empty
    assert redundancy_report_df.empty
    assert redundancy_df.empty
    assert composite_scores_df.empty
    assert composite_leaderboard_df.empty
    assert "rejection_reason" in leaderboard_df.columns
    assert "promotion_status" in leaderboard_df.columns
    assert "score_corr" in redundancy_df.columns
    assert DEFAULT_PROMOTION_THRESHOLDS.min_worst_fold_spearman_ic == pytest.approx(-0.10)
