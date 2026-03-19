from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.runner import run_alpha_research


def test_build_walk_forward_folds_returns_expected_fold_ranges() -> None:
    timestamps = pd.Series(pd.date_range("2024-01-01", periods=12, freq="D"))

    folds = build_walk_forward_folds(
        timestamps,
        train_size=5,
        test_size=2,
        step_size=2,
    )

    assert len(folds) == 3

    assert folds[0].fold_id == 1
    assert folds[0].train_start == pd.Timestamp("2024-01-01")
    assert folds[0].train_end == pd.Timestamp("2024-01-05")
    assert folds[0].test_start == pd.Timestamp("2024-01-06")
    assert folds[0].test_end == pd.Timestamp("2024-01-07")

    assert folds[1].fold_id == 2
    assert folds[1].train_end == pd.Timestamp("2024-01-07")
    assert folds[1].test_start == pd.Timestamp("2024-01-08")
    assert folds[1].test_end == pd.Timestamp("2024-01-09")


def test_run_alpha_research_writes_fold_level_walk_forward_results(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=120, freq="D"),
            "symbol": ["AAPL"] * 120,
            "close": [100.0 + i for i in range(120)],
        }
    )
    df.to_parquet(feature_dir / "AAPL.parquet", index=False)

    run_alpha_research(
        symbols=["AAPL"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[5],
        horizons=[1],
        min_rows=30,
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir=output_dir,
        train_size=40,
        test_size=10,
        step_size=10,
    )

    fold_results = pd.read_csv(output_dir / "fold_results.csv")
    leaderboard = pd.read_csv(output_dir / "leaderboard.csv")
    promoted_signals = pd.read_csv(output_dir / "promoted_signals.csv")
    redundancy_report = pd.read_csv(output_dir / "redundancy_report.csv")
    redundancy = pd.read_csv(output_dir / "redundancy_diagnostics.csv")

    assert not fold_results.empty
    assert not leaderboard.empty
    assert promoted_signals.empty
    assert redundancy_report.empty
    assert redundancy.empty

    assert "fold_id" in fold_results.columns
    assert "train_start" in fold_results.columns
    assert "train_end" in fold_results.columns
    assert "test_start" in fold_results.columns
    assert "test_end" in fold_results.columns
    assert "symbols_evaluated" in fold_results.columns
    assert "long_short_spread" in fold_results.columns

    assert "folds_tested" in leaderboard.columns
    assert "mean_long_short_spread" in leaderboard.columns
    assert "worst_fold_spearman_ic" in leaderboard.columns
    assert "rejection_reason" in leaderboard.columns
    assert "promotion_status" in leaderboard.columns
