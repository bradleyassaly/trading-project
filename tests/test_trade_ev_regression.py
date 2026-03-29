from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.trade_ev_regression import (
    build_trade_ev_regression_dataset,
    load_trade_ev_regression_model,
    score_trade_ev_regression_candidates,
    run_replay_trade_ev_regression,
)


def test_build_trade_ev_regression_dataset_uses_lifecycle_rows_and_entry_time_features(monkeypatch, tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    replay_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_id": "t1",
                "date": "2025-01-06",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": 0.9,
                "score_percentile_entry": 0.95,
                "realized_return": 0.03,
            }
        ]
    ).to_csv(replay_root / "replay_trade_ev_lifecycle.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-01", periods=60, freq="D"),
                "close": list(range(100, 160)),
                "volume": [1_000_000] * 60,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    rows, summary = build_trade_ev_regression_dataset(
        replay_root=replay_root,
        expected_horizon_days=5,
    )

    assert len(rows) == 1
    assert rows[0]["signal_family"] == "momentum"
    assert rows[0]["expected_horizon_days"] == 5
    assert rows[0]["recent_return_5d"] != 0.0
    assert summary["row_count"] == 1


def test_run_replay_trade_ev_regression_writes_artifacts_and_metrics(monkeypatch, tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    replay_root.mkdir(parents=True, exist_ok=True)
    lifecycle_rows = []
    for index, score in enumerate([0.10, 0.20, 0.30, 0.60, 0.80, 0.95], start=1):
        entry_date = pd.Timestamp("2025-01-01") + pd.Timedelta(days=index)
        exit_date = entry_date + pd.Timedelta(days=2)
        lifecycle_rows.append(
            {
                "trade_id": f"t{index}",
                "date": str(exit_date.date()),
                "entry_date": str(entry_date.date()),
                "exit_date": str(exit_date.date()),
                "symbol": "AAPL" if index % 2 else "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum" if index % 2 else "mean_reversion",
                "score_entry": score,
                "score_percentile_entry": score,
                "realized_return": (score * 0.05) - 0.01,
            }
        )
    pd.DataFrame(lifecycle_rows).to_csv(replay_root / "replay_trade_ev_lifecycle.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        base = 100 if symbol == "AAPL" else 200
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-01", periods=80, freq="D"),
                "close": [base + value for value in range(80)],
                "volume": [1_000_000] * 80,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    result = run_replay_trade_ev_regression(
        replay_root=replay_root,
        model_output_path=tmp_path / "artifacts" / "ev_model" / "ev_regression_model.pkl",
        expected_horizon_days=5,
        min_training_samples=2,
        ridge_alpha=0.5,
    )

    summary = dict(result["summary"])
    assert summary["prediction_count"] >= 1
    assert "correlation" in summary
    assert "rank_correlation" in summary
    assert "bucket_spread" in summary
    assert Path(result["artifact_paths"]["replay_trade_ev_regression_predictions_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_regression_summary_path"]).exists()
    assert Path(result["artifact_paths"]["ev_regression_model_path"]).exists()
    loaded_model = load_trade_ev_regression_model(result["artifact_paths"]["ev_regression_model_path"])
    assert loaded_model["model_type"] == "regression"
    written_summary = json.loads(
        Path(result["artifact_paths"]["replay_ev_regression_summary_path"]).read_text(encoding="utf-8")
    )
    assert written_summary["model_type"] == "regression"


def test_score_trade_ev_regression_candidates_normalizes_and_clips_scores() -> None:
    predictions = score_trade_ev_regression_candidates(
        model={
            "training_available": True,
            "signal_families": ["momentum"],
            "feature_means": [0.0] * 8,
            "feature_stds": [1.0] * 8,
            "intercept": 0.0,
            "coefficients": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "training_sample_count": 10,
        },
        candidate_rows=[
            {
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.20,
                "score_percentile": 0.20,
                "expected_horizon_days": 5,
                "recent_return_3d": 0.0,
                "recent_return_5d": 0.0,
                "recent_return_10d": 0.0,
                "recent_vol_20d": 0.0,
                "estimated_execution_cost_pct": 0.001,
                "weight_delta": 0.1,
            },
            {
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.80,
                "score_percentile": 0.80,
                "expected_horizon_days": 5,
                "recent_return_3d": 0.0,
                "recent_return_5d": 0.0,
                "recent_return_10d": 0.0,
                "recent_vol_20d": 0.0,
                "estimated_execution_cost_pct": 0.001,
                "weight_delta": 0.1,
            },
        ],
        normalize_scores=True,
        normalization_method="rank_pct",
        normalize_within="all_candidates",
        score_clip_min=-0.10,
        score_clip_max=0.10,
        use_normalized_score_for_weighting=True,
    )

    assert predictions[0]["regression_raw_ev_score"] < predictions[1]["regression_raw_ev_score"]
    assert predictions[0]["regression_normalized_ev_score"] < predictions[1]["regression_normalized_ev_score"]
    assert abs(predictions[0]["regression_ev_score_post_clip"]) <= 0.10
    assert abs(predictions[1]["regression_ev_score_post_clip"]) <= 0.10


def test_score_trade_ev_regression_candidates_applies_confidence_multiplier() -> None:
    predictions = score_trade_ev_regression_candidates(
        model={
            "training_available": True,
            "signal_families": ["momentum"],
            "feature_means": [0.0] * 8,
            "feature_stds": [1.0] * 8,
            "intercept": 0.0,
            "coefficients": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "training_sample_count": 10,
            "global_residual_std": 0.20,
            "residual_eps": 1e-6,
            "confidence_groups": {
                "momentum|q5": {"sample_count": 10, "residual_std": 0.05, "recent_rank_correlation": 0.5},
            },
            "confidence_group_min_samples": 5,
            "confidence_raw_min": 1.0 / (1e-6 + 0.20),
            "confidence_raw_max": 1.0 / (1e-6 + 0.05),
            "abs_predicted_ev_sorted": [0.1, 0.2, 0.4, 0.8],
            "global_recent_rank_correlation": 0.2,
        },
        candidate_rows=[
            {
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.80,
                "score_percentile": 0.90,
                "expected_horizon_days": 5,
                "recent_return_3d": 0.0,
                "recent_return_5d": 0.0,
                "recent_return_10d": 0.0,
                "recent_vol_20d": 0.0,
                "estimated_execution_cost_pct": 0.001,
                "weight_delta": 0.1,
            },
        ],
        use_confidence_weighting=True,
        confidence_scale=1.0,
        confidence_clip_min=0.5,
        confidence_clip_max=1.5,
        confidence_min_samples_per_bucket=5,
        confidence_shrinkage_enabled=True,
        confidence_component_residual_std_weight=1.0,
        confidence_component_magnitude_weight=0.0,
        confidence_component_model_performance_weight=0.0,
        normalize_scores=False,
        use_normalized_score_for_weighting=False,
    )

    assert predictions[0]["ev_confidence"] == pytest.approx(1.0)
    assert predictions[0]["ev_confidence_multiplier"] == pytest.approx(1.5)
    assert predictions[0]["ev_score_before_confidence"] == pytest.approx(0.8)
    assert predictions[0]["ev_score_after_confidence"] == pytest.approx(1.2)
    assert predictions[0]["ev_weighting_score"] == pytest.approx(1.2)


def test_score_trade_ev_regression_candidates_applies_shrinkage_and_multifactor_confidence() -> None:
    predictions = score_trade_ev_regression_candidates(
        model={
            "training_available": True,
            "signal_families": ["momentum"],
            "feature_means": [0.0] * 8,
            "feature_stds": [1.0] * 8,
            "intercept": 0.0,
            "coefficients": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "training_sample_count": 20,
            "global_residual_std": 0.20,
            "residual_eps": 1e-6,
            "confidence_groups": {
                "momentum|q5": {"sample_count": 2, "residual_std": 0.05, "recent_rank_correlation": 0.8},
            },
            "confidence_group_min_samples": 10,
            "confidence_raw_min": 1.0 / (1e-6 + 0.20),
            "confidence_raw_max": 1.0 / (1e-6 + 0.05),
            "abs_predicted_ev_sorted": [0.05, 0.10, 0.20, 0.40, 0.80],
            "global_recent_rank_correlation": 0.0,
        },
        candidate_rows=[
            {
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.80,
                "score_percentile": 0.90,
                "expected_horizon_days": 5,
                "recent_return_3d": 0.0,
                "recent_return_5d": 0.0,
                "recent_return_10d": 0.0,
                "recent_vol_20d": 0.0,
                "estimated_execution_cost_pct": 0.001,
                "weight_delta": 0.1,
            },
        ],
        use_confidence_weighting=True,
        confidence_scale=1.0,
        confidence_clip_min=0.5,
        confidence_clip_max=1.5,
        confidence_min_samples_per_bucket=10,
        confidence_shrinkage_enabled=True,
        confidence_component_residual_std_weight=0.6,
        confidence_component_magnitude_weight=0.2,
        confidence_component_model_performance_weight=0.2,
        normalize_scores=False,
        use_normalized_score_for_weighting=False,
    )

    row = predictions[0]
    assert row["sample_size_used"] == 2
    assert row["residual_std_bucket"] == pytest.approx(0.05)
    assert row["residual_std_global"] == pytest.approx(0.20)
    assert row["residual_std_final"] == pytest.approx(0.17)
    assert 0.0 <= row["residual_std_confidence"] <= 1.0
    assert 0.0 <= row["magnitude_confidence"] <= 1.0
    assert 0.0 <= row["model_performance_confidence"] <= 1.0
    assert 0.0 <= row["combined_confidence"] <= 1.0
    assert 0.5 <= row["ev_confidence_multiplier"] <= 1.5


def test_run_replay_trade_ev_regression_writes_confidence_artifacts(monkeypatch, tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    replay_root.mkdir(parents=True, exist_ok=True)
    lifecycle_rows = []
    for index, score in enumerate([0.15, 0.25, 0.35, 0.45, 0.70, 0.90], start=1):
        entry_date = pd.Timestamp("2025-01-01") + pd.Timedelta(days=index)
        exit_date = entry_date + pd.Timedelta(days=2)
        lifecycle_rows.append(
            {
                "trade_id": f"t{index}",
                "date": str(exit_date.date()),
                "entry_date": str(entry_date.date()),
                "exit_date": str(exit_date.date()),
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": score,
                "score_percentile_entry": score,
                "realized_return": score * 0.04,
            }
        )
    pd.DataFrame(lifecycle_rows).to_csv(replay_root / "replay_trade_ev_lifecycle.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-01", periods=90, freq="D"),
                "close": [100 + value for value in range(90)],
                "volume": [1_000_000] * 90,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    result = run_replay_trade_ev_regression(
        replay_root=replay_root,
        model_output_path=tmp_path / "artifacts" / "ev_model" / "ev_regression_model.pkl",
        expected_horizon_days=5,
        min_training_samples=2,
    )

    assert Path(result["artifact_paths"]["replay_trade_ev_confidence_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_confidence_summary_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_confidence_bucket_analysis_path"]).exists()
    summary = dict(result["summary"])
    assert "avg_ev_confidence" in summary
    assert "avg_ev_confidence_multiplier" in summary
    assert "confidence_absolute_error_correlation" in summary
    assert "top_vs_bottom_realized_return_spread" in summary


def test_run_replay_trade_ev_regression_prefers_execution_confidence_rows_for_replay_diagnostics(
    monkeypatch, tmp_path: Path
) -> None:
    replay_root = tmp_path / "replay"
    day_root = replay_root / "2025-01-03" / "paper"
    day_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_id": "t1",
                "date": "2025-01-06",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": 0.9,
                "score_percentile_entry": 0.9,
                "realized_return": 0.05,
            },
            {
                "trade_id": "t2",
                "date": "2025-01-07",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-07",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": 0.2,
                "score_percentile_entry": 0.2,
                "realized_return": -0.03,
            },
        ]
    ).to_csv(replay_root / "replay_trade_ev_lifecycle.csv", index=False)
    pd.DataFrame(
        [
            {
                "trade_id": "t1",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": 0.9,
                "score_percentile_entry": 0.9,
                "realized_return": 0.05,
            },
            {
                "trade_id": "t2",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-07",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "score_entry": 0.2,
                "score_percentile_entry": 0.2,
                "realized_return": -0.03,
            },
        ]
    ).to_csv(day_root / "trade_ev_lifecycle.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.9,
                "score_percentile": 0.9,
                "expected_horizon_days": 5,
                "recent_return_3d": 0.02,
                "recent_return_5d": 0.03,
                "recent_return_10d": 0.05,
                "recent_vol_20d": 0.2,
                "candidate_outcome": "executed",
                "regression_raw_ev_score": 0.04,
                "ev_confidence": 0.9,
                "ev_confidence_multiplier": 1.2,
                "residual_std_bucket": 0.05,
                "residual_std_global": 0.1,
                "residual_std_final": 0.06,
                "sample_size_used": 25,
                "residual_std_confidence": 0.8,
                "magnitude_confidence": 0.7,
                "model_performance_confidence": 0.9,
                "combined_confidence": 0.9,
                "ev_score_before_confidence": 0.04,
                "ev_score_after_confidence": 0.048,
                "ev_training_sample_count": 30,
                "confidence_source": "shrunk_bucket",
            },
            {
                "date": "2025-01-03",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.2,
                "score_percentile": 0.2,
                "expected_horizon_days": 5,
                "recent_return_3d": -0.01,
                "recent_return_5d": -0.02,
                "recent_return_10d": -0.03,
                "recent_vol_20d": 0.3,
                "candidate_outcome": "executed",
                "regression_raw_ev_score": -0.01,
                "ev_confidence": 0.1,
                "ev_confidence_multiplier": 0.8,
                "residual_std_bucket": 0.15,
                "residual_std_global": 0.1,
                "residual_std_final": 0.14,
                "sample_size_used": 4,
                "residual_std_confidence": 0.2,
                "magnitude_confidence": 0.3,
                "model_performance_confidence": 0.4,
                "combined_confidence": 0.1,
                "ev_score_before_confidence": -0.01,
                "ev_score_after_confidence": -0.008,
                "ev_training_sample_count": 30,
                "confidence_source": "shrunk_bucket",
            },
        ]
    ).to_csv(day_root / "trade_candidate_dataset.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-12-01", periods=90, freq="D"),
                "close": [100 + value for value in range(90)],
                "volume": [1_000_000] * 90,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    result = run_replay_trade_ev_regression(
        replay_root=replay_root,
        model_output_path=tmp_path / "artifacts" / "ev_model" / "ev_regression_model.pkl",
        expected_horizon_days=5,
        min_training_samples=50,
    )

    summary = dict(result["summary"])
    confidence_frame = pd.read_csv(result["artifact_paths"]["replay_trade_ev_confidence_path"])
    assert summary["confidence_source"] == "execution_candidate_join"
    assert summary["execution_confidence_row_count"] == 2
    assert summary["confidence_row_count"] == 2
    assert summary["prediction_count"] >= 0
    assert len(confidence_frame.index) == 2
    assert confidence_frame["ev_confidence"].max() == pytest.approx(0.9)
    assert confidence_frame["ev_confidence"].min() == pytest.approx(0.1)
