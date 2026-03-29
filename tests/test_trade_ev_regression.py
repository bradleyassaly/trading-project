from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

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
