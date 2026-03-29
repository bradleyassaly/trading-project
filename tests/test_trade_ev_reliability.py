from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.trade_ev_reliability import (
    build_trade_ev_reliability_history_dataset,
    run_replay_trade_ev_reliability,
    score_trade_ev_reliability_candidates,
    train_trade_ev_reliability_model,
)


def test_build_trade_ev_reliability_history_dataset_labels_success_and_residual(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    paper_dir = replay_root / "2025-01-03" / "paper"
    paper_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_id": "t1",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "realized_return": 0.03,
            }
        ]
    ).to_csv(paper_dir / "trade_ev_lifecycle.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "candidate_outcome": "executed",
                "signal_score": 0.9,
                "score_rank": 1,
                "score_percentile": 0.95,
                "regression_raw_ev_score": 0.02,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.04,
            }
        ]
    ).to_csv(paper_dir / "trade_candidate_dataset.csv", index=False)

    rows, summary = build_trade_ev_reliability_history_dataset(
        history_root=replay_root,
        as_of_date="2025-01-07",
        recent_window=5,
    )

    assert len(rows) == 1
    assert rows[0]["ev_success"] == 1
    assert rows[0]["realized_minus_predicted"] == pytest.approx(0.01)
    assert rows[0]["score_bucket"] == "q5"
    assert summary["row_count"] == 1
    assert summary["positive_label_rate"] == pytest.approx(1.0)


def test_score_trade_ev_reliability_candidates_returns_probabilities() -> None:
    training_rows = [
        {
            "trade_id": f"t{index}",
            "entry_date": "2025-01-01",
            "exit_date": "2025-01-02",
            "symbol": "AAPL",
            "strategy_id": "alpha",
            "signal_family": "momentum" if index % 2 else "mean_reversion",
            "score_entry": 0.1 * index,
            "score_percentile_entry": min(0.1 * index, 0.95),
            "score_bucket": "q5" if index > 3 else "q2",
            "predicted_return": 0.01 * index,
            "realized_return": 0.02 * index,
            "realized_minus_predicted": 0.01 * index,
            "ev_success": 1 if index > 2 else 0,
            "recent_return_3d": 0.0,
            "recent_return_5d": 0.0,
            "recent_return_10d": 0.0,
            "recent_vol_20d": 0.1,
            "candidate_rank_pct": 0.2 * index,
            "signal_dispersion": 0.3,
            "day_of_week": index % 5,
            "recent_model_success_rate": 0.5,
        }
        for index in range(1, 7)
    ]
    model = train_trade_ev_reliability_model(training_rows=training_rows, min_training_samples=2)

    predictions = score_trade_ev_reliability_candidates(
        model=model,
        candidate_rows=[
            {
                "date": "2025-01-07",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.8,
                "score_rank": 1,
                "score_percentile": 0.9,
                "regression_raw_ev_score": 0.03,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.1,
            }
        ],
    )

    assert len(predictions) == 1
    assert predictions[0]["prediction_available"] is True
    assert 0.0 <= predictions[0]["ev_reliability"] <= 1.0
    assert predictions[0]["ev_reliability_multiplier"] == pytest.approx(predictions[0]["ev_reliability"])


def test_run_replay_trade_ev_reliability_writes_artifacts(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    paper_dir = replay_root / "2025-01-03" / "paper"
    paper_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "trade_id": "t1",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "realized_return": 0.03,
            },
            {
                "trade_id": "t2",
                "entry_date": "2025-01-03",
                "exit_date": "2025-01-06",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "realized_return": -0.01,
            },
        ]
    ).to_csv(replay_root / "replay_trade_ev_lifecycle.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "candidate_outcome": "executed",
                "regression_raw_ev_score": 0.02,
                "ev_reliability": 0.8,
                "ev_reliability_multiplier": 0.8,
                "reliability_training_sample_count": 12,
            },
            {
                "date": "2025-01-03",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "candidate_outcome": "executed",
                "regression_raw_ev_score": -0.02,
                "ev_reliability": 0.3,
                "ev_reliability_multiplier": 0.3,
                "reliability_training_sample_count": 12,
            },
        ]
    ).to_csv(paper_dir / "trade_candidate_dataset.csv", index=False)

    result = run_replay_trade_ev_reliability(replay_root=replay_root)

    assert Path(result["artifact_paths"]["replay_trade_ev_reliability_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_reliability_analysis_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_reliability_summary_path"]).exists()
    summary = json.loads(
        Path(result["artifact_paths"]["replay_ev_reliability_summary_path"]).read_text(encoding="utf-8")
    )
    assert summary["row_count"] == 2
    assert "reliability_realized_return_correlation" in summary
