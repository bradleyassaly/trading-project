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


def test_build_trade_ev_reliability_history_dataset_populates_economic_targets(tmp_path: Path) -> None:
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
                "realized_return": -0.02,
            },
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
                "ev_weighting_score": 0.015,
                "requested_target_weight": 0.15,
                "estimated_execution_cost_pct": 0.002,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.04,
            },
            {
                "date": "2025-01-03",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "candidate_outcome": "executed",
                "signal_score": 0.4,
                "score_rank": 2,
                "score_percentile": 0.55,
                "regression_raw_ev_score": -0.01,
                "ev_weighting_score": -0.005,
                "requested_target_weight": 0.1,
                "estimated_execution_cost_pct": 0.003,
                "recent_return_3d": -0.01,
                "recent_return_5d": -0.02,
                "recent_return_10d": -0.03,
                "recent_vol_20d": 0.05,
            },
        ]
    ).to_csv(paper_dir / "trade_candidate_dataset.csv", index=False)

    rows, summary = build_trade_ev_reliability_history_dataset(
        history_root=replay_root,
        as_of_date="2025-01-07",
        recent_window=5,
        target_type="positive_net_realized_return",
        top_percentile=0.5,
        hurdle=0.01,
    )

    assert len(rows) == 2
    rows_by_symbol = {str(row["symbol"]): row for row in rows}
    assert rows_by_symbol["AAPL"]["sign_success"] == 1
    assert rows_by_symbol["AAPL"]["positive_net_realized_return"] == 1
    assert rows_by_symbol["AAPL"]["positive_realized_minus_cost_hurdle"] == 1
    assert rows_by_symbol["AAPL"]["reliability_target_value"] == 1
    assert rows_by_symbol["MSFT"]["positive_net_realized_return"] == 0
    assert rows_by_symbol["MSFT"]["reliability_target_value"] == 0
    assert summary["row_count"] == 2
    assert summary["target_type"] == "positive_net_realized_return"
    assert summary["positive_label_rate"] == pytest.approx(0.5)


def test_score_trade_ev_reliability_candidates_supports_usage_modes() -> None:
    training_rows = [
        {
            "trade_id": f"t{index}",
            "entry_date": f"2025-01-{index:02d}",
            "exit_date": f"2025-01-{index + 1:02d}",
            "symbol": "AAPL" if index % 2 else "MSFT",
            "strategy_id": "alpha",
            "signal_family": "momentum" if index % 2 else "mean_reversion",
            "score_entry": 0.1 * index,
            "score_percentile_entry": min(0.12 * index, 0.95),
            "score_bucket": "q5" if index > 3 else "q2",
            "predicted_return": 0.01 * index,
            "ev_weighting_score_entry": 0.01 * index,
            "target_weight_entry": 0.05 * index,
            "expected_horizon_days": 5,
            "estimated_execution_cost_pct": 0.001 * index,
            "recent_return_3d": 0.0,
            "recent_return_5d": 0.01,
            "recent_return_10d": 0.02,
            "recent_vol_20d": 0.1,
            "candidate_rank_pct": 0.2 * index,
            "predicted_return_rank_pct": 0.2 * index,
            "signal_dispersion": 0.3,
            "day_of_week": index % 5,
            "recent_model_hit_rate": 0.4 + (0.1 if index > 3 else 0.0),
            "recent_symbol_trade_frequency": 0.2,
            "recent_symbol_turnover": 0.4,
            "realized_return_after_costs": 0.03 if index > 3 else -0.02,
            "realized_minus_predicted_after_costs": 0.0,
            "sign_success": 1 if index > 3 else 0,
            "positive_net_realized_return": 1 if index > 3 else 0,
            "top_bucket_realized_return": 1 if index > 4 else 0,
            "positive_realized_minus_cost_hurdle": 1 if index > 4 else 0,
            "reliability_target_value": 1 if index > 3 else 0,
        }
        for index in range(1, 7)
    ]
    model = train_trade_ev_reliability_model(
        training_rows=training_rows,
        min_training_samples=2,
        target_type="positive_net_realized_return",
    )

    predictions = score_trade_ev_reliability_candidates(
        model=model,
        training_rows=training_rows,
        candidate_rows=[
            {
                "date": "2025-01-10",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_family": "momentum",
                "signal_score": 0.8,
                "score_rank": 1,
                "score_percentile": 0.9,
                "regression_raw_ev_score": 0.03,
                "ev_weighting_score": 0.03,
                "requested_target_weight": 0.2,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.1,
            },
            {
                "date": "2025-01-10",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_family": "mean_reversion",
                "signal_score": 0.2,
                "score_rank": 2,
                "score_percentile": 0.2,
                "regression_raw_ev_score": -0.01,
                "ev_weighting_score": -0.01,
                "requested_target_weight": 0.05,
                "estimated_execution_cost_pct": 0.003,
                "recent_return_3d": -0.01,
                "recent_return_5d": -0.02,
                "recent_return_10d": -0.03,
                "recent_vol_20d": 0.2,
            },
        ],
        usage_mode="hybrid",
        threshold=0.6,
        weight_multiplier_min=0.9,
        weight_multiplier_max=1.1,
        neutral_band=0.01,
        max_promoted_trades_per_day=1,
        recent_window=5,
        target_type="positive_net_realized_return",
    )

    assert len(predictions) == 2
    assert all(row["prediction_available"] is True for row in predictions)
    assert all(row["reliability_target_type"] == "positive_net_realized_return" for row in predictions)
    assert all(row["reliability_usage_mode"] == "hybrid" for row in predictions)
    assert all(0.0 <= float(row["ev_reliability"]) <= 1.0 for row in predictions)
    assert sum(bool(row["was_reliability_promoted"]) for row in predictions) <= 1
    assert any(bool(row["was_filtered_by_reliability"]) for row in predictions)


def test_run_replay_trade_ev_reliability_writes_economic_artifacts(tmp_path: Path) -> None:
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
                "ev_reliability_rank_pct": 1.0,
                "ev_reliability_multiplier": 1.1,
                "estimated_execution_cost_pct": 0.002,
                "weight_delta": 0.2,
                "reliability_turnover_delta": -0.01,
                "reliability_cost_drag_delta": -0.002,
                "was_reliability_promoted": True,
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
                "ev_reliability_rank_pct": 0.0,
                "ev_reliability_multiplier": 0.9,
                "estimated_execution_cost_pct": 0.003,
                "weight_delta": 0.1,
                "reliability_turnover_delta": 0.02,
                "reliability_cost_drag_delta": 0.003,
                "was_reliability_promoted": False,
                "reliability_training_sample_count": 12,
            },
        ]
    ).to_csv(paper_dir / "trade_candidate_dataset.csv", index=False)

    result = run_replay_trade_ev_reliability(replay_root=replay_root)

    assert Path(result["artifact_paths"]["replay_trade_ev_reliability_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_reliability_economic_analysis_path"]).exists()
    assert Path(result["artifact_paths"]["replay_ev_reliability_turnover_analysis_path"]).exists()
    summary = json.loads(
        Path(result["artifact_paths"]["replay_ev_reliability_summary_path"]).read_text(encoding="utf-8")
    )
    assert summary["row_count"] == 2
    assert "reliability_after_cost_correlation" in summary
    assert "reliability_top_vs_bottom_after_cost_spread" in summary
    assert "reliability_turnover_uplift" in summary
