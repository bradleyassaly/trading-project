from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperPortfolioState, PaperTradingConfig
from trading_platform.paper.service import generate_rebalance_orders
from trading_platform.research.trade_ev import (
    build_trade_ev_calibration,
    build_trade_ev_training_dataset,
    build_trade_ev_candidate_market_features,
    evaluate_replay_trade_ev_predictions,
    score_trade_ev_candidates,
    train_trade_ev_model,
)


def _write_day(root: Path, date: str, *, symbol: str, score_percentile: float, cost_pct: float) -> None:
    day_dir = root / date
    (day_dir / "paper").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": date,
                "symbol": symbol,
                "strategy_id": "alpha",
                "signal_score": score_percentile,
                "rank": 1,
                "score_value": score_percentile,
                "score_rank": 1,
                "score_percentile": score_percentile,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "current_position": 0,
                "target_position": 10,
                "action": "buy",
                "action_reason": "enter_new_position",
            }
        ]
    ).to_csv(day_dir / "trade_decision_log.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": symbol,
                "side": "BUY",
                "quantity": 10,
                "reference_price": 100.0,
                "gross_notional": 1_000.0,
                "total_execution_cost": 1_000.0 * cost_pct,
            }
        ]
    ).to_csv(day_dir / "paper" / "paper_fills.csv", index=False)
    (day_dir / "paper" / "paper_run_summary_latest.json").write_text(
        json.dumps({"summary": {"current_equity": 100_000.0}}),
        encoding="utf-8",
    )


def _write_candidate_day(
    root: Path,
    date: str,
    *,
    symbol: str,
    score_percentile: float,
    requested_weight_delta: float,
    estimated_cost_pct: float,
    candidate_outcome: str,
) -> None:
    day_dir = root / date
    (day_dir / "paper").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": date,
                "symbol": symbol,
                "strategy_id": "alpha",
                "signal_score": score_percentile,
                "score_rank": 1,
                "score_percentile": score_percentile,
                "current_weight": 0.0,
                "target_weight": requested_weight_delta,
                "weight_delta": requested_weight_delta,
                "requested_target_weight": requested_weight_delta,
                "requested_weight_delta": requested_weight_delta,
                "adjusted_target_weight": requested_weight_delta,
                "adjusted_weight_delta": requested_weight_delta,
                "action": "buy" if requested_weight_delta > 0 else "sell",
                "action_type": "entry" if requested_weight_delta > 0 else "reduction",
                "current_position_held": 0,
                "estimated_execution_cost_pct": estimated_cost_pct,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.02,
                "dollar_volume": 1_000_000.0,
                "candidate_status": "executed" if candidate_outcome == "executed" else "skipped",
                "candidate_outcome": candidate_outcome,
                "candidate_stage": "execution" if candidate_outcome == "executed" else "score_band",
                "skip_reason": None if candidate_outcome == "executed" else "blocked_below_entry_threshold",
                "action_reason": "executed_candidate" if candidate_outcome == "executed" else "blocked_below_entry_threshold",
                "band_decision": "passed_entry" if candidate_outcome == "executed" else "blocked_entry",
                "entry_threshold": 0.9,
                "exit_threshold": 0.65,
                "score_band_enabled": True,
                "ev_gate_enabled": False,
                "ev_gate_mode": "soft",
                "ev_gate_decision": None,
                "probability_positive": None,
            }
        ]
    ).to_csv(day_dir / "paper" / "trade_candidate_dataset.csv", index=False)
    (day_dir / "paper" / "trade_ev_training_summary.json").write_text(
        json.dumps({"training_source": "candidate_decisions", "training_sample_count": 1}),
        encoding="utf-8",
    )


def test_build_trade_ev_training_dataset_respects_cutoff(monkeypatch, tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    _write_day(replay_root, "2025-01-03", symbol="AAPL", score_percentile=0.95, cost_pct=0.001)
    _write_day(replay_root, "2025-01-06", symbol="MSFT", score_percentile=0.55, cost_pct=0.001)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=10, freq="D"),
                "close": [100, 101, 102, 103, 104, 106, 108, 110, 111, 112],
                "volume": [1_000_000] * 10,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)

    rows, summary = build_trade_ev_training_dataset(
        history_root=replay_root,
        as_of_date="2025-01-06",
        horizon_days=3,
    )

    assert len(rows) == 1
    assert rows[0]["date"] == "2025-01-03"
    assert summary["training_sample_count"] == 1


def test_build_trade_ev_training_dataset_candidate_mode_includes_executed_and_skipped(
    monkeypatch,
    tmp_path: Path,
) -> None:
    replay_root = tmp_path / "replay"
    _write_candidate_day(
        replay_root,
        "2025-01-03",
        symbol="AAPL",
        score_percentile=0.95,
        requested_weight_delta=0.10,
        estimated_cost_pct=0.001,
        candidate_outcome="executed",
    )
    _write_candidate_day(
        replay_root,
        "2025-01-06",
        symbol="MSFT",
        score_percentile=0.30,
        requested_weight_delta=0.08,
        estimated_cost_pct=0.0015,
        candidate_outcome="score_band_blocked",
    )

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=20, freq="D"),
                "close": list(range(100, 120)),
                "volume": [1_000_000] * 20,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    rows, summary = build_trade_ev_training_dataset(
        history_root=replay_root,
        as_of_date="2025-01-07",
        horizon_days=5,
        training_source="candidate_decisions",
    )

    assert len(rows) == 2
    assert {row["symbol"] for row in rows} == {"AAPL", "MSFT"}
    assert summary["training_source"] == "candidate_decisions"
    assert summary["candidate_row_count"] == 2
    assert summary["executed_row_count"] == 1
    assert summary["skipped_row_count"] >= 1


def test_build_trade_ev_training_dataset_candidate_mode_respects_cutoff(monkeypatch, tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    _write_candidate_day(
        replay_root,
        "2025-01-03",
        symbol="AAPL",
        score_percentile=0.95,
        requested_weight_delta=0.10,
        estimated_cost_pct=0.001,
        candidate_outcome="executed",
    )
    _write_candidate_day(
        replay_root,
        "2025-01-07",
        symbol="MSFT",
        score_percentile=0.70,
        requested_weight_delta=0.09,
        estimated_cost_pct=0.001,
        candidate_outcome="executed",
    )

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=20, freq="D"),
                "close": list(range(100, 120)),
                "volume": [1_000_000] * 20,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    rows, summary = build_trade_ev_training_dataset(
        history_root=replay_root,
        as_of_date="2025-01-07",
        horizon_days=5,
        training_source="candidate_decisions",
    )

    assert len(rows) == 1
    assert rows[0]["date"] == "2025-01-03"
    assert summary["training_sample_count"] == 1


def test_bucketed_trade_ev_model_scores_candidates() -> None:
    model = train_trade_ev_model(
        training_rows=[
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_score": 0.9,
                "score_rank": 1,
                "score_percentile": 0.95,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_5d": 0.02,
                "recent_vol_20d": 0.01,
                "dollar_volume": 1_000_000.0,
                "forward_gross_return": 0.02,
                "forward_net_return": 0.019,
                "positive_net_return": 1,
            },
            {
                "date": "2025-01-04",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_score": 0.3,
                "score_rank": 10,
                "score_percentile": 0.15,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_5d": -0.01,
                "recent_vol_20d": 0.02,
                "dollar_volume": 1_000_000.0,
                "forward_gross_return": -0.005,
                "forward_net_return": -0.006,
                "positive_net_return": 0,
            },
        ],
        model_type="bucketed_mean",
        min_training_samples=2,
    )

    predictions = score_trade_ev_candidates(
        model=model,
        candidate_rows=[
            {
                "date": "2025-01-07",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_score": 0.9,
                "score_rank": 1,
                "score_percentile": 0.97,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_5d": 0.02,
                "recent_vol_20d": 0.01,
                "dollar_volume": 1_000_000.0,
            },
            {
                "date": "2025-01-07",
                "symbol": "MSFT",
                "strategy_id": "alpha",
                "signal_score": 0.2,
                "score_rank": 10,
                "score_percentile": 0.10,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_5d": -0.01,
                "recent_vol_20d": 0.02,
                "dollar_volume": 1_000_000.0,
            },
        ],
        min_expected_net_return=0.001,
        min_probability_positive=None,
        risk_penalty_lambda=0.0,
    )

    assert predictions[0]["ev_gate_decision"] == "allow"
    assert predictions[1]["ev_gate_decision"] == "block"


def test_bucketed_linear_trade_ev_model_scores_candidates() -> None:
    model = train_trade_ev_model(
        training_rows=[
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_score": 0.9,
                "score_rank": 1,
                "score_percentile": 0.95,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.03,
                "recent_vol_20d": 0.01,
                "dollar_volume": 1_000_000.0,
                "forward_gross_return": 0.03,
                "forward_net_return": 0.029,
                "positive_net_return": 1,
            },
            {
                "date": "2025-01-04",
                "symbol": "MSFT",
                "strategy_id": "beta",
                "signal_score": 0.2,
                "score_rank": 10,
                "score_percentile": 0.10,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.002,
                "recent_return_3d": -0.01,
                "recent_return_5d": -0.02,
                "recent_return_10d": -0.03,
                "recent_vol_20d": 0.03,
                "dollar_volume": 500_000.0,
                "forward_gross_return": -0.02,
                "forward_net_return": -0.022,
                "positive_net_return": 0,
            },
        ],
        model_type="bucketed_linear",
        min_training_samples=2,
    )
    predictions = score_trade_ev_candidates(
        model=model,
        candidate_rows=[
            {
                "date": "2025-01-07",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "signal_score": 0.8,
                "score_rank": 2,
                "score_percentile": 0.90,
                "current_weight": 0.0,
                "target_weight": 0.1,
                "weight_delta": 0.1,
                "action": "buy",
                "action_type": "entry",
                "current_position_held": 0,
                "estimated_execution_cost_pct": 0.001,
                "recent_return_3d": 0.01,
                "recent_return_5d": 0.02,
                "recent_return_10d": 0.02,
                "recent_vol_20d": 0.01,
                "dollar_volume": 1_000_000.0,
            }
        ],
        min_expected_net_return=0.0,
        min_probability_positive=None,
        risk_penalty_lambda=0.0,
        score_clip_min=-0.5,
        score_clip_max=0.5,
        normalize_scores=False,
    )
    assert predictions[0]["ev_model_bucket"] == "linear"
    assert predictions[0]["expected_net_return"] > 0.0


def test_trade_ev_candidate_market_features_reads_recent_features(monkeypatch) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=30, freq="D"),
                "close": list(range(100, 130)),
                "volume": [1_000_000] * 30,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    features = build_trade_ev_candidate_market_features(symbol="AAPL", as_of_date="2025-01-20")
    assert features["recent_return_5d"] != 0.0
    assert features["recent_vol_20d"] >= 0.0


def test_trade_ev_calibration_builds_bucket_summary() -> None:
    calibration_rows, summary = build_trade_ev_calibration(
        prediction_rows=[
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "expected_gross_return": 0.03,
                "expected_net_return": 0.02,
                "realized_gross_return": 0.04,
                "realized_net_return": 0.03,
                "execution_cost": 0.01,
                "probability_positive": 0.7,
                "ev_weight_multiplier": 1.2,
            },
            {
                "date": "2025-01-03",
                "symbol": "MSFT",
                "strategy_id": "beta",
                "expected_gross_return": -0.01,
                "expected_net_return": -0.02,
                "realized_gross_return": -0.02,
                "realized_net_return": -0.03,
                "execution_cost": 0.01,
                "probability_positive": 0.3,
                "ev_weight_multiplier": 0.8,
            },
        ],
        bucket_count=2,
    )
    assert len(calibration_rows) == 2
    assert "bucket_rows" in summary
    assert summary["trade_count"] == 2


def test_evaluate_replay_trade_ev_predictions_uses_prediction_files(monkeypatch, tmp_path: Path) -> None:
    day_dir = tmp_path / "2025-01-03" / "paper"
    day_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "date": "2025-01-03",
                "symbol": "AAPL",
                "strategy_id": "alpha",
                "weight_delta": 0.1,
                "action": "buy",
                "expected_net_return": 0.02,
                "expected_gross_return": 0.03,
                "expected_cost": 0.01,
                "probability_positive": 0.7,
                "ev_weight_multiplier": 1.1,
            }
        ]
    ).to_csv(day_dir / "trade_ev_predictions.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=10, freq="D"),
                "close": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
                "volume": [1_000_000] * 10,
            }
        )

    monkeypatch.setattr("trading_platform.research.trade_ev.load_feature_frame", fake_load_feature_frame)
    realized_rows, bucket_rows, summary = evaluate_replay_trade_ev_predictions(
        replay_root=tmp_path,
        horizon_days=3,
    )
    assert len(realized_rows) == 1
    assert len(bucket_rows) == 1
    assert summary["trade_count"] == 1


def test_generate_rebalance_orders_ev_gate_blocks_negative_expected_value_trade(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([{"forward_net_return": -0.01}], {"training_sample_count": 1}),
    )
    monkeypatch.setattr(
        "trading_platform.paper.service.train_trade_ev_model",
        lambda **kwargs: {"training_available": True, "training_sample_count": 10},
    )
    monkeypatch.setattr(
        "trading_platform.paper.service.score_trade_ev_candidates",
        lambda **kwargs: [
            {
                **kwargs["candidate_rows"][0],
                "expected_gross_return": 0.0,
                "expected_net_return": -0.01,
                "expected_cost": 0.001,
                "probability_positive": 0.2,
                "ev_decision_score": -0.01,
                "ev_gate_threshold": 0.001,
                "ev_gate_decision": "block",
                "ev_model_bucket": "global",
                "ev_training_sample_count": 10,
                "action_reason": "blocked_by_ev_gate",
            }
        ],
    )

    result = generate_rebalance_orders(
        as_of="2025-01-07",
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.2},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.9},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            ev_gate_enabled=True,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            ev_gate_min_expected_net_return=0.001,
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert result.orders == []
    assert result.diagnostics["ev_gate_blocked_count"] == 1


def test_generate_rebalance_orders_ev_gate_falls_back_when_history_is_insufficient(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([], {"training_sample_count": 0, "warnings": ["insufficient_trade_history_for_ev_gate"]}),
    )
    monkeypatch.setattr(
        "trading_platform.paper.service.train_trade_ev_model",
        lambda **kwargs: {"training_available": False, "training_sample_count": 0},
    )

    result = generate_rebalance_orders(
        as_of="2025-01-07",
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.2},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.9},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            ev_gate_enabled=True,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            ev_gate_min_expected_net_return=0.001,
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.diagnostics["ev_gate_blocked_count"] == 0
