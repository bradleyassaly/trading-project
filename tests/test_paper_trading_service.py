from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.execution.transforms import build_executed_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.signals.registry import SIGNAL_REGISTRY

from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperPosition,
    PaperTradingConfig,
)
from trading_platform.paper.service import (
    JsonPaperStateStore,
    apply_filled_orders,
    generate_rebalance_orders,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)


def test_generate_rebalance_orders_creates_buys_and_sells() -> None:
    state = PaperPortfolioState(
        cash=5_000.0,
        positions={
            "AAPL": PaperPosition(symbol="AAPL", quantity=20, avg_price=100.0, last_price=150.0),
            "MSFT": PaperPosition(symbol="MSFT", quantity=10, avg_price=200.0, last_price=250.0),
        },
    )

    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.10, "NVDA": 0.30},
        latest_prices={"AAPL": 150.0, "MSFT": 250.0, "NVDA": 500.0},
        min_trade_dollars=1.0,
        lot_size=1,
    )

    assert {order.symbol for order in result.orders} == {"AAPL", "MSFT", "NVDA"}
    assert {order.side for order in result.orders if order.symbol == "MSFT"} == {"SELL"}
    assert {order.side for order in result.orders if order.symbol == "NVDA"} == {"BUY"}


def test_generate_rebalance_orders_skips_small_weight_change() -> None:
    state = PaperPortfolioState(
        cash=9_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0)},
    )

    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.11},
        latest_prices={"AAPL": 100.0},
        min_trade_dollars=1.0,
        min_weight_change_to_trade=0.02,
        lot_size=1,
    )

    assert result.orders == []
    assert result.diagnostics["skipped_trades_count"] == 1
    assert result.diagnostics["skipped_turnover"] == pytest.approx(0.01)
    assert result.diagnostics["effective_turnover_reduction"] == pytest.approx(1.0)


def test_generate_rebalance_orders_keeps_large_weight_change() -> None:
    state = PaperPortfolioState(
        cash=9_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0)},
    )

    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.15},
        latest_prices={"AAPL": 100.0},
        min_trade_dollars=1.0,
        min_weight_change_to_trade=0.02,
        lot_size=1,
    )

    assert len(result.orders) == 1
    assert result.orders[0].symbol == "AAPL"
    assert result.diagnostics["skipped_trades_count"] == 0
    assert result.diagnostics["skipped_turnover"] == pytest.approx(0.0)


def test_generate_rebalance_orders_reports_turnover_reduction_for_mixed_changes() -> None:
    state = PaperPortfolioState(
        cash=8_000.0,
        positions={
            "AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0),
            "MSFT": PaperPosition(symbol="MSFT", quantity=10, avg_price=100.0, last_price=100.0),
        },
    )

    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.11, "MSFT": 0.20},
        latest_prices={"AAPL": 100.0, "MSFT": 100.0},
        min_trade_dollars=1.0,
        min_weight_change_to_trade=0.02,
        lot_size=1,
    )

    assert {order.symbol for order in result.orders} == {"MSFT"}
    assert result.diagnostics["skipped_trades_count"] == 1
    assert result.diagnostics["skipped_turnover"] == pytest.approx(0.01)
    assert result.diagnostics["effective_turnover_reduction"] == pytest.approx(0.01 / 0.11)


def test_generate_rebalance_orders_blocks_new_entry_below_entry_threshold() -> None:
    result = generate_rebalance_orders(
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.20},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.40},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.60,
            exit_score_threshold=0.40,
        ),
        min_trade_dollars=1.0,
    )

    assert result.orders == []
    assert result.diagnostics["blocked_entries_count"] == 1
    assert result.diagnostics["band_decision_rows"][0]["band_decision"] == "blocked_entry"


def test_generate_rebalance_orders_allows_new_entry_above_entry_threshold() -> None:
    result = generate_rebalance_orders(
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.20},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.90},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.60,
            exit_score_threshold=0.40,
        ),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.orders[0].symbol == "AAPL"


def test_generate_rebalance_orders_holds_position_in_hold_zone() -> None:
    state = PaperPortfolioState(
        cash=9_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0)},
    )
    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.0},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.50},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.80,
            exit_score_threshold=0.30,
            hold_score_band=True,
        ),
        min_trade_dollars=1.0,
    )

    assert result.orders == []
    assert result.diagnostics["held_in_hold_zone_count"] == 1
    assert result.diagnostics["band_decision_rows"][0]["band_decision"] == "hold_zone"


def test_generate_rebalance_orders_forces_exit_below_exit_threshold() -> None:
    state = PaperPortfolioState(
        cash=9_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0)},
    )
    result = generate_rebalance_orders(
        state=state,
        latest_target_weights={"AAPL": 0.0},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.10},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.80,
            exit_score_threshold=0.30,
        ),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.orders[0].side == "SELL"
    assert result.diagnostics["forced_exit_count"] == 1


def test_generate_rebalance_orders_combines_score_bands_with_weight_hysteresis() -> None:
    result = generate_rebalance_orders(
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.01},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.90},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.80,
            exit_score_threshold=0.50,
            min_weight_change_to_trade=0.02,
        ),
        min_trade_dollars=1.0,
        min_weight_change_to_trade=0.02,
    )

    assert result.orders == []
    assert result.diagnostics["blocked_entries_count"] == 0
    assert result.diagnostics["skipped_trades_count"] == 1


def test_generate_rebalance_orders_disabled_score_bands_preserve_prior_behavior() -> None:
    result = generate_rebalance_orders(
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.20},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.10},
        config=PaperTradingConfig(symbols=["AAPL"], min_trade_dollars=1.0),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.diagnostics["score_band_enabled"] is False


def test_generate_rebalance_orders_ev_gate_soft_mode_scales_target_weight(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([{"forward_net_return": 0.02}], {"training_sample_count": 1}),
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
                "expected_gross_return": 0.02,
                "expected_net_return": 0.02,
                "expected_cost": 0.001,
                "probability_positive": 0.7,
                "raw_ev_score": 0.02,
                "normalized_ev_score": 0.4,
                "ev_score_pre_clip": 0.4,
                "ev_score_post_clip": 0.4,
                "ev_score_clipped": False,
                "ev_weighting_score": 0.4,
                "ev_decision_score": 0.02,
                "normalization_method": "rank_pct",
                "normalize_within": "all_candidates",
                "candidate_count_for_normalization": 1,
                "ev_gate_threshold": 0.001,
                "ev_gate_decision": "allow",
                "ev_model_bucket": "global",
                "ev_training_sample_count": 10,
                "action_reason": "passed_ev_gate",
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
            ev_gate_mode="soft",
            ev_gate_weight_multiplier=True,
            ev_gate_weight_scale=10.0,
            ev_gate_normalize_scores=True,
            ev_gate_normalization_method="rank_pct",
            ev_gate_use_normalized_score_for_weighting=True,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.orders[0].target_weight == pytest.approx(1.0)
    assert result.orders[0].provenance["ev_weight_multiplier"] == pytest.approx(5.0)
    assert result.diagnostics["ev_gate_mode"] == "soft"
    assert result.diagnostics["ev_weighted_exposure"] == pytest.approx(1.0)
    assert result.diagnostics["avg_ev_executed_trades"] == pytest.approx(0.02)
    assert result.diagnostics["avg_normalized_ev_executed_trades"] == pytest.approx(0.4)
    assert result.diagnostics["avg_ev_weighting_score"] == pytest.approx(0.4)
    assert result.diagnostics["candidate_dataset_row_count"] == 1
    assert result.diagnostics["candidate_executed_count"] == 1
    assert result.diagnostics["candidate_trade_rows"][0]["candidate_outcome"] == "executed"


def test_generate_rebalance_orders_ev_gate_soft_mode_can_use_raw_score_for_weighting(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([{"forward_net_return": 0.02}], {"training_sample_count": 1}),
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
                "expected_gross_return": 0.02,
                "expected_net_return": 0.02,
                "expected_cost": 0.001,
                "probability_positive": 0.7,
                "raw_ev_score": 0.02,
                "normalized_ev_score": 0.4,
                "ev_score_pre_clip": 0.4,
                "ev_score_post_clip": 0.4,
                "ev_score_clipped": False,
                "ev_weighting_score": 0.02,
                "ev_decision_score": 0.02,
                "normalization_method": "rank_pct",
                "normalize_within": "all_candidates",
                "candidate_count_for_normalization": 1,
                "ev_gate_threshold": 0.001,
                "ev_gate_decision": "allow",
                "ev_model_bucket": "global",
                "ev_training_sample_count": 10,
                "action_reason": "passed_ev_gate",
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
            ev_gate_mode="soft",
            ev_gate_weight_multiplier=True,
            ev_gate_weight_scale=10.0,
            ev_gate_normalize_scores=True,
            ev_gate_normalization_method="rank_pct",
            ev_gate_use_normalized_score_for_weighting=False,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert len(result.orders) == 1
    assert result.orders[0].target_weight == pytest.approx(0.24)


def test_generate_rebalance_orders_persists_score_band_blocked_candidate() -> None:
    result = generate_rebalance_orders(
        as_of="2025-01-07",
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.20},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.40},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            min_trade_dollars=1.0,
            entry_score_threshold=0.60,
            exit_score_threshold=0.40,
        ),
        min_trade_dollars=1.0,
    )

    assert result.orders == []
    assert result.diagnostics["candidate_dataset_row_count"] == 1
    assert result.diagnostics["candidate_skipped_count"] == 1
    assert result.diagnostics["candidate_trade_rows"][0]["candidate_outcome"] == "score_band_blocked"


def test_generate_rebalance_orders_candidate_training_source_falls_back_to_executed(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_build(**kwargs):
        calls.append((kwargs["training_source"], kwargs["target_type"]))
        if kwargs["training_source"] == "candidate_decisions":
            return [], {"training_source": "candidate_decisions", "training_sample_count": 0}
        return [{"forward_net_return": 0.02}], {"training_source": "executed_trades", "training_sample_count": 1}

    monkeypatch.setattr("trading_platform.paper.service.build_trade_ev_training_dataset", fake_build)
    monkeypatch.setattr(
        "trading_platform.paper.service.train_trade_ev_model",
        lambda **kwargs: {"training_available": True, "training_sample_count": 1},
    )
    monkeypatch.setattr(
        "trading_platform.paper.service.score_trade_ev_candidates",
        lambda **kwargs: [
            {
                **kwargs["candidate_rows"][0],
                "expected_gross_return": 0.02,
                "expected_net_return": 0.01,
                "expected_cost": 0.001,
                "probability_positive": 0.6,
                "ev_decision_score": 0.01,
                "ev_gate_threshold": 0.0,
                "ev_gate_decision": "allow",
                "ev_model_bucket": "global",
                "ev_training_sample_count": 1,
                "action_reason": "passed_ev_gate",
            }
        ],
    )

    result = generate_rebalance_orders(
        as_of="2025-01-07",
        state=PaperPortfolioState(cash=10_000.0, positions={}),
        latest_target_weights={"AAPL": 0.20},
        latest_prices={"AAPL": 100.0},
        latest_scores={"AAPL": 0.90},
        config=PaperTradingConfig(
            symbols=["AAPL"],
            ev_gate_enabled=True,
            ev_gate_mode="soft",
            ev_gate_weight_multiplier=True,
            ev_gate_weight_scale=5.0,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            ev_gate_training_source="candidate_decisions",
            ev_gate_target_type="realized_candidate_proxy",
            ev_gate_min_training_samples=1,
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert calls == [
        ("candidate_decisions", "realized_candidate_proxy"),
        ("executed_trades", "realized_candidate_proxy"),
    ]
    assert result.diagnostics["ev_gate_training_source"] == "executed_trades"
    assert result.diagnostics["ev_gate_target_type"] == "realized_candidate_proxy"
    assert result.diagnostics["ev_gate_training_summary"]["fallback_reason"] == "insufficient_candidate_history_for_ev_gate"


def test_generate_rebalance_orders_ev_gate_soft_mode_can_still_block_extreme_negative(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([{"forward_net_return": -0.05}], {"training_sample_count": 1}),
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
                "expected_gross_return": -0.04,
                "expected_net_return": -0.03,
                "expected_cost": 0.001,
                "probability_positive": 0.1,
                "ev_decision_score": -0.03,
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
            ev_gate_mode="soft",
            ev_gate_weight_multiplier=True,
            ev_gate_weight_scale=10.0,
            ev_gate_extreme_negative_threshold=-0.02,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )

    assert result.orders == []
    assert result.diagnostics["ev_gate_blocked_count"] == 1


def test_generate_rebalance_orders_ev_gate_soft_mode_respects_multiplier_caps(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.service.build_trade_ev_training_dataset",
        lambda **kwargs: ([{"forward_net_return": 0.10}], {"training_sample_count": 1}),
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
                "expected_gross_return": 0.10,
                "expected_net_return": 0.08,
                "expected_cost": 0.01,
                "probability_positive": 0.9,
                "raw_ev_score": 0.08,
                "ev_decision_score": 0.08,
                "ev_gate_threshold": 0.001,
                "ev_gate_decision": "allow",
                "ev_model_bucket": "linear",
                "ev_training_sample_count": 10,
                "action_reason": "passed_ev_gate",
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
            ev_gate_mode="soft",
            ev_gate_weight_multiplier=True,
            ev_gate_weight_scale=10.0,
            ev_gate_weight_multiplier_min=0.5,
            ev_gate_weight_multiplier_max=1.25,
            ev_gate_training_root="artifacts/daily_replay/run_current",
            min_trade_dollars=1.0,
        ),
        min_trade_dollars=1.0,
    )
    assert len(result.orders) == 1
    assert result.orders[0].provenance["ev_weight_multiplier"] == pytest.approx(1.25)


def test_apply_filled_orders_updates_cash_and_positions() -> None:
    state = PaperPortfolioState(
        cash=10_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=100.0)},
    )
    state = apply_filled_orders(
        state=state,
        orders=[],
    )
    assert state.cash == 10_000.0


def test_run_paper_trading_cycle_builds_orders(monkeypatch, tmp_path: Path) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        dates = pd.date_range("2025-01-01", periods=4, freq="D")
        close_map = {
            "AAPL": [100.0, 101.0, 102.0, 103.0],
            "MSFT": [200.0, 201.0, 202.0, 203.0],
            "NVDA": [300.0, 301.0, 302.0, 303.0],
        }
        return pd.DataFrame(
            {
                "timestamp": dates,
                "close": close_map[symbol],
            }
        )

    def fake_signal_fn(df: pd.DataFrame, **_: object) -> pd.DataFrame:
        out = df.copy()
        out["asset_return"] = out["close"].pct_change().fillna(0.0)
        score_seed = float(out["close"].iloc[-1])
        out["score"] = [score_seed - 3.0, score_seed - 2.0, score_seed - 1.0, score_seed]
        return out

    monkeypatch.setattr(
        "trading_platform.paper.service.load_feature_frame",
        fake_load_feature_frame,
    )
    monkeypatch.setitem(SIGNAL_REGISTRY, "sma_cross", fake_signal_fn)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        strategy="sma_cross",
        top_n=2,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.as_of == "2025-01-04"
    assert len(result.orders) > 0
    assert result.state.cash == 10_000.0
    assert set(result.latest_prices) == {"AAPL", "MSFT", "NVDA"}


def test_run_paper_trading_cycle_supports_xsec_strategy(monkeypatch, tmp_path: Path) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        dates = pd.date_range("2025-01-01", periods=40, freq="D")
        close_map = {
            "AAPL": [100.0 + idx for idx in range(40)],
            "MSFT": [100.0 + (idx * 0.5) for idx in range(40)],
            "NVDA": [100.0 + (idx * 1.5) for idx in range(40)],
        }
        return pd.DataFrame(
            {
                "timestamp": dates,
                "close": close_map[symbol],
                "volume": [1_000_000.0] * 40,
            }
        )

    monkeypatch.setattr(
        "trading_platform.paper.service.load_feature_frame",
        fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "trading_platform.paper.service.resolve_feature_frame_path",
        lambda symbol: str(tmp_path / f"{symbol}.parquet"),
    )

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        symbols=["AAPL", "MSFT", "NVDA"],
        strategy="xsec_momentum_topn",
        lookback_bars=5,
        skip_bars=1,
        top_n=2,
        rebalance_bars=5,
        weighting_scheme="inv_vol",
        min_avg_dollar_volume=10_000.0,
        max_turnover_per_rebalance=0.5,
        portfolio_construction_mode="transition",
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.diagnostics["preset_name"] == "xsec_nasdaq100_momentum_v1_deploy"
    assert result.diagnostics["target_construction"]["portfolio_construction_mode"] == "transition"
    assert "rebalance_timestamp" in result.diagnostics["target_construction"]
    assert result.latest_target_weights
    assert set(result.latest_prices) == {"AAPL", "MSFT", "NVDA"}


def test_execution_policy_shift_is_reflected_in_effective_weights() -> None:
    raw_weights = pd.DataFrame(
        {
            "AAPL": [1.0, 0.0],
            "MSFT": [0.0, 1.0],
        },
        index=pd.to_datetime(["2025-01-01", "2025-01-02"]),
    )
    _, effective_weights = build_executed_weights(
        raw_weights,
        policy=ExecutionPolicy(timing="next_bar", rebalance_frequency="daily"),
    )
    assert effective_weights.iloc[0].sum() == 0.0
    assert effective_weights.iloc[1]["AAPL"] == 1.0


def test_run_paper_trading_cycle_selects_composite_mode(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 1,
                "horizon": 1,
                "mean_spearman_ic": 0.1,
                "mean_long_short_spread": 0.05,
                "promotion_status": "promote",
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame(
        columns=[
            "signal_family_a",
            "lookback_a",
            "horizon_a",
            "signal_family_b",
            "lookback_b",
            "horizon_b",
            "score_corr",
            "performance_corr",
            "rank_ic_corr",
        ]
    ).to_csv(artifact_dir / "redundancy_report.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        closes = {
            "AAPL": [100.0, 105.0, 110.0],
            "MSFT": [100.0, 100.0, 100.0],
            "NVDA": [100.0, 95.0, 90.0],
        }
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=3, freq="D"),
                "close": closes[symbol],
                "volume": [1_000_000.0] * 3,
            }
        )

    monkeypatch.setattr("trading_platform.paper.composite.load_feature_frame", fake_load_feature_frame)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT", "NVDA"],
        signal_source="composite",
        composite_artifact_dir=str(artifact_dir),
        composite_horizon=1,
        composite_weighting_scheme="equal",
        composite_portfolio_mode="long_only_top_n",
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.diagnostics["signal_source"] == "composite"
    assert result.latest_target_weights == {"AAPL": pytest.approx(1.0)}
    assert set(result.latest_scores) == {"AAPL", "MSFT", "NVDA"}
    assert len(result.orders) == 1
    assert result.orders[0].symbol == "AAPL"


def test_composite_paper_trading_applies_implementability_filters_before_orders(
    monkeypatch,
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 1,
                "horizon": 1,
                "mean_spearman_ic": 0.1,
                "mean_long_short_spread": 0.05,
                "promotion_status": "promote",
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame().to_csv(artifact_dir / "redundancy_report.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        closes = {
            "AAPL": [100.0, 105.0, 110.0],
            "MSFT": [100.0, 100.0, 100.0],
        }
        volumes = {
            "AAPL": [10.0, 10.0, 10.0],
            "MSFT": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=3, freq="D"),
                "close": closes[symbol],
                "volume": volumes[symbol],
            }
        )

    monkeypatch.setattr("trading_platform.paper.composite.load_feature_frame", fake_load_feature_frame)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        signal_source="composite",
        composite_artifact_dir=str(artifact_dir),
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
        min_volume=100.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.latest_target_weights == {}
    assert result.orders == []
    assert result.diagnostics["target_construction"]["reason"] == "no_eligible_names"
    assert result.diagnostics["liquidity_exclusions"]


def test_composite_order_generation_matches_target_weights(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 1,
                "horizon": 1,
                "mean_spearman_ic": 0.1,
                "mean_long_short_spread": 0.05,
                "promotion_status": "promote",
            }
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame().to_csv(artifact_dir / "redundancy_report.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        closes = {
            "AAPL": [100.0, 105.0, 110.0],
            "MSFT": [100.0, 100.0, 100.0],
        }
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=3, freq="D"),
                "close": closes[symbol],
                "volume": [1_000_000.0] * 3,
            }
        )

    monkeypatch.setattr("trading_platform.paper.composite.load_feature_frame", fake_load_feature_frame)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        signal_source="composite",
        composite_artifact_dir=str(artifact_dir),
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.latest_target_weights == {"AAPL": pytest.approx(1.0)}
    assert len(result.orders) == 1
    assert result.orders[0].symbol == "AAPL"
    assert result.orders[0].target_quantity == 90
    assert result.orders[0].target_weight == pytest.approx(1.0)


def test_composite_paper_trading_handles_no_approved_signals(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=[
            "signal_family",
            "lookback",
            "horizon",
            "mean_spearman_ic",
            "mean_long_short_spread",
            "promotion_status",
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)
    pd.DataFrame().to_csv(artifact_dir / "redundancy_report.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=3, freq="D"),
                "close": [100.0, 101.0, 102.0],
                "volume": [1_000_000.0] * 3,
            }
        )

    monkeypatch.setattr("trading_platform.paper.composite.load_feature_frame", fake_load_feature_frame)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL"],
        signal_source="composite",
        composite_artifact_dir=str(artifact_dir),
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.latest_target_weights == {}
    assert result.orders == []
    assert result.diagnostics["reason"] == "no_approved_signals"

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path / "artifacts")
    assert paths["daily_composite_scores_path"].exists()
    assert paths["approved_target_weights_path"].exists()
    assert paths["composite_diagnostics_path"].exists()


def test_run_paper_trading_cycle_supports_ensemble_signal_source(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "alpha"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 1,
                "horizon": 1,
                "mean_spearman_ic": 0.10,
                "promotion_status": "promote",
                "total_obs": 60,
                "candidate_id": "momentum|1|1",
            },
            {
                "signal_family": "momentum",
                "lookback": 2,
                "horizon": 1,
                "mean_spearman_ic": 0.08,
                "promotion_status": "promote",
                "total_obs": 60,
                "candidate_id": "momentum|2|1",
            },
        ]
    ).to_csv(artifact_dir / "promoted_signals.csv", index=False)

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        closes = {
            "AAPL": [100.0, 105.0, 110.0],
            "MSFT": [100.0, 100.0, 100.0],
            "NVDA": [100.0, 95.0, 90.0],
        }
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=3, freq="D"),
                "close": closes[symbol],
                "volume": [1_000_000.0] * 3,
            }
        )

    monkeypatch.setattr("trading_platform.paper.composite.load_feature_frame", fake_load_feature_frame)

    result = run_paper_trading_cycle(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT", "NVDA"],
            signal_source="ensemble",
            composite_artifact_dir=str(artifact_dir),
            composite_horizon=1,
            top_n=1,
            initial_cash=10_000.0,
            min_trade_dollars=1.0,
            ensemble_enabled=True,
            ensemble_mode="candidate_weighted",
            ensemble_weight_method="equal",
            ensemble_normalize_scores="rank_pct",
            ensemble_max_members=2,
        ),
        state_store=JsonPaperStateStore(tmp_path / "paper_state.json"),
        auto_apply_fills=False,
    )

    assert result.diagnostics["signal_source"] == "ensemble"
    assert result.diagnostics["paper_execution"]["ensemble_enabled"] is True
    assert result.latest_target_weights
    assert result.orders
    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path / "artifacts")
    assert paths["paper_ensemble_decision_snapshot_path"].exists()
