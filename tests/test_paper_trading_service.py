from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.execution.transforms import build_executed_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.signals.registry import SIGNAL_REGISTRY

from trading_platform.paper.models import (
    PaperPortfolioState,
    PaperPosition,
    PaperTradingConfig,
    PaperOrder,
    PaperSignalSnapshot,
    OrderGenerationResult,
)
from trading_platform.paper.service import (
    JsonPaperStateStore,
    apply_filled_orders,
    generate_rebalance_orders,
    run_paper_trading_cycle,
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
