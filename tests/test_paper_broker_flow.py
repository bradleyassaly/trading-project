
from __future__ import annotations

from pathlib import Path

import pandas as pd

import trading_platform.paper.service as paper_service
from trading_platform.broker.base import BrokerFill
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
from trading_platform.risk.pre_trade_checks import PreTradeCheckResult


def test_run_paper_trading_cycle_auto_apply_uses_broker(
    monkeypatch,
    tmp_path: Path,
) -> None:
    asset_returns = pd.DataFrame(
        {"AAPL": [0.0, 0.01], "MSFT": [0.0, 0.02]},
        index=pd.to_datetime(["2025-01-03", "2025-01-04"]),
    )
    scores = pd.DataFrame(
        {"AAPL": [1.0, 2.0], "MSFT": [1.5, 2.5]},
        index=asset_returns.index,
    )
    closes = pd.DataFrame(
        {"AAPL": [100.0, 101.0], "MSFT": [200.0, 202.0]},
        index=asset_returns.index,
    )

    monkeypatch.setattr(
        paper_service,
        "load_signal_snapshot",
        lambda **kwargs: PaperSignalSnapshot(
            asset_returns=asset_returns,
            scores=scores,
            closes=closes,
            skipped_symbols=[],
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"AAPL": 0.5},
            {"AAPL": 0.5},
            {"selected_symbols": ["AAPL"]},
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "generate_rebalance_orders",
        lambda **kwargs: OrderGenerationResult(
            orders=[
                PaperOrder(
                    symbol="AAPL",
                    side="BUY",
                    quantity=10,
                    reference_price=101.0,
                    target_weight=0.5,
                    current_quantity=0,
                    target_quantity=10,
                    notional=1_010.0,
                    reason="rebalance_to_target",
                )
            ],
            target_weights={"AAPL": 0.5},
            diagnostics={"order_count": 1},
        ),
    )

    def fail_if_called(**kwargs):
        raise AssertionError("apply_filled_orders should not be used once broker path is enabled")

    monkeypatch.setattr(paper_service, "apply_filled_orders", fail_if_called)
    monkeypatch.setattr(
        paper_service,
        "validate_orders",
        lambda **kwargs: PreTradeCheckResult(passed=True, violations=[]),
    )

    class FakePaperBroker:
        def __init__(self, *, state, config):
            self.state = state
            self.config = config

        def submit_orders(self, orders):
            assert len(orders) == 1
            order = orders[0]
            self.state.cash -= order.quantity * order.reference_price
            self.state.positions[order.symbol] = paper_service.PaperPosition(
                symbol=order.symbol,
                quantity=order.quantity,
                avg_price=order.reference_price,
                last_price=order.reference_price,
            )
            return [
                BrokerFill(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    fill_price=order.reference_price,
                    notional=order.quantity * order.reference_price,
                    commission=0.0,
                    slippage_bps=0.0,
                )
            ]

    monkeypatch.setattr(paper_service, "PaperBroker", FakePaperBroker)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        strategy="sma_cross",
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=True,
    )

    assert result.as_of == "2025-01-04"
    assert round(result.state.cash, 2) == 8_990.00
    assert result.state.positions["AAPL"].quantity == 10
    assert result.state.last_targets == {"AAPL": 0.5}
    assert result.diagnostics["risk_checks"]["passed"] is True
    assert result.diagnostics["fill_count"] == 1


def test_run_paper_trading_cycle_no_fill_leaves_state_unchanged(
    monkeypatch,
    tmp_path: Path,
) -> None:
    asset_returns = pd.DataFrame(
        {"AAPL": [0.0, 0.01]},
        index=pd.to_datetime(["2025-01-03", "2025-01-04"]),
    )
    scores = pd.DataFrame(
        {"AAPL": [1.0, 2.0]},
        index=asset_returns.index,
    )
    closes = pd.DataFrame(
        {"AAPL": [100.0, 101.0]},
        index=asset_returns.index,
    )

    monkeypatch.setattr(
        paper_service,
        "load_signal_snapshot",
        lambda **kwargs: PaperSignalSnapshot(
            asset_returns=asset_returns,
            scores=scores,
            closes=closes,
            skipped_symbols=[],
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"AAPL": 1.0},
            {"AAPL": 1.0},
            {"selected_symbols": ["AAPL"]},
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "generate_rebalance_orders",
        lambda **kwargs: OrderGenerationResult(
            orders=[
                PaperOrder(
                    symbol="AAPL",
                    side="BUY",
                    quantity=10,
                    reference_price=101.0,
                    target_weight=1.0,
                    current_quantity=0,
                    target_quantity=10,
                    notional=1_010.0,
                    reason="rebalance_to_target",
                )
            ],
            target_weights={"AAPL": 1.0},
            diagnostics={"order_count": 1},
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "validate_orders",
        lambda **kwargs: PreTradeCheckResult(passed=True, violations=[]),
    )

    class FailPaperBroker:
        def __init__(self, *args, **kwargs):
            raise AssertionError("PaperBroker should not be constructed when auto_apply_fills=False")

    monkeypatch.setattr(paper_service, "PaperBroker", FailPaperBroker)

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL"],
        strategy="sma_cross",
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=False,
    )

    assert result.state.cash == 10_000.0
    assert result.state.positions == {}
    assert len(result.orders) == 1


def test_run_paper_trading_cycle_raises_on_failed_risk_checks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    asset_returns = pd.DataFrame(
        {"AAPL": [0.0, 0.01]},
        index=pd.to_datetime(["2025-01-03", "2025-01-04"]),
    )
    scores = pd.DataFrame(
        {"AAPL": [1.0, 2.0]},
        index=asset_returns.index,
    )
    closes = pd.DataFrame(
        {"AAPL": [100.0, 101.0]},
        index=asset_returns.index,
    )

    monkeypatch.setattr(
        paper_service,
        "load_signal_snapshot",
        lambda **kwargs: PaperSignalSnapshot(
            asset_returns=asset_returns,
            scores=scores,
            closes=closes,
            skipped_symbols=[],
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "compute_latest_target_weights",
        lambda **kwargs: (
            "2025-01-04",
            {"AAPL": 1.0},
            {"AAPL": 1.0},
            {"selected_symbols": ["AAPL"]},
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "generate_rebalance_orders",
        lambda **kwargs: OrderGenerationResult(
            orders=[
                PaperOrder(
                    symbol="AAPL",
                    side="BUY",
                    quantity=500,
                    reference_price=101.0,
                    target_weight=1.0,
                    current_quantity=0,
                    target_quantity=500,
                    notional=50_500.0,
                    reason="rebalance_to_target",
                )
            ],
            target_weights={"AAPL": 1.0},
            diagnostics={"order_count": 1},
        ),
    )
    monkeypatch.setattr(
        paper_service,
        "validate_orders",
        lambda **kwargs: PreTradeCheckResult(
            passed=False,
            violations=["AAPL order notional exceeds max"],
        ),
    )

    state_store = JsonPaperStateStore(tmp_path / "paper_state.json")
    config = PaperTradingConfig(
        symbols=["AAPL"],
        strategy="sma_cross",
        top_n=1,
        initial_cash=10_000.0,
        min_trade_dollars=1.0,
    )

    try:
        run_paper_trading_cycle(
            config=config,
            state_store=state_store,
            auto_apply_fills=True,
        )
    except ValueError as exc:
        assert "Pre-trade checks failed" in str(exc)
        assert "AAPL order notional exceeds max" in str(exc)
    else:
        raise AssertionError("Expected ValueError for failed risk checks")
