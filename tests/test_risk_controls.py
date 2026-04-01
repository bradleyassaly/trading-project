from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import (
    PaperExecutionSimulationOrder,
    PaperExecutionSimulationReport,
    PaperOrder,
    PaperPortfolioState,
    PaperTradingConfig,
    PaperTradingRunResult,
)
from trading_platform.reporting.outcome_attribution import (
    TradeAttributionAggregate,
    TradeOutcomeAttributionReport,
)
from trading_platform.risk.controls import apply_pretrade_risk_controls, build_paper_risk_control_report


def _order(*, symbol: str = "AAPL", quantity: int = 10) -> PaperOrder:
    return PaperOrder(
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        reference_price=100.0,
        target_weight=1.0,
        current_quantity=0,
        target_quantity=quantity,
        notional=float(quantity) * 100.0,
        reason="rebalance_to_target",
        provenance={"strategy_id": "alpha", "strategy_ownership": {"alpha": 1.0}},
    )


def test_apply_pretrade_risk_controls_restricts_orders_on_drawdown() -> None:
    state = PaperPortfolioState(cash=900.0, positions={}, initial_cash_basis=1_000.0)
    config = PaperTradingConfig(
        symbols=["AAPL"],
        risk_controls_enabled=True,
        risk_restrict_drawdown=0.05,
        risk_halt_drawdown=0.20,
        risk_restricted_order_quantity_scale=0.4,
    )

    orders, triggers, actions, events, operating_state = apply_pretrade_risk_controls(
        as_of="2026-04-01",
        orders=[_order(quantity=10)],
        state=state,
        config=config,
    )

    assert operating_state == "restricted"
    assert len(orders) == 1
    assert orders[0].quantity == 4
    assert triggers[0].trigger_type == "drawdown_warning"
    assert actions[0].action == "throttle_orders"
    assert events[0].new_state == "restricted"


def test_apply_pretrade_risk_controls_halts_orders_on_drawdown() -> None:
    state = PaperPortfolioState(cash=700.0, positions={}, initial_cash_basis=1_000.0)
    config = PaperTradingConfig(
        symbols=["AAPL"],
        risk_controls_enabled=True,
        risk_restrict_drawdown=0.05,
        risk_halt_drawdown=0.20,
    )

    orders, triggers, actions, events, operating_state = apply_pretrade_risk_controls(
        as_of="2026-04-01",
        orders=[_order(quantity=10)],
        state=state,
        config=config,
    )

    assert operating_state == "halted"
    assert orders == []
    assert triggers[0].trigger_type == "drawdown_breach"
    assert actions[0].action == "halt_trading"
    assert events[0].new_state == "halted"


def test_build_paper_risk_control_report_emits_scope_triggers() -> None:
    config = PaperTradingConfig(
        symbols=["AAPL"],
        risk_controls_enabled=True,
        risk_restrict_drawdown=0.05,
        risk_halt_drawdown=0.20,
        risk_restrict_forecast_gap=0.03,
        risk_halt_forecast_gap=0.05,
        risk_restrict_rejected_order_ratio=0.10,
        risk_halt_rejected_order_ratio=0.30,
        risk_restrict_execution_shortfall=0.20,
        risk_halt_execution_shortfall=0.60,
    )
    result = PaperTradingRunResult(
        as_of="2026-04-01",
        state=PaperPortfolioState(
            cash=700.0,
            positions={},
            initial_cash_basis=1_000.0,
        ),
        latest_prices={"AAPL": 100.0},
        latest_scores={},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[_order(symbol="AAPL", quantity=5)],
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=5,
                fill_price=100.0,
                notional=500.0,
            )
        ],
        attribution={"strategy_rows": [], "symbol_rows": [], "trade_rows": [], "summary": {}},
        outcome_attribution_report=TradeOutcomeAttributionReport(
            as_of="2026-04-01",
            aggregates=[
                TradeAttributionAggregate(
                    group_type="strategy",
                    group_key="alpha",
                    trade_count=2,
                    win_rate=0.5,
                    mean_predicted_net_return=0.03,
                    mean_realized_net_return=-0.04,
                    mean_forecast_gap=-0.07,
                    mean_alpha_error=-0.05,
                    mean_cost_error=0.02,
                    mean_execution_error=0.01,
                    total_realized_net_pnl=-70.0,
                    total_realized_cost=5.0,
                    regime_mismatch_count=1,
                ),
                TradeAttributionAggregate(
                    group_type="instrument",
                    group_key="AAPL",
                    trade_count=2,
                    win_rate=0.5,
                    mean_predicted_net_return=0.03,
                    mean_realized_net_return=-0.04,
                    mean_forecast_gap=-0.07,
                    mean_alpha_error=-0.05,
                    mean_cost_error=0.02,
                    mean_execution_error=0.01,
                    total_realized_net_pnl=-70.0,
                    total_realized_cost=5.0,
                    regime_mismatch_count=1,
                ),
            ],
            summary={"closed_trade_count": 2},
        ),
        execution_simulation_report=PaperExecutionSimulationReport(
            as_of="2026-04-01",
            orders=[
                PaperExecutionSimulationOrder(
                    symbol="AAPL",
                    side="BUY",
                    requested_quantity=10,
                    executable_quantity=3,
                    requested_notional=1_000.0,
                    executable_notional=300.0,
                    reference_price=100.0,
                    estimated_fill_price=100.0,
                    filled_fraction=0.3,
                    status="partial_fill",
                    rejection_reason=None,
                    provenance={"strategy_id": "alpha"},
                )
            ],
            summary={
                "requested_order_count": 1,
                "rejected_order_ratio": 0.35,
                "partial_fill_order_count": 1,
            },
        ),
    )

    report = build_paper_risk_control_report(
        result=result,
        config=config,
        starting_state=PaperPortfolioState(cash=700.0, positions={}, initial_cash_basis=1_000.0),
    )

    trigger_scopes = {(row.scope, row.trigger_type) for row in report.triggers}
    assert report.operating_state == "halted"
    assert ("portfolio", "drawdown_breach") in trigger_scopes
    assert ("strategy", "expected_realized_divergence") in trigger_scopes
    assert ("instrument", "expected_realized_divergence") in trigger_scopes
    assert ("portfolio", "execution_rejections") in trigger_scopes
    assert ("instrument", "execution_shortfall") in trigger_scopes
    assert any(row.action == "halt_trading" for row in report.actions)
    assert any(row.new_state == "halted" for row in report.events)
