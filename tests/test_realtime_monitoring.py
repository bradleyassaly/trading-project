from __future__ import annotations

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import (
    PaperExecutionSimulationOrder,
    PaperExecutionSimulationReport,
    PaperPortfolioState,
    PaperPosition,
    PaperTradingRunResult,
)
from trading_platform.reporting.realtime_monitoring import (
    RealtimeMonitoringPayload,
    build_realtime_monitoring_payload,
)


def test_realtime_monitoring_payload_round_trips_and_exposes_fill_quality() -> None:
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(
            cash=9_000.0,
            positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=110.0)},
            initial_cash_basis=10_000.0,
            cumulative_realized_pnl=25.0,
            cumulative_execution_cost=3.0,
        ),
        latest_prices={"AAPL": 110.0},
        latest_scores={"AAPL": 1.0},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[],
        requested_orders=[],
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=101.0,
                notional=1_010.0,
                slippage_bps=5.0,
            )
        ],
        diagnostics={"accounting": {"realized_pnl_delta": 25.0}},
        execution_simulation_report=PaperExecutionSimulationReport(
            as_of="2025-01-04",
            config={"enabled": True},
            orders=[
                PaperExecutionSimulationOrder(
                    symbol="AAPL",
                    side="BUY",
                    requested_quantity=10,
                    executable_quantity=10,
                    requested_notional=1_000.0,
                    executable_notional=1_010.0,
                    reference_price=100.0,
                    estimated_fill_price=101.0,
                    filled_fraction=1.0,
                    status="executable",
                    slippage_bps=5.0,
                )
            ],
            summary={"partial_fill_order_count": 0},
        ),
    )

    payload = build_realtime_monitoring_payload(result=result)

    assert RealtimeMonitoringPayload.from_dict(payload.to_dict()) == payload
    metric_names = {row.metric_name for row in payload.metrics}
    assert {"equity", "drawdown", "gross_exposure", "fill_rate", "average_fill_slippage_bps"} <= metric_names
    assert payload.summary["fill_quality_available"] is True
