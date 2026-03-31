from __future__ import annotations

from trading_platform.broker.base import BrokerFill
from trading_platform.execution.order_lifecycle import (
    OrderCancellation,
    OrderFillRecord,
    OrderIntent,
    OrderLifecycleRecord,
    OrderStatusEvent,
    SubmittedOrder,
    build_paper_order_lifecycle_records,
)
from trading_platform.execution.reconciliation import build_order_lifecycle_reconciliation_skeleton
from trading_platform.paper.models import (
    PaperExecutionSimulationOrder,
    PaperExecutionSimulationReport,
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
)


def test_order_lifecycle_models_round_trip_deterministically() -> None:
    record = OrderLifecycleRecord(
        intent=OrderIntent(
            order_id="2025-01-04|AAPL|1",
            symbol="AAPL",
            side="BUY",
            quantity=10,
            reference_price=100.0,
            target_weight=0.25,
            reason="rebalance_to_target",
        ),
        submitted_order=SubmittedOrder(
            order_id="2025-01-04|AAPL|1",
            symbol="AAPL",
            side="BUY",
            quantity=10,
            order_type="market",
            time_in_force="day",
            status="submitted",
        ),
        fills=[
            OrderFillRecord(
                order_id="2025-01-04|AAPL|1",
                fill_id="2025-01-04|AAPL|1|fill|1",
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=100.1,
                notional=1001.0,
                fill_status="filled",
            )
        ],
        cancellations=[
            OrderCancellation(
                order_id="2025-01-04|AAPL|1",
                cancellation_id="cancel-1",
                symbol="AAPL",
                cancelled_quantity=0,
                reason_code="none",
            )
        ],
        status_events=[
            OrderStatusEvent(
                order_id="2025-01-04|AAPL|1",
                event_id="event-1",
                status="filled",
                event_type="fill_received",
            )
        ],
        final_status="filled",
    )

    assert OrderLifecycleRecord.from_dict(record.to_dict()) == record


def test_build_paper_order_lifecycle_records_supports_unfilled_and_filled_orders() -> None:
    orders = [
        PaperOrder(
            symbol="AAPL",
            side="BUY",
            quantity=10,
            reference_price=100.0,
            target_weight=0.25,
            current_quantity=0,
            target_quantity=10,
            notional=1_000.0,
            reason="rebalance_to_target",
        ),
        PaperOrder(
            symbol="MSFT",
            side="SELL",
            quantity=5,
            reference_price=200.0,
            target_weight=0.0,
            current_quantity=5,
            target_quantity=0,
            notional=1_000.0,
            reason="rebalance_to_target",
        ),
    ]
    fills = [
        BrokerFill(
            symbol="AAPL",
            side="BUY",
            quantity=10,
            fill_price=100.1,
            notional=1001.0,
        )
    ]

    records = build_paper_order_lifecycle_records(as_of="2025-01-04", orders=orders, fills=fills)

    assert records[0].final_status == "filled"
    assert records[1].final_status == "intent"


def test_build_order_lifecycle_reconciliation_skeleton_reports_structured_mismatches() -> None:
    lifecycle_records = build_paper_order_lifecycle_records(
        as_of="2025-01-04",
        orders=[
            PaperOrder(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                reference_price=100.0,
                target_weight=0.25,
                current_quantity=0,
                target_quantity=10,
                notional=1_000.0,
                reason="rebalance_to_target",
            )
        ],
        fills=[],
    )
    realized_state = PaperPortfolioState(
        cash=9_000.0,
        positions={"MSFT": PaperPosition(symbol="MSFT", quantity=5, avg_price=200.0, last_price=200.0)},
    )

    result = build_order_lifecycle_reconciliation_skeleton(
        as_of="2025-01-04",
        intended_target_weights={"AAPL": 0.25},
        lifecycle_records=lifecycle_records,
        realized_state=realized_state,
    )

    assert result.diagnostics["mismatch_count"] == 2
    assert {row.reason_code for row in result.mismatches} == {
        "intent_not_submitted_or_filled",
        "realized_position_without_intent",
    }


def test_build_paper_order_lifecycle_records_uses_simulated_partial_fill_when_no_fill_is_applied() -> None:
    report = PaperExecutionSimulationReport(
        as_of="2025-01-04",
        config={"enabled": True, "fill_delay_seconds": 60.0},
        orders=[
            PaperExecutionSimulationOrder(
                symbol="AAPL",
                side="BUY",
                requested_quantity=10,
                executable_quantity=4,
                requested_notional=1_000.0,
                executable_notional=402.0,
                reference_price=100.0,
                estimated_fill_price=100.5,
                filled_fraction=0.4,
                status="clipped",
                slippage_bps=5.0,
                spread_bps=2.0,
                submission_delay_seconds=30.0,
                fill_delay_seconds=60.0,
                clipping_reason="max_position_notional_change",
            )
        ],
        summary={"partial_fill_order_count": 1},
    )

    records = build_paper_order_lifecycle_records(
        as_of="2025-01-04",
        orders=[
            PaperOrder(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                reference_price=100.0,
                target_weight=0.25,
                current_quantity=0,
                target_quantity=10,
                notional=1_000.0,
                reason="rebalance_to_target",
            )
        ],
        fills=[],
        simulation_report=report,
    )

    assert records[0].final_status == "partially_filled"
    assert records[0].fills[0].quantity == 4
    assert records[0].cancellations[0].cancelled_quantity == 6
    assert records[0].status_events[1].event_type == "execution_simulated"
    assert records[0].fills[0].metadata["fill_delay_seconds"] == 60.0
