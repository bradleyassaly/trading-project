from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerOrderRequest
from trading_platform.execution.realism import (
    ExecutableOrder,
    ExecutionConfig,
    ExecutionOrderRequest,
    ExecutionSimulationResult,
    simulate_execution,
    write_execution_artifacts,
)
from trading_platform.execution.reconciliation import ReconciliationResult
from trading_platform.live.preview import (
    LivePreviewConfig,
    LivePreviewHealthCheck,
    LivePreviewResult,
    write_live_dry_run_artifacts,
)
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.paper.service import write_paper_trading_artifacts


def test_execution_order_rounding() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=23,
                reference_price=100.0,
            )
        ],
        config=ExecutionConfig(lot_size=10),
    )

    assert result.summary["executable_order_count"] == 1
    assert result.executable_orders[0].adjusted_quantity == 20
    assert result.executable_orders[0].clipping_reason == "lot_rounding"


def test_execution_minimum_trade_filtering() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=5,
                reference_price=10.0,
            )
        ],
        config=ExecutionConfig(minimum_trade_notional=100.0),
    )

    assert result.summary["rejected_order_count"] == 1
    assert result.rejected_orders[0].rejection_reason == "below_minimum_trade_notional"


def test_execution_participation_cap_clipping() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=200,
                reference_price=10.0,
                average_dollar_volume=10_000.0,
            )
        ],
        config=ExecutionConfig(max_participation_rate=0.1),
    )

    assert result.summary["executable_order_count"] == 1
    assert result.executable_orders[0].adjusted_quantity == 100
    assert result.executable_orders[0].clipping_reason == "participation_cap"


def test_execution_illiquid_symbol_rejection() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=10,
                reference_price=10.0,
                average_dollar_volume=50_000.0,
            )
        ],
        config=ExecutionConfig(minimum_average_dollar_volume=100_000.0),
    )

    assert result.summary["rejected_order_count"] == 1
    assert result.rejected_orders[0].rejection_reason == "below_minimum_average_dollar_volume"


def test_execution_fee_and_slippage_estimation() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=10,
                reference_price=100.0,
            )
        ],
        config=ExecutionConfig(
            commission_per_share=0.01,
            commission_bps=2.0,
            slippage_model_type="fixed_bps",
            spread_proxy_bps=2.0,
            market_impact_proxy_bps=5.0,
        ),
    )

    order = result.executable_orders[0]
    assert round(order.expected_fill_price, 4) == 100.06
    assert round(order.expected_slippage_bps, 4) == 6.0
    assert round(order.expected_fees, 5) == round((10 * 0.01) + ((10 * 100.06) * 0.0002), 5)


def test_execution_short_constraint_handling() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="SELL",
                requested_quantity=10,
                reference_price=100.0,
                target_quantity=-10,
            )
        ],
        config=ExecutionConfig(short_selling_allowed=False),
    )

    assert result.summary["rejected_order_count"] == 1
    assert result.rejected_orders[0].rejection_reason == "short_selling_disabled"


def test_execution_partial_fill_reject_behavior() -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(
                symbol="AAPL",
                side="BUY",
                requested_quantity=200,
                reference_price=10.0,
                average_dollar_volume=10_000.0,
            )
        ],
        config=ExecutionConfig(max_participation_rate=0.1, partial_fill_behavior="reject"),
    )

    assert result.summary["rejected_order_count"] == 1
    assert result.rejected_orders[0].rejection_reason == "participation_cap"


def test_execution_artifacts_are_deterministic(tmp_path: Path) -> None:
    result = simulate_execution(
        requests=[
            ExecutionOrderRequest(symbol="MSFT", side="BUY", requested_quantity=10, reference_price=50.0),
            ExecutionOrderRequest(symbol="AAPL", side="BUY", requested_quantity=10, reference_price=100.0),
        ],
        config=ExecutionConfig(),
    )

    paths = write_execution_artifacts(result, tmp_path)

    assert paths["execution_summary_json_path"].exists()
    executable_df = pd.read_csv(paths["executable_orders_path"])
    assert list(executable_df["symbol"]) == ["AAPL", "MSFT"]
    summary = json.loads(paths["execution_summary_json_path"].read_text(encoding="utf-8"))
    assert summary["requested_order_count"] == 2


def test_write_paper_trading_artifacts_includes_execution_outputs(tmp_path: Path) -> None:
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(cash=100_000.0),
        latest_prices={"AAPL": 100.0},
        latest_scores={},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[],
        diagnostics={
            "execution": {
                "execution_summary": {
                    "requested_order_count": 1,
                    "executable_order_count": 1,
                    "rejected_order_count": 0,
                    "expected_total_cost": 1.25,
                    "turnover_before_constraints": 1_000.0,
                    "turnover_after_constraints": 1_000.0,
                    "liquidity_breach_count": 0,
                    "short_availability_failures": 0,
                },
                "executable_orders": [
                    ExecutableOrder(
                        symbol="AAPL",
                        side="BUY",
                        requested_quantity=10,
                        adjusted_quantity=10,
                        reference_price=100.0,
                        expected_fill_price=100.1,
                        expected_slippage_bps=10.0,
                        expected_fees=1.25,
                        estimated_notional_traded=1_001.0,
                        participation_estimate=None,
                        target_weight=1.0,
                        current_quantity=0,
                        target_quantity=10,
                    ).to_dict()
                ],
                "rejected_orders": [],
                "liquidity_constraints_report": [],
                "turnover_summary": [],
            }
        },
    )

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path)

    assert paths["execution_summary_json_path"].exists()
    summary = json.loads(paths["execution_summary_json_path"].read_text(encoding="utf-8"))
    assert summary["expected_total_cost"] == 1.25


def test_write_live_dry_run_artifacts_includes_execution_outputs(tmp_path: Path) -> None:
    execution_result = ExecutionSimulationResult(
        executable_orders=[
            ExecutableOrder(
                symbol="AAPL",
                side="BUY",
                requested_quantity=10,
                adjusted_quantity=10,
                reference_price=100.0,
                expected_fill_price=100.1,
                expected_slippage_bps=10.0,
                expected_fees=1.25,
                estimated_notional_traded=1_001.0,
                participation_estimate=None,
                target_weight=1.0,
                current_quantity=0,
                target_quantity=10,
            )
        ],
        rejected_orders=[],
        summary={
            "requested_order_count": 1,
            "executable_order_count": 1,
            "rejected_order_count": 0,
            "expected_total_cost": 1.25,
            "turnover_before_constraints": 1_000.0,
            "turnover_after_constraints": 1_000.0,
            "liquidity_breach_count": 0,
            "short_availability_failures": 0,
        },
        liquidity_rows=[],
        turnover_rows=[],
    )
    result = LivePreviewResult(
        run_id="multi_strategy|2025-01-04",
        as_of="2025-01-04",
        config=LivePreviewConfig(symbols=["AAPL"], strategy="multi_strategy", output_dir=tmp_path),
        account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
        positions={},
        open_orders=[],
        latest_prices={"AAPL": 100.0},
        target_weights={"AAPL": 1.0},
        target_diagnostics={"target_selected_symbols": "AAPL", "target_selected_count": 1},
        reconciliation=ReconciliationResult(
            orders=[LiveBrokerOrderRequest(symbol="AAPL", side="BUY", quantity=10, reason="rebalance_to_target")],
            target_quantities={"AAPL": 10},
            current_quantities={},
            diagnostics={"investable_equity": 100_000.0, "target_weight_sum": 1.0, "order_count": 1},
        ),
        adjusted_orders=[LiveBrokerOrderRequest(symbol="AAPL", side="BUY", quantity=10, reason="rebalance_to_target")],
        order_adjustment_diagnostics={},
        execution_result=execution_result,
        reconciliation_rows=[],
        health_checks=[
            LivePreviewHealthCheck(
                check_name="output_ready",
                status="pass",
                message="ok",
                timestamp="2025-01-04",
                preset=None,
                strategy="multi_strategy",
                universe=None,
            )
        ],
    )

    paths = write_live_dry_run_artifacts(result)

    assert paths["execution_summary_json_path"].exists()
    summary_payload = json.loads((tmp_path / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["execution_summary"]["expected_total_cost"] == 1.25
