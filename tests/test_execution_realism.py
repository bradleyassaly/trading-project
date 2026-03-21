from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount, LiveBrokerOrderRequest
from trading_platform.execution.models import (
    ExecutableOrder,
    ExecutionConfig,
    ExecutionRequest,
    ExecutionSimulationResult,
    ExecutionSummary,
    LiquidityDiagnostic,
    RejectedOrder,
)
from trading_platform.execution.realism import simulate_execution, write_execution_artifacts
from trading_platform.execution.reconciliation import ReconciliationResult
from trading_platform.live.preview import (
    LivePreviewConfig,
    LivePreviewHealthCheck,
    LivePreviewResult,
    write_live_dry_run_artifacts,
)
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.paper.service import write_paper_trading_artifacts


def _request(**overrides) -> ExecutionRequest:
    payload = {
        "symbol": "AAPL",
        "side": "BUY",
        "requested_shares": 10,
        "requested_notional": 1_000.0,
        "current_shares": 0,
        "target_shares": 10,
        "price": 100.0,
        "target_weight": 0.1,
    }
    payload.update(overrides)
    return ExecutionRequest(**payload)


def test_execution_order_rounding() -> None:
    result = simulate_execution(requests=[_request(requested_shares=23, requested_notional=2_300.0)], config=ExecutionConfig(lot_size=10))

    assert result.summary.executable_order_count == 1
    assert result.executable_orders[0].adjusted_shares == 20
    assert result.executable_orders[0].clipping_reason == "lot_rounding"


def test_execution_min_price_rejection() -> None:
    result = simulate_execution(requests=[_request(price=2.0, requested_notional=20.0)], config=ExecutionConfig(min_price=5.0))

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "below_min_price"


def test_execution_min_dollar_volume_rejection() -> None:
    result = simulate_execution(
        requests=[_request(average_daily_dollar_volume=50_000.0)],
        config=ExecutionConfig(min_average_dollar_volume=100_000.0),
    )

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "below_min_average_dollar_volume"


def test_execution_minimum_trade_filtering() -> None:
    result = simulate_execution(
        requests=[_request(requested_shares=2, requested_notional=20.0, price=10.0, target_shares=2)],
        config=ExecutionConfig(min_trade_notional=100.0),
    )

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "below_min_trade_notional"


def test_execution_participation_cap_clipping() -> None:
    result = simulate_execution(
        requests=[_request(requested_shares=200, requested_notional=2_000.0, price=10.0, average_daily_volume_shares=1_000.0)],
        config=ExecutionConfig(max_participation_of_adv=0.1),
    )

    assert result.summary.executable_order_count == 1
    assert result.executable_orders[0].adjusted_shares == 100
    assert result.executable_orders[0].clipping_reason == "adv_participation_cap"


def test_execution_partial_fill_reject_behavior() -> None:
    result = simulate_execution(
        requests=[_request(requested_shares=200, requested_notional=2_000.0, price=10.0, average_daily_volume_shares=1_000.0)],
        config=ExecutionConfig(max_participation_of_adv=0.1, partial_fill_behavior="reject"),
    )

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "adv_participation_cap"


def test_execution_short_disallowed_behavior() -> None:
    result = simulate_execution(
        requests=[_request(side="SELL", requested_shares=10, target_shares=-10, target_weight=-0.1)],
        config=ExecutionConfig(allow_shorts=False),
    )

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "shorts_disallowed"


def test_execution_short_borrow_block_behavior() -> None:
    result = simulate_execution(
        requests=[_request(symbol="TSLA", side="SELL", requested_shares=10, requested_notional=1_000.0, target_shares=-10, target_weight=-0.1)],
        config=ExecutionConfig(allow_shorts=True, enforce_short_borrow_proxy=True, short_borrow_blocklist=["TSLA"]),
    )

    assert result.summary.rejected_order_count == 1
    assert result.rejected_orders[0].rejection_reason == "short_borrow_unavailable"


def test_execution_max_short_gross_enforcement() -> None:
    result = simulate_execution(
        requests=[
            _request(symbol="AAPL", side="SELL", target_shares=-20, target_weight=-0.2, requested_shares=20, requested_notional=2_000.0),
            _request(symbol="MSFT", side="SELL", target_shares=-20, target_weight=-0.2, requested_shares=20, requested_notional=2_000.0),
        ],
        config=ExecutionConfig(max_short_gross_exposure=0.1),
        current_equity=10_000.0,
    )

    assert result.summary.clipped_order_count >= 1 or result.summary.rejected_order_count >= 1


def test_execution_max_turnover_enforcement() -> None:
    result = simulate_execution(
        requests=[
            _request(symbol="AAPL", requested_shares=50, requested_notional=5_000.0),
            _request(symbol="MSFT", requested_shares=50, requested_notional=5_000.0, price=50.0),
        ],
        config=ExecutionConfig(max_turnover_per_rebalance=0.2),
        current_equity=10_000.0,
    )

    assert result.summary.turnover_after_constraints <= 2_000.0 + 1e-9


def test_execution_affordability_and_cash_buffer_clipping() -> None:
    result = simulate_execution(
        requests=[_request(requested_shares=10, requested_notional=1_000.0)],
        config=ExecutionConfig(cash_buffer_bps=1_000.0, partial_fill_behavior="clip"),
        current_cash=500.0,
        current_equity=1_000.0,
    )

    assert result.summary.executable_order_count == 1
    assert result.executable_orders[0].adjusted_shares <= 4
    assert result.executable_orders[0].clipping_reason == "cash_buffer_or_affordability"


def test_execution_commission_calculations() -> None:
    per_share = simulate_execution(requests=[_request()], config=ExecutionConfig(commission_model_type="per_share", commission_per_share=0.01))
    bps = simulate_execution(requests=[_request()], config=ExecutionConfig(commission_model_type="bps", commission_bps=10.0))
    flat = simulate_execution(requests=[_request()], config=ExecutionConfig(commission_model_type="flat", flat_commission_per_order=1.5))

    assert round(per_share.executable_orders[0].commission, 6) == 0.1
    assert round(bps.executable_orders[0].commission, 6) == 1.0
    assert round(flat.executable_orders[0].commission, 6) == 1.5


def test_execution_slippage_calculations() -> None:
    fixed = simulate_execution(requests=[_request()], config=ExecutionConfig(slippage_model_type="fixed_bps", fixed_slippage_bps=5.0))
    spread = simulate_execution(requests=[_request()], config=ExecutionConfig(slippage_model_type="spread_plus_bps", half_spread_bps=2.0, fixed_slippage_bps=3.0))
    liquidity = simulate_execution(
        requests=[_request(average_daily_volume_shares=100.0)],
        config=ExecutionConfig(slippage_model_type="liquidity_scaled", half_spread_bps=2.0, liquidity_slippage_bps=10.0),
    )

    assert fixed.executable_orders[0].slippage_bps == 5.0
    assert spread.executable_orders[0].slippage_bps == 5.0
    assert liquidity.executable_orders[0].slippage_bps > 2.0


def test_execution_artifacts_are_deterministic(tmp_path: Path) -> None:
    result = simulate_execution(
        requests=[
            _request(symbol="MSFT", requested_notional=500.0, price=50.0),
            _request(symbol="AAPL", requested_notional=1_000.0, price=100.0),
        ],
        config=ExecutionConfig(),
    )

    paths = write_execution_artifacts(result, tmp_path)

    assert paths["requested_orders_path"].exists()
    assert paths["symbol_tradeability_report_path"].exists()
    executable_df = pd.read_csv(paths["executable_orders_path"])
    assert list(executable_df["symbol"]) == ["AAPL", "MSFT"]
    summary = json.loads(paths["execution_summary_json_path"].read_text(encoding="utf-8"))
    assert summary["requested_order_count"] == 2


def test_write_paper_trading_artifacts_includes_execution_outputs(tmp_path: Path) -> None:
    execution_summary = ExecutionSummary(
        requested_order_count=1,
        executable_order_count=1,
        rejected_order_count=0,
        clipped_order_count=0,
        requested_notional=1_000.0,
        executed_notional=1_001.0,
        expected_commission_total=1.25,
        expected_slippage_cost_total=1.0,
        expected_total_cost=2.25,
        turnover_before_constraints=1_000.0,
        turnover_after_constraints=1_001.0,
        rejected_order_ratio=0.0,
        clipped_order_ratio=0.0,
        liquidity_failure_count=0,
        short_borrow_failure_count=0,
        zero_executable_orders=False,
        max_participation_pct_adv=0.0,
        estimated_cost_bps_on_executed_notional=22.4775,
    )
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
                "execution_summary": execution_summary.to_dict(),
                "requested_orders": [_request().to_dict()],
                "executable_orders": [
                    ExecutableOrder(
                        symbol="AAPL",
                        side="BUY",
                        requested_shares=10,
                        requested_notional=1_000.0,
                        adjusted_shares=10,
                        adjusted_notional=1_001.0,
                        estimated_fill_price=100.1,
                        slippage_bps=10.0,
                        commission=1.25,
                        participation_pct_adv=None,
                        filled_fraction=1.0,
                        status="executable",
                    ).to_dict()
                ],
                "rejected_orders": [],
                "liquidity_constraints_report": [],
                "turnover_summary": [],
                "symbol_tradeability_report": [],
            }
        },
    )

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path)

    assert paths["execution_summary_json_path"].exists()
    summary = json.loads(paths["execution_summary_json_path"].read_text(encoding="utf-8"))
    assert summary["expected_total_cost"] == 2.25
    assert paths["requested_orders_path"].exists()


def test_write_live_dry_run_artifacts_includes_execution_outputs(tmp_path: Path) -> None:
    execution_result = ExecutionSimulationResult(
        requested_orders=[_request()],
        executable_orders=[
            ExecutableOrder(
                symbol="AAPL",
                side="BUY",
                requested_shares=10,
                requested_notional=1_000.0,
                adjusted_shares=10,
                adjusted_notional=1_001.0,
                estimated_fill_price=100.1,
                slippage_bps=10.0,
                commission=1.25,
                participation_pct_adv=None,
                filled_fraction=1.0,
                status="executable",
            )
        ],
        rejected_orders=[],
        summary=ExecutionSummary(
            requested_order_count=1,
            executable_order_count=1,
            rejected_order_count=0,
            clipped_order_count=0,
            requested_notional=1_000.0,
            executed_notional=1_001.0,
            expected_commission_total=1.25,
            expected_slippage_cost_total=1.0,
            expected_total_cost=2.25,
            turnover_before_constraints=1_000.0,
            turnover_after_constraints=1_001.0,
            rejected_order_ratio=0.0,
            clipped_order_ratio=0.0,
            liquidity_failure_count=0,
            short_borrow_failure_count=0,
            zero_executable_orders=False,
            max_participation_pct_adv=0.0,
            estimated_cost_bps_on_executed_notional=22.4775,
        ),
        liquidity_diagnostics=[
            LiquidityDiagnostic(
                symbol="AAPL",
                tradeable=True,
                reason="tradeable",
                price=100.0,
                average_daily_volume_shares=None,
                average_daily_dollar_volume=None,
                spread_bps=None,
                borrow_available=None,
                stale_market_data=False,
                requested_shares=10,
                adjusted_shares=10,
                participation_pct_adv=None,
            )
        ],
        turnover_rows=[],
        symbol_tradeability_rows=[],
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
    assert paths["live_execution_preview_summary_json_path"].exists()
    summary_payload = json.loads((tmp_path / "live_execution_preview_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["execution_summary"]["expected_total_cost"] == 2.25
