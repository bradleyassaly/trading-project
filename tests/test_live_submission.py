from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.broker.models import BrokerConfig
from trading_platform.broker.service import MockBrokerAdapter
from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerOrderRequest,
)
from trading_platform.execution.reconciliation import ReconciliationResult
from trading_platform.live.preview import LivePreviewConfig, LivePreviewResult
from trading_platform.live.submission import submit_live_orders


def _preview_result(
    tmp_path: Path,
    *,
    adjusted_orders: list[LiveBrokerOrderRequest] | None = None,
    target_weights: dict[str, float] | None = None,
    latest_prices: dict[str, float] | None = None,
    account_id: str = "acct-1",
) -> LivePreviewResult:
    latest_prices = latest_prices or {"AAPL": 100.0}
    target_weights = target_weights or {"AAPL": 0.5}
    adjusted_orders = adjusted_orders or [
        LiveBrokerOrderRequest(symbol="AAPL", side="BUY", quantity=100, order_type="market", time_in_force="day")
    ]
    return LivePreviewResult(
        run_id="run-1",
        as_of="2026-03-22T15:30:00+00:00",
        config=LivePreviewConfig(symbols=sorted(latest_prices), output_dir=tmp_path),
        account=BrokerAccount(account_id=account_id, cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
        positions={},
        open_orders=[],
        latest_prices=latest_prices,
        target_weights=target_weights,
        target_diagnostics={},
        reconciliation=ReconciliationResult(
            orders=adjusted_orders,
            target_quantities={"AAPL": 100},
            current_quantities={"AAPL": 0},
            diagnostics={"investable_equity": 100_000.0},
        ),
        adjusted_orders=adjusted_orders,
        order_adjustment_diagnostics={},
        execution_result=None,
        reconciliation_rows=[],
        health_checks=[],
    )


def _broker_config(tmp_path: Path, **overrides) -> BrokerConfig:
    payload = {
        "broker_name": "mock",
        "live_trading_enabled": True,
        "require_manual_enable_flag": False,
        "skip_submission_if_existing_open_orders": False,
        "mock_cash": 100_000.0,
        "mock_equity": 100_000.0,
    }
    payload.update(overrides)
    return BrokerConfig(**payload)


def test_live_trading_disabled_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path, live_trading_enabled=False)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert result.summary.submitted_order_count == 0
    assert any(check.check_name == "live_trading_enabled" and not check.passed for check in result.risk_checks)


def test_missing_manual_enable_flag_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(
        tmp_path,
        require_manual_enable_flag=True,
        manual_enable_flag_path=str(tmp_path / "manual_enable.flag"),
    )
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "manual_enable_flag" and not check.passed for check in result.risk_checks)


def test_kill_switch_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    kill_switch = tmp_path / "kill.switch"
    kill_switch.write_text("stop", encoding="utf-8")
    config = _broker_config(tmp_path, global_kill_switch_path=str(kill_switch))
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "global_kill_switch" and not check.passed for check in result.risk_checks)


def test_broker_health_failure_blocks_submission(tmp_path: Path) -> None:
    class FailingBroker(MockBrokerAdapter):
        def health_check(self):
            return False, "broker unavailable"

    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path)
    adapter = FailingBroker(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "broker_health" and not check.passed for check in result.risk_checks)


def test_account_mismatch_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path, account_id="acct-actual")
    config = _broker_config(tmp_path, expected_account_id="acct-expected")
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "expected_account_id" and not check.passed for check in result.risk_checks)


def test_order_count_cap_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(
        tmp_path,
        adjusted_orders=[
            LiveBrokerOrderRequest(symbol="AAPL", side="BUY", quantity=100),
            LiveBrokerOrderRequest(symbol="MSFT", side="BUY", quantity=50),
        ],
        latest_prices={"AAPL": 100.0, "MSFT": 200.0},
        target_weights={"AAPL": 0.4, "MSFT": 0.2},
    )
    config = _broker_config(tmp_path, max_orders_per_run=1)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "max_orders_per_run" and not check.passed for check in result.risk_checks)


def test_total_notional_cap_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path, max_total_notional_per_run=5_000.0)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "max_total_notional_per_run" and not check.passed for check in result.risk_checks)


def test_per_symbol_cap_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path, max_symbol_notional_per_order=5_000.0)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "max_symbol_notional_per_order" and not check.passed for check in result.risk_checks)


def test_shorts_disallowed_blocks_submission(tmp_path: Path) -> None:
    preview = _preview_result(
        tmp_path,
        adjusted_orders=[LiveBrokerOrderRequest(symbol="AAPL", side="SELL", quantity=100)],
        target_weights={"AAPL": -0.2},
    )
    config = _broker_config(tmp_path, allow_shorts_live=False)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is False
    assert any(check.check_name == "allow_shorts_live" and not check.passed for check in result.risk_checks)


def test_duplicate_open_order_protection_skips_submission(tmp_path: Path) -> None:
    open_orders_path = tmp_path / "open_orders.csv"
    pd.DataFrame(
        [
            {
                "broker_order_id": "open-1",
                "client_order_id": "",
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 100,
                "filled_quantity": 0,
                "order_type": "market",
                "time_in_force": "day",
                "status": "open",
            }
        ]
    ).to_csv(open_orders_path, index=False)
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path, mock_open_orders_path=str(open_orders_path))
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.duplicate_order_skip_count == 1
    assert any(row.status == "skipped" for row in result.broker_order_results)


def test_validate_only_does_not_submit(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=True,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is True
    assert result.summary.submitted is False
    assert result.summary.submitted_order_count == 0
    assert adapter.get_open_orders() == []


def test_successful_mock_broker_submission(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path)
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.risk_passed is True
    assert result.summary.submitted is True
    assert result.summary.submitted_order_count == 1
    assert any(row.status == "accepted" for row in result.broker_order_results)


def test_cancel_all_behavior(tmp_path: Path) -> None:
    open_orders_path = tmp_path / "open_orders.csv"
    pd.DataFrame(
        [
            {
                "broker_order_id": "open-1",
                "client_order_id": "cid-1",
                "symbol": "MSFT",
                "side": "BUY",
                "quantity": 10,
                "filled_quantity": 0,
                "order_type": "market",
                "time_in_force": "day",
                "status": "open",
            }
        ]
    ).to_csv(open_orders_path, index=False)
    preview = _preview_result(tmp_path)
    config = _broker_config(
        tmp_path,
        mock_open_orders_path=str(open_orders_path),
        cancel_existing_open_orders_before_submit=True,
        skip_submission_if_existing_open_orders=False,
    )
    adapter = MockBrokerAdapter(config)

    result = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter,
        validate_only=False,
        output_dir=tmp_path,
    )

    assert result.summary.cancel_all_invoked is True
    assert any(row.status == "cancelled" for row in result.broker_order_results)


def test_submission_artifacts_are_deterministic_for_requests(tmp_path: Path) -> None:
    preview = _preview_result(tmp_path)
    config = _broker_config(tmp_path)
    adapter_a = MockBrokerAdapter(config)
    adapter_b = MockBrokerAdapter(config)

    result_a = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter_a,
        validate_only=True,
        output_dir=tmp_path / "run_a",
    )
    result_b = submit_live_orders(
        preview_result=preview,
        broker_config=config,
        broker_adapter=adapter_b,
        validate_only=True,
        output_dir=tmp_path / "run_b",
    )

    requests_a = (tmp_path / "run_a" / "broker_order_requests.csv").read_text(encoding="utf-8")
    requests_b = (tmp_path / "run_b" / "broker_order_requests.csv").read_text(encoding="utf-8")
    summary_payload = json.loads((tmp_path / "run_a" / "live_submission_summary.json").read_text(encoding="utf-8"))

    assert requests_a == requests_b
    assert result_a.broker_order_requests[0].client_order_id == result_b.broker_order_requests[0].client_order_id
    assert summary_payload["validate_only"] is True
