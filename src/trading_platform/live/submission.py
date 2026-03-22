from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.broker.models import (
    BrokerConfig,
    BrokerExecutionSummary,
    BrokerOpenOrder,
    BrokerOrderRequest,
    BrokerOrderResult,
    LiveRiskCheckResult,
)
from trading_platform.broker.service import BrokerAdapter
from trading_platform.live.preview import LivePreviewResult


@dataclass
class LiveSubmissionResult:
    preview_result: LivePreviewResult
    broker_config: BrokerConfig
    validate_only: bool
    risk_checks: list[LiveRiskCheckResult]
    broker_order_requests: list[BrokerOrderRequest]
    broker_order_results: list[BrokerOrderResult]
    summary: BrokerExecutionSummary
    artifacts: dict[str, Path] = field(default_factory=dict)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{value}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_client_order_id(
    *,
    account_id: str | None,
    as_of: str,
    order: BrokerOrderRequest,
) -> str:
    payload = "|".join(
        [
            account_id or "unknown",
            as_of,
            order.symbol,
            order.side,
            str(order.quantity),
            order.order_type,
            order.time_in_force,
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"tp-{digest}"


def _projected_exposures(preview_result: LivePreviewResult) -> tuple[float, float, float]:
    weights = [float(value) for value in preview_result.target_weights.values()]
    gross = float(sum(abs(value) for value in weights))
    net = float(sum(weights))
    max_weight = float(max((abs(value) for value in weights), default=0.0))
    return gross, net, max_weight


def _position_change_notional(preview_result: LivePreviewResult) -> float:
    equity = float(preview_result.account.equity)
    max_change = 0.0
    for symbol, target_weight in preview_result.target_weights.items():
        price = float(preview_result.latest_prices.get(symbol, 0.0))
        current_value = float(preview_result.positions[symbol].market_value) if symbol in preview_result.positions else 0.0
        target_value = float(target_weight) * equity
        max_change = max(max_change, abs(target_value - current_value))
    return max_change


def _monitoring_status(path: str | None) -> tuple[str | None, dict[str, Any]]:
    payload = _safe_read_json(path)
    if not payload:
        return None, {}
    status = payload.get("status")
    if status is None and isinstance(payload.get("summary"), dict):
        status = payload["summary"].get("status")
    return str(status) if status is not None else None, payload


def _materially_identical_open_order(
    request: BrokerOrderRequest,
    open_orders: list[BrokerOpenOrder],
) -> BrokerOpenOrder | None:
    for open_order in open_orders:
        if str(open_order.status).lower() in {"cancelled", "canceled", "filled", "expired", "rejected"}:
            continue
        same_client = request.client_order_id is not None and request.client_order_id == open_order.client_order_id
        same_shape = (
            request.symbol == open_order.symbol
            and request.side == open_order.side
            and request.quantity == open_order.remaining_quantity
            and request.order_type == open_order.order_type
            and request.time_in_force == open_order.time_in_force
        )
        if same_client or same_shape:
            return open_order
    return None


def _transform_preview_orders(
    preview_result: LivePreviewResult,
    broker_config: BrokerConfig,
    open_orders: list[BrokerOpenOrder],
) -> tuple[list[BrokerOrderRequest], list[BrokerOrderResult]]:
    execution_by_symbol = {}
    if preview_result.execution_result is not None:
        execution_by_symbol = {
            item.symbol: item
            for item in preview_result.execution_result.executable_orders
        }

    requests: list[BrokerOrderRequest] = []
    skipped_results: list[BrokerOrderResult] = []
    for order in preview_result.adjusted_orders:
        requested_notional = float(order.quantity) * float(preview_result.latest_prices.get(order.symbol, 0.0))
        execution_row = execution_by_symbol.get(order.symbol)
        request = BrokerOrderRequest(
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            order_type=order.order_type if order.order_type in broker_config.allowed_order_types else broker_config.default_order_type,
            time_in_force=order.time_in_force,
            limit_price=order.limit_price,
            requested_notional=requested_notional,
            price_reference=float(preview_result.latest_prices.get(order.symbol, 0.0)),
            reason=order.reason,
            provenance=(execution_row.provenance if execution_row is not None else {}),
        )
        client_order_id = _build_client_order_id(
            account_id=preview_result.account.account_id,
            as_of=preview_result.as_of,
            order=request,
        )
        request = BrokerOrderRequest(
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            order_type=request.order_type,
            time_in_force=request.time_in_force,
            limit_price=request.limit_price,
            client_order_id=client_order_id,
            requested_notional=request.requested_notional,
            price_reference=request.price_reference,
            reason=request.reason,
            provenance=request.provenance,
        )
        duplicate_open_order = _materially_identical_open_order(request, open_orders)
        if duplicate_open_order is not None:
            skipped_results.append(
                BrokerOrderResult(
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    order_type=request.order_type,
                    time_in_force=request.time_in_force,
                    status="skipped",
                    submitted=False,
                    client_order_id=request.client_order_id,
                    broker_order_id=duplicate_open_order.broker_order_id,
                    message="materially identical open order already exists",
                )
            )
            continue
        requests.append(request)
    return requests, skipped_results


def _risk_check(
    *,
    check_name: str,
    passed: bool,
    hard_block: bool,
    message: str,
    severity: str = "warning",
    metric_value: Any = None,
    threshold_value: Any = None,
    context: dict[str, Any] | None = None,
) -> LiveRiskCheckResult:
    return LiveRiskCheckResult(
        check_name=check_name,
        passed=passed,
        hard_block=hard_block,
        message=message,
        severity="info" if passed else severity,
        metric_value=metric_value,
        threshold_value=threshold_value,
        context=context or {},
    )


def evaluate_live_risk_checks(
    *,
    preview_result: LivePreviewResult,
    broker_config: BrokerConfig,
    broker_adapter: BrokerAdapter,
    broker_order_requests: list[BrokerOrderRequest],
    open_orders: list[BrokerOpenOrder],
) -> tuple[list[LiveRiskCheckResult], str | None]:
    checks: list[LiveRiskCheckResult] = []
    gross, net, max_position_weight = _projected_exposures(preview_result)
    total_notional = float(sum(order.requested_notional for order in broker_order_requests))
    max_symbol_notional = float(max((order.requested_notional for order in broker_order_requests), default=0.0))
    max_position_change = _position_change_notional(preview_result)
    monitoring_status, monitoring_payload = _monitoring_status(broker_config.monitoring_status_path)
    market_timestamp = _parse_timestamp(preview_result.as_of)
    market_age_seconds = None
    if market_timestamp is not None:
        market_age_seconds = max((datetime.now(UTC) - market_timestamp).total_seconds(), 0.0)

    checks.append(
        _risk_check(
            check_name="live_trading_enabled",
            passed=bool(broker_config.live_trading_enabled),
            hard_block=True,
            message="live_trading_enabled must be true",
        )
    )
    manual_flag_present = bool(
        (not broker_config.require_manual_enable_flag)
        or (broker_config.manual_enable_flag_path and Path(broker_config.manual_enable_flag_path).exists())
    )
    checks.append(
        _risk_check(
            check_name="manual_enable_flag",
            passed=manual_flag_present,
            hard_block=True,
            message="manual enable flag required and missing" if not manual_flag_present else "manual enable satisfied",
            context={"manual_enable_flag_path": broker_config.manual_enable_flag_path},
        )
    )
    kill_switch_active = bool(
        broker_config.global_kill_switch_path and Path(broker_config.global_kill_switch_path).exists()
    )
    checks.append(
        _risk_check(
            check_name="global_kill_switch",
            passed=not kill_switch_active,
            hard_block=True,
            severity="critical",
            message="global kill switch active" if kill_switch_active else "kill switch clear",
            context={"global_kill_switch_path": broker_config.global_kill_switch_path},
        )
    )
    broker_healthy, broker_health_message = broker_adapter.health_check()
    checks.append(
        _risk_check(
            check_name="broker_health",
            passed=broker_healthy,
            hard_block=True,
            severity="critical",
            message=broker_health_message,
        )
    )
    account_id_matches = (
        True
        if not broker_config.expected_account_id
        else broker_config.expected_account_id == preview_result.account.account_id
    )
    checks.append(
        _risk_check(
            check_name="expected_account_id",
            passed=account_id_matches,
            hard_block=True,
            severity="critical",
            message=f"account_id={preview_result.account.account_id}",
            threshold_value=broker_config.expected_account_id,
        )
    )
    monitoring_ok = True
    if broker_config.require_clean_monitoring_status:
        monitoring_ok = (
            monitoring_status is not None
            and monitoring_status in set(broker_config.allowed_monitoring_statuses)
        )
    checks.append(
        _risk_check(
            check_name="monitoring_status",
            passed=monitoring_ok,
            hard_block=bool(broker_config.require_clean_monitoring_status),
            message=f"monitoring_status={monitoring_status}",
            metric_value=monitoring_status,
            threshold_value="|".join(broker_config.allowed_monitoring_statuses),
            context={"monitoring_status_path": broker_config.monitoring_status_path, "monitoring_payload": monitoring_payload},
        )
    )
    checks.append(
        _risk_check(
            check_name="non_empty_orders",
            passed=bool(broker_order_requests),
            hard_block=True,
            severity="critical",
            message=f"order_count={len(broker_order_requests)}",
        )
    )
    order_count_ok = (
        True
        if broker_config.max_orders_per_run is None
        else len(broker_order_requests) <= int(broker_config.max_orders_per_run)
    )
    checks.append(
        _risk_check(
            check_name="max_orders_per_run",
            passed=order_count_ok,
            hard_block=True,
            severity="critical",
            message=f"order_count={len(broker_order_requests)}",
            metric_value=len(broker_order_requests),
            threshold_value=broker_config.max_orders_per_run,
        )
    )
    total_notional_ok = (
        True
        if broker_config.max_total_notional_per_run is None
        else total_notional <= float(broker_config.max_total_notional_per_run)
    )
    checks.append(
        _risk_check(
            check_name="max_total_notional_per_run",
            passed=total_notional_ok,
            hard_block=True,
            severity="critical",
            message=f"total_notional={total_notional:.2f}",
            metric_value=total_notional,
            threshold_value=broker_config.max_total_notional_per_run,
        )
    )
    per_symbol_ok = (
        True
        if broker_config.max_symbol_notional_per_order is None
        else max_symbol_notional <= float(broker_config.max_symbol_notional_per_order)
    )
    checks.append(
        _risk_check(
            check_name="max_symbol_notional_per_order",
            passed=per_symbol_ok,
            hard_block=True,
            severity="critical",
            message=f"max_symbol_notional={max_symbol_notional:.2f}",
            metric_value=max_symbol_notional,
            threshold_value=broker_config.max_symbol_notional_per_order,
        )
    )
    gross_ok = (
        True
        if broker_config.max_gross_exposure is None
        else gross <= float(broker_config.max_gross_exposure)
    )
    checks.append(
        _risk_check(
            check_name="max_gross_exposure",
            passed=gross_ok,
            hard_block=True,
            severity="critical",
            message=f"projected_gross_exposure={gross:.4f}",
            metric_value=gross,
            threshold_value=broker_config.max_gross_exposure,
        )
    )
    net_ok = (
        True
        if broker_config.max_net_exposure is None
        else abs(net) <= float(broker_config.max_net_exposure)
    )
    checks.append(
        _risk_check(
            check_name="max_net_exposure",
            passed=net_ok,
            hard_block=True,
            severity="critical",
            message=f"projected_net_exposure={net:.4f}",
            metric_value=net,
            threshold_value=broker_config.max_net_exposure,
        )
    )
    position_weight_ok = (
        True
        if broker_config.max_position_weight is None
        else max_position_weight <= float(broker_config.max_position_weight)
    )
    checks.append(
        _risk_check(
            check_name="max_position_weight",
            passed=position_weight_ok,
            hard_block=True,
            severity="critical",
            message=f"projected_max_position_weight={max_position_weight:.4f}",
            metric_value=max_position_weight,
            threshold_value=broker_config.max_position_weight,
        )
    )
    position_change_ok = (
        True
        if broker_config.max_position_change_notional is None
        else max_position_change <= float(broker_config.max_position_change_notional)
    )
    checks.append(
        _risk_check(
            check_name="max_position_change_notional",
            passed=position_change_ok,
            hard_block=True,
            severity="critical",
            message=f"max_position_change_notional={max_position_change:.2f}",
            metric_value=max_position_change,
            threshold_value=broker_config.max_position_change_notional,
        )
    )
    shorts_present = any(float(weight) < 0.0 for weight in preview_result.target_weights.values())
    checks.append(
        _risk_check(
            check_name="allow_shorts_live",
            passed=(broker_config.allow_shorts_live or not shorts_present),
            hard_block=True,
            severity="critical",
            message=f"short_targets_present={shorts_present}",
        )
    )
    order_types_ok = all(order.order_type in set(broker_config.allowed_order_types) for order in broker_order_requests)
    checks.append(
        _risk_check(
            check_name="allowed_order_types",
            passed=order_types_ok,
            hard_block=True,
            severity="critical",
            message="all order types allowed" if order_types_ok else "one or more order types are not allowed",
            context={"allowed_order_types": broker_config.allowed_order_types},
        )
    )
    fresh_market_data_ok = True
    if broker_config.require_fresh_market_data:
        fresh_market_data_ok = (
            market_age_seconds is not None
            and broker_config.max_market_data_age_seconds is not None
            and market_age_seconds <= float(broker_config.max_market_data_age_seconds)
        )
    checks.append(
        _risk_check(
            check_name="market_data_freshness",
            passed=fresh_market_data_ok,
            hard_block=bool(broker_config.require_fresh_market_data),
            severity="critical",
            message=f"market_data_age_seconds={market_age_seconds}",
            metric_value=market_age_seconds,
            threshold_value=broker_config.max_market_data_age_seconds,
        )
    )
    open_orders_ok = not (
        broker_config.skip_submission_if_existing_open_orders and len(open_orders) > 0
    )
    checks.append(
        _risk_check(
            check_name="open_order_policy",
            passed=open_orders_ok,
            hard_block=bool(broker_config.skip_submission_if_existing_open_orders),
            severity="critical",
            message=f"open_order_count={len(open_orders)}",
            metric_value=len(open_orders),
        )
    )
    status = "pass"
    if any(not check.passed and check.hard_block for check in checks):
        status = "fail"
    elif any(not check.passed for check in checks):
        status = "warn"
    return checks, status


def write_live_submission_artifacts(result: LiveSubmissionResult, output_dir: str | Path) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    risk_json_path = output_path / "live_risk_checks.json"
    risk_md_path = output_path / "live_risk_checks.md"
    requests_path = output_path / "broker_order_requests.csv"
    results_path = output_path / "broker_order_results.csv"
    summary_json_path = output_path / "live_submission_summary.json"
    summary_md_path = output_path / "live_submission_summary.md"

    risk_rows = [check.to_dict() for check in result.risk_checks]
    risk_json_path.write_text(json.dumps(risk_rows, indent=2, default=str), encoding="utf-8")
    risk_md_lines = ["# Live Risk Checks", ""]
    for row in risk_rows:
        risk_md_lines.append(
            f"- `{row['severity']}` `{row['check_name']}` passed={row['passed']}: {row['message']}"
        )
    risk_md_path.write_text("\n".join(risk_md_lines) + "\n", encoding="utf-8")

    pd.DataFrame([request.to_dict() for request in result.broker_order_requests]).to_csv(requests_path, index=False)
    pd.DataFrame([order.to_dict() for order in result.broker_order_results]).to_csv(results_path, index=False)

    summary_payload = result.summary.to_dict()
    summary_payload["risk_checks"] = risk_rows
    summary_payload["validate_only"] = result.validate_only
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    summary_md_lines = [
        "# Live Submission Summary",
        "",
        f"- Broker: `{result.summary.broker_name}`",
        f"- Account: `{result.summary.account_id}`",
        f"- Validate only: `{result.validate_only}`",
        f"- Risk passed: `{result.summary.risk_passed}`",
        f"- Submitted: `{result.summary.submitted}`",
        f"- Requested orders: `{result.summary.requested_order_count}`",
        f"- Submitted orders: `{result.summary.submitted_order_count}`",
        f"- Skipped orders: `{result.summary.skipped_order_count}`",
        f"- Rejected orders: `{result.summary.rejected_order_count}`",
        f"- Duplicate skips: `{result.summary.duplicate_order_skip_count}`",
        f"- Total requested notional: `{result.summary.total_requested_notional:.2f}`",
        f"- Total submitted notional: `{result.summary.total_submitted_notional:.2f}`",
    ]
    summary_md_path.write_text("\n".join(summary_md_lines) + "\n", encoding="utf-8")
    return {
        "live_risk_checks_json_path": risk_json_path,
        "live_risk_checks_md_path": risk_md_path,
        "broker_order_requests_path": requests_path,
        "broker_order_results_path": results_path,
        "live_submission_summary_json_path": summary_json_path,
        "live_submission_summary_md_path": summary_md_path,
    }


def submit_live_orders(
    *,
    preview_result: LivePreviewResult,
    broker_config: BrokerConfig,
    broker_adapter: BrokerAdapter,
    validate_only: bool,
    output_dir: str | Path,
) -> LiveSubmissionResult:
    open_orders = broker_adapter.get_open_orders()
    broker_order_requests, skipped_results = _transform_preview_orders(
        preview_result,
        broker_config,
        open_orders,
    )
    risk_checks, risk_status = evaluate_live_risk_checks(
        preview_result=preview_result,
        broker_config=broker_config,
        broker_adapter=broker_adapter,
        broker_order_requests=broker_order_requests,
        open_orders=open_orders,
    )
    risk_passed = risk_status == "pass"
    cancel_results: list[BrokerOrderResult] = []
    submitted_results: list[BrokerOrderResult] = []
    cancel_all_invoked = False
    if risk_passed and broker_config.cancel_existing_open_orders_before_submit and open_orders:
        cancel_results = broker_adapter.cancel_all_orders()
        cancel_all_invoked = True

    if risk_passed:
        if validate_only:
            submitted_results = [
                BrokerOrderResult(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    order_type=order.order_type,
                    time_in_force=order.time_in_force,
                    status="validation_only",
                    submitted=False,
                    client_order_id=order.client_order_id,
                    message="validate-only mode",
                )
                for order in broker_order_requests
            ]
        else:
            submitted_results = broker_adapter.submit_orders(broker_order_requests)

    all_results = [*skipped_results, *cancel_results, *submitted_results]
    submitted_order_count = sum(1 for row in submitted_results if row.submitted)
    rejected_order_count = sum(1 for row in all_results if row.status in {"rejected", "failed"})
    duplicate_skip_count = sum(
        1 for row in skipped_results if row.message == "materially identical open order already exists"
    )
    total_requested_notional = float(sum(order.requested_notional for order in broker_order_requests))
    submitted_ids = {row.client_order_id for row in submitted_results if row.submitted}
    total_submitted_notional = float(
        sum(order.requested_notional for order in broker_order_requests if order.client_order_id in submitted_ids)
    )
    gross, net, max_position_weight = _projected_exposures(preview_result)
    summary = BrokerExecutionSummary(
        timestamp=_now_utc(),
        broker_name=broker_config.broker_name,
        account_id=preview_result.account.account_id,
        validate_only=validate_only,
        risk_passed=risk_passed,
        submitted=(submitted_order_count > 0 and not validate_only),
        requested_order_count=len(broker_order_requests),
        submitted_order_count=submitted_order_count,
        skipped_order_count=sum(1 for row in all_results if row.status == "skipped"),
        rejected_order_count=rejected_order_count,
        duplicate_order_skip_count=duplicate_skip_count,
        cancel_all_invoked=cancel_all_invoked,
        total_requested_notional=total_requested_notional,
        total_submitted_notional=total_submitted_notional,
        projected_gross_exposure=gross,
        projected_net_exposure=net,
        projected_max_position_weight=max_position_weight,
        hard_block_count=sum(1 for check in risk_checks if not check.passed and check.hard_block),
        warning_count=sum(1 for check in risk_checks if not check.passed and not check.hard_block),
        monitoring_status=_monitoring_status(broker_config.monitoring_status_path)[0],
    )
    result = LiveSubmissionResult(
        preview_result=preview_result,
        broker_config=broker_config,
        validate_only=validate_only,
        risk_checks=risk_checks,
        broker_order_requests=broker_order_requests,
        broker_order_results=all_results,
        summary=summary,
    )
    result.artifacts = write_live_submission_artifacts(result, output_dir)
    return result
