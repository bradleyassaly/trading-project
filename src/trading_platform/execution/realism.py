from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


SLIPPAGE_MODEL_TYPES = {"fixed_bps", "spread_plus_impact"}
PRICE_SOURCE_ASSUMPTIONS = {"close", "next_open", "vwap_proxy"}
PARTIAL_FILL_BEHAVIORS = {"clip", "reject"}
LIQUIDITY_BEHAVIORS = {"reject", "skip"}


@dataclass(frozen=True)
class ExecutionConfig:
    commission_per_share: float = 0.0
    commission_bps: float = 0.0
    slippage_model_type: str = "spread_plus_impact"
    spread_proxy_bps: float = 0.0
    market_impact_proxy_bps: float = 0.0
    max_participation_rate: float | None = None
    minimum_average_dollar_volume: float | None = None
    minimum_price: float | None = None
    lot_size: int = 1
    minimum_trade_notional: float = 25.0
    max_turnover_per_rebalance: float | None = None
    short_selling_allowed: bool = True
    short_borrow_availability: bool = True
    max_borrow_utilization: float | None = None
    price_source_assumption: str = "close"
    partial_fill_behavior: str = "clip"
    stale_quote_behavior: str = "reject"
    missing_liquidity_behavior: str = "reject"

    def __post_init__(self) -> None:
        if self.commission_per_share < 0 or self.commission_bps < 0:
            raise ValueError("commission values must be >= 0")
        if self.slippage_model_type not in SLIPPAGE_MODEL_TYPES:
            raise ValueError(f"Unsupported slippage_model_type: {self.slippage_model_type}")
        if self.max_participation_rate is not None and self.max_participation_rate < 0:
            raise ValueError("max_participation_rate must be >= 0")
        if self.minimum_average_dollar_volume is not None and self.minimum_average_dollar_volume < 0:
            raise ValueError("minimum_average_dollar_volume must be >= 0")
        if self.minimum_price is not None and self.minimum_price < 0:
            raise ValueError("minimum_price must be >= 0")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be > 0")
        if self.minimum_trade_notional < 0:
            raise ValueError("minimum_trade_notional must be >= 0")
        if self.max_turnover_per_rebalance is not None and self.max_turnover_per_rebalance < 0:
            raise ValueError("max_turnover_per_rebalance must be >= 0")
        if self.max_borrow_utilization is not None and self.max_borrow_utilization < 0:
            raise ValueError("max_borrow_utilization must be >= 0")
        if self.price_source_assumption not in PRICE_SOURCE_ASSUMPTIONS:
            raise ValueError(f"Unsupported price_source_assumption: {self.price_source_assumption}")
        if self.partial_fill_behavior not in PARTIAL_FILL_BEHAVIORS:
            raise ValueError(f"Unsupported partial_fill_behavior: {self.partial_fill_behavior}")
        if self.stale_quote_behavior not in LIQUIDITY_BEHAVIORS:
            raise ValueError(f"Unsupported stale_quote_behavior: {self.stale_quote_behavior}")
        if self.missing_liquidity_behavior not in LIQUIDITY_BEHAVIORS:
            raise ValueError(f"Unsupported missing_liquidity_behavior: {self.missing_liquidity_behavior}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionOrderRequest:
    symbol: str
    side: str
    requested_quantity: int
    reference_price: float
    target_weight: float = 0.0
    current_quantity: int = 0
    target_quantity: int = 0
    current_weight: float = 0.0
    average_dollar_volume: float | None = None
    borrow_available: bool | None = None
    reason: str = "rebalance_to_target"


@dataclass(frozen=True)
class ExecutableOrder:
    symbol: str
    side: str
    requested_quantity: int
    adjusted_quantity: int
    reference_price: float
    expected_fill_price: float
    expected_slippage_bps: float
    expected_fees: float
    estimated_notional_traded: float
    participation_estimate: float | None
    target_weight: float
    current_quantity: int
    target_quantity: int
    rejection_reason: str | None = None
    clipping_reason: str | None = None
    was_rejected: bool = False
    was_clipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSimulationResult:
    executable_orders: list[ExecutableOrder]
    rejected_orders: list[ExecutableOrder]
    summary: dict[str, Any]
    liquidity_rows: list[dict[str, Any]]
    turnover_rows: list[dict[str, Any]]


def load_execution_requests_from_csv(path: str | Path) -> list[ExecutionOrderRequest]:
    frame = pd.read_csv(path)
    required = {"symbol", "side", "requested_quantity", "reference_price"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Targets file missing required columns: {sorted(missing)}")
    rows: list[ExecutionOrderRequest] = []
    for row in frame.to_dict(orient="records"):
        rows.append(
            ExecutionOrderRequest(
                symbol=str(row["symbol"]),
                side=str(row["side"]),
                requested_quantity=int(row["requested_quantity"]),
                reference_price=float(row["reference_price"]),
                target_weight=float(row.get("target_weight", 0.0) or 0.0),
                current_quantity=int(row.get("current_quantity", 0) or 0),
                target_quantity=int(row.get("target_quantity", 0) or 0),
                current_weight=float(row.get("current_weight", 0.0) or 0.0),
                average_dollar_volume=float(row["average_dollar_volume"]) if row.get("average_dollar_volume") == row.get("average_dollar_volume") and row.get("average_dollar_volume") is not None else None,
                borrow_available=bool(row.get("borrow_available")) if row.get("borrow_available") in {True, False, 0, 1} else None,
                reason=str(row.get("reason", "rebalance_to_target")),
            )
        )
    return rows


def _round_lot(quantity: int, lot_size: int) -> int:
    return (abs(int(quantity)) // lot_size) * lot_size


def _slippage_bps(config: ExecutionConfig, participation: float | None) -> float:
    spread_component = float(config.spread_proxy_bps) * 0.5
    if config.slippage_model_type == "fixed_bps":
        return spread_component + float(config.market_impact_proxy_bps)
    impact = float(config.market_impact_proxy_bps) * float(participation or 0.0)
    return spread_component + impact


def simulate_execution(
    *,
    requests: list[ExecutionOrderRequest],
    config: ExecutionConfig,
) -> ExecutionSimulationResult:
    executable_orders: list[ExecutableOrder] = []
    rejected_orders: list[ExecutableOrder] = []
    liquidity_rows: list[dict[str, Any]] = []
    turnover_rows: list[dict[str, Any]] = []
    requested_notional_total = 0.0
    executed_notional_total = 0.0
    total_expected_cost = 0.0

    for request in sorted(requests, key=lambda item: (item.symbol, item.side, item.requested_quantity)):
        requested_qty = abs(int(request.requested_quantity))
        requested_notional = requested_qty * float(request.reference_price)
        requested_notional_total += requested_notional
        reason: str | None = None
        clipping_reason: str | None = None
        adjusted_qty = requested_qty
        participation_estimate = None

        if request.reference_price <= 0:
            reason = "stale_or_missing_quote"
        elif config.minimum_price is not None and request.reference_price < config.minimum_price:
            reason = "below_minimum_price"
        elif (
            request.side == "SELL"
            and request.target_quantity < 0
            and not config.short_selling_allowed
        ):
            reason = "short_selling_disabled"
        elif (
            request.side == "SELL"
            and request.target_quantity < 0
            and not (request.borrow_available if request.borrow_available is not None else config.short_borrow_availability)
        ):
            reason = "short_borrow_unavailable"

        adv = request.average_dollar_volume
        if reason is None and config.minimum_average_dollar_volume is not None:
            if adv is None:
                reason = "missing_liquidity_data"
            elif adv < config.minimum_average_dollar_volume:
                reason = "below_minimum_average_dollar_volume"

        if reason is None and adv is not None and config.max_participation_rate is not None and request.reference_price > 0:
            adv_shares = adv / request.reference_price
            max_qty = _round_lot(int(adv_shares * config.max_participation_rate), config.lot_size)
            if max_qty <= 0:
                reason = "participation_limit_zero"
            elif adjusted_qty > max_qty:
                if config.partial_fill_behavior == "clip":
                    adjusted_qty = max_qty
                    clipping_reason = "participation_cap"
                else:
                    reason = "participation_cap"
            participation_estimate = float(adjusted_qty / adv_shares) if adv_shares > 0 else None

        if reason is None and config.max_borrow_utilization is not None and request.target_quantity < 0 and adv:
            borrow_utilization = abs(request.target_quantity * request.reference_price) / adv if adv > 0 else None
            if borrow_utilization is not None and borrow_utilization > config.max_borrow_utilization:
                reason = "borrow_utilization_limit"

        if reason is None:
            adjusted_qty = _round_lot(adjusted_qty, config.lot_size)
            if adjusted_qty != requested_qty and clipping_reason is None:
                clipping_reason = "lot_rounding"
            if adjusted_qty <= 0:
                reason = "lot_rounding_zero"

        adjusted_notional = adjusted_qty * float(request.reference_price)
        if reason is None and adjusted_notional < config.minimum_trade_notional:
            reason = "below_minimum_trade_notional"

        slippage_bps = _slippage_bps(config, participation_estimate)
        slippage_fraction = slippage_bps / 10_000.0
        expected_fill_price = float(request.reference_price) * (1.0 + slippage_fraction if request.side == "BUY" else 1.0 - slippage_fraction)
        estimated_notional_traded = adjusted_qty * expected_fill_price if reason is None else 0.0
        expected_fees = (
            adjusted_qty * float(config.commission_per_share)
            + estimated_notional_traded * (float(config.commission_bps) / 10_000.0)
            if reason is None
            else 0.0
        )
        order = ExecutableOrder(
            symbol=request.symbol,
            side=request.side,
            requested_quantity=requested_qty,
            adjusted_quantity=0 if reason else adjusted_qty,
            reference_price=float(request.reference_price),
            expected_fill_price=expected_fill_price,
            expected_slippage_bps=slippage_bps,
            expected_fees=expected_fees,
            estimated_notional_traded=estimated_notional_traded,
            participation_estimate=participation_estimate,
            target_weight=float(request.target_weight),
            current_quantity=int(request.current_quantity),
            target_quantity=int(request.target_quantity),
            rejection_reason=reason,
            clipping_reason=clipping_reason,
            was_rejected=reason is not None,
            was_clipped=clipping_reason is not None and reason is None,
        )
        liquidity_rows.append(
            {
                "symbol": request.symbol,
                "requested_quantity": requested_qty,
                "adjusted_quantity": order.adjusted_quantity,
                "average_dollar_volume": adv,
                "participation_estimate": participation_estimate,
                "rejection_reason": reason or "",
                "clipping_reason": clipping_reason or "",
            }
        )
        turnover_rows.append(
            {
                "symbol": request.symbol,
                "requested_notional": requested_notional,
                "executed_notional": estimated_notional_traded,
                "expected_fees": expected_fees,
            }
        )
        if reason:
            rejected_orders.append(order)
        else:
            executable_orders.append(order)
            executed_notional_total += estimated_notional_traded
            total_expected_cost += expected_fees + (estimated_notional_traded * (slippage_bps / 10_000.0))

    turnover_before = requested_notional_total
    turnover_after = executed_notional_total
    if config.max_turnover_per_rebalance is not None and turnover_before > 0:
        ratio = turnover_after / turnover_before if turnover_before > 0 else 0.0
        if ratio > config.max_turnover_per_rebalance:
            scale = config.max_turnover_per_rebalance / ratio
            adjusted_executable: list[ExecutableOrder] = []
            for order in executable_orders:
                clipped_qty = _round_lot(int(order.adjusted_quantity * scale), config.lot_size)
                was_rejected = clipped_qty <= 0
                clipped_order = ExecutableOrder(
                    **{
                        **order.to_dict(),
                        "adjusted_quantity": 0 if was_rejected else clipped_qty,
                        "estimated_notional_traded": 0.0 if was_rejected else clipped_qty * order.expected_fill_price,
                        "expected_fees": 0.0 if was_rejected else (
                            clipped_qty * float(config.commission_per_share)
                            + (clipped_qty * order.expected_fill_price) * (float(config.commission_bps) / 10_000.0)
                        ),
                        "rejection_reason": "turnover_cap" if was_rejected else order.rejection_reason,
                        "clipping_reason": "turnover_cap",
                        "was_rejected": was_rejected,
                        "was_clipped": True,
                    }
                )
                if was_rejected:
                    rejected_orders.append(clipped_order)
                else:
                    adjusted_executable.append(clipped_order)
            executable_orders = adjusted_executable
            turnover_after = sum(order.estimated_notional_traded for order in executable_orders)
            total_expected_cost = sum(
                order.expected_fees + (order.estimated_notional_traded * (order.expected_slippage_bps / 10_000.0))
                for order in executable_orders
            )

    summary = {
        "requested_order_count": len(requests),
        "executable_order_count": len(executable_orders),
        "rejected_order_count": len(rejected_orders),
        "requested_notional": requested_notional_total,
        "executed_notional": turnover_after,
        "expected_total_cost": total_expected_cost,
        "turnover_before_constraints": turnover_before,
        "turnover_after_constraints": turnover_after,
        "liquidity_breach_count": sum(1 for row in liquidity_rows if row["rejection_reason"]),
        "short_availability_failures": sum(1 for row in liquidity_rows if row["rejection_reason"] == "short_borrow_unavailable"),
    }
    return ExecutionSimulationResult(
        executable_orders=executable_orders,
        rejected_orders=rejected_orders,
        summary=summary,
        liquidity_rows=liquidity_rows,
        turnover_rows=turnover_rows,
    )


def write_execution_artifacts(
    result: ExecutionSimulationResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    executable_orders_path = output_path / "executable_orders.csv"
    rejected_orders_path = output_path / "rejected_orders.csv"
    execution_summary_json_path = output_path / "execution_summary.json"
    execution_summary_md_path = output_path / "execution_summary.md"
    liquidity_constraints_path = output_path / "liquidity_constraints_report.csv"
    turnover_summary_path = output_path / "turnover_summary.csv"

    pd.DataFrame([order.to_dict() for order in result.executable_orders]).to_csv(executable_orders_path, index=False)
    pd.DataFrame([order.to_dict() for order in result.rejected_orders]).to_csv(rejected_orders_path, index=False)
    pd.DataFrame(result.liquidity_rows).to_csv(liquidity_constraints_path, index=False)
    pd.DataFrame(result.turnover_rows).to_csv(turnover_summary_path, index=False)
    execution_summary_json_path.write_text(json.dumps(result.summary, indent=2, default=str), encoding="utf-8")
    execution_summary_md_path.write_text(
        "\n".join(
            [
                "# Execution Summary",
                "",
                f"- Requested orders: `{result.summary['requested_order_count']}`",
                f"- Executable orders: `{result.summary['executable_order_count']}`",
                f"- Rejected orders: `{result.summary['rejected_order_count']}`",
                f"- Expected total cost: `{result.summary['expected_total_cost']}`",
                f"- Turnover before constraints: `{result.summary['turnover_before_constraints']}`",
                f"- Turnover after constraints: `{result.summary['turnover_after_constraints']}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "executable_orders_path": executable_orders_path,
        "rejected_orders_path": rejected_orders_path,
        "execution_summary_json_path": execution_summary_json_path,
        "execution_summary_md_path": execution_summary_md_path,
        "liquidity_constraints_report_path": liquidity_constraints_path,
        "turnover_summary_path": turnover_summary_path,
    }
