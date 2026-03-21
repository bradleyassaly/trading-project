from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.execution.models import (
    ExecutableOrder,
    ExecutionConfig,
    ExecutionRequest,
    ExecutionSimulationResult,
    ExecutionSummary,
    LiquidityDiagnostic,
    MarketDataInput,
    RejectedOrder,
)


def _round_down_to_lot(shares: int, lot_size: int) -> int:
    if shares <= 0:
        return 0
    return (int(shares) // lot_size) * lot_size


def _normalize_market_data(
    symbol: str,
    price: float,
    market_data_inputs: dict[str, MarketDataInput] | None,
) -> MarketDataInput:
    provided = (market_data_inputs or {}).get(symbol)
    if provided is not None:
        return MarketDataInput(
            symbol=symbol,
            price=float(provided.price if provided.price > 0 else price),
            average_daily_volume_shares=provided.average_daily_volume_shares,
            average_daily_dollar_volume=provided.average_daily_dollar_volume,
            spread_bps=provided.spread_bps,
            borrow_available=provided.borrow_available,
            stale=bool(provided.stale),
        )
    return MarketDataInput(symbol=symbol, price=float(price))


def build_execution_requests_from_target_weights(
    *,
    target_weights: dict[str, float],
    current_positions: dict[str, Any],
    latest_prices: dict[str, float],
    portfolio_equity: float,
    reserve_cash_pct: float = 0.0,
    market_data_inputs: dict[str, MarketDataInput] | None = None,
    provenance_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> list[ExecutionRequest]:
    investable_equity = float(portfolio_equity) * (1.0 - float(reserve_cash_pct))
    current_positions = current_positions or {}
    requests: list[ExecutionRequest] = []
    for symbol in sorted(set(target_weights) | set(current_positions)):
        market = _normalize_market_data(symbol, latest_prices.get(symbol, 0.0), market_data_inputs)
        price = float(market.price)
        current_shares = int(getattr(current_positions.get(symbol), "quantity", current_positions.get(symbol, 0)) or 0)
        target_weight = float(target_weights.get(symbol, 0.0))
        target_shares = int((investable_equity * target_weight) / price) if price > 0 else 0
        current_notional = current_shares * price
        current_weight = (current_notional / float(portfolio_equity)) if portfolio_equity else 0.0
        requested_delta = target_shares - current_shares
        if requested_delta == 0:
            continue
        requested_shares = abs(int(requested_delta))
        requests.append(
            ExecutionRequest(
                symbol=symbol,
                side="BUY" if requested_delta > 0 else "SELL",
                requested_shares=requested_shares,
                requested_notional=float(requested_shares * price),
                current_shares=current_shares,
                target_shares=target_shares,
                current_weight=float(current_weight),
                target_weight=target_weight,
                price=price,
                average_daily_volume_shares=market.average_daily_volume_shares,
                average_daily_dollar_volume=market.average_daily_dollar_volume,
                spread_bps=market.spread_bps,
                borrow_available=market.borrow_available,
                stale_market_data=market.stale,
                provenance=dict((provenance_by_symbol or {}).get(symbol, {})),
            )
        )
    return requests


def load_execution_requests_from_csv(path: str | Path) -> list[ExecutionRequest]:
    frame = pd.read_csv(path)
    required = {"symbol", "side"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Targets file missing required columns: {sorted(missing)}")
    rows: list[ExecutionRequest] = []
    for row in frame.to_dict(orient="records"):
        requested_shares = int(row.get("requested_shares", row.get("requested_quantity", 0)) or 0)
        price = float(row.get("price", row.get("reference_price", 0.0)) or 0.0)
        requested_notional = float(row.get("requested_notional", requested_shares * price) or 0.0)
        provenance = {}
        if row.get("provenance"):
            if isinstance(row["provenance"], str):
                try:
                    provenance = json.loads(row["provenance"])
                except json.JSONDecodeError:
                    provenance = {"raw": row["provenance"]}
            elif isinstance(row["provenance"], dict):
                provenance = row["provenance"]
        rows.append(
            ExecutionRequest(
                symbol=str(row["symbol"]),
                side=str(row["side"]).upper(),
                requested_shares=requested_shares,
                requested_notional=requested_notional,
                current_shares=int(row.get("current_shares", row.get("current_quantity", 0)) or 0),
                target_shares=int(row.get("target_shares", row.get("target_quantity", 0)) or 0),
                current_weight=float(row.get("current_weight", 0.0) or 0.0),
                target_weight=float(row.get("target_weight", 0.0) or 0.0),
                price=price,
                average_daily_volume_shares=float(row["average_daily_volume_shares"])
                if pd.notna(row.get("average_daily_volume_shares"))
                else None,
                average_daily_dollar_volume=float(row["average_daily_dollar_volume"])
                if pd.notna(row.get("average_daily_dollar_volume"))
                else (float(row["average_dollar_volume"]) if pd.notna(row.get("average_dollar_volume")) else None),
                spread_bps=float(row["spread_bps"]) if pd.notna(row.get("spread_bps")) else None,
                borrow_available=bool(row["borrow_available"]) if row.get("borrow_available") in {True, False, 0, 1} else None,
                stale_market_data=bool(row.get("stale_market_data", row.get("stale", False))),
                provenance=provenance,
            )
        )
    return rows


def estimate_backtest_transaction_cost_bps(config: ExecutionConfig | None) -> float:
    if config is None or not config.enabled:
        return 0.0
    commission_component = float(config.commission_bps)
    slippage_component = 0.0
    if config.slippage_model_type == "fixed_bps":
        slippage_component = float(config.fixed_slippage_bps)
    elif config.slippage_model_type == "spread_plus_bps":
        slippage_component = float(config.half_spread_bps) + float(config.fixed_slippage_bps)
    else:
        slippage_component = float(config.half_spread_bps) + float(config.liquidity_slippage_bps) * 0.5
    return commission_component + slippage_component


def _commission_for_order(config: ExecutionConfig, adjusted_shares: int, adjusted_notional: float) -> float:
    if adjusted_shares <= 0:
        return 0.0
    if config.commission_model_type == "per_share":
        return float(adjusted_shares) * float(config.commission_per_share)
    if config.commission_model_type == "flat":
        return float(config.flat_commission_per_order)
    return float(adjusted_notional) * float(config.commission_bps) / 10_000.0


def _slippage_bps(config: ExecutionConfig, request: ExecutionRequest, participation_pct_adv: float | None) -> float:
    if config.slippage_model_type == "fixed_bps":
        return float(config.fixed_slippage_bps)
    if config.slippage_model_type == "spread_plus_bps":
        spread_component = float(request.spread_bps if request.spread_bps is not None else config.half_spread_bps)
        return spread_component + float(config.fixed_slippage_bps)
    spread_component = float(request.spread_bps if request.spread_bps is not None else config.half_spread_bps)
    return spread_component + (float(config.liquidity_slippage_bps) * float(participation_pct_adv or 0.0) / 100.0)


def _estimated_fill_price(side: str, price: float, slippage_bps: float) -> float:
    multiplier = 1.0 + (slippage_bps / 10_000.0) if side == "BUY" else 1.0 - (slippage_bps / 10_000.0)
    return float(price) * multiplier


def _short_target_request(request: ExecutionRequest) -> bool:
    return request.target_shares < 0 or request.target_weight < 0.0


def _fallback_liquidity_clip_shares(request: ExecutionRequest, config: ExecutionConfig) -> int:
    if request.price <= 0:
        return 0
    if config.max_position_notional_change is not None:
        return _round_down_to_lot(int(float(config.max_position_notional_change) / request.price), config.lot_size)
    return _round_down_to_lot(1, config.lot_size)


def simulate_execution(
    *,
    requests: list[ExecutionRequest],
    config: ExecutionConfig,
    current_cash: float | None = None,
    current_equity: float | None = None,
) -> ExecutionSimulationResult:
    sorted_requests = sorted(requests, key=lambda item: (item.symbol, item.side, item.requested_shares))
    if not config.enabled:
        executable = [
            ExecutableOrder(
                symbol=request.symbol,
                side=request.side,
                requested_shares=request.requested_shares,
                requested_notional=request.requested_notional,
                adjusted_shares=request.requested_shares,
                adjusted_notional=request.requested_notional,
                estimated_fill_price=request.price,
                slippage_bps=0.0,
                commission=0.0,
                participation_pct_adv=None,
                filled_fraction=1.0,
                status="executable",
                provenance=request.provenance,
            )
            for request in sorted_requests
        ]
        summary = ExecutionSummary(
            requested_order_count=len(sorted_requests),
            executable_order_count=len(executable),
            rejected_order_count=0,
            clipped_order_count=0,
            requested_notional=sum(item.requested_notional for item in sorted_requests),
            executed_notional=sum(item.adjusted_notional for item in executable),
            expected_commission_total=0.0,
            expected_slippage_cost_total=0.0,
            expected_total_cost=0.0,
            turnover_before_constraints=sum(item.requested_notional for item in sorted_requests),
            turnover_after_constraints=sum(item.adjusted_notional for item in executable),
            rejected_order_ratio=0.0,
            clipped_order_ratio=0.0,
            liquidity_failure_count=0,
            short_borrow_failure_count=0,
            zero_executable_orders=len(executable) == 0 and len(sorted_requests) > 0,
            max_participation_pct_adv=0.0,
            estimated_cost_bps_on_executed_notional=0.0,
        )
        return ExecutionSimulationResult(
            requested_orders=sorted_requests,
            executable_orders=executable,
            rejected_orders=[],
            summary=summary,
            liquidity_diagnostics=[],
            turnover_rows=[],
            symbol_tradeability_rows=[],
        )

    provisional: list[dict[str, Any]] = []
    rejected_orders: list[RejectedOrder] = []
    liquidity_diagnostics: list[LiquidityDiagnostic] = []

    for request in sorted_requests:
        adjusted_shares = int(request.requested_shares)
        rejection_reason: str | None = None
        clipping_reason: str | None = None
        tradeable = True
        tradeability_reason = "tradeable"
        participation_pct_adv: float | None = None
        price = float(request.price)

        if price <= 0:
            rejection_reason = "missing_or_nonpositive_price"
        elif request.stale_market_data and config.stale_market_data_behavior == "reject":
            rejection_reason = "stale_market_data"
        elif config.min_price is not None and price < config.min_price:
            rejection_reason = "below_min_price"
        elif (
            config.min_average_dollar_volume is not None
            and request.average_daily_dollar_volume is not None
            and request.average_daily_dollar_volume < config.min_average_dollar_volume
        ):
            rejection_reason = "below_min_average_dollar_volume"
        elif (
            config.min_average_dollar_volume is not None
            and request.average_daily_dollar_volume is None
            and config.missing_liquidity_behavior == "reject"
        ):
            rejection_reason = "missing_liquidity_data"

        if rejection_reason is None and config.max_position_notional_change is not None and price > 0:
            max_shares = _round_down_to_lot(int(float(config.max_position_notional_change) / price), config.lot_size)
            if max_shares <= 0:
                rejection_reason = "max_position_notional_change_zero"
            elif adjusted_shares > max_shares:
                adjusted_shares = max_shares
                clipping_reason = "max_position_notional_change"

        if rejection_reason is None and adjusted_shares * price < float(config.min_trade_notional):
            rejection_reason = "below_min_trade_notional"

        if rejection_reason is None:
            rounded_shares = _round_down_to_lot(adjusted_shares, config.lot_size)
            if rounded_shares != adjusted_shares:
                adjusted_shares = rounded_shares
                clipping_reason = clipping_reason or "lot_rounding"
            if adjusted_shares <= 0:
                rejection_reason = "lot_rounding_zero"

        if rejection_reason is None and request.average_daily_volume_shares is not None and request.average_daily_volume_shares > 0:
            participation_pct_adv = (float(adjusted_shares) / float(request.average_daily_volume_shares)) * 100.0
        elif rejection_reason is None and request.average_daily_dollar_volume is not None and request.price > 0:
            implied_adv = float(request.average_daily_dollar_volume) / request.price
            participation_pct_adv = (float(adjusted_shares) / implied_adv) * 100.0 if implied_adv > 0 else None

        if rejection_reason is None and config.max_participation_of_adv is not None:
            adv_shares = request.average_daily_volume_shares
            if adv_shares is None and request.average_daily_dollar_volume is not None and price > 0:
                adv_shares = float(request.average_daily_dollar_volume) / price
            if adv_shares is None or adv_shares <= 0:
                if config.missing_liquidity_behavior == "warn_and_clip":
                    adjusted_shares = _fallback_liquidity_clip_shares(request, config)
                    clipping_reason = clipping_reason or "missing_liquidity_data_warn_and_clip"
                else:
                    rejection_reason = "missing_liquidity_data"
            else:
                max_shares = _round_down_to_lot(int(float(adv_shares) * float(config.max_participation_of_adv)), config.lot_size)
                if max_shares <= 0:
                    rejection_reason = "adv_participation_cap_zero"
                elif adjusted_shares > max_shares:
                    if config.partial_fill_behavior == "reject":
                        rejection_reason = "adv_participation_cap"
                    else:
                        adjusted_shares = max_shares
                        clipping_reason = clipping_reason or "adv_participation_cap"
                        participation_pct_adv = (float(adjusted_shares) / float(adv_shares)) * 100.0

        if rejection_reason is None and _short_target_request(request):
            if not config.allow_shorts:
                rejection_reason = "shorts_disallowed"
            elif config.enforce_short_borrow_proxy and (
                request.symbol in set(config.short_borrow_blocklist)
                or request.borrow_available is False
            ):
                rejection_reason = "short_borrow_unavailable"

        if rejection_reason is None and adjusted_shares * price < float(config.min_trade_notional):
            rejection_reason = "below_min_trade_notional"

        provisional.append(
            {
                "request": request,
                "adjusted_shares": adjusted_shares,
                "clipping_reason": clipping_reason,
                "participation_pct_adv": participation_pct_adv,
                "rejection_reason": rejection_reason,
            }
        )
        if rejection_reason is not None:
            tradeable = False
            tradeability_reason = rejection_reason
            rejected_orders.append(
                RejectedOrder(
                    symbol=request.symbol,
                    side=request.side,
                    requested_shares=request.requested_shares,
                    requested_notional=request.requested_notional,
                    adjusted_shares=0,
                    adjusted_notional=0.0,
                    estimated_fill_price=price,
                    slippage_bps=0.0,
                    commission=0.0,
                    participation_pct_adv=participation_pct_adv,
                    filled_fraction=0.0,
                    status="rejected",
                    rejection_reason=rejection_reason,
                    provenance=request.provenance,
                )
            )
            adjusted_shares = 0
        liquidity_diagnostics.append(
            LiquidityDiagnostic(
                symbol=request.symbol,
                tradeable=tradeable,
                reason=tradeability_reason if not clipping_reason else f"{tradeability_reason}|{clipping_reason}",
                price=price,
                average_daily_volume_shares=request.average_daily_volume_shares,
                average_daily_dollar_volume=request.average_daily_dollar_volume,
                spread_bps=request.spread_bps,
                borrow_available=request.borrow_available,
                stale_market_data=request.stale_market_data,
                requested_shares=request.requested_shares,
                adjusted_shares=adjusted_shares,
                participation_pct_adv=participation_pct_adv,
                provenance=request.provenance,
            )
        )

    if config.max_short_gross_exposure is not None and current_equity:
        short_candidates = [
            row for row in provisional
            if row["rejection_reason"] is None and row["adjusted_shares"] > 0 and _short_target_request(row["request"])
        ]
        short_gross = sum(row["adjusted_shares"] * row["request"].price for row in short_candidates)
        max_short_notional = float(config.max_short_gross_exposure) * float(current_equity)
        if short_gross > max_short_notional and short_gross > 0:
            scale = max_short_notional / short_gross
            for row in short_candidates:
                clipped = _round_down_to_lot(int(row["adjusted_shares"] * scale), config.lot_size)
                if clipped <= 0:
                    row["rejection_reason"] = "max_short_gross_exposure"
                    row["adjusted_shares"] = 0
                    rejected_orders.append(
                        RejectedOrder(
                            symbol=row["request"].symbol,
                            side=row["request"].side,
                            requested_shares=row["request"].requested_shares,
                            requested_notional=row["request"].requested_notional,
                            adjusted_shares=0,
                            adjusted_notional=0.0,
                            estimated_fill_price=row["request"].price,
                            slippage_bps=0.0,
                            commission=0.0,
                            participation_pct_adv=row["participation_pct_adv"],
                            filled_fraction=0.0,
                            status="rejected",
                            rejection_reason="max_short_gross_exposure",
                            provenance=row["request"].provenance,
                        )
                    )
                else:
                    row["adjusted_shares"] = clipped
                    row["clipping_reason"] = row["clipping_reason"] or "max_short_gross_exposure"

    active_rows = [row for row in provisional if row["rejection_reason"] is None and row["adjusted_shares"] > 0]
    requested_notional_total = float(sum(request.requested_notional for request in sorted_requests))
    turnover_before = requested_notional_total
    turnover_after = sum(row["adjusted_shares"] * row["request"].price for row in active_rows)

    if config.max_turnover_per_rebalance is not None and turnover_after > 0:
        max_turnover_notional = (
            float(current_equity) * float(config.max_turnover_per_rebalance)
            if current_equity is not None
            else requested_notional_total * float(config.max_turnover_per_rebalance)
        )
        if turnover_after > max_turnover_notional:
            scale = max_turnover_notional / turnover_after if turnover_after > 0 else 0.0
            for row in active_rows:
                clipped = _round_down_to_lot(int(row["adjusted_shares"] * scale), config.lot_size)
                if clipped <= 0:
                    row["rejection_reason"] = "max_turnover_per_rebalance"
                    row["adjusted_shares"] = 0
                    rejected_orders.append(
                        RejectedOrder(
                            symbol=row["request"].symbol,
                            side=row["request"].side,
                            requested_shares=row["request"].requested_shares,
                            requested_notional=row["request"].requested_notional,
                            adjusted_shares=0,
                            adjusted_notional=0.0,
                            estimated_fill_price=row["request"].price,
                            slippage_bps=0.0,
                            commission=0.0,
                            participation_pct_adv=row["participation_pct_adv"],
                            filled_fraction=0.0,
                            status="rejected",
                            rejection_reason="max_turnover_per_rebalance",
                            provenance=row["request"].provenance,
                        )
                    )
                else:
                    row["adjusted_shares"] = clipped
                    row["clipping_reason"] = row["clipping_reason"] or "max_turnover_per_rebalance"

    active_rows = [row for row in provisional if row["rejection_reason"] is None and row["adjusted_shares"] > 0]
    if current_cash is not None and current_equity is not None:
        available_cash = float(current_cash) - (float(current_equity) * float(config.cash_buffer_bps) / 10_000.0)
        for row in sorted(active_rows, key=lambda item: (0 if item["request"].side == "SELL" else 1, item["request"].symbol)):
            request = row["request"]
            notional = float(row["adjusted_shares"] * request.price)
            if request.side == "SELL":
                available_cash += notional
                continue
            if notional <= available_cash + 1e-12:
                available_cash -= notional
                continue
            affordable_shares = _round_down_to_lot(int(max(available_cash, 0.0) / request.price), config.lot_size)
            if affordable_shares <= 0 or config.partial_fill_behavior == "reject":
                row["rejection_reason"] = "cash_buffer_or_affordability"
                row["adjusted_shares"] = 0
                rejected_orders.append(
                    RejectedOrder(
                        symbol=request.symbol,
                        side=request.side,
                        requested_shares=request.requested_shares,
                        requested_notional=request.requested_notional,
                        adjusted_shares=0,
                        adjusted_notional=0.0,
                        estimated_fill_price=request.price,
                        slippage_bps=0.0,
                        commission=0.0,
                        participation_pct_adv=row["participation_pct_adv"],
                        filled_fraction=0.0,
                        status="rejected",
                        rejection_reason="cash_buffer_or_affordability",
                        provenance=request.provenance,
                    )
                )
            else:
                row["adjusted_shares"] = affordable_shares
                row["clipping_reason"] = row["clipping_reason"] or "cash_buffer_or_affordability"
                available_cash -= affordable_shares * request.price

    executable_orders: list[ExecutableOrder] = []
    turnover_rows: list[dict[str, Any]] = []
    symbol_tradeability_rows: list[dict[str, Any]] = []
    commission_total = 0.0
    slippage_cost_total = 0.0
    clipped_count = 0
    max_participation_pct_adv = 0.0

    seen_rejections: set[tuple[str, str, int, str]] = set()
    deduped_rejected: list[RejectedOrder] = []
    for item in rejected_orders:
        key = (item.symbol, item.side, item.requested_shares, item.rejection_reason)
        if key not in seen_rejections:
            seen_rejections.add(key)
            deduped_rejected.append(item)
    rejected_orders = deduped_rejected

    for row in provisional:
        request = row["request"]
        adjusted_shares = int(row["adjusted_shares"])
        if row["rejection_reason"] is None and adjusted_shares > 0:
            participation_pct_adv = row["participation_pct_adv"]
            slippage_bps = _slippage_bps(config, request, participation_pct_adv)
            fill_price = _estimated_fill_price(request.side, request.price, slippage_bps)
            adjusted_notional = float(adjusted_shares * fill_price)
            commission = _commission_for_order(config, adjusted_shares, adjusted_notional)
            filled_fraction = float(adjusted_shares / request.requested_shares) if request.requested_shares else 0.0
            status = "clipped" if adjusted_shares != request.requested_shares or row["clipping_reason"] else "executable"
            executable_orders.append(
                ExecutableOrder(
                    symbol=request.symbol,
                    side=request.side,
                    requested_shares=request.requested_shares,
                    requested_notional=request.requested_notional,
                    adjusted_shares=adjusted_shares,
                    adjusted_notional=adjusted_notional,
                    estimated_fill_price=fill_price,
                    slippage_bps=slippage_bps,
                    commission=commission,
                    participation_pct_adv=participation_pct_adv,
                    filled_fraction=filled_fraction,
                    status=status,
                    clipping_reason=row["clipping_reason"],
                    provenance=request.provenance,
                )
            )
            commission_total += commission
            slippage_cost_total += abs(adjusted_shares * (fill_price - request.price))
            if status == "clipped":
                clipped_count += 1
            max_participation_pct_adv = max(max_participation_pct_adv, float(participation_pct_adv or 0.0))
            turnover_rows.append(
                {
                    "symbol": request.symbol,
                    "requested_notional": request.requested_notional,
                    "adjusted_notional": adjusted_notional,
                    "status": status,
                    "clipping_reason": row["clipping_reason"] or "",
                    "provenance": json.dumps(request.provenance, sort_keys=True) if request.provenance else "",
                }
            )
        symbol_tradeability_rows.append(
            {
                "symbol": request.symbol,
                "requested_shares": request.requested_shares,
                "adjusted_shares": adjusted_shares,
                "tradeable": bool(row["rejection_reason"] is None and adjusted_shares > 0),
                "status": "rejected" if row["rejection_reason"] else ("clipped" if row["clipping_reason"] else "executable"),
                "reason": row["rejection_reason"] or row["clipping_reason"] or "",
                "price": request.price,
                "average_daily_volume_shares": request.average_daily_volume_shares,
                "average_daily_dollar_volume": request.average_daily_dollar_volume,
                "participation_pct_adv": row["participation_pct_adv"],
                "stale_market_data": request.stale_market_data,
                "borrow_available": request.borrow_available,
                "provenance": json.dumps(request.provenance, sort_keys=True) if request.provenance else "",
            }
        )

    executed_notional = float(sum(order.adjusted_notional for order in executable_orders))
    summary = ExecutionSummary(
        requested_order_count=len(sorted_requests),
        executable_order_count=len(executable_orders),
        rejected_order_count=len(rejected_orders),
        clipped_order_count=clipped_count,
        requested_notional=requested_notional_total,
        executed_notional=executed_notional,
        expected_commission_total=commission_total,
        expected_slippage_cost_total=slippage_cost_total,
        expected_total_cost=commission_total + slippage_cost_total,
        turnover_before_constraints=turnover_before,
        turnover_after_constraints=executed_notional,
        rejected_order_ratio=(len(rejected_orders) / len(sorted_requests)) if sorted_requests else 0.0,
        clipped_order_ratio=(clipped_count / len(sorted_requests)) if sorted_requests else 0.0,
        liquidity_failure_count=sum(
            1
            for item in rejected_orders
            if item.rejection_reason in {
                "missing_liquidity_data",
                "below_min_average_dollar_volume",
                "adv_participation_cap",
                "adv_participation_cap_zero",
            }
        ),
        short_borrow_failure_count=sum(1 for item in rejected_orders if item.rejection_reason == "short_borrow_unavailable"),
        zero_executable_orders=(len(executable_orders) == 0 and len(sorted_requests) > 0),
        max_participation_pct_adv=max_participation_pct_adv,
        estimated_cost_bps_on_executed_notional=((commission_total + slippage_cost_total) / executed_notional * 10_000.0) if executed_notional > 0 else 0.0,
    )
    return ExecutionSimulationResult(
        requested_orders=sorted_requests,
        executable_orders=executable_orders,
        rejected_orders=rejected_orders,
        summary=summary,
        liquidity_diagnostics=liquidity_diagnostics,
        turnover_rows=turnover_rows,
        symbol_tradeability_rows=symbol_tradeability_rows,
    )


def write_execution_artifacts(
    result: ExecutionSimulationResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    requested_orders_path = output_path / "requested_orders.csv"
    executable_orders_path = output_path / "executable_orders.csv"
    rejected_orders_path = output_path / "rejected_orders.csv"
    execution_summary_json_path = output_path / "execution_summary.json"
    execution_summary_md_path = output_path / "execution_summary.md"
    liquidity_constraints_path = output_path / "liquidity_constraints_report.csv"
    turnover_summary_path = output_path / "turnover_summary.csv"
    symbol_tradeability_path = output_path / "symbol_tradeability_report.csv"

    pd.DataFrame([order.to_dict() for order in result.requested_orders]).to_csv(requested_orders_path, index=False)
    pd.DataFrame([order.to_dict() for order in result.executable_orders]).to_csv(executable_orders_path, index=False)
    pd.DataFrame([order.to_dict() for order in result.rejected_orders]).to_csv(rejected_orders_path, index=False)
    pd.DataFrame([row.to_dict() for row in result.liquidity_diagnostics]).to_csv(liquidity_constraints_path, index=False)
    pd.DataFrame(result.turnover_rows).to_csv(turnover_summary_path, index=False)
    pd.DataFrame(result.symbol_tradeability_rows).to_csv(symbol_tradeability_path, index=False)
    execution_summary_json_path.write_text(json.dumps(result.summary.to_dict(), indent=2, default=str), encoding="utf-8")
    execution_summary_md_path.write_text(
        "\n".join(
            [
                "# Execution Summary",
                "",
                f"- Requested orders: `{result.summary.requested_order_count}`",
                f"- Executable orders: `{result.summary.executable_order_count}`",
                f"- Rejected orders: `{result.summary.rejected_order_count}`",
                f"- Clipped orders: `{result.summary.clipped_order_count}`",
                f"- Requested notional: `{result.summary.requested_notional}`",
                f"- Executed notional: `{result.summary.executed_notional}`",
                f"- Expected commission total: `{result.summary.expected_commission_total}`",
                f"- Expected slippage cost total: `{result.summary.expected_slippage_cost_total}`",
                f"- Expected total cost: `{result.summary.expected_total_cost}`",
                f"- Turnover before constraints: `{result.summary.turnover_before_constraints}`",
                f"- Turnover after constraints: `{result.summary.turnover_after_constraints}`",
                f"- Rejected order ratio: `{result.summary.rejected_order_ratio}`",
                f"- Clipped order ratio: `{result.summary.clipped_order_ratio}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "requested_orders_path": requested_orders_path,
        "executable_orders_path": executable_orders_path,
        "rejected_orders_path": rejected_orders_path,
        "execution_summary_json_path": execution_summary_json_path,
        "execution_summary_md_path": execution_summary_md_path,
        "liquidity_constraints_report_path": liquidity_constraints_path,
        "turnover_summary_path": turnover_summary_path,
        "symbol_tradeability_report_path": symbol_tradeability_path,
    }
