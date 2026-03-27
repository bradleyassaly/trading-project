from __future__ import annotations

from dataclasses import replace

from trading_platform.paper.models import PaperOrder, PaperTradingConfig


def validate_slippage_config(config: PaperTradingConfig) -> None:
    model = str(config.slippage_model or "none").lower()
    if model not in {"none", "fixed_bps"}:
        raise ValueError(f"Unsupported paper slippage model: {config.slippage_model}")
    if float(config.slippage_buy_bps) < 0 or float(config.slippage_sell_bps) < 0:
        raise ValueError("Paper slippage bps must be non-negative")
    if float(config.commission_bps) < 0.0:
        raise ValueError("Paper commission bps must be non-negative")
    if float(config.minimum_commission) < 0.0:
        raise ValueError("Paper minimum commission must be non-negative")
    if float(config.spread_bps) < 0.0:
        raise ValueError("Paper spread bps must be non-negative")


def apply_slippage(price: float, side: str, config: PaperTradingConfig) -> tuple[float, float]:
    validate_slippage_config(config)
    model = str(config.slippage_model or "none").lower()
    if model == "none":
        return float(price), 0.0
    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported order side for slippage: {side}")
    bps = float(config.slippage_buy_bps if normalized_side == "BUY" else config.slippage_sell_bps)
    if model == "fixed_bps":
        direction = 1.0 if normalized_side == "BUY" else -1.0
        slipped = float(price) * (1.0 + direction * (bps / 10_000.0))
        return float(slipped), bps
    raise ValueError(f"Unsupported paper slippage model: {config.slippage_model}")


def apply_order_slippage(order: PaperOrder, config: PaperTradingConfig) -> PaperOrder:
    base_price = float(order.reference_price)
    slipped_price, extra_bps = apply_slippage(base_price, order.side, config)
    quantity = abs(int(order.quantity))
    gross_notional = float(quantity) * float(base_price)
    slippage_cost = abs(float(slipped_price) - float(base_price)) * float(quantity)
    spread_bps = float(config.spread_bps) if bool(config.enable_cost_model) else 0.0
    half_spread_bps = spread_bps / 2.0
    normalized_side = str(order.side).upper()
    direction = 1.0 if normalized_side == "BUY" else -1.0
    spread_price = float(slipped_price) * (1.0 + direction * (half_spread_bps / 10_000.0))
    spread_cost = abs(float(spread_price) - float(slipped_price)) * float(quantity)
    commission_cost = 0.0
    if bool(config.enable_cost_model) and quantity > 0 and gross_notional > 0.0:
        commission_cost = max(
            float(config.minimum_commission),
            gross_notional * (float(config.commission_bps) / 10_000.0),
        )
    commission_per_share = (commission_cost / float(quantity)) if quantity > 0 else 0.0
    effective_fill_price = float(spread_price) + (direction * commission_per_share)
    total_execution_cost = float(slippage_cost + spread_cost + commission_cost)
    return replace(
        order,
        expected_fill_price=effective_fill_price,
        expected_gross_notional=gross_notional,
        expected_slippage_bps=float(order.expected_slippage_bps) + float(extra_bps),
        expected_spread_bps=half_spread_bps,
        expected_slippage_cost=float(slippage_cost),
        expected_spread_cost=float(spread_cost),
        expected_commission_cost=float(commission_cost),
        expected_total_execution_cost=float(total_execution_cost),
        expected_fees=float(commission_cost),
        notional=float(effective_fill_price) * float(order.quantity),
        cost_model=(
            "paper_v2_cost_model"
            if bool(config.enable_cost_model) or float(extra_bps) != 0.0
            else "disabled"
        ),
    )
