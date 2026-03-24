from __future__ import annotations

from dataclasses import replace

from trading_platform.paper.models import PaperOrder, PaperTradingConfig


def validate_slippage_config(config: PaperTradingConfig) -> None:
    model = str(config.slippage_model or "none").lower()
    if model not in {"none", "fixed_bps"}:
        raise ValueError(f"Unsupported paper slippage model: {config.slippage_model}")
    if float(config.slippage_buy_bps) < 0 or float(config.slippage_sell_bps) < 0:
        raise ValueError("Paper slippage bps must be non-negative")


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
    base_price = float(order.expected_fill_price or order.reference_price)
    slipped_price, extra_bps = apply_slippage(base_price, order.side, config)
    return replace(
        order,
        expected_fill_price=slipped_price,
        expected_slippage_bps=float(order.expected_slippage_bps) + float(extra_bps),
        notional=float(slipped_price) * float(order.quantity),
    )
