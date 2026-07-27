"""BUY-share quantization must never submit below the CLOB $1 minimum.

2026-07-27: int() truncation turned $1.00 probe stakes into $0.85–$0.96
submissions (5 sh @ 0.17 = $0.85 etc.), which the exchange rejects with
"invalid amount for a marketable BUY order, min size: $1". 6 of 18 live
attempts over 3 days died this way — every $1 probe at a price that does
not evenly divide $1.00.
"""
import pytest

from trading_platform.polymarket.clob_client import (
    CLOB_MIN_BUY_USDC,
    CLOB_MIN_SHARES,
    quantize_buy_shares,
)


# The four exact rejections observed in prod live_trades (error_msg amounts).
@pytest.mark.parametrize(
    "size_usdc,target,old_bad_usdc",
    [
        (1.00, 0.17, 0.85),
        (1.00, 0.18, 0.90),
        (1.00, 0.15, 0.90),
        (1.00, 0.19, 0.95),
        (1.00, 0.16, 0.96),
    ],
)
def test_prod_rejections_now_clear_minimum(size_usdc, target, old_bad_usdc):
    shares, usdc = quantize_buy_shares(size_usdc, target)
    assert usdc >= CLOB_MIN_BUY_USDC
    assert usdc > old_bad_usdc
    # One extra share is all it should take from the truncated count.
    assert usdc <= old_bad_usdc + target + 0.01


def test_exact_division_unchanged():
    # $1.00 @ 0.20 = exactly 5 shares — no bump needed.
    assert quantize_buy_shares(1.00, 0.20) == (5, 1.00)


def test_above_minimum_untouched():
    # Floored notional already >= $1: behavior identical to the old code.
    shares, usdc = quantize_buy_shares(2.00, 0.35)
    assert (shares, usdc) == (5, 1.75)


def test_min_share_floor_still_applies():
    # High price, small stake: the 5-share floor dominates (the caller's
    # 3x-overshoot guard is what rejects these, not the quantizer).
    shares, usdc = quantize_buy_shares(1.00, 0.50)
    assert shares == CLOB_MIN_SHARES
    assert usdc == 2.50


def test_never_below_minimum_across_price_grid():
    # Property: for every stake >= $1 and every valid tick price, the
    # submitted notional clears the exchange minimum.
    for cents in range(1, 100):
        target = cents / 100.0
        for size in (1.00, 1.05, 1.53, 1.83, 2.00, 5.00):
            shares, usdc = quantize_buy_shares(size, target)
            assert usdc >= CLOB_MIN_BUY_USDC, (
                f"{size=} {target=} -> {shares} sh = ${usdc}"
            )
            assert shares >= CLOB_MIN_SHARES
