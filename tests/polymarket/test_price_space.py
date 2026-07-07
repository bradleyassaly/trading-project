"""Locks the YES-space storage convention for live_trades prices.

Established empirically 2026-07-07 (36/38 SELL entries stored YES-space).
These tests exist because the recurring price-space confusion caused 5+
ledger-corrupting production bugs in untested code (system audit 2026-07-07).
"""
from trading_platform.polymarket import price_space as ps


def test_held_token_price_buy_is_yes():
    assert ps.held_token_price("BUY", 0.30) == 0.30
    assert ps.held_token_price("YES", 0.72) == 0.72


def test_held_token_price_sell_is_complement():
    assert abs(ps.held_token_price("SELL", 0.91) - 0.09) < 1e-9
    assert abs(ps.held_token_price("NO", 0.20) - 0.80) < 1e-9


def test_to_yes_space_roundtrip():
    # A data-api fill is quoted in the held token's own space; converting to
    # YES-space and back through held_token_price must recover it.
    for direction in ("BUY", "SELL"):
        held = 0.23
        yes = ps.to_yes_space(direction, held)
        assert abs(ps.held_token_price(direction, yes) - held) < 1e-9


def test_realized_pnl_buy_win():
    # 10 YES shares bought at 0.30, resolves YES (1.0): +$7.00
    assert abs(ps.realized_pnl("BUY", 10, 0.30, 1.0) - 7.0) < 1e-9


def test_realized_pnl_sell_win():
    # SELL entry YES=0.70 => NO cost 0.30; market resolves NO (YES exit 0.0):
    # NO pays 1.0, 10 shares -> +$7.00. The old (entry - fill) formula gave
    # the wrong magnitude/sign here.
    assert abs(ps.realized_pnl("SELL", 10, 0.70, 0.0) - 7.0) < 1e-9


def test_realized_pnl_sell_loss():
    # SELL entry YES=0.70 (NO cost 0.30); market resolves YES (exit 1.0):
    # NO worthless, lose the 0.30/share cost -> ~-$3.00 (held-price floor of
    # 0.001 leaves a cent of residual value, so -2.99 is the correct result).
    assert abs(ps.realized_pnl("SELL", 10, 0.70, 1.0) - (-3.0)) < 0.02


def test_floor_prevents_zero_cost():
    assert ps.held_token_price("BUY", 0.0) >= 0.0009
    assert ps.held_token_price("SELL", 1.0) >= 0.0009
