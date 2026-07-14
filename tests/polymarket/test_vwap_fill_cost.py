"""#2 Stage A: depth-walked VWAP fill-cost helper for the live executor.

The live depth guard only checks aggregate dollars at/under a 5% limit;
_vwap_fill_cost measures the actual price impact of walking the ask ladder
for our size. Instrument-only — nothing gates on it yet — but the arithmetic
must be right for the telemetry to be trustworthy.
"""
from __future__ import annotations

import pytest

from trading_platform.polymarket.polymarket_live_executor import _vwap_fill_cost


def test_deep_book_at_mid_zero_slippage():
    asks = [{"price": 0.20, "size": 1000}]  # $200 available at mid
    out = _vwap_fill_cost(asks, mid=0.20, notional_usd=100)
    assert out is not None
    assert out["vwap"] == pytest.approx(0.20)
    assert out["slippage_c"] == pytest.approx(0.0)
    assert out["shortfall_usd"] == pytest.approx(0.0)


def test_walks_into_deeper_levels():
    # $20 fills at 0.20, remaining $80 fills at 0.25.
    asks = [{"price": 0.20, "size": 100}, {"price": 0.25, "size": 1000}]
    out = _vwap_fill_cost(asks, mid=0.20, notional_usd=100)
    # vwap = 100 spent / (100 + 320) shares = 0.238095
    assert out["vwap"] == pytest.approx(0.238095, abs=1e-5)
    assert out["slippage_c"] == pytest.approx(3.8095, abs=1e-3)
    assert out["shortfall_usd"] == pytest.approx(0.0)


def test_shortfall_when_book_too_thin():
    asks = [{"price": 0.20, "size": 100}]  # only $20 available
    out = _vwap_fill_cost(asks, mid=0.20, notional_usd=100)
    assert out["shortfall_usd"] == pytest.approx(80.0)
    assert out["slippage_c"] == pytest.approx(0.0)  # what filled, filled at mid


def test_bad_inputs_return_none():
    assert _vwap_fill_cost([{"price": 0.2, "size": 100}], mid=0, notional_usd=100) is None
    assert _vwap_fill_cost([{"price": 0.2, "size": 100}], mid=0.2, notional_usd=0) is None
    assert _vwap_fill_cost([], mid=0.2, notional_usd=100) is None
    assert _vwap_fill_cost(None, mid=0.2, notional_usd=100) is None
    # malformed levels are skipped, not fatal
    assert _vwap_fill_cost([{"price": None, "size": 1}], mid=0.2, notional_usd=100) is None


def test_ignores_zero_and_negative_levels():
    asks = [{"price": 0.0, "size": 100}, {"price": 0.20, "size": 1000}]
    out = _vwap_fill_cost(asks, mid=0.20, notional_usd=50)
    assert out["vwap"] == pytest.approx(0.20)
    assert out["slippage_c"] == pytest.approx(0.0)
