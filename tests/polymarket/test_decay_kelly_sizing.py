"""2026-07-09: honest-Kelly sizing for measured-+wedge resolution_decay
trades. Lifts the stake above the flat $5 floor ONLY on trades that pass
the decay veto (a real wedge), quarter-Kelly, hard-capped, floored at the
CLOB $5 min. Must never authorize a -wedge trade or exceed the cap."""
import inspect

from trading_platform.polymarket import polymarket_live_executor as ple
from trading_platform.polymarket.polymarket_live_executor import (
    PolymarketLiveExecutor, DECAY_KELLY_MAX_USD,
)


def _ex(equity=290.0):
    ex = object.__new__(PolymarketLiveExecutor)
    ex._current_equity = lambda: equity   # avoid DB
    return ex


def test_kelly_math_crown_jewel():
    # sports/12-24h/0.10-0.20: honest P 0.226 at price 0.15.
    # b=(0.85/0.15)=5.667; f* = 0.226 - 0.774/5.667 = 0.0894.
    # ¼-Kelly × $290 = 0.0894*0.25*290 = $6.48.
    stake = _ex(290.0)._decay_kelly_usd(0.226, 0.15)
    assert abs(stake - 6.48) < 0.15


def test_floor_at_clob_min():
    # A wafer-thin wedge → sub-$5 Kelly → floored to $5 (CLOB minimum).
    stake = _ex(290.0)._decay_kelly_usd(0.151, 0.15)
    assert stake == 5.0


def test_hard_cap():
    # An absurd wedge (P 0.9 at price 0.10) would Kelly huge → capped.
    stake = _ex(290.0)._decay_kelly_usd(0.90, 0.10)
    assert stake == DECAY_KELLY_MAX_USD


def test_no_wedge_returns_floor():
    # decay_p below/at price → f<=0 → $5 floor (never sizes a -wedge up).
    assert _ex(290.0)._decay_kelly_usd(0.10, 0.15) == 5.0
    assert _ex(290.0)._decay_kelly_usd(0.15, 0.15) == 5.0


def test_scales_with_equity():
    small = _ex(100.0)._decay_kelly_usd(0.30, 0.15)
    big = _ex(1000.0)._decay_kelly_usd(0.30, 0.15)
    assert big > small  # larger bankroll → larger Kelly stake (until cap)


def test_degenerate_price_safe():
    assert _ex()._decay_kelly_usd(0.5, 0.0) == 5.0
    assert _ex()._decay_kelly_usd(0.5, 1.0) == 5.0


# ---------------------------------------------------------------------------
# Wiring: only veto-passing measured wedges get lifted; depth/cap respected
# ---------------------------------------------------------------------------

def test_wiring_veto_pass_only_and_depth_clamped():
    src = inspect.getsource(ple)
    # stashed only when decay_p is a real wedge (> px*(1+edge))
    assert 'float(_decay_p) > _gate_px * (1.0 + _min_edge)' in src
    assert 'signal["_decay_kelly_usd"]' in src
    # flag to disable instantly
    assert 'DECAY_KELLY_SIZING' in src
    # cap lift is clamped to fillable_depth/1.5 (proven capacity)
    assert 'fillable_depth / 1.5' in src
    # never below the CLOB floor
    assert 'max(5.0, _dk_fit)' in src


def test_disabled_by_flag(monkeypatch):
    # With the flag off, no _decay_kelly_usd should be stashed — verified at
    # source level (the stash is guarded by the env check).
    src = inspect.getsource(PolymarketLiveExecutor.execute) \
        if hasattr(PolymarketLiveExecutor, "execute") else inspect.getsource(ple)
    assert 'os.environ.get("DECAY_KELLY_SIZING", "1")' in src
