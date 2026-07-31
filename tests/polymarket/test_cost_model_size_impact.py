"""2026-07-31: size impact is ZERO at our clip sizes (chain-verified).

The slippage schedule charged 0.003 (0.3c) to every stake <= $50. On a
measured 0.66c true entry cost that is a ~36% over-penalty on every paper
trade — and over-penalising silently kills viable strategies, the Type II
failure this platform can least afford.

Evidence: 12/12 sampled live fills show fill_price == best_ask_at_decision
exactly on $1-16 orders (our wallet's on-chain OrderFilled events), and
the decision-time depth walk agrees (vwap_slippage_c == half-spread).
"""
from trading_platform.polymarket.cost_model import CostModel


def test_no_size_impact_at_our_clip_sizes():
    cm = CostModel()
    for stake in (1.0, 5.0, 16.0, 25.0):
        assert cm._lookup_slippage(stake) == 0.0, stake


def test_schedule_untouched_above_measured_range():
    # We have no evidence above the threshold — the original schedule stands.
    cm = CostModel()
    assert cm._lookup_slippage(50.0) == 0.003
    assert cm._lookup_slippage(200.0) == 0.005
    assert cm._lookup_slippage(1000.0) == 0.015


def test_entry_cost_is_half_spread_only_for_small_clips():
    # At p=0.255 the half-spread floor (0.006) dominates; with impact gone
    # the modeled entry cost should match the measured ~0.66c.
    cm = CostModel()
    est = cm.entry_cost(raw_price=0.255, side="BUY", stake=5.0)
    assert est.slippage_cost == 0.0
    assert abs(est.total_cost - 0.006) < 1e-9
    assert abs(est.effective_price - 0.261) < 1e-6


def test_threshold_is_configurable_and_zero_disables():
    cm = CostModel(no_impact_below_usd=0.0)
    assert cm._lookup_slippage(5.0) == 0.003  # old behaviour restored


def test_zero_and_negative_stake_still_free():
    cm = CostModel()
    assert cm._lookup_slippage(0) == 0
    assert cm._lookup_slippage(-5) == 0
