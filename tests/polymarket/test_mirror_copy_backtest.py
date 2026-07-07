"""N6: mirror-exit copy diagnostic — exit-price waterfall + return math."""
from trading_platform.polymarket import mirror_copy_backtest as mcb


def test_copy_return_profit_on_scalp():
    # Buy YES at 0.20, leader sells at 0.60, negligible latency -> big win.
    r = mcb._copy_return(0.20, 0.60, latency_s=3)
    assert r > 1.5  # (0.60-0.20)/0.20 ~ +2.0, minus tiny slip


def test_copy_return_loss():
    r = mcb._copy_return(0.60, 0.20, latency_s=3)
    assert r < 0  # sold below entry


def test_latency_haircut_hurts_poller_more():
    fast = mcb._copy_return(0.30, 0.50, mcb.FAST_LANE_LATENCY_S)
    poller = mcb._copy_return(0.30, 0.50, mcb.POLLER_LATENCY_S)
    assert poller <= fast  # more latency -> worse fills -> lower return


def test_return_clipped():
    # 0.10 -> 0.99 would be +8.9x; within the 9.0 clip but extreme longshots cap.
    r = mcb._copy_return(0.10, 0.99, latency_s=0)
    assert r <= mcb.RET_CLIP_HI


def test_resolution_payout_yes_win():
    # asset is the YES token, prices show YES won (slot 0 = 0.999).
    p = mcb._resolution_payout("YES_TOK", "YES_TOK", "NO_TOK",
                               '["0.999", "0.001"]', True)
    assert p == 1.0


def test_resolution_payout_no_token_win():
    # We hold the NO token and NO won (slot 1 high) -> our token pays 1.0.
    p = mcb._resolution_payout("NO_TOK", "YES_TOK", "NO_TOK",
                               '["0.001", "0.999"]', True)
    assert p == 1.0


def test_resolution_payout_unresolved():
    assert mcb._resolution_payout("t", "y", "n", '["0.5","0.5"]', True) is None
    assert mcb._resolution_payout("t", "y", "n", None, True) is None
    assert mcb._resolution_payout("t", "y", "n", '["0.99","0.01"]', False) is None


def test_cohort_stats():
    s = mcb._cohort_stats([1.0, -0.5, 2.0])
    assert s["n"] == 3 and s["wr"] == round(2 / 3, 3)
    assert abs(s["total"] - 2.5) < 1e-9
