"""N6 + C1/C2: mirror-exit copy diagnostic — exit waterfall, payout fallback,
fill-evidence modeling, cost wiring, cluster verdicts."""
import inspect

from trading_platform.polymarket import mirror_copy_backtest as mcb
from trading_platform.polymarket.cost_model import CostModel


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


# ---------------------------------------------------------------------------
# C1+C2 amendment: payout fallback, fill waterfall, booking, verdicts
# ---------------------------------------------------------------------------

def test_payout_from_pnl_sign():
    # enrich_resolution: pnl = size*(1-price) if won else -size*price
    assert mcb._resolution_payout_from_pnl(3.2) == 1.0
    assert mcb._resolution_payout_from_pnl(-1.1) == 0.0
    assert mcb._resolution_payout_from_pnl(0) == 0.0
    assert mcb._resolution_payout_from_pnl(None) == 0.0


def test_effective_exit_confirmed():
    # A taker printed AT the leader's sell price → full (slipped) fill.
    px, kind = mcb._effective_exit(0.60, 3, best_print=0.60, best_buy=None,
                                   covered=True)
    assert kind == "mirror_confirmed"
    assert px == max(0.001, 0.60 - mcb.SLIP_PER_SEC * 3)


def test_effective_exit_degraded_to_evidence():
    # Best crossing evidence was 5c below the leader's price — we'd have
    # sold into that, not the leader's fill.
    px, kind = mcb._effective_exit(0.60, 3, best_print=0.55, best_buy=0.50,
                                   covered=True)
    assert kind == "mirror_degraded"
    assert px == 0.55


def test_effective_exit_unfilled_on_covered_silence():
    # Market HAS tick coverage and nothing traded in the window → the
    # resting mirror sell does not fill; caller books resolution.
    px, kind = mcb._effective_exit(0.60, 3, best_print=None, best_buy=None,
                                   covered=True)
    assert (px, kind) == (None, "unfilled")


def test_effective_exit_no_coverage_policies():
    px, kind = mcb._effective_exit(0.60, 3, None, None, covered=False,
                                   policy="assume-fill")
    assert kind == "mirror_assumed_nocoverage" and px is not None
    px, kind = mcb._effective_exit(0.60, 3, None, None, covered=False,
                                   policy="resolution")
    assert (px, kind) == (None, "unfilled")


def _row(**kw):
    base = {"wallet": "w", "condition_id": "c", "category": "sports",
            "asset": "tok", "price": 0.40, "timestamp": 1, "pnl": None,
            "sell_ts": None, "sell_price": None, "outcome_prices": None,
            "closed": None, "yes_token_id": "tok", "no_token_id": "no",
            "event_slug": "ev1", "print_fast": None, "print_poll": None,
            "buy_fast": None, "buy_poll": None, "covered": 0}
    base.update(kw)
    return base


def test_book_copy_never_sold_books_resolution_from_pnl():
    # The 83% survivorship fix: never-sold row with a failed markets join
    # books the pnl-sign payout instead of vanishing. Leader lost → exit 0.
    out = mcb._book_copy(_row(pnl=-2.0), 3, 5.0, None, "assume-fill")
    assert out["exit_kind"] == "resolution_native"
    assert out["pnl"] == -5.0  # total loss of the $5 stake


def test_book_copy_unfilled_mirror_falls_to_resolution():
    # Leader sold at 0.60 but nothing crossed in a covered market → the
    # mirror exit is a fiction; the copy rides to resolution (leader WON
    # here, so payout 1.0 — but our copy's pnl books at resolution, not
    # at the leader's exit).
    out = mcb._book_copy(_row(sell_ts=10, sell_price=0.60, covered=1,
                              pnl=3.0), 3, 5.0, None, "assume-fill")
    assert out["exit_kind"] == "unfilled"
    assert out["pnl"] > 0  # (1.0 - 0.40)/0.40 clipped/staked


def test_book_copy_costs_reduce_mirror_but_not_resolution():
    cm = CostModel()
    row_mirror = _row(sell_ts=10, sell_price=0.60, covered=1,
                      print_fast=0.60, pnl=3.0)
    gross = mcb._book_copy(row_mirror, 3, 5.0, None, "assume-fill")
    net = mcb._book_copy(row_mirror, 3, 5.0, cm, "assume-fill")
    assert net["pnl"] < gross["pnl"]  # spread+slip paid on entry AND exit
    row_res = _row(pnl=3.0)
    gross_r = mcb._book_copy(row_res, 3, 5.0, None, "assume-fill")
    net_r = mcb._book_copy(row_res, 3, 5.0, cm, "assume-fill")
    # resolution exit is cost-exempt: only the ENTRY cost differs
    assert net_r["pnl"] < gross_r["pnl"]
    assert (gross_r["pnl"] - net_r["pnl"]) < (gross["pnl"] - net["pnl"]) + 1e-6


def test_book_copy_perfect_fill_ignores_evidence():
    row = _row(sell_ts=10, sell_price=0.60, covered=1)  # no evidence at all
    out = mcb._book_copy(row, 3, 5.0, None, "assume-fill", perfect_fill=True)
    assert out["exit_kind"] == "mirror_confirmed"


def _fake_out(global_avg, global_wallets, copies, clusters,
              qualified=6, wallets_min_n=50):
    return {"total_copies": copies,
            "poller": {"top_decile": {"avg": global_avg,
                                      "n_wallets": global_wallets},
                       "qualified_count": qualified,
                       "wallets_with_min_n": wallets_min_n,
                       "clusters": clusters}}


def test_verdict_amendment2_zero_qualified_is_kill():
    # 42k copies, 223 wallets at min_n, 0 qualified — overwhelming negative
    # evidence is a KILL, not "insufficient data" (Amendment 2).
    v, d = mcb._verdict(_fake_out(0.0, 0, 42_665, {},
                                  qualified=0, wallets_min_n=223))
    assert v == "KILL" and "Amendment 2" in d


def test_verdict_amendment2_needs_scale():
    # Small universe with 0 qualified stays INSUFFICIENT.
    v, _ = mcb._verdict(_fake_out(0.0, 0, 500, {},
                                  qualified=0, wallets_min_n=20))
    assert v == "INSUFFICIENT"


def test_verdict_amendment2_cluster_can_still_save():
    v, _ = mcb._verdict(_fake_out(0.0, 0, 42_665,
                                  {"sports": {"status": "measurable", "avg": 0.30}},
                                  qualified=0, wallets_min_n=223))
    assert v == "KEEP"


def test_verdict_kill_when_global_and_clusters_fail():
    v, _ = mcb._verdict(_fake_out(0.05, 6, 500, {
        "sports": {"status": "measurable", "avg": 0.02},
        "politics": {"status": "insufficient", "avg": 9.9}}))
    assert v == "KILL"


def test_verdict_cluster_can_save_from_kill():
    v, d = mcb._verdict(_fake_out(0.05, 6, 500, {
        "sports": {"status": "measurable", "avg": 0.30}}))
    assert v == "KEEP" and "sports" in d


def test_verdict_global_keep():
    v, _ = mcb._verdict(_fake_out(1.50, 6, 500, {}))
    assert v == "KEEP"


def test_verdict_insufficient_never_improvises_kill():
    # Global guard fired; a measurable cluster fails the floor — the
    # registered amendment says INSUFFICIENT, not KILL.
    v, _ = mcb._verdict(_fake_out(0.05, 2, 500, {
        "sports": {"status": "measurable", "avg": 0.02}}))
    assert v == "INSUFFICIENT"


def test_fetch_sql_matches_same_asset():
    # Cross-token exit regression: a leader selling the market's OTHER token
    # must not supply our exit price.
    src = inspect.getsource(mcb._fetch_mirror_trades)
    assert src.count("s.asset = f.asset") >= 1
    assert "s2.asset = f.asset" in src
