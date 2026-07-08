"""Pre-registered new-alpha exploration (family: alpha_candidates_2026_07).

Read-only. Runs candidate strategy predicates through the A1 research
harness (net-of-cost, point-in-time, event-clustered CI, walk-forward
folds, within-event shuffle test) and records every result to the FDR
ledger. Each candidate is tied to a SPECIFIC prior finding so we are
testing a belief, not fishing:

  smart_cheap    — resolution_decay-shaped BUYs where a PROVEN-accurate
                   wallet is the buyer (wallet graph as a FEATURE, the one
                   use the copy-KILL explicitly permits). If the market is
                   self-calibrated (A2), wallet quality is the only thing
                   that could add edge on cheap near-resolution flow.
  big_conviction — cheap BUYs with a large stake. Tests C5's untested
                   premise that leader bet SIZE predicts correctness.
  favorite_side  — the 0.60-0.90 favorite side. Tests the A2 self-
                   calibration finding from the opposite direction.
  uncrowded_cheap— cheap BUYs with ZERO co-entries in the visible 24h.
                   Inverts the harness shakedown finding that CROWDING is
                   anti-signal (-0.74/$): is uncrowded flow better?

Verdict 'pass' needs event-clustered CI lo>0 AND ex-top-3>0 AND shuffle
p<=0.05 AND same-sign folds at n_events>=30; promotion additionally needs
family BH-FDR q<=0.10. Family budget 20 (research_fdr_families.md).
"""
from __future__ import annotations

import logging

from trading_platform.polymarket.signal_research_harness import Hypothesis, replay
from trading_platform.polymarket.research_ledger import bh_fdr_report

FAMILY = "alpha_candidates_2026_07"

CANDIDATES = [
    Hypothesis(
        "smart_cheap", FAMILY,
        "cheap BUY (0.05-0.40) by a wallet with trailing WR>=60% on n>=20",
        {"price": [0.05, 0.40], "wallet_wr_min": 0.60, "wallet_n_min": 20},
        lambda t, fv: (0.05 <= fv.price <= 0.40
                       and fv.wallet_trailing_n(90) >= 20
                       and (fv.wallet_trailing_wr(90) or 0) >= 0.60)),
    Hypothesis(
        "big_conviction", FAMILY,
        "cheap BUY (0.05-0.40) with stake >= $100 (leader-conviction size)",
        {"price": [0.05, 0.40], "size_usd_min": 100},
        lambda t, fv: 0.05 <= fv.price <= 0.40 and fv.size_usd >= 100),
    Hypothesis(
        "favorite_side", FAMILY,
        "favorite-side BUY (0.60-0.90) — self-calibration test, other side",
        {"price": [0.60, 0.90]},
        lambda t, fv: 0.60 <= fv.price <= 0.90),
    Hypothesis(
        "uncrowded_cheap", FAMILY,
        "cheap BUY (0.05-0.40) with ZERO visible co-entries in 24h",
        {"price": [0.05, 0.40], "max_coentries": 0},
        lambda t, fv: (0.05 <= fv.price <= 0.40
                       and fv.n_wallets_same_side_24h() == 0)),
]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(f"\nALPHA-CANDIDATE EXPLORATION — family {FAMILY}, 90d, net-of-cost\n")
    for h in CANDIDATES:
        out = replay(h, days=90, record=True)
        print(f"{h.hypothesis_id:<16} verdict={out['verdict']:<12} "
              f"n={out['n_trades']:<6} events={out['n_events']:<5} "
              f"EV_net={out['ev_net']:+.4f}/$ (gross {out['ev_gross']:+.4f}) "
              f"exTop3=${out['ex_top3_ev']:+.3f} "
              f"CI=[{out['ci_lo']:+.4f},{out['ci_hi']:+.4f}] "
              f"p_shuf={out['p_shuffle']} folds={out['fold_evs']}")
    print("\nBH-FDR report:")
    rep = bh_fdr_report(FAMILY)
    print(f"  tested={rep['tested']} fdr_passing={rep['fdr_passing']}")
    for e in rep["entries"]:
        print(f"    {e['hypothesis_id']:<16} p={e['p']:.4f} "
              f"q_thresh={e.get('bh_threshold')} fdr_pass={e['fdr_pass']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
