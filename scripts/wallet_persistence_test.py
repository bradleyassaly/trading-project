"""Per-wallet copyability: the persistence test (read-only diagnostic).

Question (user, 2026-07-09): are there INDIVIDUAL wallets we should copy
exclusively? Two untested angles vs prior kills:
  1. SIZE-PROPORTIONAL copying — replicate each wallet's own conviction
     weighting (the thing that makes them +20.6% notional-weighted while
     flat-stake copy is -36%). All prior copy tests were flat-stake.
  2. PERSISTENCE — select top wallets on window 1 (first 45d), score them
     OUT-OF-SAMPLE on window 2 (last 45d). If per-wallet returns don't
     persist, wallet selection is luck-chasing regardless of sizing.

Honesty: entries pay the 33-min lag drift + CostModel spread/slippage;
exits at resolution (pnl sign; resolution exits cost-exempt); returns
clipped [-1, +9]; selection uses ONLY window-1 data. Does NOT reopen the
pre-registered copy-entry kill — a positive result goes to the ledger and
a shadow lane, never straight to live.
"""
from __future__ import annotations

import time
from collections import defaultdict

from trading_platform.polymarket.db_connection import db
from trading_platform.polymarket.cost_model import CostModel

DAYS = 90
LAG_DRIFT = 0.00002 * 33 * 60      # entry price drift over the indexing lag
MIN_N_SELECT = 20                   # window-1 floor to be rankable
MIN_N_SCORE = 10                    # window-2 floor to be scoreable
CLIP_LO, CLIP_HI = -1.0, 9.0
_cost = CostModel()


def _net_ret(price: float, won: bool) -> float:
    eff = _cost.entry_cost(min(0.999, price + LAG_DRIFT), "BUY", 5.0).effective_price
    r = (1.0 - eff) / max(eff, 1e-3) if won else -1.0
    return max(CLIP_LO, min(CLIP_HI, r))


def _wallet_stats(rows) -> dict:
    """rows: (notional, price, won, event). Returns flat + proportional net."""
    n = len(rows)
    flat = [_net_ret(p, w) for (_, p, w, _) in rows]
    tot_notional = sum(no for (no, _, _, _) in rows)
    prop = (sum(no * _net_ret(p, w) for (no, p, w, _) in rows)
            / max(tot_notional, 1e-9))
    trimmed = sorted(flat)[:-3] if n > 3 else flat
    return {"n": n, "events": len({e for (_, _, _, e) in rows}),
            "flat_ev": sum(flat) / n,
            "flat_ex_top3": sum(trimmed) / max(len(trimmed), 1),
            "prop_ret": prop, "notional": tot_notional,
            "wr": sum(1 for (_, _, w, _) in rows if w) / n}


def main() -> int:
    import sys
    record = "--record" in sys.argv
    now = int(time.time())
    cut0, cut1 = now - DAYS * 86400, now - (DAYS // 2) * 86400
    with db() as conn:
        raw = conn.execute(
            """SELECT wallet, size*price, price, pnl, timestamp,
                      COALESCE(event_slug, condition_id)
                 FROM wallet_trades
                WHERE side='BUY' AND market_resolved=1 AND pnl_reliable=1
                  AND pnl IS NOT NULL AND size*price > 0 AND timestamp > %s
                  AND price BETWEEN 0.02 AND 0.98""",
            (cut0,)).fetchall()
    w1: dict[str, list] = defaultdict(list)
    w2: dict[str, list] = defaultdict(list)
    for wallet, notional, price, pnl, ts, ev in raw:
        row = (float(notional), float(price), (pnl or 0) > 0, ev)
        (w1 if ts < cut1 else w2)[wallet.lower()].append(row)

    s1 = {w: _wallet_stats(r) for w, r in w1.items() if len(r) >= MIN_N_SELECT}
    s2 = {w: _wallet_stats(r) for w, r in w2.items() if len(r) >= MIN_N_SCORE}
    both = sorted(set(s1) & set(s2))
    print(f"{len(raw)} resolved BUYs | rankable in W1: {len(s1)} wallets | "
          f"scoreable in W2: {len(s2)} | overlap: {len(both)}\n")

    # ── persistence: Spearman rank correlation W1 -> W2 ────────────────────
    def spearman(pairs):
        n = len(pairs)
        if n < 10:
            return None
        def ranks(vals):
            order = sorted(range(n), key=lambda i: vals[i])
            rk = [0.0] * n
            for pos, i in enumerate(order):
                rk[i] = pos
            return rk
        a = ranks([p[0] for p in pairs]); b = ranks([p[1] for p in pairs])
        ma, mb = sum(a) / n, sum(b) / n
        cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
        va = sum((x - ma) ** 2 for x in a); vb = sum((x - mb) ** 2 for x in b)
        return cov / max((va * vb) ** 0.5, 1e-12)

    for metric in ("prop_ret", "flat_ex_top3", "wr"):
        rho = spearman([(s1[w][metric], s2[w][metric]) for w in both])
        print(f"persistence (Spearman rho, W1->W2) on {metric:<13}: "
              f"{rho:+.3f}" if rho is not None else f"{metric}: n too small")

    # ── OOS scoreboard: select top-5 / top-decile on W1, score on W2 ───────
    _record_payload = {}
    for metric in ("prop_ret", "flat_ex_top3"):
        ranked = sorted(both, key=lambda w: -s1[w][metric])
        for label, sel in (("top-5", ranked[:5]),
                           ("top-decile", ranked[:max(1, len(ranked) // 10)])):
            pooled = [r for w in sel for r in w2[w]]
            st = _wallet_stats(pooled) if pooled else None
            if st:
                print(f"\nselect by W1 {metric} -> {label} ({len(sel)} wallets), "
                      f"scored OOS on W2:")
                print(f"  n={st['n']} events={st['events']} wr={st['wr']:.2f} "
                      f"flat_ev={st['flat_ev']:+.3f}/$ "
                      f"exTop3={st['flat_ex_top3']:+.3f} "
                      f"PROPORTIONAL={st['prop_ret']:+.3f}/$")
                if metric == "prop_ret" and label == "top-5":
                    _record_payload = {"st": st, "sel": sel}
                if label == "top-5":
                    for w in sel:
                        print(f"    {w[:14]} W1[{metric}]={s1[w][metric]:+.3f} "
                              f"-> W2 prop={s2[w]['prop_ret']:+.3f} "
                              f"flat={s2[w]['flat_ev']:+.3f} n={s2[w]['n']}")

    # ── standing ledger record (G2): the top-5-proportional OOS result ─────
    if record and _record_payload:
        from trading_platform.polymarket.research_ledger import register_run
        st, sel = _record_payload["st"], _record_payload["sel"]
        pos_wallets = sum(1 for w in sel if s2[w]["prop_ret"] > 0)
        if st["events"] < 30:
            verdict = "insufficient"
        elif st["prop_ret"] > 0 and pos_wallets >= 4:
            verdict = "pass"
        else:
            verdict = "fail"
        rid = register_run(
            hypothesis_id="top5_prop_oos", family="wallet_persistence",
            description="top-5 W1 prop-return wallets, size-proportional "
                        "copy, scored OOS on W2 (net of lag+cost)",
            params={"days": DAYS, "min_n_select": MIN_N_SELECT,
                    "wallets": [w[:14] for w in sel]},
            window_days=DAYS, lag_s=33 * 60,
            results={"n_trades": st["n"], "n_events": st["events"],
                     "n_wallets": len(sel), "ev_net": round(st["prop_ret"], 4),
                     "ev_gross": round(st["flat_ev"], 4),
                     "ex_top3_ev": round(st["flat_ex_top3"], 4),
                     "wr": round(st["wr"], 3), "ci_lo": None, "ci_hi": None,
                     "fold_evs": [], "p_shuffle": None,
                     "pos_wallets_of_5": pos_wallets},
            verdict=verdict)
        print(f"\n[ledger] recorded run id={rid} verdict={verdict} "
              f"(events={st['events']}, pos_wallets={pos_wallets}/5)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
