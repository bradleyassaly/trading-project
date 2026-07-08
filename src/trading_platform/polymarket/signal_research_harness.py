"""Counterfactual signal-research harness (roadmap A1) — READ ONLY.

Replays wallet_trades → resolution (~250k resolved BUYs / 60d) with strict
point-in-time discipline, net-of-cost EV as the primary output, and
governance baked in: every run lands in the research ledger (G2), families
have hard pre-registered budgets, and the pass verdict requires an
event-clustered CI above zero, positive ex-top-3 EV, a within-event shuffle
test, and same-sign folds. Replaces months of live discovery cycles with a
minutes-long replay — for a throughput-constrained book, this is the
whole ballgame.

Point-in-time rules (the honesty core):
  * Flow features (prices, sizes, co-entry counts) see only rows with
    ts <= trade_ts − INDEXING_LAG_S (the 33-min data-api indexing lag —
    what a live copy of this predicate could actually have known).
  * Outcome-dependent features (trailing WR/EV) additionally require
    ts <= trade_ts − OUTCOME_EMBARGO_S (7d): resolution TIMESTAMPS are
    unknowable for ~93% of markets (the P1 resolved_at gap), so a fixed
    embargo is the only defensible bound. Documented limitation: features
    of markets that resolved inside the embargo window are excluded even
    though a live system might have known them.

Resolution join: token_won = (pnl > 0). enrich_resolution sets
pnl = size·(1−price) if the held token won else −size·price, and the fetch
filters pnl_reliable=1 — the SIGN encodes the outcome. Do NOT use
markets.closed/outcome_prices (closed is never 1 in prod).

Manual tool: python -m trading_platform.polymarket.signal_research_harness
             --list-builtin | --run <name> [--days 60] [--json]
             | --fdr-report --family <F>
Never scheduled; the ledger insert is the only DB write.
"""
from __future__ import annotations

import argparse
import bisect
import json
import logging
import os
import random
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.cost_model import CostModel

logger = logging.getLogger(__name__)

INDEXING_LAG_S = int(os.environ.get("HARNESS_LAG_S", str(33 * 60)))
OUTCOME_EMBARGO_S = int(os.environ.get("HARNESS_OUTCOME_EMBARGO_S", str(7 * 86400)))
LAG_DRIFT_PER_S = float(os.environ.get("MIRROR_SLIP_PER_SEC", "0.00002"))
RET_CLIP_LO, RET_CLIP_HI = -1.0, 9.0
MIN_EVENTS_FOR_VERDICT = 30


@dataclass(frozen=True)
class Hypothesis:
    hypothesis_id: str
    family: str
    description: str
    params: dict
    filter_fn: Callable[[dict, "FeatureView"], bool] = field(repr=False)


class FeatureView:
    """Lazy point-in-time feature accessor for ONE candidate trade.

    Backed by per-wallet and per-token ordered indices built once from the
    single fetch. Every accessor enforces the visibility rules in the
    module docstring.
    """

    def __init__(self, trade: dict, wallet_hist: dict, token_ts: dict,
                 lag_s: int):
        self._t = trade
        self._wh = wallet_hist      # wallet -> prefix-sum index (see
                                    # _build_wallet_index); O(log n) windows
        self._tok = token_ts        # (cid) -> sorted ts list of BUYs
        self._lag = lag_s

    # -- lag-free intrinsics ------------------------------------------------
    @property
    def price(self) -> float:
        return float(self._t["price"])

    @property
    def size_usd(self) -> float:
        return float(self._t["size"]) * float(self._t["price"])

    @property
    def category(self) -> str:
        return (self._t.get("category") or "other").lower()

    # -- lag-shifted flow features -------------------------------------------
    def n_wallets_same_side_24h(self) -> int:
        """Distinct earlier BUY prints on this market in the last 24h,
        visible as of trade_ts − lag."""
        ts_list = self._tok.get(self._t["condition_id"]) or []
        hi = bisect.bisect_right(ts_list, self._t["timestamp"] - self._lag)
        lo = bisect.bisect_left(ts_list, self._t["timestamp"] - self._lag - 86400)
        return max(0, hi - lo)

    # -- embargoed outcome features -------------------------------------------
    def _window(self, days: int):
        """(idx, wins, stake, pnl) over the wallet's prior BUYs in
        [trade_ts − days, trade_ts − OUTCOME_EMBARGO_S]. O(log n) via
        prefix sums (was O(n) per call → O(N²) over the stream, which
        stalled the 250k-row replay). Returns None if the wallet is unseen."""
        wl = self._wh.get(self._t["wallet"])
        if not wl:
            return None
        ts = wl["ts"]
        hi = bisect.bisect_right(ts, self._t["timestamp"] - OUTCOME_EMBARGO_S)
        lo = bisect.bisect_left(ts, self._t["timestamp"] - days * 86400)
        if hi <= lo:
            return (0, 0, 0.0, 0.0)
        return (hi - lo,
                wl["cwins"][hi] - wl["cwins"][lo],
                wl["cstake"][hi] - wl["cstake"][lo],
                wl["cpnl"][hi] - wl["cpnl"][lo])

    def wallet_trailing_n(self, days: int = 30) -> int:
        w = self._window(days)
        return w[0] if w else 0

    def wallet_trailing_wr(self, days: int = 30) -> float | None:
        w = self._window(days)
        if not w or w[0] <= 0:
            return None
        return w[1] / w[0]

    def wallet_trailing_ev(self, days: int = 30) -> float | None:
        w = self._window(days)
        if not w or w[0] <= 0 or w[2] <= 0:
            return None
        return w[3] / w[2]


def _build_wallet_index(trades: list[dict]) -> dict[str, dict]:
    """Per-wallet prefix sums over ts-ordered BUYs, so FeatureView trailing
    windows are O(log n). trades MUST be ascending by timestamp (the fetch
    ORDERs BY timestamp). Shared by replay() and the tests so the fit-time
    and test-time index structures can never drift."""
    idx: dict[str, dict] = {}
    for t in trades:
        wl = idx.setdefault(t["wallet"], {"ts": [], "cwins": [0],
                                          "cstake": [0.0], "cpnl": [0.0]})
        wl["ts"].append(t["timestamp"])
        won = 1 if (t.get("pnl") or 0) > 0 else 0
        stake = float(t.get("size") or 0) * float(t.get("price") or 0)
        wl["cwins"].append(wl["cwins"][-1] + won)
        wl["cstake"].append(wl["cstake"][-1] + stake)
        wl["cpnl"].append(wl["cpnl"][-1] + float(t.get("pnl") or 0))
    return idx


def _fetch(conn, days: int) -> list[dict]:
    cutoff = int(time.time()) - days * 86400
    rows = conn.execute(
        """SELECT wallet, condition_id, event_slug, category, price, size,
                  pnl, timestamp
             FROM wallet_trades
            WHERE side = 'BUY' AND market_resolved = 1
              AND pnl IS NOT NULL AND pnl_reliable = 1
              AND timestamp > ? AND size * price > 0
            ORDER BY timestamp""",
        (cutoff,),
    ).fetchall()
    cols = ["wallet", "condition_id", "event_slug", "category", "price",
            "size", "pnl", "timestamp"]
    return [dict(zip(cols, r)) for r in rows]


def _ex_top3(pnls: list[float]) -> float:
    trimmed = sorted(pnls)[:-3] if len(pnls) > 3 else pnls
    return sum(trimmed) / max(len(trimmed), 1)


def event_bootstrap_ci(event_pnls: dict[str, list[float]], stakes_total: float,
                       n_iter: int = 1000, seed: int = 42,
                       ) -> tuple[float, float]:
    """95% CI on per-dollar EV, resampling EVENTS with replacement (G3)."""
    keys = list(event_pnls)
    k = len(keys)
    if k < 5 or stakes_total <= 0:
        ev = sum(p for v in event_pnls.values() for p in v) / max(stakes_total, 1e-9)
        return (ev - 1.0, ev + 1.0)
    per_event = {e: sum(v) for e, v in event_pnls.items()}
    stake_per_event = stakes_total / k  # flat-stake replay: uniform by count
    rng = random.Random(seed)
    stats = []
    for _ in range(n_iter):
        tot = 0.0
        n_tr = 0
        for _ in range(k):
            e = keys[rng.randrange(k)]
            tot += per_event[e]
            n_tr += len(event_pnls[e])
        stats.append(tot / max(stake_per_event * k, 1e-9))
    stats.sort()
    return (stats[int(0.025 * n_iter)], stats[int(0.975 * n_iter)])


def _shuffle_p(entries: list[dict], observed_ev: float, stake: float,
               n_shuffle: int = 200, seed: int = 42) -> float:
    """Within-event label permutation: reassign token_won labels across the
    filtered set (preserving the overall win count), recompute EV. p =
    frac(shuffled EV >= observed)."""
    if not entries:
        return 1.0
    labels = [e["won"] for e in entries]
    rets_if_won = [e["ret_won"] for e in entries]
    rng = random.Random(seed)
    n = len(entries)
    ge = 0
    for _ in range(n_shuffle):
        rng.shuffle(labels)
        pnl = sum((rets_if_won[i] if labels[i] else -1.0) * stake
                  for i in range(n))
        ev = pnl / (n * stake)
        if ev >= observed_ev:
            ge += 1
    return ge / n_shuffle


def replay(h: Hypothesis, *, days: int = 60, stake: float = 5.0,
           lag_s: int = INDEXING_LAG_S, cost: CostModel | None = None,
           n_folds: int = 5, n_shuffle: int = 200, seed: int = 42,
           record: bool = True, db_path: str | None = None) -> dict[str, Any]:
    cost = cost or CostModel()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        trades = _fetch(conn, days)
    finally:
        try: conn.close()
        except Exception: pass

    # G2 hard cap BEFORE the expensive work: a new id in a full family
    # must fail fast (register_run would raise later anyway).
    if record:
        from trading_platform.polymarket import research_ledger as rl
        rl.ensure_schema(db_path=db_path)

    # point-in-time indices (prefix-sum wallet index → O(log n) windows)
    wallet_hist = _build_wallet_index(trades)
    token_ts: dict[str, list[int]] = {}
    for t in trades:
        token_ts.setdefault(t["condition_id"], []).append(t["timestamp"])

    entries: list[dict] = []
    seen_cids: set[str] = set()
    for t in trades:
        if t["condition_id"] in seen_cids:
            continue
        fv = FeatureView(t, wallet_hist, token_ts, lag_s)
        try:
            if not h.filter_fn(t, fv):
                continue
        except Exception:
            continue
        seen_cids.add(t["condition_id"])
        won = (t["pnl"] or 0) > 0
        eff_entry = cost.entry_cost(
            min(0.999, float(t["price"]) + LAG_DRIFT_PER_S * lag_s),
            "BUY", stake).effective_price
        ret_won = max(RET_CLIP_LO, min(RET_CLIP_HI,
                                       (1.0 - eff_entry) / max(eff_entry, 1e-3)))
        net_ret = ret_won if won else -1.0
        raw_stake = float(t["size"]) * float(t["price"])
        gross_ret = max(RET_CLIP_LO, min(RET_CLIP_HI,
                                         float(t["pnl"]) / max(raw_stake, 1e-9)))
        entries.append({
            "event": t.get("event_slug") or t["condition_id"],
            "wallet": t["wallet"], "ts": t["timestamp"],
            "won": won, "ret_won": ret_won,
            "net_pnl": net_ret * stake, "gross_ret": gross_ret,
        })

    n = len(entries)
    if n == 0:
        results = {"n_trades": 0, "n_events": 0, "n_wallets": 0,
                   "ev_net": 0.0, "ev_gross": 0.0, "wr": 0.0,
                   "ex_top3_ev": 0.0, "ci_lo": 0.0, "ci_hi": 0.0,
                   "fold_evs": [], "p_shuffle": None,
                   "total_pnl": 0.0}
        verdict = "insufficient"
    else:
        total_pnl = sum(e["net_pnl"] for e in entries)
        ev_net = total_pnl / (n * stake)
        ev_gross = sum(e["gross_ret"] for e in entries) / n
        wr = sum(1 for e in entries if e["won"]) / n
        ex3 = _ex_top3([e["net_pnl"] for e in entries])
        event_pnls: dict[str, list[float]] = {}
        for e in entries:
            event_pnls.setdefault(e["event"], []).append(e["net_pnl"])
        ci_lo, ci_hi = event_bootstrap_ci(event_pnls, n * stake, seed=seed)
        # walk-forward folds with event purge: an event belongs wholly to
        # the fold of its FIRST trade.
        first_ts = {}
        for e in entries:
            first_ts.setdefault(e["event"], e["ts"])
        events_sorted = sorted(first_ts, key=first_ts.get)
        fold_of = {ev: min(i * n_folds // max(len(events_sorted), 1), n_folds - 1)
                   for i, ev in enumerate(events_sorted)}
        fold_pnl = [0.0] * n_folds
        fold_n = [0] * n_folds
        for e in entries:
            f = fold_of[e["event"]]
            fold_pnl[f] += e["net_pnl"]
            fold_n[f] += 1
        fold_evs = [round(fold_pnl[i] / (fold_n[i] * stake), 4)
                    for i in range(n_folds) if fold_n[i] > 0]
        p_shuffle = _shuffle_p(entries, ev_net, stake, n_shuffle, seed)
        results = {
            "n_trades": n, "n_events": len(event_pnls),
            "n_wallets": len({e["wallet"] for e in entries}),
            "total_pnl": round(total_pnl, 2),
            "ev_net": round(ev_net, 4), "ev_gross": round(ev_gross, 4),
            "wr": round(wr, 3), "ex_top3_ev": round(ex3, 4),
            "ci_lo": round(ci_lo, 4), "ci_hi": round(ci_hi, 4),
            "fold_evs": fold_evs, "p_shuffle": round(p_shuffle, 4),
        }
        if len(event_pnls) < MIN_EVENTS_FOR_VERDICT:
            verdict = "insufficient"
        elif (ci_lo > 0 and ex3 > 0 and p_shuffle <= 0.05
                and fold_evs and all(f > 0 for f in fold_evs)):
            verdict = "pass"
        else:
            verdict = "fail"

    results["verdict"] = verdict
    results["hypothesis_id"] = h.hypothesis_id
    if record:
        from trading_platform.polymarket import research_ledger as rl
        results["ledger_id"] = rl.register_run(
            hypothesis_id=h.hypothesis_id, family=h.family,
            description=h.description, params=h.params,
            window_days=days, lag_s=lag_s, results=results,
            verdict=verdict, db_path=db_path)
    return results


# ── built-in example hypotheses (pre-registered family: 'a1_examples') ────
BUILTIN: dict[str, Hypothesis] = {
    "longshot_band": Hypothesis(
        "longshot_band", "a1_examples",
        "BUYs in the 0.05-0.15 band — does cheap longshot flow earn net?",
        {"price_lo": 0.05, "price_hi": 0.15},
        lambda t, fv: 0.05 <= fv.price <= 0.15),
    "politics_midband": Hypothesis(
        "politics_midband", "a1_examples",
        "Politics BUYs 0.20-0.50 (the ex-top-3 lottery question, net)",
        {"category": "politics", "price_lo": 0.20, "price_hi": 0.50},
        lambda t, fv: fv.category == "politics" and 0.20 <= fv.price <= 0.50),
    "crowded_entry": Hypothesis(
        "crowded_entry", "a1_examples",
        "BUYs where >=5 other prints hit the market in the visible 24h",
        {"min_coentries": 5},
        lambda t, fv: fv.n_wallets_same_side_24h() >= 5),
}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", type=str, default=None)
    ap.add_argument("--list-builtin", action="store_true")
    ap.add_argument("--fdr-report", action="store_true")
    ap.add_argument("--family", type=str, default="a1_examples")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--lag-s", type=int, default=INDEXING_LAG_S)
    ap.add_argument("--no-record", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.list_builtin:
        for name, h in BUILTIN.items():
            print(f"  {name:<22} [{h.family}] {h.description}")
        return 0
    if args.fdr_report:
        from trading_platform.polymarket.research_ledger import bh_fdr_report
        print(json.dumps(bh_fdr_report(args.family), indent=2))
        return 0
    if not args.run or args.run not in BUILTIN:
        print(f"--run must be one of: {', '.join(BUILTIN)}")
        return 2
    out = replay(BUILTIN[args.run], days=args.days, lag_s=args.lag_s,
                 record=not args.no_record)
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"{args.run}: verdict={out['verdict']} n={out['n_trades']} "
              f"events={out['n_events']} EV_net={out['ev_net']:+.4f}/$ "
              f"(gross {out['ev_gross']:+.4f}) exTop3=${out['ex_top3_ev']:+.2f} "
              f"CI=[{out['ci_lo']:+.4f},{out['ci_hi']:+.4f}] "
              f"p_shuffle={out['p_shuffle']} folds={out['fold_evs']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
