"""Mirror-exit copy-trading diagnostic (roadmap N6 + C1/C2) — READ ONLY.

The hold-to-resolution copyability backtest (naive_copy_backtest.py) found NO
wallet copyable and its own docstring admits it "undervalues wallets whose edge
is scalping (buy 0.40 -> sell 0.60 before close)". This diagnostic copies each
wallet's first BUY per market at a fixed stake, exits when the leader exits
(their next SELL of the SAME token), and books hold-to-resolution otherwise.

C1+C2 amendment (see reports/mirror_copy_kill_rule.md, Amendment 1 —
registered BEFORE the decision run):
  * Survivorship fix: run 1 silently dropped 83% of copies (never-sold rows
    whose markets-join payout failed) — the "everyone is a scalper" headline
    was an artifact. Payout now falls back to the sign of the leader's
    enriched pnl; ZERO copies are dropped.
  * Cross-token fix: the mirror sell must be of the same asset we hold.
  * Fill-evidence waterfall: a mirror exit only counts as filled at the
    leader's price if a taker print (market_ticks) or another wallet's BUY
    on the same token crossed within [sell_ts+latency, +FILL_WINDOW_S];
    lower evidence degrades the exit to the evidence price; covered markets
    with NO evidence book resolution instead ("unfilled").
  * Costs ON by default (cost_model.CostModel; resolution exits exempt).
  * Per-cluster (category) verdicts alongside the global one.

Two latency cohorts are reported SEPARATELY and never blended:
  * fast_lane  — chain-direct dispatch, ~3s from leader fill to our fill
  * poller     — the production fallback, ~5min (p50) lag

This module NEVER writes to the DB and is NOT scheduled — manual analysis:
`python -m trading_platform.polymarket.mirror_copy_backtest --days 120`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.cost_model import CostModel

logger = logging.getLogger(__name__)

STAKE_USD = 5.0
PRICE_MIN, PRICE_MAX = 0.10, 0.85
RET_CLIP_LO, RET_CLIP_HI = -1.0, 9.0

FAST_LANE_LATENCY_S = 3
POLLER_LATENCY_S = 300
# YES-space price slip per second of latency. Default is deliberately small;
# calibrate via env once real fill data accrues.
SLIP_PER_SEC = float(os.environ.get("MIRROR_SLIP_PER_SEC", "0.00002"))
# Flat round-trip cost fraction. If set (non-zero), OVERRIDES CostModel for
# back-compat with run 1/2; default path is the tiered CostModel.
COST_BPS = float(os.environ.get("MIRROR_COST_BPS", "0"))
# How long after (leader sell + our latency) a crossing taker still counts
# as evidence that our resting mirror sell would have filled.
FILL_WINDOW_S = int(os.environ.get("MIRROR_FILL_WINDOW_S", "900"))
# Evidence within this much of the leader's sell price counts as confirming
# the full leader price (one tick of tolerance).
CROSS_TOL = 0.01

_CLUSTER_MIN_TOP_DECILE = 3
_CLUSTER_MIN_COPIES = 75


def _fetch_mirror_trades(conn, days: int, fill_window: int = FILL_WINDOW_S) -> list[tuple]:
    """First resolved BUY per (wallet, market) + that wallet's earliest
    subsequent SELL of the SAME TOKEN (cross-token exits poisoned run 1/2),
    the leader's enriched pnl (payout fallback), markets metadata, and
    per-lane fill evidence:
      print_*  — best taker print on our token in the lane's fill window
      buy_*    — best other-wallet BUY on our token in the same window
      covered  — does market_ticks know this market at all (0 ⇒ evidence
                 absence means "no coverage", not "no takers")
    """
    cutoff = int(time.time()) - days * 86400
    fw = int(fill_window)
    lat_f, lat_p = FAST_LANE_LATENCY_S, POLLER_LATENCY_S
    return conn.execute(
        f"""
        WITH firsts AS (
            SELECT wt.wallet, wt.condition_id, wt.category, wt.asset,
                   wt.price, wt.size, wt.timestamp, wt.pnl,
                   ROW_NUMBER() OVER (
                       PARTITION BY wt.wallet, wt.condition_id
                       ORDER BY wt.timestamp
                   ) AS rn
              FROM wallet_trades wt
             WHERE wt.side = 'BUY'
               AND wt.market_resolved = 1
               AND wt.pnl IS NOT NULL
               AND wt.pnl_reliable = 1
               AND wt.timestamp > ?
               AND wt.price BETWEEN ? AND ?
               AND wt.size * wt.price > 0
               AND LOWER(COALESCE(wt.category, '')) NOT IN ('science')
        ),
        mirror AS (
            SELECT f.wallet, f.condition_id, f.category, f.asset,
                   f.price, f.timestamp, f.pnl,
                   (SELECT MIN(s.timestamp) FROM wallet_trades s
                     WHERE s.wallet = f.wallet AND s.condition_id = f.condition_id
                       AND s.asset = f.asset
                       AND s.side = 'SELL' AND s.timestamp > f.timestamp) AS sell_ts,
                   (SELECT s2.price FROM wallet_trades s2
                     WHERE s2.wallet = f.wallet AND s2.condition_id = f.condition_id
                       AND s2.asset = f.asset
                       AND s2.side = 'SELL' AND s2.timestamp > f.timestamp
                     ORDER BY s2.timestamp ASC LIMIT 1) AS sell_price
              FROM firsts f
             WHERE f.rn = 1
        )
        SELECT m.wallet, m.condition_id, m.category, m.asset, m.price,
               m.timestamp, m.pnl, m.sell_ts, m.sell_price,
               mk.outcome_prices, mk.closed, mk.yes_token_id, mk.no_token_id,
               mk.event_slug,
               CASE WHEN m.sell_ts IS NULL THEN NULL ELSE
                   (SELECT MAX(t.price) FROM market_ticks t
                     WHERE t.condition_id = m.condition_id
                       AND t.token_id = m.asset
                       AND t.timestamp BETWEEN m.sell_ts + {lat_f}
                                           AND m.sell_ts + {lat_f} + {fw})
               END AS print_fast,
               CASE WHEN m.sell_ts IS NULL THEN NULL ELSE
                   (SELECT MAX(t.price) FROM market_ticks t
                     WHERE t.condition_id = m.condition_id
                       AND t.token_id = m.asset
                       AND t.timestamp BETWEEN m.sell_ts + {lat_p}
                                           AND m.sell_ts + {lat_p} + {fw})
               END AS print_poll,
               CASE WHEN m.sell_ts IS NULL THEN NULL ELSE
                   (SELECT MAX(o.price) FROM wallet_trades o
                     WHERE o.asset = m.asset AND o.wallet <> m.wallet
                       AND o.side = 'BUY'
                       AND o.timestamp BETWEEN m.sell_ts + {lat_f}
                                           AND m.sell_ts + {lat_f} + {fw})
               END AS buy_fast,
               CASE WHEN m.sell_ts IS NULL THEN NULL ELSE
                   (SELECT MAX(o.price) FROM wallet_trades o
                     WHERE o.asset = m.asset AND o.wallet <> m.wallet
                       AND o.side = 'BUY'
                       AND o.timestamp BETWEEN m.sell_ts + {lat_p}
                                           AND m.sell_ts + {lat_p} + {fw})
               END AS buy_poll,
               CASE WHEN m.sell_ts IS NULL THEN 0
                    WHEN EXISTS (SELECT 1 FROM market_ticks t2
                                  WHERE t2.condition_id = m.condition_id)
                    THEN 1 ELSE 0
               END AS covered
          FROM mirror m
          LEFT JOIN markets mk ON mk.condition_id = m.condition_id
        """,
        (cutoff, PRICE_MIN, PRICE_MAX),
    ).fetchall()


def _resolution_payout(asset, yes_tok, no_tok, prices_raw, closed) -> float | None:
    """Hold-to-resolution payout (1.0/0.0) of the token we hold (the BUY asset),
    from the markets snapshot. None if unresolved/unknown."""
    if not closed or not prices_raw:
        return None
    try:
        prices = [float(x) for x in json.loads(prices_raw)]
    except (ValueError, TypeError):
        return None
    idx = 1 if str(asset) == str(no_tok) else 0
    try:
        px = prices[idx]
    except IndexError:
        return None
    if px >= 0.99:
        return 1.0
    if px <= 0.01:
        return 0.0
    return None


def _resolution_payout_from_pnl(pnl) -> float:
    """Payout of the held token from the SIGN of the leader's enriched pnl.

    Valid because enrich_resolution sets pnl = size·(1−price) when the held
    token won, else −size·price — the sign IS the payout. This closes the
    83% survivorship drop: run 1 skipped every never-sold copy whose markets
    join failed, which deleted the hold-to-resolution losers wholesale.
    """
    return 1.0 if (pnl or 0) > 0 else 0.0


def _effective_exit(sell_yes: float, latency_s: int,
                    best_print: float | None, best_buy: float | None,
                    covered: bool, policy: str = "assume-fill",
                    ) -> tuple[float | None, str]:
    """Fill-evidence waterfall for one mirror-exit leg.

    Returns (exit_price | None, kind). None ⇒ the mirror sell does not fill
    and the caller books resolution payout instead ('unfilled').
    """
    eff = max(0.001, sell_yes - SLIP_PER_SEC * latency_s)
    evidence = max((p for p in (best_print, best_buy) if p is not None),
                   default=None)
    if evidence is not None:
        if evidence >= sell_yes - CROSS_TOL:
            return (eff, "mirror_confirmed")
        return (min(eff, evidence), "mirror_degraded")
    if covered or policy == "resolution":
        return (None, "unfilled")
    return (eff, "mirror_assumed_nocoverage")


def _copy_return(entry_yes: float, exit_yes: float, latency_s: int) -> float:
    """Legacy gross per-dollar return (latency slip + optional flat COST_BPS).
    Kept for back-compat/tests; the amended booking path is _book_copy."""
    from trading_platform.polymarket import price_space
    slip = SLIP_PER_SEC * latency_s
    eff_entry = min(PRICE_MAX, entry_yes + slip)
    eff_exit = max(0.001, exit_yes - slip)
    entry_cost = price_space.held_token_price("BUY", eff_entry)
    exit_val = price_space.held_token_price("BUY", eff_exit)
    ret = (exit_val - entry_cost) / max(entry_cost, 0.001)
    ret -= 2 * COST_BPS
    return max(RET_CLIP_LO, min(RET_CLIP_HI, ret))


def _book_copy(row: dict, latency_s: int, stake: float,
               cost_model: CostModel | None, policy: str,
               perfect_fill: bool = False) -> dict:
    """Book ONE copy for one lane. Pure — returns
    {pnl, exit_kind, category, event_slug}. Never drops a row."""
    entry_gross = min(PRICE_MAX, float(row["price"]) + SLIP_PER_SEC * latency_s)
    if cost_model is not None:
        entry_px = cost_model.entry_cost(entry_gross, "BUY", stake).effective_price
    else:
        entry_px = entry_gross

    exit_px: float | None = None
    kind = "resolution_native"
    if row.get("sell_price") is not None:
        sell_yes = float(row["sell_price"])
        if perfect_fill:
            exit_px_gross, kind = (max(0.001, sell_yes - SLIP_PER_SEC * latency_s),
                                   "mirror_confirmed")
        else:
            bp = row.get("print_fast") if latency_s == FAST_LANE_LATENCY_S else row.get("print_poll")
            bb = row.get("buy_fast") if latency_s == FAST_LANE_LATENCY_S else row.get("buy_poll")
            exit_px_gross, kind = _effective_exit(
                sell_yes, latency_s,
                float(bp) if bp is not None else None,
                float(bb) if bb is not None else None,
                bool(row.get("covered")), policy)
        if exit_px_gross is not None:
            if cost_model is not None:
                exit_px = cost_model.exit_cost(
                    exit_px_gross, "BUY", stake, is_resolution=False,
                    entry_price=entry_px).effective_price
            else:
                exit_px = exit_px_gross

    if exit_px is None:  # never sold, or mirror unfilled → resolution
        if kind == "unfilled":
            pass  # keep the unfilled label for accounting
        payout = _resolution_payout(row.get("asset"), row.get("yes_token_id"),
                                    row.get("no_token_id"),
                                    row.get("outcome_prices"), row.get("closed"))
        if payout is None:
            payout = _resolution_payout_from_pnl(row.get("pnl"))
        exit_px = payout  # resolution exits are cost-exempt (CostModel rule)
        if kind != "unfilled":
            kind = "resolution_native"

    ret = (exit_px - entry_px) / max(entry_px, 0.001)
    if COST_BPS:  # flat env override (back-compat) — replaces CostModel
        ret -= 2 * COST_BPS
    ret = max(RET_CLIP_LO, min(RET_CLIP_HI, ret))
    return {"pnl": stake * ret, "exit_kind": kind,
            "category": (row.get("category") or "other").lower(),
            "event_slug": row.get("event_slug")}


def _cohort_stats(pnls: list[float]) -> dict:
    n = len(pnls)
    if n == 0:
        return {"n": 0, "total": 0.0, "avg": 0.0, "wr": 0.0}
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    return {"n": n, "total": round(total, 2),
            "avg": round(total / n, 4), "wr": round(wins / n, 3)}


def _rank_wallets(per_wallet: dict, lane_copies: dict, min_n: int,
                  min_wr: float) -> tuple[list[dict], list[dict]]:
    """Qualification + top-decile selection over a set of copies.
    lane_copies: wallet -> list of copy dicts (this lane, this subset)."""
    results = []
    for wallet, copies in lane_copies.items():
        pnls = [c["pnl"] for c in copies]
        n = len(pnls)
        if n < min_n:
            continue
        wins = sum(1 for p in pnls if p > 0)
        wr = wins / n
        trimmed = sorted(pnls)[:-3] if n > 3 else pnls
        ex_top3 = sum(trimmed) / max(len(trimmed), 1)
        results.append({
            "wallet": wallet, "n": n, "wr": round(wr, 3),
            "total": round(sum(pnls), 2),
            "avg": round(sum(pnls) / n, 4),
            "ex_top3_ev": round(ex_top3, 4),
            "qualifies": bool(wr >= min_wr and ex_top3 > 0),
        })
    results.sort(key=lambda r: r["ex_top3_ev"], reverse=True)
    qualified = [r for r in results if r["qualifies"]]
    k = max(1, len(qualified) // 10) if qualified else 0
    return results, qualified[:k]


def run_backtest(days: int = 60, min_n: int = 30, min_wr: float = 0.55,
                 stake: float = STAKE_USD, fill_window: int = FILL_WINDOW_S,
                 policy: str = "assume-fill",
                 perfect_fill: bool = False) -> dict[str, Any]:
    conn = get_connection()
    try:
        rows_raw = _fetch_mirror_trades(conn, days, fill_window)
    finally:
        try: conn.close()
        except Exception: pass

    cols = ["wallet", "condition_id", "category", "asset", "price",
            "timestamp", "pnl", "sell_ts", "sell_price", "outcome_prices",
            "closed", "yes_token_id", "no_token_id", "event_slug",
            "print_fast", "print_poll", "buy_fast", "buy_poll", "covered"]
    rows = [dict(zip(cols, r)) for r in rows_raw]

    cost_model = None if (COST_BPS or perfect_fill) else CostModel()

    lanes = {"fast_lane": FAST_LANE_LATENCY_S, "poller": POLLER_LATENCY_S}
    booked: dict[str, list[dict]] = {ln: [] for ln in lanes}
    for row in rows:
        for ln, lat in lanes.items():
            booked[ln].append({**_book_copy(row, lat, stake, cost_model,
                                            policy, perfect_fill),
                               "wallet": row["wallet"]})

    def _lane_report(ln: str) -> dict:
        copies = booked[ln]
        by_wallet: dict[str, list[dict]] = {}
        fill_acct: dict[str, int] = {}
        for c in copies:
            by_wallet.setdefault(c["wallet"], []).append(c)
            fill_acct[c["exit_kind"]] = fill_acct.get(c["exit_kind"], 0) + 1
        results, top_decile = _rank_wallets({}, by_wallet, min_n, min_wr)
        td_copies = [c for r in top_decile for c in by_wallet[r["wallet"]]]
        td_events = len({c["event_slug"] for c in td_copies if c["event_slug"]})
        qualified_n = sum(1 for r in results if r["qualifies"])

        clusters: dict[str, dict] = {}
        min_n_cluster = max(10, min_n // 3)
        for cat in sorted({c["category"] for c in copies}):
            cat_by_wallet: dict[str, list[dict]] = {}
            for c in copies:
                if c["category"] == cat:
                    cat_by_wallet.setdefault(c["wallet"], []).append(c)
            c_results, c_top = _rank_wallets({}, cat_by_wallet,
                                             min_n_cluster, min_wr)
            c_td_copies = [c for r in c_top for c in cat_by_wallet[r["wallet"]]]
            stats = _cohort_stats([c["pnl"] for c in c_td_copies])
            measurable = (len(c_top) >= _CLUSTER_MIN_TOP_DECILE
                          and stats["n"] >= _CLUSTER_MIN_COPIES)
            clusters[cat] = {
                "qualified": sum(1 for r in c_results if r["qualifies"]),
                "top_decile_wallets": len(c_top),
                "status": "measurable" if measurable else "insufficient",
                **stats,
            }

        return {
            "wallets_with_min_n": len(results),
            "qualified_count": qualified_n,
            "top_decile": {"n_wallets": len(top_decile),
                           "distinct_events": td_events,
                           **_cohort_stats([c["pnl"] for c in td_copies])},
            "fill_accounting": fill_acct,
            "clusters": clusters,
            "top20": results[:20],
        }

    return {
        "window_days": days, "stake_usd": stake, "min_n": min_n,
        "min_wr": min_wr,
        "total_copies": len(rows),
        "copies_with_mirror_sell": sum(1 for r in rows if r["sell_price"] is not None),
        "slip_per_sec": SLIP_PER_SEC,
        "cost_source": ("flat_bps_env" if COST_BPS
                        else ("none (perfect-fill)" if perfect_fill else "cost_model")),
        "fill_window_s": fill_window, "no_evidence_policy": policy,
        "perfect_fill": perfect_fill,
        "fast_lane": _lane_report("fast_lane"),
        "poller": _lane_report("poller"),
    }


def _verdict(out: dict) -> tuple[str, str]:
    """Amended kill-rule decision (Amendments 1+2). Returns (verdict, detail)."""
    td = out["poller"]["top_decile"]
    metric, n_td, copies = td["avg"], td["n_wallets"], out["total_copies"]
    global_ok = n_td >= 5 and copies >= 150
    clusters = out["poller"]["clusters"]
    measurable = {c: v for c, v in clusters.items() if v["status"] == "measurable"}
    clusters_clearing = [c for c, v in measurable.items() if v["avg"] > 0.10]

    # Amendment 2: an all-negative universe is a verdict, not missing data.
    qualified = out["poller"].get("qualified_count", 0)
    wallets_min_n = out["poller"].get("wallets_with_min_n", 0)
    if (copies >= 10_000 and wallets_min_n >= 100 and qualified == 0
            and not clusters_clearing):
        return ("KILL", f"0 qualified wallets across {copies} copies / "
                        f"{wallets_min_n} wallets at min_n — no copyable "
                        f"cohort exists (Amendment 2)")

    if global_ok and metric > 0.10:
        return ("KEEP", f"global poller top-decile ${metric:+.4f}/trade > +$0.10")
    if clusters_clearing:
        return ("KEEP", f"cluster(s) clear the floor: {clusters_clearing}")
    if global_ok:  # measurable and <= floor, and no cluster clears
        return ("KILL", f"global ${metric:+.4f}/trade <= +$0.10 and no "
                        f"measurable cluster clears the floor")
    # Global guard fired. Per the registered amendment, KILL requires a
    # MEASURABLE global at/below the floor — an insufficient global with
    # failing-but-measurable clusters stays INSUFFICIENT (with the cluster
    # picture printed for the operator), it does not improvise a kill.
    return ("INSUFFICIENT",
            f"global guard fired (top-decile wallets={n_td}<5 or "
            f"copies={copies}<150); measurable clusters: "
            f"{sorted(measurable) or 'none'}, none clearing the floor")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--min-wr", type=float, default=0.55)
    ap.add_argument("--fill-window", type=int, default=FILL_WINDOW_S)
    ap.add_argument("--no-evidence-policy", choices=("assume-fill", "resolution"),
                    default="assume-fill")
    ap.add_argument("--perfect-fill", action="store_true",
                    help="run-1 comparability: every mirror sell fills at the "
                         "leader's price, costs off. Diagnostic only — cannot "
                         "drive the verdict.")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out", type=str, default=None,
                    help="also write the JSON result to this path")
    args = ap.parse_args()

    out = run_backtest(days=args.days, min_n=args.min_n, min_wr=args.min_wr,
                       fill_window=args.fill_window,
                       policy=args.no_evidence_policy,
                       perfect_fill=args.perfect_fill)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"[written] {args.out}")
    if args.json:
        print(json.dumps(out, indent=2))
        return 0

    print(f"\nMIRROR-EXIT COPY DIAGNOSTIC — last {out['window_days']}d, "
          f"${out['stake_usd']:.0f}/copy, min_n={out['min_n']}, "
          f"min_wr={out['min_wr']:.0%}, costs={out['cost_source']}, "
          f"fill_window={out['fill_window_s']}s"
          f"{' [PERFECT-FILL]' if out['perfect_fill'] else ''}")
    print(f"copies: {out['total_copies']} total, "
          f"{out['copies_with_mirror_sell']} with a mirror sell "
          f"({out['copies_with_mirror_sell']/max(out['total_copies'],1):.0%})")
    for lane in ("fast_lane", "poller"):
        c = out[lane]
        td = c["top_decile"]
        print(f"\n[{lane}] qualified={c['qualified_count']}/{c['wallets_with_min_n']}  "
              f"top-decile ({td['n_wallets']} wallets, {td['distinct_events']} events): "
              f"n={td['n']} net_avg=${td['avg']:+.4f}/trade  WR={td['wr']:.0%}  "
              f"total=${td['total']:+.2f}")
        print(f"    fills: {c['fill_accounting']}")
        meas = {k: v for k, v in c["clusters"].items() if v["status"] == "measurable"}
        for cat, v in sorted(meas.items(), key=lambda x: -x[1]["avg"]):
            print(f"    cluster {cat:<15} top-dec {v['top_decile_wallets']}w "
                  f"n={v['n']:>4} avg=${v['avg']:+.4f} WR={v['wr']:.0%}")
        insuf = [k for k, v in c["clusters"].items() if v["status"] != "measurable"]
        if insuf:
            print(f"    (insufficient clusters: {', '.join(insuf)})")

    if out["perfect_fill"]:
        print("\n[PERFECT-FILL run — comparability only; no verdict]")
        return 0
    verdict, detail = _verdict(out)
    td = out["poller"]["top_decile"]
    print(f"\nKILL-RULE METRIC (poller top-decile net EV/trade): ${td['avg']:+.4f}")
    print(f"  => {verdict}: {detail}")
    if verdict == "KEEP":
        print("     Shadow-pilot gated by the standard promotion ladder — "
              "do NOT go live directly.")
    elif verdict == "KILL":
        print("     Retire live copy-entry; keep the wallet graph as a "
              "feature source only (see reports/mirror_copy_kill_rule.md).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
