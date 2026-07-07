"""Mirror-exit copy-trading diagnostic (roadmap N6) — READ ONLY.

The hold-to-resolution copyability backtest (naive_copy_backtest.py) found NO
wallet copyable and its own docstring admits it "undervalues wallets whose edge
is scalping (buy 0.40 -> sell 0.60 before close)". This diagnostic closes that
gap: it copies each wallet's first BUY per market at a fixed stake, then EXITS
WHEN THE LEADER EXITS (their next SELL on that market), falling back to
hold-to-resolution only if they never sold before the market resolved.

It answers the one measurement the pre-registered kill rule
(reports/mirror_copy_kill_rule.md) is written against: is any wallet copyable
under mirror exits, with a realistic latency haircut, net of cost?

Two latency cohorts are reported SEPARATELY and never blended:
  * fast_lane  — chain-direct dispatch, ~3s from leader fill to our fill
  * poller     — the current production reality, ~5min (p50) lag

Latency degrades our ENTRY (we fill worse than the leader) and our mirror EXIT
(we sell later/worse) by a per-second price slip. A round-trip cost is netted
out separately so the two haircuts are independently attributable.

This module NEVER writes to the DB and is NOT scheduled — it is manual analysis
tooling: `python -m trading_platform.polymarket.mirror_copy_backtest`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket import price_space

logger = logging.getLogger(__name__)

STAKE_USD = 5.0
PRICE_MIN, PRICE_MAX = 0.10, 0.85
RET_CLIP_LO, RET_CLIP_HI = -1.0, 9.0

FAST_LANE_LATENCY_S = 3
POLLER_LATENCY_S = 300
# YES-space price slip per second of latency. Default is deliberately small;
# calibrate via env once real fill data accrues.
SLIP_PER_SEC = float(os.environ.get("MIRROR_SLIP_PER_SEC", "0.00002"))
# Round-trip cost as a fraction of stake (spread + fees), applied entry+exit.
COST_BPS = float(os.environ.get("MIRROR_COST_BPS", "0"))


def _fetch_mirror_trades(conn, days: int) -> list[tuple]:
    """First resolved BUY per (wallet, market) + that wallet's earliest
    subsequent SELL on the same market (the mirror exit), plus the market's
    resolution for the hold-to-resolution fallback."""
    cutoff = int(time.time()) - days * 86400
    return conn.execute(
        """
        WITH firsts AS (
            SELECT wt.wallet, wt.condition_id, wt.category, wt.asset,
                   wt.price, wt.size, wt.timestamp,
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
        )
        SELECT f.wallet, f.condition_id, f.category, f.asset, f.price,
               f.timestamp,
               (SELECT MIN(s.timestamp) FROM wallet_trades s
                 WHERE s.wallet = f.wallet AND s.condition_id = f.condition_id
                   AND s.side = 'SELL' AND s.timestamp > f.timestamp) AS sell_ts,
               (SELECT s2.price FROM wallet_trades s2
                 WHERE s2.wallet = f.wallet AND s2.condition_id = f.condition_id
                   AND s2.side = 'SELL' AND s2.timestamp > f.timestamp
                 ORDER BY s2.timestamp ASC LIMIT 1) AS sell_price,
               m.outcome_prices, m.closed, m.yes_token_id, m.no_token_id
          FROM firsts f
          LEFT JOIN markets m ON m.condition_id = f.condition_id
         WHERE f.rn = 1
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


def _copy_return(entry_yes: float, exit_yes: float, latency_s: int) -> float:
    """Per-dollar return of copying a BUY at `entry_yes`, exiting at `exit_yes`,
    with a latency haircut on both legs (we fill worse). BUY holds YES."""
    slip = SLIP_PER_SEC * latency_s
    eff_entry = min(PRICE_MAX, entry_yes + slip)      # we pay up on entry
    eff_exit = max(0.001, exit_yes - slip)            # we sell down on exit
    entry_cost = price_space.held_token_price("BUY", eff_entry)
    exit_val = price_space.held_token_price("BUY", eff_exit)
    ret = (exit_val - entry_cost) / max(entry_cost, 0.001)
    ret -= 2 * COST_BPS  # round-trip cost fraction
    return max(RET_CLIP_LO, min(RET_CLIP_HI, ret))


def _cohort_stats(pnls: list[float]) -> dict:
    n = len(pnls)
    if n == 0:
        return {"n": 0, "total": 0.0, "avg": 0.0, "wr": 0.0}
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    return {"n": n, "total": round(total, 2),
            "avg": round(total / n, 4), "wr": round(wins / n, 3)}


def run_backtest(days: int = 60, min_n: int = 30, min_wr: float = 0.55,
                 stake: float = STAKE_USD) -> dict[str, Any]:
    conn = get_connection()
    try:
        rows = _fetch_mirror_trades(conn, days)
    finally:
        try: conn.close()
        except Exception: pass

    # per_wallet[wallet] = {"fast": [pnls], "poller": [pnls]}
    per_wallet: dict[str, dict[str, list[float]]] = {}
    n_mirror = 0
    n_resolution = 0
    for (wallet, cid, cat, asset, entry_px, entry_ts,
         sell_ts, sell_px, prices_raw, closed, yes_tok, no_tok) in rows:
        entry_yes = float(entry_px)
        if sell_px is not None:
            exit_yes = float(sell_px)   # leader's actual exit price (YES-space)
            n_mirror += 1
        else:
            payout = _resolution_payout(asset, yes_tok, no_tok, prices_raw, closed)
            if payout is None:
                continue
            exit_yes = payout           # held to resolution: 1.0 or 0.0
            n_resolution += 1
        w = per_wallet.setdefault(wallet, {"fast": [], "poller": []})
        w["fast"].append(stake * _copy_return(entry_yes, exit_yes, FAST_LANE_LATENCY_S))
        w["poller"].append(stake * _copy_return(entry_yes, exit_yes, POLLER_LATENCY_S))

    def _cohort(lane: str) -> dict:
        results = []
        for wallet, lanes in per_wallet.items():
            pnls = lanes[lane]
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
        # Top decile of QUALIFIED wallets by ex-top-3 EV.
        k = max(1, len(qualified) // 10) if qualified else 0
        top_decile_wallets = qualified[:k]
        td_pnls = [p for r in top_decile_wallets
                   for p in per_wallet[r["wallet"]][lane]]
        return {
            "wallets_with_min_n": len(results),
            "qualified_count": len(qualified),
            "top_decile": {"n_wallets": len(top_decile_wallets),
                           **_cohort_stats(td_pnls)},
            "top20": results[:20],
        }

    return {
        "window_days": days, "stake_usd": stake, "min_n": min_n, "min_wr": min_wr,
        "wallets_tested": len(per_wallet),
        "exits_via_mirror": n_mirror,
        "exits_via_resolution": n_resolution,
        "slip_per_sec": SLIP_PER_SEC, "cost_bps": COST_BPS,
        "fast_lane": _cohort("fast"),
        "poller": _cohort("poller"),
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--min-wr", type=float, default=0.55)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    out = run_backtest(days=args.days, min_n=args.min_n, min_wr=args.min_wr)
    if args.json:
        print(json.dumps(out, indent=2))
        return 0

    print(f"\nMIRROR-EXIT COPY DIAGNOSTIC — last {out['window_days']}d, "
          f"${out['stake_usd']:.0f}/copy, min_n={out['min_n']}, min_wr={out['min_wr']:.0%}")
    print(f"wallets tested: {out['wallets_tested']}  "
          f"(exits: {out['exits_via_mirror']} mirror / {out['exits_via_resolution']} resolution)  "
          f"slip/s={out['slip_per_sec']} cost_bps={out['cost_bps']}")
    for lane in ("fast_lane", "poller"):
        c = out[lane]
        td = c["top_decile"]
        print(f"\n[{lane}] qualified={c['qualified_count']}/{c['wallets_with_min_n']}  "
              f"top-decile ({td['n_wallets']} wallets): n={td['n']} "
              f"net_avg=${td['avg']:+.4f}/trade  WR={td['wr']:.0%}  total=${td['total']:+.2f}")
        for r in c["top20"][:5]:
            flag = "PASS" if r["qualifies"] else ""
            print(f"    {r['wallet'][:14]}  n={r['n']:>3}  WR={r['wr']:.0%}  "
                  f"exTop3=${r['ex_top3_ev']:+.4f}/tr  {flag}")

    verdict_metric = out["poller"]["top_decile"]["avg"]
    n_td = out["poller"]["top_decile"]["n_wallets"]
    total_copies = out["exits_via_mirror"] + out["exits_via_resolution"]
    print(f"\nKILL-RULE METRIC (poller top-decile net EV/trade): ${verdict_metric:+.4f}")
    if n_td < 5 or total_copies < 150:
        print(f"  INSUFFICIENT DATA (top-decile wallets={n_td}<5 or copies={total_copies}<150) "
              f"— widen --days before concluding (see reports/mirror_copy_kill_rule.md).")
    elif verdict_metric <= 0.10:
        print("  => KILL: below the +$0.10/trade floor. Retire live copy-entry; "
              "keep the wallet graph as a feature source only.")
    else:
        print("  => KEEP (investigate): above floor. Shadow-pilot gated by the "
              "standard promotion ladder — do NOT go live directly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
