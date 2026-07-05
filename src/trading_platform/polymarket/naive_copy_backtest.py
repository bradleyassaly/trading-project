"""Per-wallet naive-copy backtest — who can we profitably copy?

The naive_copy shadow lane answers this question at ~5 copies/day, which
means weeks of waiting per cohort iteration. This backtest answers it
retrospectively from wallet_trades, which the sync pipeline already
enriches with resolution outcomes and per-trade pnl (~1k new rows/day,
164k resolved BUYs per 60d window).

Simulation frame (identical to the naive_copy shadow lane):
  * copy each wallet's FIRST BUY per market (dedup like _already_copied)
  * price band 0.10-0.85, science excluded, horizon 6h-720h when known
  * $5 fixed stake at the whale's fill price, perfect fill
  * hold to resolution; copy return = whale per-dollar return
    (pnl / (size*price)) clipped to [-1, +9]

CAVEAT the reader must keep in mind: hold-to-resolution undervalues
wallets whose edge is scalping (buy 0.40 → sell 0.60 before close).
Those wallets show as losers here even if their realized trading is
profitable. This is the right frame for the NAIVE copy lane, which
holds to resolution; the full copy system with mirror exits could do
better than these numbers.

Qualification gate (same shape as the resolution_decay BUY re-entry
criteria from 2026-05): n >= min_n, WR >= min_wr, and ex-top-3 EV > 0 —
the average per-trade copy pnl after dropping the wallet's 3 largest
wins, which kills lottery-ticket distributions that look good only
because of outliers we would probably have missed.

Usage:
    python -m trading_platform.polymarket.naive_copy_backtest
    python -m trading_platform.polymarket.naive_copy_backtest --days 90 --min-n 20
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

STAKE_USD = 5.0
PRICE_MIN, PRICE_MAX = 0.10, 0.85
HORIZON_MIN_H, HORIZON_MAX_H = 6, 720
BLOCKED_CATEGORIES = ("science",)
# Per-dollar return clip: a $0.10 longshot resolving YES pays 9x; anything
# beyond that is a data artifact (bad size/price row), not a real fill.
RET_CLIP_LO, RET_CLIP_HI = -1.0, 9.0


def _fetch_copy_trades(conn, days: int) -> list[tuple]:
    """First resolved BUY per (wallet, market) inside the copy filters.

    Horizon is a soft filter, mirroring naive_copy_signal: enforced when
    the market's end date is known (markets table), allowed when unknown.
    """
    cutoff = int(time.time()) - days * 86400
    return conn.execute(
        """
        WITH firsts AS (
            SELECT wt.wallet, wt.condition_id, wt.category,
                   wt.price, wt.size, wt.pnl, wt.timestamp,
                   ROW_NUMBER() OVER (
                       PARTITION BY wt.wallet, wt.condition_id
                       ORDER BY wt.timestamp
                   ) AS rn,
                   m.end_date_iso
              FROM wallet_trades wt
              LEFT JOIN markets m ON m.condition_id = wt.condition_id
             WHERE wt.side = 'BUY'
               AND wt.market_resolved = 1
               AND wt.pnl IS NOT NULL
               AND wt.pnl_reliable = 1
               AND wt.timestamp > ?
               AND wt.price BETWEEN ? AND ?
               AND wt.size * wt.price > 0
               AND LOWER(COALESCE(wt.category, '')) NOT IN ('science')
        )
        SELECT wallet, condition_id, category, price, size, pnl,
               timestamp, end_date_iso
          FROM firsts
         WHERE rn = 1
        """,
        (cutoff, PRICE_MIN, PRICE_MAX),
    ).fetchall()


def _horizon_ok(entry_ts: int, end_date_iso) -> bool:
    if not end_date_iso:
        return True  # soft filter — same as the live lane
    try:
        if hasattr(end_date_iso, "timestamp"):
            end_ts = end_date_iso.timestamp()
        else:
            from datetime import datetime
            end_ts = datetime.fromisoformat(
                str(end_date_iso).replace("Z", "+00:00")
            ).timestamp()
    except (ValueError, TypeError):
        return True
    horizon_h = (end_ts - entry_ts) / 3600.0
    return HORIZON_MIN_H <= horizon_h <= HORIZON_MAX_H


def run_backtest(
    days: int = 60,
    min_n: int = 30,
    min_wr: float = 0.55,
    stake: float = STAKE_USD,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        rows = _fetch_copy_trades(conn, days)

        per_wallet: dict[str, list[float]] = {}
        per_wallet_cats: dict[str, dict[str, int]] = {}
        skipped_horizon = 0
        for wallet, cid, cat, price, size, pnl, ts, end_iso in rows:
            if not _horizon_ok(int(ts or 0), end_iso):
                skipped_horizon += 1
                continue
            cost = float(size) * float(price)
            ret = max(RET_CLIP_LO, min(RET_CLIP_HI, float(pnl) / cost))
            per_wallet.setdefault(wallet, []).append(stake * ret)
            cats = per_wallet_cats.setdefault(wallet, {})
            key = (cat or "unknown").lower()
            cats[key] = cats.get(key, 0) + 1

        results = []
        for wallet, pnls in per_wallet.items():
            n = len(pnls)
            if n < min_n:
                continue
            wins = sum(1 for p in pnls if p > 0)
            wr = wins / n
            total = sum(pnls)
            # ex-top-3: drop the 3 largest wins, then average per trade.
            # A wallet that is only profitable because of 3 lottery hits
            # is not copyable — we'd have needed to catch exactly those.
            trimmed = sorted(pnls)[:-3] if n > 3 else pnls
            ex_top3_ev = sum(trimmed) / max(len(trimmed), 1)
            cats = per_wallet_cats.get(wallet, {})
            top_cat = max(cats, key=cats.get) if cats else "unknown"
            results.append({
                "wallet": wallet,
                "n": n,
                "wr": round(wr, 3),
                "total_copy_pnl": round(total, 2),
                "avg_pnl_per_trade": round(total / n, 3),
                "ex_top3_ev": round(ex_top3_ev, 3),
                "top_category": top_cat,
                "qualifies": bool(wr >= min_wr and ex_top3_ev > 0),
            })

        results.sort(key=lambda r: r["ex_top3_ev"], reverse=True)
        qualified = [r for r in results if r["qualifies"]]

        # ── Comparison: our live system over the same window ────────────
        cutoff = int(time.time()) - days * 86400
        live = conn.execute(
            """SELECT COUNT(*),
                      COALESCE(SUM(realized_pnl), 0),
                      COALESCE(SUM(size_usd), 0),
                      SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
                 FROM live_trades
                WHERE dry_run = 0 AND realized_pnl IS NOT NULL
                  AND size_usd > 0
                  AND (CASE WHEN exit_reason = 'resolved_databook'
                            AND COALESCE(resolution_date, 0) <= 0 THEN 0
                            WHEN COALESCE(resolution_date, 0) > 0
                            AND resolution_date < exit_ts THEN resolution_date
                            ELSE exit_ts END) >= ?""",
            (cutoff,),
        ).fetchone()
        live_n, live_pnl, live_staked, live_wins = (
            int(live[0] or 0), float(live[1] or 0),
            float(live[2] or 0), int(live[3] or 0),
        )

        # Naive-copy shadow lane actuals (resolved shadows only; expiry
        # write-offs are evidence of nothing and are excluded).
        shadow = conn.execute(
            """SELECT COUNT(*),
                      COALESCE(SUM(realized_pnl), 0),
                      SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
                 FROM live_trades
                WHERE signal_type = 'naive_copy_buy' AND dry_run = 1
                  AND exit_reason = 'shadow_resolved'"""
        ).fetchone()
        shadow_n, shadow_pnl, shadow_wins = (
            int(shadow[0] or 0), float(shadow[1] or 0), int(shadow[2] or 0),
        )

        # Simulated aggregate over qualified wallets (what the naive-copy
        # lane would have earned had it copied only the qualified set).
        q_pnls = [p for r in qualified for p in per_wallet[r["wallet"]]]

        return {
            "window_days": days,
            "stake_usd": stake,
            "min_n": min_n,
            "min_wr": min_wr,
            "wallets_tested": len(per_wallet),
            "wallets_with_min_n": len(results),
            "skipped_horizon": skipped_horizon,
            "qualified": qualified,
            "top20": results[:20],
            "qualified_aggregate": {
                "n_trades": len(q_pnls),
                "total_copy_pnl": round(sum(q_pnls), 2),
                "avg_pnl_per_trade": round(
                    sum(q_pnls) / max(len(q_pnls), 1), 3),
            },
            "our_system": {
                "n_closed": live_n,
                "realized_pnl": round(live_pnl, 2),
                "staked": round(live_staked, 2),
                "pnl_per_dollar": round(live_pnl / live_staked, 4)
                    if live_staked else None,
                "wr": round(live_wins / live_n, 3) if live_n else None,
            },
            "shadow_lane": {
                "n_resolved": shadow_n,
                "realized_pnl": round(shadow_pnl, 2),
                "wr": round(shadow_wins / shadow_n, 3) if shadow_n else None,
            },
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--min-wr", type=float, default=0.55)
    ap.add_argument("--json", action="store_true", help="dump full JSON")
    args = ap.parse_args()

    out = run_backtest(days=args.days, min_n=args.min_n, min_wr=args.min_wr)

    if args.json:
        print(json.dumps(out, indent=2))
        return 0

    print(f"\nNAIVE-COPY WALLET BACKTEST — last {out['window_days']}d, "
          f"${out['stake_usd']:.0f}/copy, min_n={out['min_n']}, "
          f"min_wr={out['min_wr']:.0%}")
    print(f"wallets tested: {out['wallets_tested']}  "
          f"(>= min_n trades: {out['wallets_with_min_n']}, "
          f"horizon-skipped trades: {out['skipped_horizon']})")

    print(f"\n{'wallet':<14}{'n':>5}{'WR':>7}{'total$':>9}"
          f"{'avg$/tr':>9}{'exTop3$':>9}  {'category':<12}{'PASS'}")
    for r in out["top20"]:
        print(f"{r['wallet'][:12]:<14}{r['n']:>5}{r['wr']:>7.0%}"
              f"{r['total_copy_pnl']:>9.2f}{r['avg_pnl_per_trade']:>9.3f}"
              f"{r['ex_top3_ev']:>9.3f}  {r['top_category']:<12}"
              f"{'YES' if r['qualifies'] else ''}")

    q = out["qualified_aggregate"]
    print(f"\nQualified wallets: {len(out['qualified'])}  "
          f"→ simulated {q['n_trades']} copies, "
          f"${q['total_copy_pnl']:+.2f} total, "
          f"${q['avg_pnl_per_trade']:+.3f}/trade")

    s = out["our_system"]
    print(f"\nOur live system, same {out['window_days']}d window "
          f"(economic-dated): n={s['n_closed']} "
          f"pnl=${s['realized_pnl']:+.2f} "
          f"per-$={s['pnl_per_dollar'] if s['pnl_per_dollar'] is not None else 'n/a'} "
          f"WR={s['wr'] if s['wr'] is not None else 'n/a'}")
    sh = out["shadow_lane"]
    print(f"Shadow lane actuals (all time, resolved only): "
          f"n={sh['n_resolved']} pnl=${sh['realized_pnl']:+.2f} "
          f"WR={sh['wr'] if sh['wr'] is not None else 'n/a'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
