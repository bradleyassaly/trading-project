"""Venue map — WHERE is trading cheap enough to have a chance?

2026-07-31. Five dead strategies all ran in ONE venue: illiquid sports
longshots at $0.05-0.40, minutes from resolution. We varied the signal
every time and never varied the venue — while the cost decomposition says
venue dominates the arithmetic. Our execution cost is ~0.66c in ABSOLUTE
terms regardless of price, so it is 5.1% of a $0.13 position and 1.3% of
a $0.50 one: the same trade faces a 4x higher hurdle on a longshot.

This maps, per (category x price band), the three things that decide
whether ANY strategy can work there:

  1. COST — effective spread via the ROLL ESTIMATOR:
         spread = 2 * sqrt(-cov(dP_t, dP_t-1))
     Roll (1984) infers the effective spread from a transaction price
     series alone, which is what we have (order_book_snapshots stores
     depth and mid but no best bid/ask prices). When the serial
     covariance is POSITIVE the estimator is undefined — and that is
     itself informative: it means prices trend rather than bounce, i.e.
     one-way flow. We report that share instead of hiding it.

  2. CONTINUITY — prints per hour and the median gap between them. A
     market that trades twice an hour cannot support a resting quote.

  3. TWO-SIDEDNESS — the tick-rule sign-change rate. Genuine two-way
     flow alternates buyer- and seller-initiated prints (~50%); a market
     drifting one way toward resolution does not, and a resting order
     there only fills when it is about to be wrong (adverse selection).

Verdict per cell is deliberately conservative: a venue is MAKEABLE only
if it trades continuously, bounces rather than trends, and its spread is
wide enough to be worth capturing but not so wide it signals a dead book.

Read-only. Run ISOLATED — never exec heavy work into the live scheduler:
  docker compose run --rm --no-deps scheduler \
      python scripts/venue_map.py [--days 4] [--min-prints 150]
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("venue_map")

OUR_COST_C = 0.66          # chain-verified one-way cost (half-spread)
BANDS = ((0.0, 0.15, "a $0.00-0.15"), (0.15, 0.35, "b $0.15-0.35"),
         (0.35, 0.65, "c $0.35-0.65"), (0.65, 1.01, "d $0.65-1.00"))


def _band(px: float) -> str:
    for lo, hi, label in BANDS:
        if lo <= px < hi:
            return label
    return "d $0.65-1.00"


def _roll_spread_c(prices: list[float]) -> float | None:
    """Roll (1984) effective spread in cents; None when prices trend
    (positive serial covariance -> estimator undefined)."""
    if len(prices) < 30:
        return None
    d = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    if len(d) < 20:
        return None
    m = statistics.fmean(d)
    cov = statistics.fmean(
        [(d[i] - m) * (d[i - 1] - m) for i in range(1, len(d))])
    if cov >= 0:
        return None          # trending, not bouncing
    return 2.0 * ((-cov) ** 0.5) * 100.0


def survey(days: int = 4, min_prints: int = 150, max_tokens: int = 400,
           db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        now = int(float(conn.execute(
            "SELECT EXTRACT(EPOCH FROM NOW())").fetchone()[0]))
        floor_ts = now - days * 86400

        toks = conn.execute(
            """SELECT wt.asset, COALESCE(m.subcategory, wt.category, 'other') cat,
                      COUNT(*) n
                 FROM wallet_trades wt
                 LEFT JOIN markets m ON m.condition_id = wt.condition_id
                WHERE wt.timestamp >= ? AND wt.side = 'BUY'
                  AND wt.asset IS NOT NULL
                GROUP BY wt.asset, COALESCE(m.subcategory, wt.category, 'other')
               HAVING COUNT(*) >= ?
                ORDER BY n DESC
                LIMIT ?""",
            (floor_ts, min_prints, max_tokens),
        ).fetchall()

        cells: dict[tuple[str, str], dict] = {}
        trending = 0
        for asset, cat, _n in toks:
            rows = conn.execute(
                """SELECT timestamp, price FROM wallet_trades
                    WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                    ORDER BY timestamp""",
                (str(asset), floor_ts),
            ).fetchall()
            if len(rows) < min_prints:
                continue
            ts = [int(r[0]) for r in rows]
            px = [float(r[1]) for r in rows]
            span_h = max((ts[-1] - ts[0]) / 3600.0, 0.01)
            gaps = [ts[i] - ts[i - 1] for i in range(1, len(ts))]

            # tick-rule sign changes = two-sidedness
            signs, prev, s = [], None, 0
            for p in px:
                if prev is not None:
                    s = 1 if p > prev else (-1 if p < prev else s)
                    if s != 0:
                        signs.append(s)
                prev = p
            flips = sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1])
            two_sided = flips / max(len(signs) - 1, 1)

            roll_c = _roll_spread_c(px)
            if roll_c is None:
                trending += 1

            key = (cat, _band(statistics.median(px)))
            c = cells.setdefault(key, {
                "tokens": 0, "prints": 0, "hours": 0.0, "gaps": [],
                "two_sided": [], "roll": [], "med_px": [], "trending": 0})
            c["tokens"] += 1
            c["prints"] += len(px)
            c["hours"] += span_h
            c["gaps"].append(statistics.median(gaps) if gaps else 0)
            c["two_sided"].append(two_sided)
            c["med_px"].append(statistics.median(px))
            if roll_c is None:
                c["trending"] += 1
            else:
                c["roll"].append(roll_c)

        out_cells = []
        for (cat, band), c in cells.items():
            if c["tokens"] < 2:
                continue
            roll = statistics.median(c["roll"]) if c["roll"] else None
            med_px = statistics.median(c["med_px"])
            prints_hr = c["prints"] / max(c["hours"], 0.01)
            med_gap = statistics.median(c["gaps"])
            two_sided = statistics.fmean(c["two_sided"])
            cost_pct = OUR_COST_C / (med_px * 100) * 100 if med_px else None
            # Conservative verdict: needs continuity AND bounce AND a
            # spread worth capturing (>= our own cost) but not a dead-book
            # chasm (> 8c means nobody is quoting tightly).
            makeable = bool(
                roll is not None
                and prints_hr >= 20
                and med_gap <= 120
                and two_sided >= 0.35
                and OUR_COST_C <= roll <= 8.0)
            out_cells.append({
                "category": cat, "band": band, "tokens": c["tokens"],
                "median_price": round(med_px, 3),
                "prints_per_hour": round(prints_hr, 1),
                "median_gap_s": round(med_gap, 1),
                "two_sided_rate": round(two_sided, 3),
                "roll_spread_c": round(roll, 2) if roll is not None else None,
                "trending_tokens": c["trending"],
                "our_cost_pct_of_position": round(cost_pct, 2) if cost_pct else None,
                "makeable": makeable,
            })
        out_cells.sort(key=lambda r: (not r["makeable"], -r["prints_per_hour"]))
        out = {
            "cohort_days": days,
            "tokens_scanned": len(toks),
            "cells": out_cells,
            "tokens_trending_no_roll": trending,
            "makeable_cells": sum(1 for r in out_cells if r["makeable"]),
        }
        _write_report(out)
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _write_report(out: dict) -> None:
    day = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    path = PROJECT_ROOT / "reports" / f"venue_map_{day}.md"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Venue map — where is trading cheap enough? ({day})",
            "",
            "Our cost is ~0.66c ABSOLUTE regardless of price, so it is 5.1% "
            "of a $0.13 position and 1.3% of a $0.50 one. Five dead "
            "strategies all ran in one venue; this maps the others.",
            "",
            "Spread via the Roll estimator (trade prices only — book "
            "snapshots have no bid/ask columns). `trending` tokens have "
            "POSITIVE serial covariance, so Roll is undefined: prices walk "
            "one way instead of bouncing, which is itself a no-make signal.",
            "",
            f"- Tokens scanned: {out['tokens_scanned']}",
            f"- Tokens trending (no Roll): {out['tokens_trending_no_roll']}",
            f"- **Makeable cells: {out['makeable_cells']}**",
            "",
            "| category | band | tok | med px | prints/h | gap s | 2-sided | "
            "Roll ¢ | our cost % | makeable |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for r in out["cells"]:
            lines.append(
                f"| {r['category']} | {r['band']} | {r['tokens']} | "
                f"{r['median_price']} | {r['prints_per_hour']} | "
                f"{r['median_gap_s']} | {r['two_sided_rate']} | "
                f"{r['roll_spread_c'] if r['roll_spread_c'] is not None else '—'} | "
                f"{r['our_cost_pct_of_position']}% | "
                f"{'YES' if r['makeable'] else 'no'} |")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("[venue_map] report → %s", path)
    except Exception as exc:
        logger.warning("[venue_map] report write failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=4)
    ap.add_argument("--min-prints", type=int, default=150)
    args = ap.parse_args()
    print(json.dumps(survey(days=args.days, min_prints=args.min_prints),
                     indent=1)[:4000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
