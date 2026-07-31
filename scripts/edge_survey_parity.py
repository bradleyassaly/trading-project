"""Edge survey #2 — YES/NO parity (merge/split arbitrage).

QUESTION (2026-07-31): a YES share and a NO share of the same condition
together redeem for exactly $1.00 (CTF merge). So whenever YES and NO can
both be BOUGHT for a combined price under $1, buying both and merging is
risk-free profit — no forecast, no resolution risk, no directional
exposure. Symmetrically, when both can be SOLD for over $1, split-and-sell
is risk-free.

Does that window exist on Polymarket, how often, and for how much?

METHOD — proof by executed fills, worst-case priced.

v1 of this script (2026-07-31) was WRONG and is worth recording: it
bucketed prints into 10s windows and took MIN(price) on each leg
independently. That manufactured phantom arbitrage out of intra-bucket
volatility — it reported combined prices of $0.29 (a 3.4x risk-free
return, obviously fake). The raw tape showed why: on a 5-minute "Bitcoin
Up or Down" market, prints ranged 0.31→0.40 INSIDE A SINGLE SECOND, so
the YES minimum and the NO minimum came from different instants and were
never simultaneously executable. That is the same error class that
produced the fantasy paper P&L (measuring an opportunity nobody could
have taken).

v2 is deliberately conservative on every axis:
  * legs must trade in the SAME SECOND (not a multi-second bucket)
  * each leg is priced at its WORST print in that second (MAX, i.e. the
    highest price a buyer paid) — so the pair is claimed only when EVERY
    observed fill on both legs summed to under $1
  * size is the smaller leg's traded USD
This can still be beaten by sub-second sequencing, so treat any surviving
signal as an upper bound that needs live quote confirmation before a
single dollar is risked.

Profit per $1 of the pair = 1 - (p + q), before the ~2-3c round-trip
toll on TWO legs, so the interesting threshold is well under $0.97.

Profit per $1 of the pair = 1 - (p + q), before the ~2-3c round-trip
toll, so the interesting threshold is a combined price meaningfully
under $0.97, not merely under $1.00.

Run (ISOLATED — never exec heavy work into the live scheduler):
  docker compose run --rm --no-deps scheduler \
      python scripts/edge_survey_parity.py [--days 5] [--bucket-s 10]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("edge_survey_parity")

# Combined-price bands. <1.00 is arithmetically an arb; only well under
# 0.97 survives the execution toll on two legs.
BANDS = ((0.995, "0.995-1.00 (noise)"),
         (0.98, "0.98-0.995 (below toll)"),
         (0.97, "0.97-0.98 (marginal)"),
         (0.95, "0.95-0.97 (tradeable)"),
         (0.90, "0.90-0.95 (rich)"),
         (0.0, "<0.90 (very rich)"))


def _band(total: float) -> str:
    for lo, label in BANDS:
        if total >= lo:
            return label
    return "<0.90 (very rich)"


def survey(days: int = 5, bucket_s: int = 10, limit_conds: int = 3000,
           db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        now = int(float(conn.execute(
            "SELECT EXTRACT(EPOCH FROM NOW())").fetchone()[0]))
        floor_ts = now - days * 86400

        # Conditions where BOTH legs traded in the window — only those can
        # show an executable pair.
        rows = conn.execute(
            """WITH mk AS (
                   SELECT condition_id, yes_token_id, no_token_id
                     FROM markets
                    WHERE yes_token_id IS NOT NULL
                      AND no_token_id IS NOT NULL
               )
               SELECT mk.condition_id, mk.yes_token_id, mk.no_token_id
                 FROM mk
                WHERE EXISTS (SELECT 1 FROM wallet_trades w
                               WHERE w.asset = mk.yes_token_id
                                 AND w.timestamp >= ? LIMIT 1)
                  AND EXISTS (SELECT 1 FROM wallet_trades w
                               WHERE w.asset = mk.no_token_id
                                 AND w.timestamp >= ? LIMIT 1)
                LIMIT ?""",
            (floor_ts, floor_ts, limit_conds),
        ).fetchall()

        n_conds = len(rows)
        n_conds_with_pair = 0
        band_counts: dict[str, int] = {}
        opportunities: list[dict] = []
        total_arb_usd = 0.0

        for cid, yes_tok, no_tok in rows:
            # cheapest BUY print per time bucket, per leg
            # WORST-case price per leg per SECOND (MAX = highest a buyer
            # paid). Claiming a pair only when every fill on both legs in
            # that second still summed under $1 — see the v1 post-mortem
            # in the module docstring.
            pairs = conn.execute(
                """WITH y AS (
                       SELECT (timestamp / ?) AS b, MAX(price) px,
                              SUM(size * price) usd
                         FROM wallet_trades
                        WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                        GROUP BY 1
                   ), n AS (
                       SELECT (timestamp / ?) AS b, MAX(price) px,
                              SUM(size * price) usd
                         FROM wallet_trades
                        WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                        GROUP BY 1
                   )
                   SELECT y.b, y.px, n.px, LEAST(y.usd, n.usd)
                     FROM y JOIN n ON n.b = y.b
                    WHERE y.px + n.px < 1.0
                    ORDER BY (y.px + n.px)
                    LIMIT 50""",
                (bucket_s, str(yes_tok), floor_ts,
                 bucket_s, str(no_tok), floor_ts),
            ).fetchall()
            if not pairs:
                continue
            n_conds_with_pair += 1
            best = None
            for b, ypx, npx, usd in pairs:
                total = float(ypx) + float(npx)
                band_counts[_band(total)] = band_counts.get(_band(total), 0) + 1
                # realizable size is the smaller leg; profit = (1-total)/pair
                arb_usd = float(usd or 0) * (1.0 - total)
                total_arb_usd += arb_usd
                if best is None or total < best["combined"]:
                    best = {"cid": cid[:14], "combined": round(total, 4),
                            "yes": round(float(ypx), 3),
                            "no": round(float(npx), 3),
                            "min_leg_usd": round(float(usd or 0), 2),
                            "ts": int(b) * bucket_s}
            if best:
                opportunities.append(best)

        opportunities.sort(key=lambda o: o["combined"])
        out = {
            "cohort_days": days,
            "bucket_seconds": bucket_s,
            "conditions_both_legs_traded": n_conds,
            "conditions_with_sub_1_pair": n_conds_with_pair,
            "pair_observations_by_band": band_counts,
            "theoretical_arb_usd_total": round(total_arb_usd, 2),
            "best_opportunities": opportunities[:15],
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
    path = PROJECT_ROOT / "reports" / f"edge_survey_parity_{day}.md"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Edge survey — YES/NO parity ({day})",
            "",
            "YES + NO redeem for exactly $1.00 (CTF merge). Buying both "
            "for under $1 combined is risk-free profit. Measured by "
            "EXECUTED fills on both legs inside the same "
            f"{out['bucket_seconds']}s bucket — both sides provably "
            "available, so this undercounts rather than invents.",
            "",
            f"- Conditions with both legs trading ({out['cohort_days']}d): "
            f"{out['conditions_both_legs_traded']}",
            f"- Conditions showing a sub-$1.00 pair: "
            f"**{out['conditions_with_sub_1_pair']}**",
            f"- Theoretical gross arb (before the ~2-3c two-leg toll): "
            f"**${out['theoretical_arb_usd_total']:,.2f}**",
            "",
            "## Pair observations by combined price",
            "",
            "| combined price | observations |", "|---|---|",
        ]
        for k, v in sorted(out["pair_observations_by_band"].items(),
                           key=lambda kv: -kv[1]):
            lines.append(f"| {k} | {v} |")
        lines += ["", "## Richest pairs", "",
                  "| condition | YES | NO | combined | smaller leg USD |",
                  "|---|---|---|---|---|"]
        for o in out["best_opportunities"]:
            lines.append(f"| {o['cid']} | {o['yes']} | {o['no']} | "
                         f"{o['combined']} | ${o['min_leg_usd']:,.2f} |")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("[parity] report → %s", path)
    except Exception as exc:
        logger.warning("[parity] report write failed: %s", exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--bucket-s", type=int, default=10)
    ap.add_argument("--limit-conds", type=int, default=3000)
    args = ap.parse_args()
    print(json.dumps(survey(days=args.days, bucket_s=args.bucket_s,
                            limit_conds=args.limit_conds), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
