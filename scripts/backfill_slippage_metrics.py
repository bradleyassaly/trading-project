"""Backfill execution-cost metrics from the CORRECTED fill price.

2026-07-31. `slippage_c` is written at INSERT time from the provisional
fill (the posted limit = best_ask + tick). `order_reconciler` later
corrects `fill_price` to the true executed price but — until today — left
`slippage_c` untouched, so every reconciled row carried a constant +1c
(exactly one tick) of phantom execution cost.

Measured before this fix, n=67 live fills:
    stored slippage_c   1.371c   <- reads as a FULL spread
    true (fill - mid)   0.662c   <- the real cost: the HALF spread
    full spread         1.295c

The number matters because cost_model.py uses these metrics to discount
paper P&L to "realistic" levels — so every paper strategy has been
penalised roughly one extra spread, and anything killed on marginal
economics was judged against an inflated toll.

On-chain verification (12/12 sampled trades, our wallet's OrderFilled
events) confirms fill_price == best_ask_at_decision exactly: execution
is clean, we never chase and never pay the padded limit. The cost IS the
half-spread and nothing else.

Recomputes, only where the inputs exist and only for rows that are
actually wrong (guarded so re-runs are no-ops):
    slippage_c        = (fill_price - mid_at_decision) * 100
    slippage          = |fill_price - entry_price| / entry_price
    slippage_signed   = adverse-direction slippage (positive = bad)
    slippage_cost_usd = |fill_price - entry_price| * shares

Run (ISOLATED — never exec heavy work into the live scheduler):
  docker compose run --rm --no-deps scheduler \
      python scripts/backfill_slippage_metrics.py [--apply]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("backfill_slippage")

TOL = 0.0005  # only rewrite rows whose stored value is genuinely off


def run(apply: bool = False, db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        before = conn.execute(
            """SELECT COUNT(*),
                      AVG(slippage_c),
                      AVG((fill_price - mid_at_decision) * 100)
                 FROM live_trades
                WHERE dry_run = 0 AND fill_price IS NOT NULL
                  AND mid_at_decision IS NOT NULL""",
        ).fetchone()
        n_rows = int(before[0] or 0)
        stored_avg = float(before[1] or 0)
        true_avg = float(before[2] or 0)

        wrong = int(conn.execute(
            """SELECT COUNT(*) FROM live_trades
                WHERE dry_run = 0 AND fill_price IS NOT NULL
                  AND mid_at_decision IS NOT NULL
                  AND (slippage_c IS NULL
                       OR ABS(slippage_c - (fill_price - mid_at_decision) * 100)
                          > ?)""",
            (TOL,),
        ).fetchone()[0])

        out = {
            "rows_with_inputs": n_rows,
            "rows_needing_fix": wrong,
            "stored_avg_slippage_c": round(stored_avg, 4),
            "true_avg_slippage_c": round(true_avg, 4),
            "overstatement_c": round(stored_avg - true_avg, 4),
            "applied": False,
        }

        if apply and wrong:
            conn.execute(
                """UPDATE live_trades
                      SET slippage_c =
                              ROUND(((fill_price - mid_at_decision) * 100)::numeric, 4),
                          slippage = CASE
                              WHEN entry_price IS NOT NULL AND entry_price > 0
                              THEN ROUND((ABS(fill_price - entry_price)
                                          / entry_price)::numeric, 6)
                              ELSE slippage END,
                          slippage_signed = CASE
                              WHEN entry_price IS NOT NULL AND entry_price > 0
                              THEN ROUND((
                                  CASE WHEN UPPER(COALESCE(direction,'BUY')) = 'SELL'
                                       THEN -(fill_price - entry_price)
                                       ELSE (fill_price - entry_price) END
                                  / entry_price)::numeric, 6)
                              ELSE slippage_signed END,
                          slippage_cost_usd = CASE
                              WHEN entry_price IS NOT NULL AND shares IS NOT NULL
                              THEN ROUND((ABS(fill_price - entry_price)
                                          * shares)::numeric, 4)
                              ELSE slippage_cost_usd END
                    WHERE dry_run = 0 AND fill_price IS NOT NULL
                      AND mid_at_decision IS NOT NULL
                      AND (slippage_c IS NULL
                           OR ABS(slippage_c - (fill_price - mid_at_decision) * 100)
                              > ?)""",
                (TOL,),
            )
            conn.commit()
            after = conn.execute(
                """SELECT AVG(slippage_c) FROM live_trades
                    WHERE dry_run = 0 AND fill_price IS NOT NULL
                      AND mid_at_decision IS NOT NULL""",
            ).fetchone()
            out["applied"] = True
            out["post_avg_slippage_c"] = round(float(after[0] or 0), 4)
        logger.info("[backfill_slippage] %s", json.dumps(out))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    print(json.dumps(run(apply=args.apply), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
