"""Post-fill drift — is the spread income, or bait?

2026-08-01. The venue map found crypto/bitcoin $0.35-0.65 continuously
two-sided (3,447 prints/hour, bounce rate 0.469, Roll spread 2.47c). But
continuous flow only proves a resting order WILL fill — not that it fills
PROFITABLY. A maker gets filled precisely when someone informed wants the
other side. This measures that directly.

MECHANIC. Classify each print with the tick rule: +1 buyer-initiated
(it lifted an ask -> a MAKER SOLD), -1 seller-initiated (it hit a bid ->
a MAKER BOUGHT). Then look forward H seconds:

    adverse_c = sign * (future_price - print_price) * 100

Positive adverse_c means price moved AGAINST the maker who just filled
(price rose after they sold, or fell after they bought). Average it.

THE TEST. A maker earns about half the spread per fill (Roll/2 ~ 1.24c
here). Making is income only if:

    adverse_c  <  half_spread_earned

If adverse drift exceeds the half-spread, the "spread" is bait: you are
paid 1.24c to take a position that immediately loses more than that. That
is exactly how the 2026-07-16 fade-losers test concluded edge accrues to
the RESTING counterparty — this asks whether that is true here too, from
the other side of the trade.

Read-only. Run ISOLATED:
  docker compose run --rm --no-deps scheduler \
      python scripts/adverse_selection_survey.py [--days 3]
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

logger = logging.getLogger("adverse_selection")

HORIZONS_S = (5, 30, 120)
HALF_SPREAD_EARNED_C = 1.24      # Roll 2.47c / 2, from the venue map
CATEGORY = "crypto/bitcoin"
PX_LO, PX_HI = 0.35, 0.65


def survey(days: int = 3, max_tokens: int = 120, db_path: str | None = None) -> dict:
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        now = int(float(conn.execute(
            "SELECT EXTRACT(EPOCH FROM NOW())").fetchone()[0]))
        floor_ts = now - days * 86400
        toks = conn.execute(
            """SELECT wt.asset, COUNT(*) n
                 FROM wallet_trades wt
                 JOIN markets m ON m.condition_id = wt.condition_id
                WHERE wt.timestamp >= ? AND wt.side = 'BUY'
                  AND m.subcategory = ?
                GROUP BY wt.asset HAVING COUNT(*) >= 100
                ORDER BY n DESC LIMIT ?""",
            (floor_ts, CATEGORY, max_tokens),
        ).fetchall()

        adverse: dict[int, list[float]] = {h: [] for h in HORIZONS_S}
        n_fills = 0
        for asset, _n in toks:
            rows = conn.execute(
                """SELECT timestamp, price FROM wallet_trades
                    WHERE asset = ? AND side = 'BUY' AND timestamp >= ?
                    ORDER BY timestamp""",
                (str(asset), floor_ts),
            ).fetchall()
            pts = [(int(t), float(p)) for t, p in rows
                   if PX_LO <= float(p) <= PX_HI]
            if len(pts) < 50:
                continue
            sign = 0
            for i in range(1, len(pts)):
                t, p = pts[i]
                prev_p = pts[i - 1][1]
                if p > prev_p:
                    sign = 1
                elif p < prev_p:
                    sign = -1
                if sign == 0:
                    continue
                n_fills += 1
                for h in HORIZONS_S:
                    tgt = t + h
                    fut = None
                    for j in range(i + 1, len(pts)):
                        if pts[j][0] >= tgt:
                            fut = pts[j][1]
                            break
                    if fut is None:
                        continue
                    adverse[h].append(sign * (fut - p) * 100.0)

        out = {"category": CATEGORY, "price_band": [PX_LO, PX_HI],
               "cohort_days": days, "tokens": len(toks),
               "classified_fills": n_fills,
               "half_spread_earned_c": HALF_SPREAD_EARNED_C, "horizons": {}}
        for h in HORIZONS_S:
            v = adverse[h]
            if len(v) < 100:
                continue
            mean_adv = statistics.fmean(v)
            out["horizons"][f"{h}s"] = {
                "n": len(v),
                "mean_adverse_c": round(mean_adv, 4),
                "median_adverse_c": round(statistics.median(v), 4),
                "net_edge_c": round(HALF_SPREAD_EARNED_C - mean_adv, 4),
                "verdict": ("spread is INCOME"
                            if mean_adv < HALF_SPREAD_EARNED_C
                            else "spread is BAIT — adverse drift exceeds it"),
            }
        logger.info("[adverse] %s", json.dumps(out))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    args = ap.parse_args()
    print(json.dumps(survey(days=args.days), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
