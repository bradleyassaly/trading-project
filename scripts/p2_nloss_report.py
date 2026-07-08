"""P2 Step 0 (mandatory before any calibration rewiring): measure the label
n-loss from switching calibration labels to resolution-channel truth only.

The roadmap's warning: resolution-only labels + N4's holdout floors together
may STARVE the one live slice (resolution_decay) of trainable data. This
prints, per (signal_type × direction): the current label count under the
paper fitter's source, the canonical-truth-join label count, and the
trailing-7d canonical labeling rate with days-to-floor estimates.

Read-only. Run inside the scheduler container.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_platform.polymarket.db_connection import get_connection  # noqa: E402

N4_SLICE_FLOOR = 21   # per-slice train+holdout minimum (15 train + 6 holdout)
N4_GLOBAL_FLOOR = 28  # 20 train + 8 holdout


def main() -> int:
    now = int(time.time())
    conn = get_connection()
    try:
        print(f"\nP2 N-LOSS REPORT — {time.strftime('%Y-%m-%d')}")
        print(f"{'signal × direction':<38}{'cur30':>7}{'cur60':>7}"
              f"{'truth30':>9}{'truth60':>9}{'loss%':>7}")
        for days in (None,):
            pass
        cur = {}
        truth = {}
        for days in (30, 60):
            cutoff = now - days * 86400
            for st, d, n in conn.execute(
                    """SELECT signal_type, side, COUNT(*)
                         FROM polymarket_paper_trades
                        WHERE archived = 0 AND exit_ts IS NOT NULL
                          AND entry_ts > %s AND outcome IN ('win','loss')
                          AND confidence_raw IS NOT NULL
                        GROUP BY signal_type, side""", (cutoff,)).fetchall():
                cur[(st, d, days)] = int(n)
            for st, d, n in conn.execute(
                    """SELECT pt.signal_type, pt.side, COUNT(*)
                         FROM polymarket_paper_trades pt
                         JOIN market_resolutions mr
                           ON mr.condition_id = LOWER(pt.condition_id)
                        WHERE pt.archived = 0 AND pt.entry_ts > %s
                          AND pt.confidence_raw IS NOT NULL
                          AND mr.resolves_yes IS NOT NULL
                        GROUP BY pt.signal_type, pt.side""", (cutoff,)).fetchall():
                truth[(st, d, days)] = int(n)
        keys = sorted({(st, d) for (st, d, _) in list(cur) + list(truth)})
        for st, d in keys:
            c30 = cur.get((st, d, 30), 0)
            c60 = cur.get((st, d, 60), 0)
            t30 = truth.get((st, d, 30), 0)
            t60 = truth.get((st, d, 60), 0)
            loss = (1 - t30 / c30) * 100 if c30 else 0
            flag = "  <— STARVED vs floor" if t30 < N4_SLICE_FLOOR else ""
            print(f"{(st or '?')[:30] + ' × ' + (d or '?'):<38}{c30:>7}{c60:>7}"
                  f"{t30:>9}{t60:>9}{loss:>6.0f}%{flag}")

        # Forward labeling rate: canonical rows recorded in the last 7d.
        r7 = conn.execute(
            "SELECT COUNT(*) FROM market_resolutions WHERE recorded_at > %s",
            (now - 7 * 86400,)).fetchone()[0]
        print(f"\ncanonical labels recorded last 7d: {r7} "
              f"({r7 / 7:.0f}/day — table only grows; the writers are "
              f"gamma_bulk daily, uma_gamma 4h, data_api on enrich)")
        print(f"N4 floors: per-slice {N4_SLICE_FLOOR}, global {N4_GLOBAL_FLOOR}.")
        print("Rule from the recon spec: cut a slice over to truth labels "
              "only when truth-n >= max(21, 0.5 × current-n).")
    finally:
        try: conn.close()
        except Exception: pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
