"""Auto-archive stale paper positions.

Paper trades that haven't resolved within N days are likely on
markets that won't resolve at all (UMA disputes, ambiguous resolution,
or markets that closed without payout). Without archiving:
  - They eat the max_positions cap, blocking new signals
  - They count toward open exposure stats, distorting risk views
  - They never appear in PnL stats (exit_ts is NULL)

Archive criteria (all must hold):
  - archived = 0
  - exit_ts IS NULL (still "open")
  - entry_ts < now - STALE_DAYS

Archive action:
  - Set archived = 1
  - Set exit_ts = now
  - Set outcome = 'archived_stale'
  - Set realized_pnl = 0 (we don't know what happened)

Run via scheduler at 6h cadence. Idempotent.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# Days before a position is considered stale and auto-archived.
# 14 days is conservative — markets that are going to resolve typically
# do so within 1-2 weeks of entry. Anything older is structurally stuck.
STALE_DAYS = 14


def run_archiver() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    try:
        cutoff = int(time.time()) - STALE_DAYS * 86400
        # Count first
        n_stale = conn.execute(
            """SELECT COUNT(*) FROM polymarket_paper_trades
                WHERE archived = 0 AND exit_ts IS NULL
                  AND entry_ts < ?""",
            (cutoff,),
        ).fetchone()[0]
        if n_stale == 0:
            return {
                "elapsed_seconds": round(time.time() - t0, 2),
                "stale_positions_found": 0,
                "archived": 0,
            }
        # Archive
        now = int(time.time())
        conn.execute(
            """UPDATE polymarket_paper_trades
                SET archived = 1,
                    exit_ts = ?,
                    outcome = 'archived_stale',
                    realized_pnl = COALESCE(realized_pnl, 0)
                WHERE archived = 0 AND exit_ts IS NULL
                  AND entry_ts < ?""",
            (now, cutoff),
        )
        conn.commit()
        # Verify
        n_open_after = conn.execute(
            "SELECT COUNT(*) FROM polymarket_paper_trades "
            "WHERE archived = 0 AND exit_ts IS NULL"
        ).fetchone()[0]
        return {
            "elapsed_seconds": round(time.time() - t0, 2),
            "stale_positions_found": int(n_stale),
            "archived": int(n_stale),
            "open_positions_remaining": int(n_open_after),
            "stale_days_threshold": STALE_DAYS,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_archiver())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
