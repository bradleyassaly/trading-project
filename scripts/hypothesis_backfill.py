"""Self-healing hypothesis resolver.

Every 2h the scheduler runs this. It looks for closed paper trades
whose matching `trade_hypotheses` row was never marked resolved
(``actual_outcome IS NULL``) and calls ``mark_resolved`` for each.

Before 2026-04-18 this drift went silent for weeks — 17 closed trades
had orphan hypothesis rows, which kept the thesis scorecard stuck at
0% measured accuracy. The primary write path in ``check_and_resolve_open_trades``
calls ``mark_resolved`` directly, but any trade that resolves through a
different code path (``check_exits``, manual UPDATE, etc.) escapes it.
This job is the catch-all.
"""
from __future__ import annotations

import logging

from trading_platform.polymarket.db_connection import db
from trading_platform.polymarket.trade_hypotheses import mark_resolved
from trading_platform.polymarket.wallet_db import WalletDB

logger = logging.getLogger(__name__)


def main() -> int:
    path = WalletDB.default_path()
    with db() as conn:
        rows = conn.execute(
            """SELECT h.trade_id, pt.outcome, pt.realized_pnl
                 FROM trade_hypotheses h
                 JOIN polymarket_paper_trades pt ON pt.id = h.trade_id
                WHERE h.actual_outcome IS NULL
                  AND pt.exit_ts IS NOT NULL
                  AND pt.outcome IS NOT NULL"""
        ).fetchall()

    n_ok = 0
    for trade_id, outcome, pnl in rows:
        if mark_resolved(path, trade_id=trade_id, outcome=outcome, realized_pnl=float(pnl or 0)):
            n_ok += 1
    print(f"[hypothesis_backfill] resolved {n_ok}/{len(rows)}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
