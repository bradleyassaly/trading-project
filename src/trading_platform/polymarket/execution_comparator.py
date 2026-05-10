"""Paper vs live trade comparator."""
from __future__ import annotations

import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection


class ExecutionComparator:
    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path

    def compare(self, days: int = 7) -> dict[str, Any]:
        conn = get_connection(self._db_path)
        cutoff = int(time.time()) - days * 86400
        try:
            paper = conn.execute(
                """SELECT COUNT(*) n,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                       SUM(realized_pnl) pnl
                   FROM polymarket_paper_trades
                   WHERE entry_ts > %s AND exit_ts IS NOT NULL""",
                (cutoff,),
            ).fetchone()
            live = conn.execute(
                """SELECT COUNT(*) n,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                       SUM(realized_pnl) pnl,
                       -- directional unfavorable slippage_pct
                       AVG(CASE
                           WHEN direction='BUY'
                           THEN GREATEST(0, (fill_price - entry_price) / NULLIF(entry_price, 0))
                           ELSE GREATEST(0, (entry_price - fill_price) / NULLIF(entry_price, 0))
                       END) slip_pct,
                       AVG(fill_time_ms) fill_ms
                   FROM live_trades
                   WHERE attempted_at > %s AND exit_ts IS NOT NULL AND dry_run=0
                     AND fill_price IS NOT NULL AND entry_price > 0
                     AND ABS(fill_price - entry_price) / entry_price < 0.5""",
                (cutoff,),
            ).fetchone()
            return {
                "paper_trades": paper[0] or 0,
                "paper_pnl": round(float(paper[2] or 0), 2),
                "paper_wr": round((paper[1] or 0) / max(paper[0] or 1, 1), 4),
                "live_trades": live[0] or 0,
                "live_pnl": round(float(live[2] or 0), 2),
                "live_wr": round((live[1] or 0) / max(live[0] or 1, 1), 4),
                "friction": round(float(paper[2] or 0) - float(live[2] or 0), 2),
                "avg_slippage_pct": round(float(live[3] or 0), 6),
                "avg_fill_ms": round(float(live[4] or 0), 0),
            }
        finally:
            try:
                conn.close()
            except Exception:
                pass
