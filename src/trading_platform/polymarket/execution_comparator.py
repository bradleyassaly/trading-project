"""Paper vs live trade comparator."""
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Any

_DEFAULT_DB = Path(__file__).resolve().parents[3] / "data" / "polymarket" / "wallet_intelligence.db"


class ExecutionComparator:
    def __init__(self, db_path: str | Path | None = None) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)

    def compare(self, days: int = 7) -> dict[str, Any]:
        conn = sqlite3.connect(self._db_path, timeout=30)
        try:
            paper = conn.execute(f"""
                SELECT COUNT(*) n,
                    SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                    SUM(realized_pnl) pnl
                FROM polymarket_paper_trades
                WHERE entry_ts > unixepoch('now', '-{days} days') AND exit_ts IS NOT NULL
            """).fetchone()
            live = conn.execute(f"""
                SELECT COUNT(*) n,
                    SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                    SUM(realized_pnl) pnl,
                    AVG(slippage) slip, AVG(fill_time_ms) fill_ms
                FROM live_trades
                WHERE filled_at > unixepoch('now', '-{days} days') AND exit_ts IS NOT NULL AND dry_run=0
            """).fetchone()
            return {
                "paper_trades": paper[0] or 0,
                "paper_pnl": round(paper[2] or 0, 2),
                "paper_wr": round((paper[1] or 0) / max(paper[0] or 1, 1), 4),
                "live_trades": live[0] or 0,
                "live_pnl": round(live[2] or 0, 2),
                "live_wr": round((live[1] or 0) / max(live[0] or 1, 1), 4),
                "friction": round((paper[2] or 0) - (live[2] or 0), 2),
                "avg_slippage": round(live[3] or 0, 4),
                "avg_fill_ms": round(live[4] or 0, 0),
            }
        finally:
            conn.close()
