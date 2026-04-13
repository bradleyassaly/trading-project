"""
Paper trading analytics — performance breakdowns by signal, category,
wallet, exit reason, and cost impact.

Powers the Paper Trading GUI tabs and the daily digest metrics.
"""
from __future__ import annotations

import logging
import statistics
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")


class PaperAnalytics:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or _DEFAULT_DB

    def _conn(self):
        return connect_wallet_db(self.db_path)

    # ── Signal performance ──────────────────────────────────────────────────

    def signal_performance(self) -> list[dict[str, Any]]:
        """Per-signal-type: trades, wins, WR, avg_return, profit_factor."""
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT signal_type,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed,
                       SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN 1 ELSE 0 END) AS open,
                       AVG(CASE WHEN return_pct IS NOT NULL THEN return_pct END) AS avg_return,
                       AVG(CASE WHEN exit_ts IS NOT NULL THEN (exit_ts - entry_ts) / 86400.0 END) AS avg_hold_days,
                       SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) AS gross_win,
                       SUM(CASE WHEN realized_pnl < 0 THEN -realized_pnl ELSE 0 END) AS gross_loss
                FROM polymarket_paper_trades
                WHERE archived = 0
                GROUP BY signal_type
                ORDER BY COUNT(*) DESC
            """).fetchall()
            result = []
            for r in rows:
                sig, total, wins, closed, open_n, avg_ret, hold, gw, gl = r
                wr = round(wins / closed, 3) if closed > 0 else None
                pf = round(gw / gl, 2) if gl and gl > 0 else None
                result.append({
                    "signal_type": sig, "total": total, "wins": wins or 0,
                    "closed": closed or 0, "open": open_n or 0,
                    "win_rate": wr, "avg_return_pct": round(avg_ret, 3) if avg_ret else None,
                    "avg_hold_days": round(hold, 1) if hold else None,
                    "profit_factor": pf,
                })
            return result
        finally:
            conn.close()

    # ── Category performance ────────────────────────────────────────────────

    def category_performance(self) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT category,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed,
                       SUM(realized_pnl) AS total_pnl,
                       AVG(return_pct) AS avg_return
                FROM polymarket_paper_trades
                WHERE archived = 0 AND exit_ts IS NOT NULL
                GROUP BY category
                ORDER BY SUM(realized_pnl) DESC
            """).fetchall()
            return [
                {"category": r[0], "total": r[1], "wins": r[2], "closed": r[3],
                 "total_pnl": round(r[4] or 0, 2),
                 "avg_return": round(r[5], 3) if r[5] else None}
                for r in rows
            ]
        finally:
            conn.close()

    # ── Wallet performance ──────────────────────────────────────────────────

    def wallet_performance(self) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT wallet,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed,
                       SUM(realized_pnl) AS total_pnl
                FROM polymarket_paper_trades
                WHERE archived = 0 AND exit_ts IS NOT NULL
                  AND wallet NOT IN ('velocity_detector','order_book_monitor','')
                GROUP BY wallet
                ORDER BY SUM(realized_pnl) DESC
            """).fetchall()
            return [
                {"wallet": r[0], "total": r[1], "wins": r[2], "closed": r[3],
                 "total_pnl": round(r[4] or 0, 2),
                 "win_rate": round(r[2] / r[3], 3) if r[3] > 0 else None}
                for r in rows
            ]
        finally:
            conn.close()

    # ── Exit analysis ───────────────────────────────────────────────────────

    def exit_analysis(self) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT COALESCE(exit_reason, 'resolution') AS reason,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                       AVG(return_pct) AS avg_return,
                       SUM(realized_pnl) AS total_pnl
                FROM polymarket_paper_trades
                WHERE archived = 0 AND exit_ts IS NOT NULL
                GROUP BY reason
                ORDER BY COUNT(*) DESC
            """).fetchall()
            return [
                {"exit_reason": r[0], "total": r[1], "wins": r[2],
                 "avg_return": round(r[3], 3) if r[3] else None,
                 "total_pnl": round(r[4] or 0, 2)}
                for r in rows
            ]
        finally:
            conn.close()

    # ── Drawdown analysis ───────────────────────────────────────────────────

    def drawdown_analysis(self) -> dict[str, Any]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT total_equity FROM paper_equity_snapshots
                ORDER BY ts ASC
            """).fetchall()
        except Exception:
            return {"available": False, "reason": "No equity snapshots"}
        finally:
            conn.close()

        if len(rows) < 2:
            return {"available": False, "reason": "Need 2+ equity snapshots"}

        equities = [r[0] for r in rows]
        peak = equities[0]
        max_dd = 0
        max_dd_pct = 0
        for eq in equities:
            peak = max(peak, eq)
            dd = peak - eq
            dd_pct = dd / peak if peak > 0 else 0
            if dd_pct > max_dd_pct:
                max_dd = dd
                max_dd_pct = dd_pct

        current = equities[-1]
        current_peak = max(equities)
        current_dd = (current_peak - current) / current_peak if current_peak > 0 else 0

        return {
            "available": True,
            "max_drawdown_usd": round(max_dd, 2),
            "max_drawdown_pct": round(max_dd_pct, 4),
            "current_drawdown_pct": round(current_dd, 4),
            "current_equity": round(current, 2),
            "peak_equity": round(current_peak, 2),
            "snapshots": len(equities),
        }

    # ── Portfolio summary ───────────────────────────────────────────────────

    def portfolio_summary(self) -> dict[str, Any]:
        """Quick summary for the page header."""
        conn = self._conn()
        try:
            total = conn.execute("SELECT COUNT(*) FROM polymarket_paper_trades WHERE archived=0").fetchone()[0]
            open_n = conn.execute("SELECT COUNT(*) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived=0").fetchone()[0]
            closed = conn.execute("SELECT COUNT(*) FROM polymarket_paper_trades WHERE exit_ts IS NOT NULL AND archived=0").fetchone()[0]
            wins = conn.execute("SELECT COUNT(*) FROM polymarket_paper_trades WHERE outcome='win' AND archived=0").fetchone()[0]
            total_pnl = conn.execute("SELECT COALESCE(SUM(realized_pnl),0) FROM polymarket_paper_trades WHERE exit_ts IS NOT NULL AND archived=0").fetchone()[0]
            deployed = conn.execute("SELECT COALESCE(SUM(size_usd),0) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived=0").fetchone()[0]

            starting = 10_000
            wr = round(wins / closed, 3) if closed > 0 else None

            return {
                "starting_bankroll": starting,
                "total_trades": total,
                "open_positions": open_n,
                "closed_trades": closed,
                "wins": wins or 0,
                "win_rate": wr,
                "realized_pnl": round(total_pnl, 2),
                "deployed": round(deployed, 2),
                "equity_estimate": round(starting + total_pnl - deployed, 2),
            }
        finally:
            conn.close()
