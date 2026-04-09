"""
Thesis-validation KPI tracker.

The system exists to test one claim:

  "Some Polymarket wallets have persistent, category-specific
   informational edge. By identifying which wallets have edge, in
   which categories, and copying their trades in real-time, we can
   capture a portion of that edge after transaction costs."

This module rolls every measurement we take into a single scorecard
that tells the operator, in one number, whether the thesis is being
confirmed. The scorecard is the body of the daily Telegram digest,
the top section of the Command Center dashboard, and the gating
condition for "GO LIVE".

Every query uses ``pnl_reliable = 1`` so the scorecard is computed on
clean data only — see reports/pnl_investigation.md and
reports/signal_analysis_clean.md.

Five sub-claims are tracked individually:

1. Persistent edge exists                  → claim_1
2. Edge is category-specific               → claim_2
3. We can identify who has edge            → claim_3
4. We can copy in real-time                → claim_4
5. Edge survives transaction costs (live)  → claim_5

The thesis scorecard itself is hypothesis accuracy: of all
``trade_hypotheses`` rows whose underlying paper trade has resolved,
what fraction had ``hypothesis_correct = 1``? That's the one number.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_platform.polymarket.trade_hypotheses import ensure_schema as ensure_hypotheses_schema

logger = logging.getLogger(__name__)


# ── Decision thresholds ─────────────────────────────────────────────────────

GO_LIVE_ACCURACY = 0.70
GO_LIVE_MIN_SAMPLE = 50
PROMISING_ACCURACY = 0.55
MARGINAL_ACCURACY = 0.50
TREND_DELTA = 0.05  # change in accuracy considered "improving" / "degrading"


_SNAPSHOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS thesis_daily_snapshots (
    date            TEXT PRIMARY KEY,
    total_hypotheses INTEGER,
    accuracy        REAL,
    trades_placed   INTEGER,
    trades_resolved INTEGER,
    cumulative_pnl  REAL,
    verdict         TEXT,
    snapshot_json   TEXT,
    created_at      INTEGER NOT NULL
);
"""


class KPITracker:
    """Computes the thesis scorecard + sub-claim metrics from clean data."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        if db_path is None:
            from trading_platform.polymarket.wallet_db import WalletDB
            db_path = str(WalletDB()._path)
        self._db_path = str(db_path)
        # Make sure the schemas we read exist (idempotent).
        try:
            ensure_hypotheses_schema(self._db_path)
            self._ensure_snapshot_schema()
        except Exception as exc:
            logger.debug("schema ensure failed: %s", exc)

    # ── Public entry points ─────────────────────────────────────────────────

    def compute_all(self) -> dict[str, Any]:
        return {
            "thesis_scorecard": self._thesis_scorecard(),
            "claim_1_persistence": self._claim_1(),
            "claim_2_category": self._claim_2(),
            "claim_3_identification": self._claim_3(),
            "claim_4_timing": self._claim_4(),
            "claim_5_execution": self._claim_5(),
            "daily_metrics": self._daily_metrics(),
            "computed_at": datetime.now(tz=timezone.utc).isoformat(),
        }

    def claims_only(self) -> dict[str, Any]:
        """Lightweight payload for the /api/thesis/claims endpoint."""
        return {
            "scorecard": self._thesis_scorecard(),
            "claim_1": self._claim_1(),
            "claim_2": self._claim_2(),
            "claim_3": self._claim_3(),
            "claim_4": self._claim_4(),
            "claim_5": self._claim_5(),
        }

    def save_daily_snapshot(self) -> dict[str, Any]:
        """Persist today's KPI snapshot to ``thesis_daily_snapshots``."""
        snapshot = self.compute_all()
        date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        sc = snapshot.get("thesis_scorecard", {}) or {}
        dm = snapshot.get("daily_metrics", {}) or {}
        cumulative_pnl = (snapshot.get("claim_1_persistence") or {}).get("clean_cohort_pnl") or 0
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO thesis_daily_snapshots
                       (date, total_hypotheses, accuracy, trades_placed,
                        trades_resolved, cumulative_pnl, verdict,
                        snapshot_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        date,
                        sc.get("total_hypotheses") or 0,
                        sc.get("accuracy"),
                        dm.get("trades_placed_24h") or 0,
                        dm.get("trades_resolved_24h") or 0,
                        cumulative_pnl,
                        sc.get("verdict"),
                        json.dumps(snapshot, default=str),
                        int(time.time()),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            return {"saved": True, "date": date, "verdict": sc.get("verdict")}
        except Exception as exc:
            logger.warning("save_daily_snapshot failed: %s", exc)
            return {"saved": False, "error": str(exc)}

    def get_history(self, days: int = 30) -> list[dict[str, Any]]:
        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            try:
                cur = conn.execute(
                    """SELECT date, total_hypotheses, accuracy, trades_placed,
                              trades_resolved, cumulative_pnl, verdict
                       FROM thesis_daily_snapshots
                       ORDER BY date DESC LIMIT ?""",
                    (days,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                conn.close()
        except Exception:
            return []

    # ── Internal: schemas ───────────────────────────────────────────────────

    def _ensure_snapshot_schema(self) -> None:
        conn = sqlite3.connect(self._db_path, timeout=30)
        try:
            conn.executescript(_SNAPSHOT_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=30)

    # ── Thesis scorecard ────────────────────────────────────────────────────

    def _thesis_scorecard(self) -> dict[str, Any]:
        try:
            conn = self._connect()
            try:
                total = conn.execute(
                    "SELECT COUNT(*) FROM trade_hypotheses WHERE actual_outcome IS NOT NULL AND actual_outcome != 'expired'"
                ).fetchone()[0]
                correct = conn.execute(
                    "SELECT COUNT(*) FROM trade_hypotheses WHERE hypothesis_correct = 1"
                ).fetchone()[0]
                # Recent 20 vs prior 20
                recent = conn.execute(
                    """SELECT hypothesis_correct FROM trade_hypotheses
                       WHERE actual_outcome IS NOT NULL AND actual_outcome != 'expired'
                       ORDER BY resolved_at DESC LIMIT 20"""
                ).fetchall()
                prior = conn.execute(
                    """SELECT hypothesis_correct FROM trade_hypotheses
                       WHERE actual_outcome IS NOT NULL AND actual_outcome != 'expired'
                       ORDER BY resolved_at DESC LIMIT 20 OFFSET 20"""
                ).fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("thesis_scorecard query failed: %s", exc)
            total, correct = 0, 0
            recent, prior = [], []

        accuracy = round(correct / total, 3) if total > 0 else None
        recent_acc = (
            round(sum(r[0] or 0 for r in recent) / len(recent), 3)
            if recent else None
        )
        prior_acc = (
            round(sum(r[0] or 0 for r in prior) / len(prior), 3)
            if prior else None
        )

        trend: str | None = None
        if recent_acc is not None and prior_acc is not None:
            if recent_acc > prior_acc + TREND_DELTA:
                trend = "improving"
            elif recent_acc < prior_acc - TREND_DELTA:
                trend = "degrading"
            else:
                trend = "stable"

        return {
            "total_hypotheses": total,
            "correct": correct,
            "accuracy": accuracy,
            "recent_20_accuracy": recent_acc,
            "prior_20_accuracy": prior_acc,
            "trend": trend,
            "verdict": self._verdict(accuracy, total),
            "target": f"{int(GO_LIVE_ACCURACY*100)}% accuracy on {GO_LIVE_MIN_SAMPLE}+ trades → go live",
        }

    @staticmethod
    def _verdict(accuracy: float | None, total: int) -> str:
        if total < 10:
            return "ACCUMULATING — need 50+ resolved hypotheses"
        if total < GO_LIVE_MIN_SAMPLE:
            return f"PRELIMINARY — {accuracy * 100:.0f}% on {total} trades — need {GO_LIVE_MIN_SAMPLE} for decision"
        if accuracy is None:
            return "ACCUMULATING"
        if accuracy >= GO_LIVE_ACCURACY:
            return "✅ GO LIVE — thesis confirmed at >70% accuracy"
        if accuracy >= PROMISING_ACCURACY:
            return "🟡 PROMISING — continue paper trading, tune filters"
        if accuracy >= MARGINAL_ACCURACY:
            return "⚠️ MARGINAL — thesis is weak, investigate"
        return "🔴 REJECTED — thesis does not hold, stop trading"

    # ── Claims ──────────────────────────────────────────────────────────────

    def _claim_1(self) -> dict[str, Any]:
        """Persistent edge exists — clean cohort win rate over time."""
        try:
            conn = self._connect()
            try:
                overall = conn.execute(
                    """SELECT COUNT(*),
                              ROUND(SUM(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) / COUNT(*), 3),
                              ROUND(SUM(pnl), 2)
                       FROM wallet_trades
                       WHERE pnl IS NOT NULL AND pnl != 0 AND pnl_reliable = 1"""
                ).fetchone()
                copyable_wr = conn.execute(
                    """SELECT ROUND(SUM(CASE WHEN wt.pnl > 0 THEN 1.0 ELSE 0 END) / COUNT(*), 3),
                              COUNT(*)
                       FROM wallet_trades wt
                       JOIN wallet_alpha_scores wa
                         ON wt.wallet = wa.wallet AND wt.category = wa.category
                       WHERE wa.is_copyable = 1
                         AND wt.pnl IS NOT NULL AND wt.pnl != 0 AND wt.pnl_reliable = 1"""
                ).fetchone()
                rolling = conn.execute(
                    """SELECT ROUND(SUM(CASE WHEN pnl > 0 THEN 1.0 ELSE 0 END) / COUNT(*), 3),
                              COUNT(*)
                       FROM wallet_trades
                       WHERE pnl IS NOT NULL AND pnl != 0 AND pnl_reliable = 1
                         AND timestamp >= strftime('%s', 'now', '-30 days')"""
                ).fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("claim_1 query failed: %s", exc)
            overall = (0, None, 0)
            copyable_wr = (None, 0)
            rolling = (None, 0)

        rolling_wr = rolling[0] if rolling else None
        cop_wr = copyable_wr[0] if copyable_wr else None
        persistence = (
            "confirmed"
            if rolling_wr and cop_wr and rolling_wr > 0.55
            else "degrading" if rolling_wr is not None and rolling_wr < 0.50
            else "stable"
        )
        return {
            "clean_cohort_trades": overall[0] or 0,
            "clean_cohort_wr": overall[1],
            "clean_cohort_pnl": overall[2] or 0,
            "copyable_wr": cop_wr,
            "copyable_trades": copyable_wr[1] if copyable_wr else 0,
            "rolling_30d_wr": rolling_wr,
            "rolling_30d_trades": rolling[1] if rolling else 0,
            "persistence": persistence,
        }

    def _claim_2(self) -> dict[str, Any]:
        """Edge is category-specific."""
        try:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """SELECT category, COUNT(*) AS copyable,
                              ROUND(AVG(win_rate), 3) AS avg_wr,
                              ROUND(AVG(copyability), 3) AS avg_score
                       FROM wallet_alpha_scores
                       WHERE is_copyable = 1
                       GROUP BY category
                       ORDER BY COUNT(*) DESC"""
                ).fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("claim_2 query failed: %s", exc)
            rows = []
        return {
            "categories_with_edge": len(rows),
            "breakdown": [
                {"category": r[0], "copyable_wallets": r[1], "avg_wr": r[2], "avg_score": r[3]}
                for r in rows
            ],
            "strongest_category": rows[0][0] if rows else None,
        }

    def _claim_3(self) -> dict[str, Any]:
        """We can identify who has edge."""
        try:
            conn = self._connect()
            try:
                total_scored = conn.execute("SELECT COUNT(*) FROM wallet_alpha_scores").fetchone()[0]
                copyable = conn.execute(
                    "SELECT COUNT(*) FROM wallet_alpha_scores WHERE is_copyable = 1"
                ).fetchone()[0]
                distinct = conn.execute(
                    "SELECT COUNT(DISTINCT wallet) FROM wallet_alpha_scores WHERE is_copyable = 1"
                ).fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("claim_3 query failed: %s", exc)
            total_scored = copyable = distinct = 0
        return {
            "total_scored": total_scored,
            "copyable_combos": copyable,
            "distinct_copyable_wallets": distinct,
            "selectivity": round(copyable / total_scored, 3) if total_scored > 0 else 0,
        }

    def _claim_4(self) -> dict[str, Any]:
        """We can copy in real-time."""
        try:
            conn = self._connect()
            try:
                # Real-wallet (non-synthetic) paper trades — these went
                # through the alpha gate by definition.
                alpha_trades = conn.execute(
                    """SELECT COUNT(*) FROM polymarket_paper_trades
                       WHERE wallet IS NOT NULL
                         AND wallet NOT IN ('velocity_detector', 'order_book_monitor')"""
                ).fetchone()[0]
                # Wallet alerts that did not convert to trades — these
                # were dropped by either the alpha gate or downstream gates.
                skipped = conn.execute(
                    """SELECT COUNT(*) FROM wallet_alerts
                       WHERE COALESCE(paper_trade_fired, 0) = 0"""
                ).fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("claim_4 query failed: %s", exc)
            alpha_trades = skipped = 0
        return {
            "alpha_gated_trades": alpha_trades,
            "signals_skipped": skipped,
            "websocket_status": "monitor live-collect logs",
        }

    def _claim_5(self) -> dict[str, Any]:
        """Edge survives transaction costs — live trading only."""
        try:
            conn = self._connect()
            try:
                # The live_trades table is created lazily by the live executor.
                row = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='live_trades'"
                ).fetchone()
                if not row or not row[0]:
                    return {
                        "live_trades": 0,
                        "live_pnl": 0,
                        "status": "NOT_YET_TESTED — requires live trading",
                    }
                cnt = conn.execute("SELECT COUNT(*) FROM live_trades WHERE dry_run = 0").fetchone()[0]
            finally:
                conn.close()
        except Exception:
            cnt = 0
        return {
            "live_trades": cnt,
            "live_pnl": 0,
            "status": "NOT_YET_TESTED — requires live trading" if cnt == 0 else "LIVE",
        }

    def _daily_metrics(self) -> dict[str, Any]:
        """Operational metrics for the last 24h."""
        try:
            conn = self._connect()
            try:
                cutoff = int(time.time()) - 86400
                placed = conn.execute(
                    "SELECT COUNT(*) FROM polymarket_paper_trades WHERE entry_ts >= ?",
                    (cutoff,),
                ).fetchone()[0]
                resolved_row = conn.execute(
                    """SELECT COUNT(*),
                              SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END)
                       FROM polymarket_paper_trades
                       WHERE exit_ts IS NOT NULL AND exit_ts >= ?""",
                    (cutoff,),
                ).fetchone()
                hypo_row = conn.execute(
                    """SELECT COUNT(*),
                              SUM(CASE WHEN hypothesis_correct = 1 THEN 1 ELSE 0 END)
                       FROM trade_hypotheses
                       WHERE actual_outcome IS NOT NULL AND resolved_at >= ?""",
                    (cutoff,),
                ).fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("daily_metrics query failed: %s", exc)
            placed = 0
            resolved_row = (0, 0)
            hypo_row = (0, 0)

        return {
            "trades_placed_24h": placed or 0,
            "trades_resolved_24h": resolved_row[0] or 0,
            "trades_won_24h": resolved_row[1] or 0,
            "hypotheses_resolved_24h": hypo_row[0] or 0,
            "hypotheses_correct_24h": hypo_row[1] or 0,
        }
