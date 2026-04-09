"""
Per-wallet, per-category copyability scoring.

After the PnL investigation (reports/pnl_investigation.md) revealed
that 53% of resolved trades had unreliable PnL, the signal engine can
no longer trust generic ``tier_multiplier`` × ``conviction_multiplier``
formulas. Instead, every signal decision should be conditioned on the
**specific wallet's proven track record in the specific category** the
signal fires for.

This module computes that. For each (wallet, category) pair with at
least ``MIN_SAMPLE`` resolved trades flagged ``pnl_reliable = 1``, it
writes a row to ``wallet_alpha_scores`` with:

- ``win_rate``           — lifetime WR on clean trades only
- ``win_rate_30d``       — rolling 30-day WR (recency)
- ``avg_pnl``            — average $ per trade
- ``recency_score``      — linear decay over 90d since last trade
- ``copyability``        — composite weighted score in [0, 1]
- ``is_copyable``        — boolean gate (WR≥0.55 + sample + EV + recency)

The signal engine looks up ``get_wallet_alpha(wallet, category)`` and
gates execution on that. Wallets that aren't copyable in the category
do not get traded — period.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Tunables ────────────────────────────────────────────────────────────────

MIN_SAMPLE = 10           # minimum resolved clean trades to score
MIN_WR_COPYABLE = 0.55    # minimum WR to be flagged copyable
RECENCY_DECAY_DAYS = 90   # linear decay over 90 days for recency_score
COPYABLE_MIN_RECENCY = 0.30  # ~63 days max stale-ness

WEIGHT_WR = 0.40
WEIGHT_WR_30D = 0.25
WEIGHT_SAMPLE = 0.20
WEIGHT_RECENCY = 0.15

SAMPLE_SATURATION = 50    # WeightSample = min(1.0, sample / SAMPLE_SATURATION)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_alpha_scores (
    wallet          TEXT NOT NULL,
    category        TEXT NOT NULL,
    resolved_trades INTEGER NOT NULL,
    win_rate        REAL NOT NULL,
    win_rate_30d    REAL,
    avg_pnl         REAL,
    total_pnl       REAL,
    profit_factor   REAL,
    avg_bet_size    REAL,
    recency_score   REAL,
    copyability     REAL,
    is_copyable     INTEGER,
    last_trade_at   INTEGER,
    computed_at     INTEGER NOT NULL,
    PRIMARY KEY (wallet, category)
);
CREATE INDEX IF NOT EXISTS idx_alpha_copyable ON wallet_alpha_scores(is_copyable, category);
CREATE INDEX IF NOT EXISTS idx_alpha_score ON wallet_alpha_scores(copyability DESC);
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    conn.commit()


def compute_alpha_scores(db_path: str | Path) -> dict[str, Any]:
    """Recompute every wallet × category alpha score from scratch.

    Reads only ``pnl_reliable = 1`` trades. Returns a summary dict.
    """
    t0 = time.time()
    conn = sqlite3.connect(str(db_path), timeout=30)
    _ensure_schema(conn)

    now = int(time.time())
    cutoff_30d = now - 30 * 86400

    # Pull all eligible (wallet, category) combos in a single query.
    rows = conn.execute(
        """SELECT wt.wallet, wt.category,
                  COUNT(*) AS resolved,
                  SUM(CASE WHEN wt.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                  SUM(CASE WHEN wt.pnl < 0 THEN 1 ELSE 0 END) AS losses,
                  SUM(wt.pnl) AS total_pnl,
                  AVG(wt.pnl) AS avg_pnl,
                  AVG(wt.size * wt.price) AS avg_bet,
                  MAX(wt.timestamp) AS last_trade_ts,
                  SUM(CASE WHEN wt.timestamp >= ? AND wt.pnl > 0 THEN 1 ELSE 0 END) AS wins_30d,
                  SUM(CASE WHEN wt.timestamp >= ? AND wt.pnl < 0 THEN 1 ELSE 0 END) AS losses_30d,
                  SUM(CASE WHEN wt.pnl > 0 THEN wt.pnl ELSE 0 END) AS gross_win,
                  SUM(CASE WHEN wt.pnl < 0 THEN -wt.pnl ELSE 0 END) AS gross_loss
           FROM wallet_trades wt
           WHERE wt.pnl IS NOT NULL AND wt.pnl != 0
             AND wt.pnl_reliable = 1
             AND wt.category IS NOT NULL
           GROUP BY wt.wallet, wt.category
           HAVING COUNT(*) >= ?""",
        (cutoff_30d, cutoff_30d, MIN_SAMPLE),
    ).fetchall()

    # Wipe + rewrite — small enough for full rebuild.
    conn.execute("DELETE FROM wallet_alpha_scores")

    inserted = 0
    copyable = 0
    for r in rows:
        (wallet, category, resolved, wins, losses, total_pnl, avg_pnl,
         avg_bet, last_trade_ts, wins_30d, losses_30d, gw, gl) = r

        decided = wins + losses
        wr = wins / decided if decided > 0 else 0.0

        decided_30d = (wins_30d or 0) + (losses_30d or 0)
        wr_30d = (wins_30d / decided_30d) if decided_30d > 0 else wr

        # Profit factor: gross_win / gross_loss; cap to prevent inf.
        if gl and gl > 0:
            pf = round(min(gw / gl, 99.0), 3)
        else:
            pf = None

        # Recency: linear decay 1.0 → 0.1 over 90 days.
        if last_trade_ts:
            days_ago = max(0, (now - int(last_trade_ts)) / 86400)
            recency = max(0.10, 1.0 - (days_ago / RECENCY_DECAY_DAYS))
        else:
            recency = 0.10

        sample_conf = min(1.0, resolved / SAMPLE_SATURATION)

        copyability = (
            wr * WEIGHT_WR
            + wr_30d * WEIGHT_WR_30D
            + sample_conf * WEIGHT_SAMPLE
            + recency * WEIGHT_RECENCY
        )
        copyability = round(min(1.0, max(0.0, copyability)), 4)

        is_copy = (
            wr >= MIN_WR_COPYABLE
            and resolved >= MIN_SAMPLE
            and (avg_pnl or 0) > 0
            and recency >= COPYABLE_MIN_RECENCY
        )
        if is_copy:
            copyable += 1

        conn.execute(
            """INSERT INTO wallet_alpha_scores
               (wallet, category, resolved_trades, win_rate, win_rate_30d,
                avg_pnl, total_pnl, profit_factor, avg_bet_size,
                recency_score, copyability, is_copyable,
                last_trade_at, computed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (wallet, category, resolved, round(wr, 4), round(wr_30d, 4),
             round(avg_pnl or 0, 2), round(total_pnl or 0, 2), pf,
             round(avg_bet or 0, 2),
             round(recency, 4), copyability, 1 if is_copy else 0,
             int(last_trade_ts) if last_trade_ts else None, now),
        )
        inserted += 1

    conn.commit()
    conn.close()
    elapsed = round(time.time() - t0, 1)
    logger.info(
        "[ALPHA] computed %d wallet×category scores, %d copyable, in %.1fs",
        inserted, copyable, elapsed,
    )
    return {
        "scored": inserted,
        "copyable": copyable,
        "elapsed_seconds": elapsed,
    }


def get_wallet_alpha(
    db_path: str | Path,
    wallet: str,
    category: str,
) -> float:
    """Look up the wallet's copyability score for one category.

    Returns 0.0 if no row exists or ``is_copyable = 0``. The signal
    engine uses 0.0 as a "skip this signal" sentinel.
    """
    if not wallet or not category:
        return 0.0
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        try:
            row = conn.execute(
                "SELECT copyability, is_copyable FROM wallet_alpha_scores WHERE wallet = ? AND category = ?",
                (wallet, category),
            ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("get_wallet_alpha lookup failed: %s", exc)
        return 0.0
    if not row or not row[1]:
        return 0.0
    return float(row[0])


def get_wallet_alpha_full(
    db_path: str | Path,
    wallet: str,
) -> list[dict[str, Any]]:
    """Return every category alpha row for one wallet."""
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        try:
            cur = conn.execute(
                """SELECT category, resolved_trades, win_rate, win_rate_30d,
                          avg_pnl, total_pnl, profit_factor, recency_score,
                          copyability, is_copyable, last_trade_at
                   FROM wallet_alpha_scores WHERE wallet = ?
                   ORDER BY copyability DESC""",
                (wallet,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception:
        return []


def get_category_leaderboard(
    db_path: str | Path,
    category: str,
    *,
    min_score: float = 0.5,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Top copyable wallets in a category, ranked by copyability."""
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        try:
            cur = conn.execute(
                """SELECT wallet, resolved_trades, win_rate, win_rate_30d,
                          avg_pnl, total_pnl, copyability, last_trade_at
                   FROM wallet_alpha_scores
                   WHERE category = ? AND is_copyable = 1 AND copyability >= ?
                   ORDER BY copyability DESC LIMIT ?""",
                (category, min_score, limit),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception:
        return []


def get_summary(db_path: str | Path) -> dict[str, Any]:
    """High-level overview for ``GET /api/alpha/summary``."""
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        try:
            total = conn.execute("SELECT COUNT(*) FROM wallet_alpha_scores").fetchone()[0]
            copyable = conn.execute("SELECT COUNT(*) FROM wallet_alpha_scores WHERE is_copyable = 1").fetchone()[0]
            distinct_wallets = conn.execute(
                "SELECT COUNT(DISTINCT wallet) FROM wallet_alpha_scores WHERE is_copyable = 1"
            ).fetchone()[0]
            by_cat = conn.execute(
                """SELECT category, COUNT(*) AS scored,
                          SUM(CASE WHEN is_copyable = 1 THEN 1 ELSE 0 END) AS copyable,
                          ROUND(AVG(win_rate), 3) AS avg_wr
                   FROM wallet_alpha_scores GROUP BY category ORDER BY copyable DESC"""
            ).fetchall()
            return {
                "available": True,
                "total_scored": total,
                "total_copyable": copyable,
                "distinct_copyable_wallets": distinct_wallets,
                "by_category": [
                    {"category": r[0], "scored": r[1], "copyable": r[2], "avg_wr": r[3]}
                    for r in by_cat
                ],
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}
