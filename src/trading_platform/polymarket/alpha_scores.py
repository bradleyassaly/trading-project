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

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)

# ── Tunables ────────────────────────────────────────────────────────────────

MIN_SAMPLE = 10           # minimum resolved clean trades to score
# Align with kill_switch.MIN_WIN_RATE (0.52). Previously 0.55 — the stricter
# gate locked out ~147 wallet-category rows that are positive-EV (avg_pnl>0)
# just with lower raw WR. Cost model covers the 52% boundary. Widens the
# copyable set ~50% which accelerates sample accumulation for signal calibration.
MIN_WR_COPYABLE = 0.52
# Longshot alternative gate: profit factor >= 1.5 captures wallets that
# lose >50% of trades but win big when they hit. Several whales we were
# missing: 0xd7375270e4 (45.8% WR, +$453k PnL), 0xa7c1f91472 (45% WR,
# +$36k), 0x9d63202c6d (48% WR). Combined with WR >= 0.35 floor to
# reject random-walk wallets.
MIN_PF_LONGSHOT = 1.5
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


# Columns added in the decision-evaluation layer. ALTER TABLE migration
# runs in _ensure_schema so existing DBs get the columns without rebuild.
_DISTRIBUTION_COLS = [
    ("avg_win_pnl", "REAL"),
    ("avg_loss_pnl", "REAL"),
    ("median_pnl", "REAL"),
    ("pnl_stddev", "REAL"),
    ("max_win", "REAL"),
    ("max_loss", "REAL"),
    ("avg_entry_price", "REAL"),
    ("sharpe_ratio", "REAL"),
    ("avg_hold_days", "REAL"),
    ("kelly_fraction", "REAL"),
    ("streak_current", "INTEGER"),
    ("streak_max_win", "INTEGER"),
    ("streak_max_loss", "INTEGER"),
]


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(wallet_alpha_scores)").fetchall()}
    for col, typedef in _DISTRIBUTION_COLS:
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE wallet_alpha_scores ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass
    conn.commit()


def compute_alpha_scores(db_path: str | Path) -> dict[str, Any]:
    """Recompute every wallet × category alpha score from scratch.

    Reads only ``pnl_reliable = 1`` trades. Returns a summary dict.
    """
    t0 = time.time()
    conn = connect_wallet_db(db_path)
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

        # Dual-gate copyability: admit either
        #   (a) mean-WR strategies: WR >= 0.52 with positive avg_pnl, OR
        #   (b) longshot strategies: high profit_factor (>= 1.5) — wallets
        #       that lose 55%+ of trades but pick massive asymmetric payoffs.
        #       0xd7375270e4 is the canonical example: 72 trades, 45.8% WR,
        #       +$453k total PnL — previously rejected by the WR gate alone.
        mean_strategy = wr >= MIN_WR_COPYABLE and (avg_pnl or 0) > 0
        longshot_strategy = (
            (pf is not None and pf >= MIN_PF_LONGSHOT)
            and (avg_pnl or 0) > 0
            and wr >= 0.35  # floor: below this is likely noise/negative-skew
        )
        is_copy = (
            (mean_strategy or longshot_strategy)
            and resolved >= MIN_SAMPLE
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

    # Second pass: compute distribution stats for each copyable wallet×category.
    # These are expensive per-trade queries so we only do them for copyable combos.
    _compute_distribution_stats(conn)
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


def _compute_distribution_stats(conn: sqlite3.Connection) -> None:
    """Compute return-distribution stats for copyable wallet×category combos."""
    import statistics

    combos = conn.execute(
        "SELECT wallet, category FROM wallet_alpha_scores WHERE is_copyable = 1"
    ).fetchall()

    for wallet, category in combos:
        trades = conn.execute(
            """SELECT pnl, price, timestamp
               FROM wallet_trades
               WHERE wallet = ? AND category = ?
                 AND pnl IS NOT NULL AND pnl != 0 AND pnl_reliable = 1
               ORDER BY timestamp ASC""",
            (wallet, category),
        ).fetchall()

        if not trades:
            continue

        pnls = [t[0] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        prices = [t[1] for t in trades if t[1] and t[1] > 0]
        timestamps = [t[2] for t in trades if t[2]]

        avg_win = statistics.mean(wins) if wins else 0
        avg_loss = statistics.mean(losses) if losses else 0
        median = statistics.median(pnls)
        stddev = statistics.stdev(pnls) if len(pnls) > 1 else 0
        max_win = max(pnls) if pnls else 0
        max_loss = min(pnls) if pnls else 0
        avg_price = statistics.mean(prices) if prices else None

        # Sharpe: mean(pnl) / stdev(pnl)
        sharpe = (statistics.mean(pnls) / stddev) if stddev > 0 else 0

        # Average hold: rough estimate from timestamp spread
        avg_hold = None
        if len(timestamps) >= 2:
            span_days = (max(timestamps) - min(timestamps)) / 86400
            avg_hold = span_days / len(timestamps) if len(timestamps) > 0 else None

        # Kelly: (wr * avg_win - (1-wr) * |avg_loss|) / avg_win
        wr = len(wins) / len(pnls) if pnls else 0
        if avg_win > 0:
            kelly = (wr * avg_win - (1 - wr) * abs(avg_loss)) / avg_win
            kelly = max(0.0, min(kelly, 0.25))
        else:
            kelly = 0.0

        # Streaks
        outcomes = ["W" if p > 0 else "L" for p in pnls]
        cur_streak, max_w, max_l = _compute_streaks(outcomes)

        conn.execute(
            """UPDATE wallet_alpha_scores SET
                   avg_win_pnl = ?, avg_loss_pnl = ?, median_pnl = ?,
                   pnl_stddev = ?, max_win = ?, max_loss = ?,
                   avg_entry_price = ?, sharpe_ratio = ?, avg_hold_days = ?,
                   kelly_fraction = ?, streak_current = ?,
                   streak_max_win = ?, streak_max_loss = ?
               WHERE wallet = ? AND category = ?""",
            (
                round(avg_win, 2), round(avg_loss, 2), round(median, 2),
                round(stddev, 2), round(max_win, 2), round(max_loss, 2),
                round(avg_price, 4) if avg_price else None,
                round(sharpe, 4), round(avg_hold, 2) if avg_hold else None,
                round(kelly, 4), cur_streak, max_w, max_l,
                wallet, category,
            ),
        )


def _compute_streaks(outcomes: list[str]) -> tuple[int, int, int]:
    """Return (current_streak, max_win_streak, max_loss_streak).

    current_streak is positive for wins, negative for losses.
    """
    if not outcomes:
        return 0, 0, 0

    cur = 0
    max_w = 0
    max_l = 0
    run = 0

    for o in outcomes:
        if o == "W":
            run = run + 1 if run > 0 else 1
            max_w = max(max_w, run)
        else:
            run = run - 1 if run < 0 else -1
            max_l = max(max_l, abs(run))
    cur = run
    return cur, max_w, max_l


def get_wallet_alpha(
    db_path: str | Path,
    wallet: str,
    category: str,
) -> float:
    """Look up the wallet's copyability score for one category.

    Returns 0.0 if no row exists or ``is_copyable = 0``. The signal
    engine uses 0.0 as a "skip this signal" sentinel.

    NOTE: this function collapses "unscored" and "not copyable" into the
    same 0.0 return. Callers that need to distinguish them (to bypass
    unscored wallets while blocking known-bad ones) should use
    ``get_wallet_alpha_status`` instead.
    """
    if not wallet or not category:
        return 0.0
    try:
        conn = connect_wallet_db(db_path)
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


def get_wallet_alpha_status(
    db_path: str | Path,
    wallet: str,
    category: str,
) -> tuple[str, float]:
    """Return (status, score) for a wallet × category.

    status is one of:
      * ``"copyable"``      — row exists and is_copyable=1 (use score)
      * ``"not_copyable"``  — row exists and is_copyable=0 (BLOCK signal)
      * ``"unscored"``      — no row (untested; caller may bypass at low size)

    This three-way distinction was added 2026-04-18 after the
    `accumulation` signal was found to be 0/6 correct on trades from a
    -$70K lifetime wallet. The old two-way return caused the paper
    executor to "bypass with 0 alpha" on both unscored AND explicitly
    non-copyable wallets, silently copying losing wallets that had
    real data saying "don't copy me".
    """
    if not wallet or not category:
        return ("unscored", 0.0)
    try:
        conn = connect_wallet_db(db_path)
        try:
            row = conn.execute(
                "SELECT copyability, is_copyable FROM wallet_alpha_scores WHERE wallet = ? AND category = ?",
                (wallet, category),
            ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("get_wallet_alpha_status lookup failed: %s", exc)
        return ("unscored", 0.0)
    if not row:
        return ("unscored", 0.0)
    is_copyable = int(row[1] or 0) == 1
    score = float(row[0] or 0.0)
    return ("copyable" if is_copyable else "not_copyable", score)


def get_wallet_alpha_full(
    db_path: str | Path,
    wallet: str,
) -> list[dict[str, Any]]:
    """Return every category alpha row for one wallet."""
    try:
        conn = connect_wallet_db(db_path)
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
        conn = connect_wallet_db(db_path)
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
        conn = connect_wallet_db(db_path)
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
