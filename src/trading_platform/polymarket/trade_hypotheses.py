"""
Trade hypothesis generation.

A "trade hypothesis" is the structured rationale behind a paper trade:
*why* the system thinks this specific signal × wallet × market is
worth taking, in plain English the operator can scan in 5 seconds.

Stored in ``trade_hypotheses``, joined to ``polymarket_paper_trades``
by ``trade_id`` after the trade is placed. Used as the body of
Telegram trade alerts (Part 4 of the signal_analysis_clean rollout)
and exposed via the API for the dashboard's trade-detail panel.

A hypothesis is built from facts already in our DB:
- The wallet's alpha score in this category (from wallet_alpha_scores)
- The wallet's most recent N trades in this category (from wallet_trades)
- The market's current state (from polymarket_paper_trades + Gamma)
- The signal type that fired (from market_signals)
- Convergence count (other watched wallets aligned same direction)

The output is a dataclass + ``thesis`` text + ``confidence_factors``
list — both human-readable and machine-parseable.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS trade_hypotheses (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id      INTEGER,
    wallet        TEXT NOT NULL,
    category      TEXT,
    signal_type   TEXT NOT NULL,
    market_slug   TEXT,
    market_question TEXT,
    direction     TEXT,
    entry_price   REAL,
    alpha_score   REAL,
    wallet_wr     REAL,
    wallet_resolved INTEGER,
    convergence_count INTEGER,
    thesis        TEXT NOT NULL,
    confidence_factors TEXT,  -- JSON list of {factor, value, weight}
    created_at    INTEGER NOT NULL,
    -- Resolution fields populated when the underlying trade resolves.
    -- A hypothesis is "correct" iff the wallet's predicted direction
    -- matched the market's resolution outcome (i.e. the trade was a win).
    actual_outcome     TEXT,        -- 'win' / 'loss' / 'expired'
    hypothesis_correct INTEGER,     -- 1 if thesis confirmed, 0 if rejected, NULL if unresolved
    realized_pnl       REAL,
    resolved_at        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_th_trade ON trade_hypotheses(trade_id);
CREATE INDEX IF NOT EXISTS idx_th_wallet ON trade_hypotheses(wallet);
CREATE INDEX IF NOT EXISTS idx_th_created ON trade_hypotheses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_th_resolved ON trade_hypotheses(resolved_at DESC);
"""

# Columns that may need ALTER TABLE on existing DBs (the table was
# created in a previous session without the resolution fields).
_RESOLUTION_COLS = [
    ("actual_outcome", "TEXT"),
    ("hypothesis_correct", "INTEGER"),
    ("realized_pnl", "REAL"),
    ("resolved_at", "INTEGER"),
]


@dataclass
class TradeHypothesis:
    """Structured rationale for a single paper trade."""
    wallet: str
    category: str
    signal_type: str
    market_question: str
    direction: str
    entry_price: float
    alpha_score: float | None = None
    wallet_wr: float | None = None
    wallet_resolved: int | None = None
    convergence_count: int | None = None
    thesis: str = ""
    confidence_factors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "category": self.category,
            "signal_type": self.signal_type,
            "market_question": self.market_question,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "alpha_score": self.alpha_score,
            "wallet_wr": self.wallet_wr,
            "wallet_resolved": self.wallet_resolved,
            "convergence_count": self.convergence_count,
            "thesis": self.thesis,
            "confidence_factors": list(self.confidence_factors),
        }


def ensure_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.executescript(_SCHEMA)
        # Forward migration for the resolution columns added after the
        # table first shipped.
        existing = {r[1] for r in conn.execute("PRAGMA table_info(trade_hypotheses)").fetchall()}
        for col, typedef in _RESOLUTION_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE trade_hypotheses ADD COLUMN {col} {typedef}")
                except sqlite3.OperationalError:
                    pass
        conn.commit()
    finally:
        conn.close()


def mark_resolved(
    db_path: str,
    *,
    trade_id: int,
    outcome: str,
    realized_pnl: float | None,
) -> bool:
    """Mark a hypothesis as resolved when its underlying paper trade closes.

    A hypothesis is **correct** iff the realized P&L is positive (i.e. the
    wallet's predicted direction matched the resolution outcome). Expired
    trades (markets vanished from Gamma) are recorded as `outcome='expired'`
    with `hypothesis_correct=NULL` since neither side was confirmed.
    """
    try:
        ensure_schema(db_path)
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            if outcome == "expired":
                correct = None
            else:
                correct = 1 if (realized_pnl or 0) > 0 else 0
            cur = conn.execute(
                """UPDATE trade_hypotheses
                   SET actual_outcome = ?, hypothesis_correct = ?,
                       realized_pnl = ?, resolved_at = ?
                   WHERE trade_id = ? AND actual_outcome IS NULL""",
                (outcome, correct, realized_pnl, int(time.time()), trade_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("mark_resolved failed for trade_id=%s: %s", trade_id, exc)
        return False


def build_hypothesis(
    db_path: str,
    *,
    wallet: str,
    category: str,
    signal_type: str,
    market_slug: str,
    market_question: str,
    direction: str,
    entry_price: float,
    convergence_count: int = 0,
) -> TradeHypothesis:
    """Compose a hypothesis for a paper trade about to be placed.

    Reads facts from wallet_alpha_scores + wallet_trades. Synthesizes a
    plain-English ``thesis`` line and a list of weighted confidence
    factors. Never raises — falls back to a minimal hypothesis if any
    DB lookup fails.
    """
    h = TradeHypothesis(
        wallet=wallet,
        category=category or "other",
        signal_type=signal_type,
        market_question=market_question or "",
        direction=direction,
        entry_price=float(entry_price or 0),
        convergence_count=convergence_count,
    )

    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            # Alpha score (clean-data, per-category)
            row = conn.execute(
                """SELECT copyability, win_rate, win_rate_30d, resolved_trades,
                          avg_pnl, total_pnl, last_trade_at
                   FROM wallet_alpha_scores
                   WHERE wallet = ? AND category = ?""",
                (wallet, h.category),
            ).fetchone()
            if row:
                h.alpha_score = row[0]
                h.wallet_wr = row[1]
                h.wallet_resolved = row[3]
                wr_30d = row[2]
                avg_pnl = row[4] or 0
                total_pnl = row[5] or 0
                last_trade_at = row[6]
            else:
                wr_30d = avg_pnl = total_pnl = last_trade_at = None

            # Wallet's most recent N resolved trades for context
            recent = conn.execute(
                """SELECT pnl, slug FROM wallet_trades
                   WHERE wallet = ? AND category = ?
                     AND pnl IS NOT NULL AND pnl != 0 AND pnl_reliable = 1
                   ORDER BY timestamp DESC LIMIT 5""",
                (wallet, h.category),
            ).fetchall()
            recent_wr = (
                sum(1 for r in recent if r[0] > 0) / len(recent)
                if recent else None
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("hypothesis lookup failed: %s", exc)
        wr_30d = avg_pnl = total_pnl = last_trade_at = recent_wr = None

    # ── Compose the thesis text ─────────────────────────────────────────
    parts: list[str] = []

    wallet_short = wallet[:10] + "…" if len(wallet) > 10 else wallet

    # Header line: who is doing what
    parts.append(
        f"{wallet_short} just {direction} on \"{market_question[:60]}\" at {entry_price:.3f}."
    )

    # Track record line
    if h.wallet_wr is not None and h.wallet_resolved is not None:
        parts.append(
            f"This wallet has hit {h.wallet_wr * 100:.0f}% on {h.wallet_resolved} "
            f"resolved {h.category} trades."
        )
        if wr_30d is not None and h.wallet_resolved >= 5:
            wr_delta = wr_30d - h.wallet_wr
            if abs(wr_delta) > 0.05:
                trend = "trending up" if wr_delta > 0 else "trending down"
                parts.append(
                    f"Recent 30-day WR is {wr_30d * 100:.0f}% — {trend}."
                )
    else:
        parts.append(
            f"No alpha score yet for this wallet in {h.category} (insufficient sample)."
        )

    # Convergence
    if convergence_count and convergence_count >= 2:
        parts.append(
            f"{convergence_count} watched wallets are aligned in the same direction — "
            "convergence has the strongest historical edge in our data (3+ whales = 81.7% WR)."
        )

    # Avg pnl
    if avg_pnl and h.wallet_resolved:
        if avg_pnl > 0:
            parts.append(
                f"Avg P&L per trade in this category: +${avg_pnl:.0f}."
            )
        else:
            parts.append(
                f"⚠ Avg P&L per trade in this category: −${abs(avg_pnl):.0f} "
                "(positive WR but negative expectancy — small bet warranted)."
            )

    # Recent form
    if recent and recent_wr is not None:
        parts.append(
            f"Last {len(recent)} resolved {h.category} trades: "
            f"{int(round(recent_wr * len(recent)))}/{len(recent)} winners."
        )

    # Closing statement (bullish or cautionary)
    if h.alpha_score is not None and h.alpha_score >= 0.75:
        parts.append("**High-conviction copy.**")
    elif h.alpha_score is not None and h.alpha_score >= 0.55:
        parts.append("Moderate-conviction copy.")
    elif h.alpha_score is not None:
        parts.append("Low-conviction copy — monitor closely.")
    else:
        parts.append("Speculative — wallet has no proven edge here.")

    h.thesis = " ".join(parts)

    # ── Build confidence-factor list ────────────────────────────────────
    factors: list[dict[str, Any]] = []
    if h.alpha_score is not None:
        factors.append({"factor": "alpha_score", "value": round(h.alpha_score, 3), "weight": 0.40})
    if h.wallet_wr is not None:
        factors.append({"factor": "lifetime_wr", "value": round(h.wallet_wr, 3), "weight": 0.20})
    if wr_30d is not None:
        factors.append({"factor": "wr_30d", "value": round(wr_30d, 3), "weight": 0.15})
    if convergence_count and convergence_count >= 2:
        factors.append({"factor": "convergence", "value": convergence_count, "weight": 0.15})
    if h.wallet_resolved is not None:
        factors.append({"factor": "sample_size", "value": h.wallet_resolved, "weight": 0.10})
    h.confidence_factors = factors

    return h


def persist_hypothesis(db_path: str, hypo: TradeHypothesis, trade_id: int | None = None) -> int | None:
    """Write a hypothesis to ``trade_hypotheses``. Returns the row id."""
    try:
        ensure_schema(db_path)
        import json as _json
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            cur = conn.execute(
                """INSERT INTO trade_hypotheses
                   (trade_id, wallet, category, signal_type, market_slug,
                    market_question, direction, entry_price, alpha_score,
                    wallet_wr, wallet_resolved, convergence_count,
                    thesis, confidence_factors, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    trade_id, hypo.wallet, hypo.category, hypo.signal_type,
                    None, hypo.market_question, hypo.direction, hypo.entry_price,
                    hypo.alpha_score, hypo.wallet_wr, hypo.wallet_resolved,
                    hypo.convergence_count, hypo.thesis,
                    _json.dumps(hypo.confidence_factors),
                    int(time.time()),
                ),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("persist_hypothesis failed: %s", exc)
        return None


def get_recent_hypotheses(db_path: str, limit: int = 20) -> list[dict[str, Any]]:
    """For the API + dashboard."""
    try:
        ensure_schema(db_path)
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            cur = conn.execute(
                """SELECT id, trade_id, wallet, category, signal_type,
                          market_question, direction, entry_price, alpha_score,
                          wallet_wr, wallet_resolved, convergence_count,
                          thesis, confidence_factors, created_at
                   FROM trade_hypotheses
                   ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            import json as _json
            rows = []
            for r in cur.fetchall():
                d = dict(zip(cols, r))
                if d.get("confidence_factors"):
                    try:
                        d["confidence_factors"] = _json.loads(d["confidence_factors"])
                    except Exception:
                        d["confidence_factors"] = []
                rows.append(d)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("get_recent_hypotheses failed: %s", exc)
        return []
