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

from trading_platform.polymarket.db import connect_wallet_db

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
"""
# resolved_at index is created AFTER ALTER TABLE migration in ensure_schema()
# to avoid "no such column" on pre-migration DBs.

# Columns that may need ALTER TABLE on existing DBs (the table was
# created in a previous session without the resolution fields).
_RESOLUTION_COLS = [
    ("actual_outcome", "TEXT"),
    ("hypothesis_correct", "INTEGER"),
    ("realized_pnl", "REAL"),
    ("resolved_at", "INTEGER"),
]

# Decision-evaluation columns — expected distribution + execution context.
_DECISION_COLS = [
    ("expected_avg_win", "REAL"),
    ("expected_avg_loss", "REAL"),
    ("expected_profit_factor", "REAL"),
    ("expected_sharpe", "REAL"),
    ("expected_kelly", "REAL"),
    ("wallet_streak", "TEXT"),
    ("position_size_reason", "TEXT"),
    ("time_since_whale_trade", "INTEGER"),
    ("ev_after_spread", "REAL"),
    ("post_analysis_json", "TEXT"),
    ("gate_results_json", "TEXT"),
    ("market_spread_at_entry", "REAL"),
    ("drawdown_at_entry", "REAL"),
    ("size_multiplier", "REAL"),
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
    # Decision-evaluation fields (populated from wallet_alpha_scores distribution)
    expected_avg_win: float | None = None
    expected_avg_loss: float | None = None
    expected_profit_factor: float | None = None
    expected_sharpe: float | None = None
    expected_kelly: float | None = None
    wallet_streak: str | None = None
    position_size_reason: str | None = None
    time_since_whale_trade: int | None = None
    ev_after_spread: float | None = None
    gate_results_json: str | None = None
    market_spread_at_entry: float | None = None
    drawdown_at_entry: float | None = None
    size_multiplier: float | None = None

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
    conn = connect_wallet_db(db_path)
    try:
        conn.executescript(_SCHEMA)
        # Forward migration for the resolution columns added after the
        # table first shipped.
        existing = {r[1] for r in conn.execute("PRAGMA table_info(trade_hypotheses)").fetchall()}
        for col, typedef in _RESOLUTION_COLS + _DECISION_COLS:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE trade_hypotheses ADD COLUMN {col} {typedef}")
                except sqlite3.OperationalError:
                    pass
        # Create resolved_at index after the column is guaranteed to exist
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_th_resolved ON trade_hypotheses(resolved_at DESC)")
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
    """Mark a hypothesis as resolved and compute post-trade analysis.

    A hypothesis is **correct** iff the realized P&L is positive (i.e. the
    wallet's predicted direction matched the resolution outcome). Expired
    trades (markets vanished from Gamma) are recorded as `outcome='expired'`
    with `hypothesis_correct=NULL` since neither side was confirmed.
    """
    try:
        ensure_schema(db_path)
        conn = connect_wallet_db(db_path)
        try:
            if outcome == "expired":
                correct = None
            else:
                correct = 1 if (realized_pnl or 0) > 0 else 0

            # Fetch the hypothesis to compute post-trade analysis
            hypo = conn.execute(
                """SELECT expected_avg_win, expected_avg_loss, wallet_wr,
                          time_since_whale_trade, convergence_count,
                          ev_after_spread, expected_profit_factor
                   FROM trade_hypotheses
                   WHERE trade_id = ? AND actual_outcome IS NULL""",
                (trade_id,),
            ).fetchone()

            post_analysis = _compute_post_analysis(
                hypo, realized_pnl, correct, outcome,
            ) if hypo else None

            import json as _json
            cur = conn.execute(
                """UPDATE trade_hypotheses
                   SET actual_outcome = ?, hypothesis_correct = ?,
                       realized_pnl = ?, resolved_at = ?,
                       post_analysis_json = ?
                   WHERE trade_id = ? AND actual_outcome IS NULL""",
                (outcome, correct, realized_pnl, int(time.time()),
                 _json.dumps(post_analysis) if post_analysis else None,
                 trade_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("mark_resolved failed for trade_id=%s: %s", trade_id, exc)
        return False


def _compute_post_analysis(
    hypo_row: tuple,
    realized_pnl: float | None,
    correct: int | None,
    outcome: str,
) -> dict[str, Any]:
    """Compute what we learned from this trade.

    ``hypo_row`` is (expected_avg_win, expected_avg_loss, wallet_wr,
    time_since_whale_trade, convergence_count, ev_after_spread,
    expected_profit_factor).
    """
    exp_win, exp_loss, wr, lag, conv, ev_spread, pf = hypo_row
    analysis: dict[str, Any] = {}

    analysis["prediction_correct"] = correct == 1 if correct is not None else None

    # PnL prediction error (if we had expected values)
    if exp_win is not None and exp_loss is not None and wr is not None and realized_pnl is not None:
        expected_pnl = wr * exp_win + (1 - wr) * exp_loss
        analysis["expected_pnl"] = round(expected_pnl, 2)
        analysis["pnl_prediction_error"] = round(realized_pnl - expected_pnl, 2)
        analysis["pnl_direction"] = (
            "overestimate" if realized_pnl < expected_pnl else "underestimate"
        )

    # Detection lag assessment
    if lag is not None:
        analysis["detection_lag_seconds"] = lag
        analysis["lag_assessment"] = (
            "fast (<60s)" if lag < 60
            else "moderate (1-5min)" if lag < 300
            else "slow (>5min)"
        )

    # Convergence impact
    if conv is not None and conv >= 2:
        analysis["convergence_count"] = conv
        analysis["convergence_helped"] = correct == 1

    # EV after spread accuracy
    if ev_spread is not None and realized_pnl is not None:
        analysis["ev_after_spread"] = ev_spread

    return analysis


def get_calibration_data(db_path: str) -> dict[str, Any]:
    """Compute WR calibration, EV calibration, and lag analysis.

    Returns empty/placeholder data if fewer than 5 resolved hypotheses.
    """
    try:
        ensure_schema(db_path)
        conn = connect_wallet_db(db_path)
        try:
            resolved = conn.execute(
                """SELECT wallet_wr, ev_after_spread, realized_pnl,
                          actual_outcome, hypothesis_correct,
                          post_analysis_json, convergence_count
                   FROM trade_hypotheses
                   WHERE actual_outcome IS NOT NULL AND actual_outcome != 'expired'"""
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        resolved = []

    total = len(resolved)
    if total < 5:
        return {
            "sufficient_data": False,
            "resolved_count": total,
            "min_required": 5,
        }

    import json as _json

    # WR calibration: group by predicted WR bucket
    wr_buckets: dict[float, dict] = {}
    lags: list[int] = []
    pnl_errors: list[float] = []
    conv_wins = conv_total = 0

    for wr, ev, pnl, outcome, correct, post_json, conv in resolved:
        # WR bucket
        if wr is not None:
            bucket = round(wr * 10) / 10
            if bucket not in wr_buckets:
                wr_buckets[bucket] = {"predicted": bucket, "wins": 0, "total": 0}
            wr_buckets[bucket]["total"] += 1
            if correct == 1:
                wr_buckets[bucket]["wins"] += 1

        # Post-trade analysis
        if post_json:
            try:
                pa = _json.loads(post_json)
                if pa.get("detection_lag_seconds") is not None:
                    lags.append(pa["detection_lag_seconds"])
                if pa.get("pnl_prediction_error") is not None:
                    pnl_errors.append(pa["pnl_prediction_error"])
                if pa.get("convergence_count") and pa["convergence_count"] >= 2:
                    conv_total += 1
                    if pa.get("convergence_helped"):
                        conv_wins += 1
            except Exception:
                pass

    # Format WR calibration
    wr_cal = []
    for bucket in sorted(wr_buckets):
        b = wr_buckets[bucket]
        actual = b["wins"] / b["total"] if b["total"] > 0 else 0
        wr_cal.append({
            "predicted": bucket,
            "actual": round(actual, 3),
            "sample": b["total"],
            "calibration": (
                "well-calibrated" if abs(bucket - actual) < 0.15
                else "overconfident" if actual < bucket
                else "underconfident"
            ),
        })

    import statistics

    return {
        "sufficient_data": True,
        "resolved_count": total,
        "wr_calibration": wr_cal,
        "avg_pnl_prediction_error": round(statistics.mean(pnl_errors), 2) if pnl_errors else None,
        "avg_detection_lag": round(statistics.mean(lags)) if lags else None,
        "fast_detections": sum(1 for l in lags if l < 60) if lags else 0,
        "total_lag_samples": len(lags),
        "convergence_wr": round(conv_wins / conv_total, 3) if conv_total > 0 else None,
        "convergence_sample": conv_total,
    }


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
        conn = connect_wallet_db(db_path)
        try:
            # Alpha score + distribution stats (clean-data, per-category)
            row = conn.execute(
                """SELECT copyability, win_rate, win_rate_30d, resolved_trades,
                          avg_pnl, total_pnl, last_trade_at,
                          avg_win_pnl, avg_loss_pnl, profit_factor,
                          sharpe_ratio, kelly_fraction, streak_current,
                          streak_max_win, streak_max_loss
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
                # Distribution stats for decision evaluation
                h.expected_avg_win = row[7]
                h.expected_avg_loss = row[8]
                h.expected_profit_factor = row[9]
                h.expected_sharpe = row[10]
                h.expected_kelly = row[11]
                streak_cur = row[12]
                if streak_cur is not None:
                    h.wallet_streak = f"{'W' if streak_cur > 0 else 'L'}{abs(streak_cur)}"
                # EV after typical 2% spread
                if h.wallet_wr is not None and h.expected_avg_win and h.expected_avg_loss:
                    raw_ev = h.wallet_wr * h.expected_avg_win + (1 - h.wallet_wr) * h.expected_avg_loss
                    h.ev_after_spread = round(raw_ev - abs(raw_ev) * 0.02, 4)
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
        recent = []

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

    # Distribution data
    if h.expected_avg_win and h.expected_avg_loss:
        parts.append(
            f"Avg win: +${h.expected_avg_win:.0f}, avg loss: ${h.expected_avg_loss:.0f}."
        )
    if h.expected_profit_factor and h.expected_profit_factor < 99:
        parts.append(f"PF: {h.expected_profit_factor:.1f}x.")
    if h.expected_kelly and h.expected_kelly > 0:
        parts.append(f"Kelly: {h.expected_kelly * 100:.1f}%.")
    if h.wallet_streak:
        parts.append(f"Streak: {h.wallet_streak}.")
    if h.ev_after_spread is not None:
        parts.append(f"EV after spread: {h.ev_after_spread:+.2f}/trade.")

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
        conn = connect_wallet_db(db_path)
        try:
            cur = conn.execute(
                """INSERT INTO trade_hypotheses
                   (trade_id, wallet, category, signal_type, market_slug,
                    market_question, direction, entry_price, alpha_score,
                    wallet_wr, wallet_resolved, convergence_count,
                    thesis, confidence_factors, created_at,
                    expected_avg_win, expected_avg_loss, expected_profit_factor,
                    expected_sharpe, expected_kelly, wallet_streak,
                    position_size_reason, time_since_whale_trade, ev_after_spread,
                    gate_results_json, market_spread_at_entry,
                    drawdown_at_entry, size_multiplier)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    trade_id, hypo.wallet, hypo.category, hypo.signal_type,
                    None, hypo.market_question, hypo.direction, hypo.entry_price,
                    hypo.alpha_score, hypo.wallet_wr, hypo.wallet_resolved,
                    hypo.convergence_count, hypo.thesis,
                    _json.dumps(hypo.confidence_factors),
                    int(time.time()),
                    hypo.expected_avg_win, hypo.expected_avg_loss,
                    hypo.expected_profit_factor, hypo.expected_sharpe,
                    hypo.expected_kelly, hypo.wallet_streak,
                    hypo.position_size_reason, hypo.time_since_whale_trade,
                    hypo.ev_after_spread,
                    hypo.gate_results_json, hypo.market_spread_at_entry,
                    hypo.drawdown_at_entry, hypo.size_multiplier,
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
        conn = connect_wallet_db(db_path)
        try:
            cur = conn.execute(
                """SELECT id, trade_id, wallet, category, signal_type,
                          market_question, direction, entry_price, alpha_score,
                          wallet_wr, wallet_resolved, convergence_count,
                          thesis, confidence_factors, created_at,
                          actual_outcome, hypothesis_correct, resolved_at
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
