"""Auto-tier promotion based on paper-trade evidence.

The SIGNAL_CALIBRATION_TIER dict is currently a static constant edited
by hand. This makes the system slow to react when:
  - A new signal accumulates positive paper EV → should be promoted to A
  - An existing tier-A signal regresses → should be demoted to B/C
  - A tier-C signal proves itself → should graduate

This module reads polymarket_paper_trades + signal_backtest_results
and writes tier verdicts to signal_tier_overrides. The paper executor
reads this table at trade-time, falling back to the hardcoded dict
when no override exists.

Tier rules (paper-evidence + backtest-evidence consensus):
  A: paper n>=20 + paper EV >= 0.05 + backtest EV >= 0.05
  B: (paper n>=10 + paper EV >= 0.0) OR (backtest EV >= 0.05 + paper n<20)
  C: paper EV < 0 OR backtest EV < 0
  unknown: no data either way → fall through to static dict

Run daily. Idempotent.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# 2026-07-02: raised 20 → 30 to match the standing constraint that no
# promotion happens on fewer than 30 resolved trades (ROADMAP_2026-04-24
# core constraint #2; false-discovery control across parallel signals).
PAPER_TIER_A_MIN_N = 30
PAPER_TIER_A_MIN_EV = 0.05
PAPER_TIER_B_MIN_N = 10
BACKTEST_TIER_A_MIN_EV = 0.05
BACKTEST_TIER_C_MAX_EV = -0.05  # below this in backtest = demote
LOOKBACK_DAYS = 30


_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_tier_overrides (
    signal_type      TEXT PRIMARY KEY,
    tier             TEXT NOT NULL,
    paper_n          BIGINT,
    paper_ev         DOUBLE PRECISION,
    backtest_n       BIGINT,
    backtest_ev      DOUBLE PRECISION,
    promoted_at      BIGINT NOT NULL,
    reason           TEXT
);
CREATE INDEX IF NOT EXISTS idx_sto_tier ON signal_tier_overrides(tier);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def _decide_tier(paper_n: int, paper_ev: float,
                 bt_n: int, bt_ev: float) -> tuple[str, str]:
    """Return (tier, human-readable reason). Tier ∈ {A, B, C, unknown}."""
    if bt_n >= 100 and bt_ev <= BACKTEST_TIER_C_MAX_EV:
        return "C", f"backtest n={bt_n}, EV={bt_ev*100:+.1f}% (chronically negative)"
    if paper_n >= PAPER_TIER_A_MIN_N and paper_ev >= PAPER_TIER_A_MIN_EV:
        if bt_n == 0 or bt_ev >= 0:
            return "A", f"paper n={paper_n} EV={paper_ev*100:+.1f}%, backtest agrees"
    if bt_n >= 50 and bt_ev >= BACKTEST_TIER_A_MIN_EV and paper_n >= PAPER_TIER_B_MIN_N:
        return "A", f"backtest n={bt_n} EV={bt_ev*100:+.1f}% + paper n={paper_n} positive"
    if paper_n >= PAPER_TIER_B_MIN_N and paper_ev >= 0:
        return "B", f"paper n={paper_n} EV={paper_ev*100:+.1f}%"
    if bt_n >= 50 and bt_ev >= BACKTEST_TIER_A_MIN_EV:
        return "B", f"backtest n={bt_n} EV={bt_ev*100:+.1f}%, awaiting paper validation"
    if paper_n >= PAPER_TIER_B_MIN_N and paper_ev < 0:
        return "C", f"paper n={paper_n} EV={paper_ev*100:+.1f}%"
    return "unknown", f"insufficient data (paper n={paper_n}, backtest n={bt_n})"


def run_promoter() -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection()
    try:
        _ensure_schema(conn)
        # Pull all signals with any paper or backtest evidence
        cutoff = int(time.time()) - LOOKBACK_DAYS * 86400
        paper_rows = {
            r[0]: (int(r[1] or 0), float(r[2] or 0))
            for r in conn.execute(
                """SELECT signal_type, COUNT(*) AS n,
                          AVG(realized_pnl / NULLIF(size_usd, 0)) AS avg_ev
                     FROM polymarket_paper_trades
                    WHERE archived = 0 AND exit_ts IS NOT NULL
                      AND entry_ts > ?
                      AND outcome IN ('win','loss')
                      AND realized_pnl IS NOT NULL AND size_usd > 0
                    GROUP BY signal_type""",
                (cutoff,),
            ).fetchall()
        }
        bt_rows = {
            r[0]: (int(r[1] or 0), float(r[2]) if r[2] is not None else 0.0)
            for r in conn.execute(
                """SELECT signal_type, resolved, ev FROM signal_backtest_results sb
                    WHERE run_ts = (SELECT MAX(run_ts) FROM signal_backtest_results
                                    WHERE signal_type = sb.signal_type)
                      AND resolved IS NOT NULL"""
            ).fetchall()
        }
        all_signals = set(paper_rows) | set(bt_rows)
        n_promoted = 0
        n_demoted = 0
        verdicts = []
        now = int(time.time())
        for sig in all_signals:
            paper_n, paper_ev = paper_rows.get(sig, (0, 0.0))
            bt_n, bt_ev = bt_rows.get(sig, (0, 0.0))
            tier, reason = _decide_tier(paper_n, paper_ev, bt_n, bt_ev)
            verdicts.append({
                "signal": sig, "tier": tier, "reason": reason,
                "paper_n": paper_n, "paper_ev": round(paper_ev, 4),
                "backtest_n": bt_n, "backtest_ev": round(bt_ev, 4),
            })
            # Read previous override
            prev = conn.execute(
                "SELECT tier FROM signal_tier_overrides WHERE signal_type = ?",
                (sig,),
            ).fetchone()
            try:
                conn.execute(
                    """INSERT INTO signal_tier_overrides
                         (signal_type, tier, paper_n, paper_ev,
                          backtest_n, backtest_ev, promoted_at, reason)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type) DO UPDATE SET
                         tier = EXCLUDED.tier,
                         paper_n = EXCLUDED.paper_n,
                         paper_ev = EXCLUDED.paper_ev,
                         backtest_n = EXCLUDED.backtest_n,
                         backtest_ev = EXCLUDED.backtest_ev,
                         promoted_at = EXCLUDED.promoted_at,
                         reason = EXCLUDED.reason""",
                    (sig, tier, paper_n, round(paper_ev, 4),
                     bt_n, round(bt_ev, 4), now, reason[:200]),
                )
                if prev is None or prev[0] != tier:
                    if prev is None:
                        n_promoted += 1
                    elif tier > (prev[0] or "Z"):  # A > B > C alphabetically
                        n_demoted += 1
                    else:
                        n_promoted += 1
            except Exception as exc:
                logger.debug("tier upsert failed %s: %s", sig, exc)
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "signals_evaluated": len(all_signals),
            "tier_changes": n_promoted + n_demoted,
            "verdicts": verdicts,
        }
    finally:
        try: conn.close()
        except Exception: pass


def get_tier_override(signal_type: str, db_path: str | None = None) -> str | None:
    """Read the runtime tier for a signal (or None to fall back to static dict)."""
    try:
        conn = get_connection(db_path) if db_path else get_connection()
    except Exception:
        return None
    try:
        try:
            r = conn.execute(
                "SELECT tier FROM signal_tier_overrides WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
        except Exception:
            return None
        return r[0] if r else None
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_promoter()
    print(f"evaluated {out['signals_evaluated']} signals, {out['tier_changes']} tier changes in {out['elapsed_seconds']}s")
    for v in sorted(out["verdicts"], key=lambda x: (x["tier"], x["signal"])):
        print(f"  [{v['tier']}] {v['signal']:25s} pn={v['paper_n']:>3d} pev={v['paper_ev']*100:+.1f}% bn={v['backtest_n']:>4d} bev={v['backtest_ev']*100:+.1f}% — {v['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
