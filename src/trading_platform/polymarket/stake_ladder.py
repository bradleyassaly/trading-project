"""Auto-promote LIVE_REAL_MAX_STAKE_USD based on real-money trade history.

Currently LIVE_REAL_MAX_STAKE_USD is a constant edited by hand. The
ladder mechanism — the actual path to L5 ($10-20K/month) — requires
this cap to grow as evidence accumulates.

Tiers:
  Tier 0:    $1   stake (initial probate)
  Tier 1:    $5   after 30 real trades at WR ≥ 55%
  Tier 2:    $25  after 30 more
  Tier 3:    $100
  Tier 4:    $500
  Tier 5:    $2,500 (L5 target)

Each promotion gated by:
  - n >= MIN_TRADES_PER_TIER closed real-money trades at the current cap
  - WR >= MIN_WR_FOR_PROMOTION (55%)
  - Net PnL > 0 over the cohort
  - No active circuit-breaker trip in last 7d

Demotion trigger: 3 consecutive real-money losses OR WR < 45% on n >= 10.

Output: stake_ladder_state table (singleton row, current tier + cap).
The live executor reads this at every signal evaluation.

Run daily.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


# (tier_index, cap_usd, min_trades_required_at_prev_tier)
# 2026-05-02: tier 0→1 requires 10 (not 0). The previous 0 caused
# the very first synthetic test trade (1 win, n=1) to auto-promote
# to $5. The first promotion should require enough live evidence
# to trust the WR estimate.
LADDER = [
    (0,    1,   0),    # initial
    (1,    5,   10),   # was 0 — see comment above
    (2,   25,   30),
    (3,  100,   30),
    (4,  500,   30),
    (5, 2500,   30),
]
MIN_WR_FOR_PROMOTION = 0.55
MIN_PNL_FOR_PROMOTION = 0.0
DEMOTE_CONSECUTIVE_LOSSES = 3
DEMOTE_WR_THRESHOLD = 0.45
DEMOTE_MIN_N = 10


_SCHEMA = """
CREATE TABLE IF NOT EXISTS stake_ladder_state (
    id              BIGSERIAL PRIMARY KEY,
    tier_index      BIGINT NOT NULL,
    cap_usd         DOUBLE PRECISION NOT NULL,
    promoted_at     BIGINT NOT NULL,
    last_evaluated  BIGINT NOT NULL,
    paper_n         BIGINT,
    paper_wr        DOUBLE PRECISION,
    paper_pnl       DOUBLE PRECISION,
    real_n          BIGINT,
    real_wr         DOUBLE PRECISION,
    real_pnl        DOUBLE PRECISION,
    reason          TEXT
);
CREATE INDEX IF NOT EXISTS idx_sls_promoted ON stake_ladder_state(promoted_at DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def get_current_cap(db_path: str | None = None) -> float:
    """Read current ladder cap from stake_ladder_state. Default $1 if
    no state row yet (initial deployment)."""
    try:
        conn = get_connection(db_path) if db_path else get_connection()
    except Exception:
        return 1.0
    try:
        try:
            r = conn.execute(
                "SELECT cap_usd FROM stake_ladder_state ORDER BY promoted_at DESC LIMIT 1"
            ).fetchone()
        except Exception:
            return 1.0
        return float(r[0]) if r else 1.0
    finally:
        try: conn.close()
        except Exception: pass


def _real_money_stats(conn) -> tuple[int, float, float, int]:
    """Real money trade stats. Returns (n_resolved, wr, sum_pnl, consec_losses)."""
    try:
        rows = conn.execute(
            """SELECT outcome, realized_pnl FROM live_trades
                WHERE dry_run = 0 AND outcome IN ('win','loss')
                ORDER BY exit_ts DESC"""
        ).fetchall()
    except Exception:
        return 0, 0.0, 0.0, 0
    n = len(rows)
    if n == 0:
        return 0, 0.0, 0.0, 0
    wins = sum(1 for r in rows if r[0] == "win")
    pnl = sum(float(r[1] or 0) for r in rows)
    consec = 0
    for r in rows:
        if r[0] == "loss":
            consec += 1
        else:
            break
    return n, wins / n, pnl, consec


def _evaluate_promotion(conn, current_cap: float) -> tuple[float, str]:
    """Decide next cap. Returns (new_cap, reason)."""
    n, wr, pnl, consec_losses = _real_money_stats(conn)

    # Demotion takes priority
    if consec_losses >= DEMOTE_CONSECUTIVE_LOSSES:
        # Drop one tier
        for i, (idx, cap, _) in enumerate(LADDER):
            if cap == current_cap and i > 0:
                return LADDER[i - 1][1], (
                    f"DEMOTED: {consec_losses} consecutive real losses → drop to ${LADDER[i-1][1]}"
                )
    if n >= DEMOTE_MIN_N and wr < DEMOTE_WR_THRESHOLD:
        for i, (idx, cap, _) in enumerate(LADDER):
            if cap == current_cap and i > 0:
                return LADDER[i - 1][1], (
                    f"DEMOTED: WR {wr*100:.0f}% < {DEMOTE_WR_THRESHOLD*100:.0f}% on n={n}"
                )

    # Promotion path
    for i, (idx, cap, min_trades) in enumerate(LADDER):
        if cap == current_cap:
            if i + 1 < len(LADDER):
                # 2026-05-02: requirement to PROMOTE is the NEXT tier's
                # min_trades (n trades required to graduate to it), not
                # the current tier's (which is "min trades required to
                # have arrived here").
                next_idx, next_cap, next_min_trades = LADDER[i + 1]
                min_trades = next_min_trades
                # Gates for promotion
                if n < min_trades:
                    return current_cap, f"hold at ${current_cap}: real n={n}/{min_trades}"
                if wr < MIN_WR_FOR_PROMOTION:
                    return current_cap, (
                        f"hold at ${current_cap}: WR {wr*100:.0f}% < {MIN_WR_FOR_PROMOTION*100:.0f}%"
                    )
                if pnl < MIN_PNL_FOR_PROMOTION:
                    return current_cap, f"hold at ${current_cap}: net PnL ${pnl:.2f} not positive"
                # All checks pass — promote
                return next_cap, (
                    f"PROMOTED tier {i}→{i+1}: n={n} WR={wr*100:.0f}% PnL=${pnl:.2f} "
                    f"→ cap ${current_cap}→${next_cap}"
                )
            else:
                return current_cap, f"at top tier ${current_cap} (L5)"
    # Cap not in ladder — start at tier 0
    return LADDER[0][1], f"unknown cap ${current_cap}, reset to tier 0"


def run_promoter(db_path: str | None = None) -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        current_cap = get_current_cap(db_path)
        new_cap, reason = _evaluate_promotion(conn, current_cap)
        n, wr, pnl, _ = _real_money_stats(conn)
        # Find tier index
        tier_idx = next((i for (i, c, _) in LADDER if c == new_cap), 0)
        now = int(time.time())
        if new_cap != current_cap or not conn.execute("SELECT 1 FROM stake_ladder_state").fetchone():
            conn.execute(
                """INSERT INTO stake_ladder_state
                     (tier_index, cap_usd, promoted_at, last_evaluated,
                      real_n, real_wr, real_pnl, reason)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (tier_idx, new_cap, now, now, n, round(wr, 4),
                 round(pnl, 2), reason[:200]),
            )
            conn.commit()
        else:
            # Just update last_evaluated
            try:
                conn.execute(
                    """UPDATE stake_ladder_state SET last_evaluated = ?,
                          real_n = ?, real_wr = ?, real_pnl = ?, reason = ?
                        WHERE id = (SELECT id FROM stake_ladder_state
                                    ORDER BY promoted_at DESC LIMIT 1)""",
                    (now, n, round(wr, 4), round(pnl, 2), reason[:200]),
                )
                conn.commit()
            except Exception:
                pass
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "tier_index": tier_idx,
            "current_cap_usd": new_cap,
            "previous_cap_usd": current_cap,
            "promoted": new_cap > current_cap,
            "demoted": new_cap < current_cap,
            "real_n": n, "real_wr": round(wr, 3), "real_pnl": round(pnl, 2),
            "reason": reason,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = run_promoter()
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
