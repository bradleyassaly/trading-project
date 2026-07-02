"""Auto-promote (signal_type, subdomain) tuples to elevated stake.

When a slice reaches n>=30 / WR>=60% / 95% Wilson lower bound > 50% /
positive PnL, write it to `stake_multiplier_overrides`. The paper executor reads this table at
signal-time and applies the multiplier on top of the static
STAKE_MULTIPLIERS dict — so the highest-validated slices get extra
stake without code change.

Today's data already has whale_entry × sports at 6/6 / +$889.69 —
will auto-promote to 1.5× when n hits 10 (or stays here if not yet).
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS stake_multiplier_overrides (
    signal_type      TEXT NOT NULL,
    subdomain        TEXT NOT NULL,
    multiplier       DOUBLE PRECISION NOT NULL,
    n_resolved       BIGINT,
    wr               DOUBLE PRECISION,
    pnl              DOUBLE PRECISION,
    promoted_at      BIGINT NOT NULL,
    expires_at       BIGINT,
    PRIMARY KEY (signal_type, subdomain)
);
CREATE INDEX IF NOT EXISTS idx_smo_promoted ON stake_multiplier_overrides(promoted_at DESC);
"""

# Promotion criteria.
# 2026-07-02: tightened for false-discovery control. The old bar
# (n>=10, WR>=60%) is hit by a fair coin ~38% of the time — and with
# hundreds of (signal × subdomain) slices tested in parallel, noise
# slices WILL qualify and then get sized up. New bar: n>=30 AND the
# 95% Wilson lower bound of WR must clear 0.50, i.e. the slice must be
# statistically distinguishable from a coin flip before stake rises.
MIN_RESOLVED = 30
MIN_WR = 0.60
MIN_PNL = 0.01
WILSON_Z = 1.96          # 95% one-sided lower bound
WILSON_LB_FLOOR = 0.50   # must beat a coin flip at the lower bound

# Demotion criteria (recompute daily; row drops if no longer qualifies)
DEMO_MIN_WR = 0.45  # below this, expire the override


def _wilson_lower_bound(wins: int, n: int, z: float = WILSON_Z) -> float:
    """95% Wilson score lower bound for a binomial proportion."""
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1 + z * z / n
    centre = phat + z * z / (2 * n)
    margin = z * ((phat * (1 - phat) + z * z / (4 * n)) / n) ** 0.5
    return (centre - margin) / denom


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception: pass


def _multiplier_for(wr: float, pnl: float, n: int) -> float:
    """Higher WR + bigger n → higher boost. Capped at 1.6x."""
    if wr < 0.50:
        return 1.0
    base = 1.0 + (wr - 0.50) * 1.5  # 50% → 1.0x, 70% → 1.3x
    n_factor = min(1.0, n / 30)     # full boost at n>=30
    boost = 1.0 + (base - 1.0) * n_factor
    return round(min(1.6, max(1.0, boost)), 2)


def run_promoter(db_path: str | None = None) -> dict[str, Any]:
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        cutoff = int(time.time()) - 30 * 86400
        try:
            rows = conn.execute(
                """SELECT pt.signal_type,
                          COALESCE(m.subcategory, pt.category) AS subdomain,
                          COUNT(*) AS n,
                          SUM(CASE WHEN pt.outcome='win' THEN 1 ELSE 0 END) AS wins,
                          SUM(pt.realized_pnl) AS pnl
                     FROM polymarket_paper_trades pt
                     LEFT JOIN markets m ON m.condition_id = pt.condition_id
                    WHERE pt.archived = 0
                      AND pt.exit_ts IS NOT NULL
                      AND pt.entry_ts > ?
                      AND pt.outcome IN ('win', 'loss')
                    GROUP BY pt.signal_type, COALESCE(m.subcategory, pt.category)
                   HAVING COUNT(*) >= ?""",
                (cutoff, MIN_RESOLVED),
            ).fetchall()
        except Exception as exc:
            return {"error": str(exc)[:200]}

        n_promoted = 0
        n_demoted = 0
        n_held = 0
        candidates: list[dict] = []
        now = int(time.time())
        expiry = now + 30 * 86400  # 30d freshness window

        # Pass 1: promote / refresh qualifying tuples
        promoted_keys: set[tuple[str, str]] = set()
        for r in rows:
            sig, sub, n, wins, pnl = r[0], r[1], int(r[2]), int(r[3] or 0), float(r[4] or 0)
            if not sig or not sub:
                continue
            wr = wins / n if n else 0
            wilson_lb = _wilson_lower_bound(wins, n)
            cand = {"signal_type": sig, "subdomain": sub,
                    "n": n, "wr": round(wr, 3), "pnl": round(pnl, 2),
                    "wilson_lb": round(wilson_lb, 3)}
            candidates.append(cand)
            if wr >= MIN_WR and pnl >= MIN_PNL and wilson_lb > WILSON_LB_FLOOR:
                mult = _multiplier_for(wr, pnl, n)
                try:
                    conn.execute(
                        """INSERT INTO stake_multiplier_overrides
                             (signal_type, subdomain, multiplier, n_resolved,
                              wr, pnl, promoted_at, expires_at)
                           VALUES (?,?,?,?,?,?,?,?)
                           ON CONFLICT (signal_type, subdomain) DO UPDATE SET
                             multiplier = EXCLUDED.multiplier,
                             n_resolved = EXCLUDED.n_resolved,
                             wr = EXCLUDED.wr,
                             pnl = EXCLUDED.pnl,
                             promoted_at = EXCLUDED.promoted_at,
                             expires_at = EXCLUDED.expires_at""",
                        (sig, sub, mult, n, round(wr, 4), round(pnl, 2),
                         now, expiry),
                    )
                    n_promoted += 1
                    promoted_keys.add((sig, sub))
                except Exception as exc:
                    logger.debug("promote failed %s/%s: %s", sig, sub, exc)

        # Pass 2: demote any existing override whose tuple no longer
        # qualifies. We DELETE rather than just expire so the gate
        # has a clean read.
        try:
            existing = conn.execute(
                "SELECT signal_type, subdomain, wr FROM stake_multiplier_overrides"
            ).fetchall()
            for r in existing:
                sig, sub, wr = r[0], r[1], float(r[2] or 0)
                if (sig, sub) not in promoted_keys and wr < DEMO_MIN_WR:
                    conn.execute(
                        "DELETE FROM stake_multiplier_overrides "
                        "WHERE signal_type = ? AND subdomain = ?",
                        (sig, sub),
                    )
                    n_demoted += 1
                elif (sig, sub) not in promoted_keys:
                    # Hold (still above demo threshold; data may be stale)
                    n_held += 1
        except Exception:
            pass

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "candidates_evaluated": len(candidates),
            "promoted_or_refreshed": n_promoted,
            "demoted": n_demoted,
            "held": n_held,
            "top_candidates": sorted(candidates, key=lambda c: -c["pnl"])[:5],
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_promoter())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
