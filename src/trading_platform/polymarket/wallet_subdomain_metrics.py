"""Per-(wallet, subdomain) z-score table.

Mirrors `wallet_category_zscore` but at sub-domain granularity. The
core insight (validated 2026-04-27): swisstony has $6.67M PM PnL and
189 tennis trades but is -$5,859 realized PnL on tennis. Category-
level scoring would have boosted them as a "sports specialist" when
tennis is actually a losing sport for them.

Per-subdomain z-score answers the precise question: "Is this wallet
significantly above-baseline in this exact sport / region?"

Computed daily after subcategory_classifier + pnl_reconstruction so
the input data is fresh.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_subdomain_zscore (
    wallet              TEXT NOT NULL,
    subdomain           TEXT NOT NULL,
    n_in_subdomain      BIGINT,
    sub_wr              DOUBLE PRECISION,
    lifetime_wr         DOUBLE PRECISION,
    z_score             DOUBLE PRECISION,
    is_specialist_z     BIGINT,
    sub_pnl             DOUBLE PRECISION,
    last_updated        BIGINT NOT NULL,
    PRIMARY KEY (wallet, subdomain)
);
CREATE INDEX IF NOT EXISTS idx_wsz_subdomain ON wallet_subdomain_zscore(subdomain, z_score DESC);
CREATE INDEX IF NOT EXISTS idx_wsz_specialist ON wallet_subdomain_zscore(is_specialist_z, z_score DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass


def _z(cat_wins: int, cat_n: int,
       lifetime_wins: int, lifetime_n: int) -> float:
    """Same z-score formula as wallet_category_zscore."""
    if cat_n < 5 or lifetime_n < 20:
        return 0.0
    p_lifetime = lifetime_wins / lifetime_n
    p_cat = cat_wins / cat_n
    var = p_lifetime * (1 - p_lifetime) / cat_n
    if var <= 0:
        return 0.0
    return (p_cat - p_lifetime) / math.sqrt(var)


def run_pipeline(db_path: str | None = None) -> dict[str, Any]:
    """Compute per-(wallet, subdomain) z-score from wallet_trades."""
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        try:
            rows = conn.execute(
                """SELECT wt.wallet, wt.subcategory,
                          COUNT(*) AS n,
                          SUM(CASE WHEN wt.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                          SUM(wt.pnl) AS sub_pnl,
                          (SELECT COUNT(*) FROM wallet_trades wt2
                            WHERE wt2.wallet = wt.wallet
                              AND wt2.pnl IS NOT NULL
                              AND wt2.pnl_reliable = 1) AS total_n,
                          (SELECT SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)
                             FROM wallet_trades wt3
                            WHERE wt3.wallet = wt.wallet
                              AND wt3.pnl IS NOT NULL
                              AND wt3.pnl_reliable = 1) AS total_wins
                     FROM wallet_trades wt
                    WHERE wt.subcategory IS NOT NULL
                      AND wt.pnl IS NOT NULL
                      AND wt.pnl_reliable = 1
                    GROUP BY wt.wallet, wt.subcategory
                   HAVING COUNT(*) >= 5""",
            ).fetchall()
        except Exception as exc:
            return {"error": str(exc)[:200], "elapsed_seconds": round(time.time()-t0, 1)}

        now = int(time.time())
        n_written = 0
        n_specialists = 0
        for r in rows:
            w, sub, n_c, w_c, sub_pnl, n_t, w_t = r
            if not sub:
                continue
            z = _z(int(w_c or 0), int(n_c), int(w_t or 0), int(n_t or 0))
            cat_wr = (w_c / n_c) if n_c else 0
            life_wr = (w_t / n_t) if n_t else 0
            spec = 1 if z >= 1.0 else 0
            if spec:
                n_specialists += 1
            try:
                conn.execute(
                    """INSERT INTO wallet_subdomain_zscore
                         (wallet, subdomain, n_in_subdomain, sub_wr,
                          lifetime_wr, z_score, is_specialist_z,
                          sub_pnl, last_updated)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT (wallet, subdomain) DO UPDATE SET
                         n_in_subdomain = EXCLUDED.n_in_subdomain,
                         sub_wr = EXCLUDED.sub_wr,
                         lifetime_wr = EXCLUDED.lifetime_wr,
                         z_score = EXCLUDED.z_score,
                         is_specialist_z = EXCLUDED.is_specialist_z,
                         sub_pnl = EXCLUDED.sub_pnl,
                         last_updated = EXCLUDED.last_updated""",
                    (w, sub, int(n_c), cat_wr, life_wr, z, spec,
                     float(sub_pnl or 0), now),
                )
                n_written += 1
            except Exception as exc:
                logger.debug("zscore upsert failed %s/%s: %s", w[:12], sub, exc)
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "rows_written": n_written,
            "specialists_z>=1.0": n_specialists,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
