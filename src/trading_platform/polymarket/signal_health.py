"""Signal health metrics — IC decay + pairwise correlation pruning.

Layer 3 elite-comparison gap. Without this we have no defense against
signal decay (a signal that worked in March doesn't work in April) or
correlated redundancy (two signals firing on the same underlying event,
inflating apparent diversity).

Two metrics, both computed daily and surfaced in
`signal_health` table:

1. **Information Coefficient (IC)** per signal type per rolling window.
   IC = Spearman correlation between the signal's predicted edge
   (alpha_score) and the realized hypothesis_correct outcome over the
   window. Signals with IC < 0 in 14d trailing window are decay
   candidates.

2. **Pairwise correlation** of signal-fire times across markets. If
   two signal types fire on >70% of the same (market, side) tuples
   within 60s of each other, they're carrying the same alpha — the
   one with lower IC gets deprecated.

Both write a row per signal_type per run with `last_updated` so the
`/api/signal_health` endpoint serves the freshest snapshot.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_health (
    signal_type      TEXT PRIMARY KEY,
    n_resolved_30d   BIGINT,
    ic_30d           DOUBLE PRECISION,
    ic_14d           DOUBLE PRECISION,
    ic_trend         TEXT,
    correlated_with  TEXT,
    correlation_max  DOUBLE PRECISION,
    decay_flag       BIGINT,
    last_updated     BIGINT NOT NULL
);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass


def spearman_corr(xs: list[float], ys: list[float]) -> float:
    """Spearman rank-correlation, no numpy. Returns 0 on degenerate input."""
    n = len(xs)
    if n < 5 or len(ys) != n:
        return 0.0

    def _ranks(vs: list[float]) -> list[float]:
        sorted_idx = sorted(range(len(vs)), key=lambda i: vs[i])
        ranks = [0.0] * len(vs)
        i = 0
        while i < len(sorted_idx):
            j = i
            while j + 1 < len(sorted_idx) and vs[sorted_idx[j + 1]] == vs[sorted_idx[i]]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                ranks[sorted_idx[k]] = avg_rank
            i = j + 1
        return ranks

    rx = _ranks(xs)
    ry = _ranks(ys)
    mean_x = sum(rx) / n
    mean_y = sum(ry) / n
    cov = sum((rx[i] - mean_x) * (ry[i] - mean_y) for i in range(n))
    var_x = sum((r - mean_x) ** 2 for r in rx) ** 0.5
    var_y = sum((r - mean_y) ** 2 for r in ry) ** 0.5
    if var_x == 0 or var_y == 0:
        return 0.0
    return cov / (var_x * var_y)


def compute_ic(window_days: int, signal_type: str, conn) -> tuple[float, int]:
    """Spearman IC between alpha_score and hypothesis_correct over window.
    Returns (ic, n_resolved)."""
    cutoff = int(time.time()) - window_days * 86400
    try:
        rows = conn.execute(
            """SELECT alpha_score, hypothesis_correct
                 FROM trade_hypotheses
                WHERE signal_type = ?
                  AND resolved_at IS NOT NULL
                  AND resolved_at > ?
                  AND hypothesis_correct IN (0, 1)
                  AND alpha_score IS NOT NULL""",
            (signal_type, cutoff),
        ).fetchall()
    except Exception:
        return (0.0, 0)
    if len(rows) < 5:
        return (0.0, len(rows))
    xs = [float(r[0]) for r in rows]
    ys = [float(r[1]) for r in rows]
    return (spearman_corr(xs, ys), len(rows))


def compute_pairwise_corr(conn, window_days: int = 30) -> dict[tuple[str, str], float]:
    """For each pair of signal types, fraction of (cid, side) tuples
    where both fire within 60s of each other. Returns dict keyed by
    sorted (a, b)."""
    cutoff = int(time.time()) - window_days * 86400
    try:
        rows = conn.execute(
            """SELECT signal_type, condition_id, direction, fired_at
                 FROM signal_outcomes
                WHERE fired_at > ?
                  AND condition_id IS NOT NULL
                ORDER BY condition_id, direction, fired_at""",
            (cutoff,),
        ).fetchall()
    except Exception:
        return {}

    # Group by (cid, side) → list of (signal_type, ts)
    grouped: dict[tuple[str, str], list[tuple[str, int]]] = {}
    for sig_type, cid, side, fired in rows:
        key = (cid, side or "")
        grouped.setdefault(key, []).append((sig_type, int(fired or 0)))

    # For each group, count co-fires within 60s
    co_fires: dict[tuple[str, str], int] = {}
    appears: dict[str, int] = {}
    for events in grouped.values():
        seen_in_group = set()
        events.sort(key=lambda x: x[1])
        for i, (sig_a, ts_a) in enumerate(events):
            seen_in_group.add(sig_a)
            for j in range(i + 1, len(events)):
                sig_b, ts_b = events[j]
                if ts_b - ts_a > 60:
                    break
                if sig_a == sig_b:
                    continue
                key = tuple(sorted((sig_a, sig_b)))
                co_fires[key] = co_fires.get(key, 0) + 1
        for sig in seen_in_group:
            appears[sig] = appears.get(sig, 0) + 1

    out: dict[tuple[str, str], float] = {}
    for (a, b), n_co in co_fires.items():
        denom = max(appears.get(a, 0), appears.get(b, 0))
        if denom > 0:
            out[(a, b)] = n_co / denom
    return out


def run_pipeline(db_path: str | None = None) -> dict[str, Any]:
    """Compute IC + correlations and upsert into signal_health."""
    t0 = time.time()
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        # Distinct signal types resolved in last 30d
        sig_rows = conn.execute(
            """SELECT DISTINCT signal_type
                 FROM trade_hypotheses
                WHERE resolved_at IS NOT NULL
                  AND resolved_at > ?""",
            (int(time.time()) - 30 * 86400,),
        ).fetchall()
        sigs = [r[0] for r in sig_rows if r[0]]

        pairwise = compute_pairwise_corr(conn, window_days=30)
        # For each signal, find its highest-correlation peer
        max_corr_for: dict[str, tuple[str, float]] = {}
        for (a, b), c in pairwise.items():
            if c > max_corr_for.get(a, ("", 0))[1]:
                max_corr_for[a] = (b, c)
            if c > max_corr_for.get(b, ("", 0))[1]:
                max_corr_for[b] = (a, c)

        now = int(time.time())
        n_decay = 0
        n_corr_redundant = 0
        for sig in sigs:
            ic_30, n_30 = compute_ic(30, sig, conn)
            ic_14, _ = compute_ic(14, sig, conn)
            trend = "stable"
            if ic_30 != 0 and ic_14 != 0:
                if ic_14 < 0 and ic_30 > 0:
                    trend = "decaying"
                elif ic_14 > ic_30 + 0.1:
                    trend = "improving"
                elif ic_14 < ic_30 - 0.1:
                    trend = "weakening"
            decay = 1 if (ic_14 < 0 and n_30 >= 10) else 0
            if decay:
                n_decay += 1
            peer, peer_corr = max_corr_for.get(sig, ("", 0.0))
            if peer_corr >= 0.7:
                n_corr_redundant += 1
            try:
                conn.execute(
                    """INSERT INTO signal_health
                         (signal_type, n_resolved_30d, ic_30d, ic_14d,
                          ic_trend, correlated_with, correlation_max,
                          decay_flag, last_updated)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type) DO UPDATE SET
                         n_resolved_30d=EXCLUDED.n_resolved_30d,
                         ic_30d=EXCLUDED.ic_30d,
                         ic_14d=EXCLUDED.ic_14d,
                         ic_trend=EXCLUDED.ic_trend,
                         correlated_with=EXCLUDED.correlated_with,
                         correlation_max=EXCLUDED.correlation_max,
                         decay_flag=EXCLUDED.decay_flag,
                         last_updated=EXCLUDED.last_updated""",
                    (sig, n_30, round(ic_30, 4), round(ic_14, 4),
                     trend, peer if peer_corr >= 0.5 else None,
                     round(peer_corr, 3) if peer_corr else 0,
                     decay, now),
                )
            except Exception as exc:
                logger.debug("signal_health upsert %s failed: %s", sig, exc)
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "signals_processed": len(sigs),
            "decay_flagged": n_decay,
            "correlation_redundant": n_corr_redundant,
            "pairwise_corr_pairs": len(pairwise),
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
