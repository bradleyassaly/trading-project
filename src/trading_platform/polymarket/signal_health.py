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


# Signals that can NEVER be auto-deprecated by correlation dedup (roadmap N9).
# The two edges with reconciled-positive history — a transient IC dip must not
# silently kill them (cf. the 2026-04-27 lesson: whale_entry's $889 alpha was
# destroyed by IC-alone retirement).
PROTECTED_SIGNALS = frozenset({"resolution_decay", "whale_entry_filtered"})

# Minimum IC-sample floor before a signal may be auto-deprecated (mirror the
# decay_flag n>=10 floor — never kill on thin data).
_DEDUP_MIN_N = 10
_DEDUP_CORR_THRESHOLD = 0.7

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
    auto_deprecated  BIGINT DEFAULT 0,
    deprecated_reason TEXT,
    last_updated     BIGINT NOT NULL
);
"""

# Idempotent ALTERs so the already-migrated Postgres prod table gains the N9
# columns (CREATE TABLE IF NOT EXISTS won't add them). PG supports ADD COLUMN
# IF NOT EXISTS; the sqlite test lane already has them from the fresh CREATE,
# and the try/except swallows its lack of IF-NOT-EXISTS-on-ADD-COLUMN.
_MIGRATIONS = [
    "ALTER TABLE signal_health ADD COLUMN IF NOT EXISTS auto_deprecated BIGINT DEFAULT 0",
    "ALTER TABLE signal_health ADD COLUMN IF NOT EXISTS deprecated_reason TEXT",
]


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass
    for stmt in _MIGRATIONS:
        try:
            conn.execute(stmt)
        except Exception:
            pass


def _corr_dedup_decision(sig, ic_sig, n_sig, peer, peer_corr, ic_peer,
                         peer_known: bool = True) -> tuple[int, str | None]:
    """N9 dedup decision: should ``sig`` be auto-deprecated against its top
    correlate? Returns (auto_deprecated, reason). ``sig`` is the victim iff
    the pair is correlated (>=0.7; note correlation_max can exceed 1.0, so no
    upper clamp), sig is NOT protected, has n>=10, and is strictly the
    lower-IC member (ties skipped to avoid run-to-run flip-flop). Protected
    signals and thin-data signals are never deprecated; the survivor keeps
    trading.
    """
    if (peer and peer_known and peer_corr >= _DEDUP_CORR_THRESHOLD
            and sig not in PROTECTED_SIGNALS and n_sig >= _DEDUP_MIN_N
            and ic_peer > ic_sig):
        return 1, (f"corr={peer_corr:.2f} with {peer}, "
                   f"IC {ic_sig:+.3f} < {ic_peer:+.3f}")
    return 0, None


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


# Signals we actively watch for decay — alert when IC30 first crosses below threshold.
_MONITORED_SIGNALS: frozenset[str] = frozenset({
    "specialist_entry",
    "whale_entry_filtered",
    "resolution_decay",
})
# Alert threshold: IC30 below this warrants a Telegram heads-up even before full decay.
_IC_WARN_THRESHOLD = 0.02


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

        # Snapshot previous state so we only alert on *transitions*, not every run.
        try:
            prev_rows = conn.execute(
                "SELECT signal_type, decay_flag, ic_30d FROM signal_health"
            ).fetchall()
            prev_decay: dict[str, int] = {r[0]: int(r[1] or 0) for r in prev_rows}
            prev_ic30: dict[str, float] = {r[0]: float(r[2] or 0) for r in prev_rows}
        except Exception:
            prev_decay, prev_ic30 = {}, {}

        now = int(time.time())
        n_decay = 0
        n_corr_redundant = 0
        n_auto_deprecated = 0
        newly_decayed: list[tuple[str, float, float, int]] = []   # (sig, ic30, ic14, n)
        ic_warnings: list[tuple[str, float, float, int]] = []     # monitored, IC30 crossed below threshold

        # N9 pre-pass: compute (ic_30, n_30) for every signal once so the
        # per-signal loop can compare a correlated PAIR's IC to pick the victim.
        ic_by_sig: dict[str, tuple[float, int]] = {}
        for sig in sigs:
            ic_by_sig[sig] = compute_ic(30, sig, conn)

        for sig in sigs:
            ic_30, n_30 = ic_by_sig[sig]
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
                # Alert only when transitioning from healthy → decayed (not on repeat runs).
                if prev_decay.get(sig, 0) == 0:
                    newly_decayed.append((sig, ic_30, ic_14, n_30))
            # Warn when a monitored signal's IC30 first crosses below the warn threshold.
            if (
                sig in _MONITORED_SIGNALS
                and ic_30 < _IC_WARN_THRESHOLD
                and prev_ic30.get(sig, 1.0) >= _IC_WARN_THRESHOLD
            ):
                ic_warnings.append((sig, ic_30, ic_14, n_30))
            peer, peer_corr = max_corr_for.get(sig, ("", 0.0))
            if peer_corr >= _DEDUP_CORR_THRESHOLD:
                n_corr_redundant += 1

            # N9: auto-deprecate the lower-IC member of a correlated pair
            # (redundant signals triple-count the same event into Kelly). The
            # decision is recomputed from scratch each run — a signal whose
            # correlate dropped below threshold, or whose IC recovered above
            # the survivor's, is resurrected (auto_deprecated reset to 0).
            _ic_peer = ic_by_sig.get(peer, (0.0, 0))[0] if peer else 0.0
            auto_dep, dep_reason = _corr_dedup_decision(
                sig, ic_30, n_30, peer, peer_corr, _ic_peer, peer in ic_by_sig)
            if auto_dep:
                n_auto_deprecated += 1
            try:
                conn.execute(
                    """INSERT INTO signal_health
                         (signal_type, n_resolved_30d, ic_30d, ic_14d,
                          ic_trend, correlated_with, correlation_max,
                          decay_flag, auto_deprecated, deprecated_reason,
                          last_updated)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT (signal_type) DO UPDATE SET
                         n_resolved_30d=EXCLUDED.n_resolved_30d,
                         ic_30d=EXCLUDED.ic_30d,
                         ic_14d=EXCLUDED.ic_14d,
                         ic_trend=EXCLUDED.ic_trend,
                         correlated_with=EXCLUDED.correlated_with,
                         correlation_max=EXCLUDED.correlation_max,
                         decay_flag=EXCLUDED.decay_flag,
                         auto_deprecated=EXCLUDED.auto_deprecated,
                         deprecated_reason=EXCLUDED.deprecated_reason,
                         last_updated=EXCLUDED.last_updated""",
                    (sig, n_30, round(ic_30, 4), round(ic_14, 4),
                     trend, peer if peer_corr >= 0.5 else None,
                     round(peer_corr, 3) if peer_corr else 0,
                     decay, auto_dep, dep_reason, now),
                )
            except Exception as exc:
                logger.debug("signal_health upsert %s failed: %s", sig, exc)
        conn.commit()

        # Telegram alert — fires only on first-time decay transitions or IC30 threshold crossings.
        if newly_decayed or ic_warnings:
            try:
                from dotenv import load_dotenv
                load_dotenv()
                from trading_platform.polymarket.telegram_alerts import get_alerter
                alerter = get_alerter()
                if alerter.enabled:
                    lines = ["⚠️ <b>Signal Health Alert</b>"]
                    if newly_decayed:
                        lines.append("\n<b>New decay detected (IC14&lt;0, n≥10):</b>")
                        for sig, ic30, ic14, n in newly_decayed:
                            lines.append(f"  • {sig}: IC30={ic30:+.3f} IC14={ic14:+.3f} n={n}")
                    if ic_warnings:
                        lines.append(f"\n<b>IC30 dropped below {_IC_WARN_THRESHOLD:.2f} (monitored signal):</b>")
                        for sig, ic30, ic14, n in ic_warnings:
                            lines.append(f"  • {sig}: IC30={ic30:+.3f} IC14={ic14:+.3f} n={n}")
                    alerter._send("\n".join(lines))
            except Exception as exc:
                logger.warning("signal_health Telegram alert failed: %s", exc)

        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "signals_processed": len(sigs),
            "decay_flagged": n_decay,
            "correlation_redundant": n_corr_redundant,
            "auto_deprecated": n_auto_deprecated,
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
