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
import os
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
CREATE TABLE IF NOT EXISTS slice_gate (
    signal_type      TEXT NOT NULL,
    category         TEXT NOT NULL,
    status           TEXT NOT NULL,
    n_resolved       BIGINT,
    wins             BIGINT,
    wr               DOUBLE PRECISION,
    avg_entry        DOUBLE PRECISION,
    wilson_lb        DOUBLE PRECISION,
    ev_lb            DOUBLE PRECISION,
    sum_pnl          DOUBLE PRECISION,
    reason           TEXT,
    computed_at      BIGINT NOT NULL,
    PRIMARY KEY (signal_type, category)
);
CREATE TABLE IF NOT EXISTS slice_gate_meta (
    id               SMALLINT PRIMARY KEY DEFAULT 1,
    last_run_at      BIGINT NOT NULL,
    candidates_tested BIGINT,
    n_demoted        BIGINT,
    n_killed         BIGINT
);
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

# Pass 4 (LIVE-truth overlay) window/floor. Live flow is ~1/20th of paper
# volume, so the window is wider (60d vs paper's 30d) to accumulate n; the
# floor matches the unprotected demote floor in _slice_gate_decision (the
# decision function's own n>=20/30 conjuncts still apply on top).
LIVE_GATE_WINDOW_DAYS = int(os.environ.get("SLICE_GATE_LIVE_WINDOW_DAYS", "60"))
LIVE_GATE_MIN_N = 20


def _wilson_lower_bound(wins: int, n: int, z: float = WILSON_Z) -> float:
    """95% Wilson score lower bound for a binomial proportion."""
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1 + z * z / n
    centre = phat + z * z / (2 * n)
    margin = z * ((phat * (1 - phat) + z * z / (4 * n)) / n) ** 0.5
    return (centre - margin) / denom


def _wilson_upper_bound(wins: int, n: int, z: float = WILSON_Z) -> float:
    """Symmetric Wilson upper bound — the DEMOTER's optimistic WR."""
    if n <= 0:
        return 1.0
    phat = wins / n
    denom = 1 + z * z / n
    centre = phat + z * z / (2 * n)
    margin = z * ((phat * (1 - phat) + z * z / (4 * n)) / n) ** 0.5
    return (centre + margin) / denom


def _slice_gate_decision(n: int, wins: int, avg_entry: float,
                         sum_pnl: float, protected: bool,
                         ) -> tuple[str | None, str]:
    """P5: pure demotion/kill decision for one (signal × subdomain) slice.

    KILL   — even the OPTIMISTIC (Wilson upper) win rate can't pay for the
             average entry price AND realized pnl is negative: confidently
             -EV, block paper bankroll + live.
    DEMOTE — hold-to-resolution EV lower bound < 0 AND pnl <= 0: block live
             capital only (paper keeps collecting; the slice can un-demote).
    The sum_pnl conjunct is mandatory: WR alone would falsely kill
    lottery-shaped slices (confluence_2plus × crypto: WR 22%, +$2110).
    Protected signals (the live edges) get a higher n floor.
    """
    if n <= 0 or not avg_entry or avg_entry <= 0:
        return (None, "no_data")
    wilson_lb = _wilson_lower_bound(wins, n)
    wilson_ub = _wilson_upper_bound(wins, n)
    ev_lb = wilson_lb / avg_entry - 1.0
    min_n = 30 if protected else 20
    if n >= 30 and wilson_ub < avg_entry and sum_pnl < 0:
        return ("killed",
                f"wilson_ub={wilson_ub:.3f} < avg_entry={avg_entry:.3f} "
                f"and pnl={sum_pnl:+.2f} at n={n}")
    if n >= min_n and ev_lb < 0 and sum_pnl <= 0:
        return ("demoted",
                f"ev_lb={ev_lb:+.3f} < 0 and pnl={sum_pnl:+.2f} at n={n}")
    return (None, f"ok ev_lb={ev_lb:+.3f} pnl={sum_pnl:+.2f} n={n}")


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
                          SUM(pt.realized_pnl) AS pnl,
                          AVG(pt.entry_price) AS avg_entry
                     FROM polymarket_paper_trades pt
                     LEFT JOIN markets m ON m.condition_id = pt.condition_id
                    WHERE pt.archived = 0
                      AND pt.exit_ts IS NOT NULL
                      AND pt.entry_ts > ?
                      AND pt.outcome IN ('win', 'loss')
                    GROUP BY pt.signal_type, COALESCE(m.subcategory, pt.category)
                   HAVING COUNT(*) >= 20""",
                (cutoff,),
            ).fetchall()
        except Exception as exc:
            return {"error": str(exc)[:200]}

        n_promoted = 0
        n_demoted = 0
        n_held = 0
        candidates: list[dict] = []
        now = int(time.time())
        expiry = now + 30 * 86400  # 30d freshness window

        # Pass 1: promote / refresh qualifying tuples (n >= MIN_RESOLVED;
        # the query's HAVING floor is lower because pass 3 demotes at n>=20)
        promoted_keys: set[tuple[str, str]] = set()
        for r in rows:
            sig, sub, n, wins, pnl = r[0], r[1], int(r[2]), int(r[3] or 0), float(r[4] or 0)
            avg_entry = float(r[5] or 0)
            if not sig or not sub:
                continue
            wr = wins / n if n else 0
            wilson_lb = _wilson_lower_bound(wins, n)
            cand = {"signal_type": sig, "subdomain": sub,
                    "n": n, "wins": wins, "wr": round(wr, 3),
                    "pnl": round(pnl, 2), "avg_entry": round(avg_entry, 4),
                    "wilson_lb": round(wilson_lb, 3)}
            candidates.append(cand)
            if (n >= MIN_RESOLVED and wr >= MIN_WR and pnl >= MIN_PNL
                    and wilson_lb > WILSON_LB_FLOOR):
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
        # qualifies. We DELETE rather than just expire so the gate has a
        # clean read. P5 fix: compare against the CURRENT window's WR (the
        # candidates just computed), not the wr stored at promote time — a
        # slice promoted at 60% that decayed to 30% was held forever.
        current_wr = {(c["signal_type"], c["subdomain"]): c["wr"]
                      for c in candidates}
        try:
            existing = conn.execute(
                "SELECT signal_type, subdomain, wr FROM stake_multiplier_overrides"
            ).fetchall()
            for r in existing:
                sig, sub, stored_wr = r[0], r[1], float(r[2] or 0)
                if (sig, sub) in promoted_keys:
                    continue
                wr_now = current_wr.get((sig, sub), stored_wr)
                if wr_now < DEMO_MIN_WR:
                    conn.execute(
                        "DELETE FROM stake_multiplier_overrides "
                        "WHERE signal_type = ? AND subdomain = ?",
                        (sig, sub),
                    )
                    n_demoted += 1
                else:
                    # Hold (still above demo threshold; data may be stale)
                    n_held += 1
        except Exception:
            pass

        # Pass 3 (P5): negative-slice gate. Full refresh — an EMPTY
        # slice_gate after a successful pass is a valid state, distinguished
        # from never-ran/stale by the meta beacon.
        n_gate_demoted = 0
        n_gate_killed = 0
        try:
            from trading_platform.polymarket.signal_health import PROTECTED_SIGNALS
        except Exception:
            PROTECTED_SIGNALS = frozenset()
        try:
            conn.execute("DELETE FROM slice_gate")
            for c in candidates:
                status, reason = _slice_gate_decision(
                    c["n"], c["wins"], c["avg_entry"], c["pnl"],
                    protected=c["signal_type"] in PROTECTED_SIGNALS)
                if status is None:
                    continue
                wilson_lb = c["wilson_lb"]
                ev_lb = (wilson_lb / c["avg_entry"] - 1.0) if c["avg_entry"] else None
                conn.execute(
                    """INSERT INTO slice_gate
                         (signal_type, category, status, n_resolved, wins,
                          wr, avg_entry, wilson_lb, ev_lb, sum_pnl, reason,
                          computed_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (c["signal_type"], c["subdomain"], status, c["n"],
                     c["wins"], c["wr"], c["avg_entry"], wilson_lb,
                     round(ev_lb, 4) if ev_lb is not None else None,
                     c["pnl"], reason, now),
                )
                if status == "killed":
                    n_gate_killed += 1
                else:
                    n_gate_demoted += 1
                # A demoted/killed slice must not keep a stake BOOST row.
                conn.execute(
                    "DELETE FROM stake_multiplier_overrides "
                    "WHERE signal_type = ? AND subdomain = ?",
                    (c["signal_type"], c["subdomain"]),
                )
        except Exception as exc:
            logger.warning("slice_gate pass failed: %s", exc)

        # Pass 4 (2026-07-27): LIVE-truth overlay. Pass 3 is computed from
        # polymarket_paper_trades — and paper fills are fantasy-priced (same
        # signal, same 30d window: resolution_decay paper +$7.33/trade vs
        # live −$0.48/trade). A paper-healthy slice therefore SHIELDS a
        # live-bleeding one from the gate — the exact 2026-07 state:
        # resolution_decay×sports showed paper +$11.4k while the live cohort
        # ran EV/$ −0.127 (post-band-fix n=29), and the paper-fed gate never
        # fired. Where a slice has enough LIVE closes to judge, the live
        # verdict REPLACES the paper verdict — in BOTH directions:
        #   * live-negative overrides paper-ok   → demote/kill a paper star
        #   * live-ok (n>=LIVE_GATE_MIN_N) overrides paper-negative → clear
        # Slices without sufficient live n keep the paper verdict: paper is
        # still the only evidence for never-promoted slices. Exact-key
        # replacement only; parent/child bridging stays in get_slice_gate.
        # Provenance rides in the reason ("LIVE:" prefix) — no schema change.
        n_live_eval = 0
        n_live_demoted = 0
        n_live_killed = 0
        n_live_cleared = 0
        try:
            live_cutoff = now - LIVE_GATE_WINDOW_DAYS * 86400
            live_rows = conn.execute(
                """SELECT lt.signal_type,
                          COALESCE(m.subcategory, lt.category, 'other') AS subdomain,
                          COUNT(*) AS n,
                          SUM(CASE WHEN lt.realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                          SUM(lt.realized_pnl) AS pnl,
                          AVG(lt.entry_price) AS avg_entry
                     FROM live_trades lt
                     LEFT JOIN markets m ON m.condition_id = lt.condition_id
                    WHERE lt.dry_run = 0
                      AND lt.status = 'matched'
                      AND lt.exit_ts IS NOT NULL
                      AND lt.exit_ts > ?
                      AND lt.realized_pnl IS NOT NULL
                    GROUP BY lt.signal_type,
                             COALESCE(m.subcategory, lt.category, 'other')
                   HAVING COUNT(*) >= ?""",
                (live_cutoff, LIVE_GATE_MIN_N),
            ).fetchall()
            for r in live_rows:
                sig, sub = r[0], r[1]
                n, wins = int(r[2]), int(r[3] or 0)
                pnl = float(r[4] or 0.0)
                avg_entry = float(r[5] or 0.0)
                if not sig or not sub:
                    continue
                n_live_eval += 1
                status, reason = _slice_gate_decision(
                    n, wins, avg_entry, pnl,
                    protected=sig in PROTECTED_SIGNALS)
                prior = conn.execute(
                    "SELECT status FROM slice_gate "
                    "WHERE signal_type = ? AND category = ?",
                    (sig, sub),
                ).fetchone()
                conn.execute(
                    "DELETE FROM slice_gate "
                    "WHERE signal_type = ? AND category = ?",
                    (sig, sub),
                )
                if status is None:
                    if prior:
                        n_live_cleared += 1
                        logger.info(
                            "[slice_gate][LIVE] cleared paper %s for %s × %s (%s)",
                            prior[0], sig, sub, reason)
                    continue
                wilson_lb = _wilson_lower_bound(wins, n)
                ev_lb = (wilson_lb / avg_entry - 1.0) if avg_entry else None
                conn.execute(
                    """INSERT INTO slice_gate
                         (signal_type, category, status, n_resolved, wins,
                          wr, avg_entry, wilson_lb, ev_lb, sum_pnl, reason,
                          computed_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (sig, sub, status, n, wins,
                     round(wins / n, 4) if n else 0.0,
                     round(avg_entry, 4), round(wilson_lb, 4),
                     round(ev_lb, 4) if ev_lb is not None else None,
                     round(pnl, 2), "LIVE: " + reason, now),
                )
                if status == "killed":
                    n_live_killed += 1
                else:
                    n_live_demoted += 1
                logger.info("[slice_gate][LIVE] %s %s × %s — %s",
                            status, sig, sub, reason)
                # A live-negative slice must not keep a stake BOOST row
                # either (paper stats may still look promotable).
                conn.execute(
                    "DELETE FROM stake_multiplier_overrides "
                    "WHERE signal_type = ? AND subdomain = ?",
                    (sig, sub),
                )
        except Exception as exc:
            logger.warning("slice_gate LIVE overlay failed: %s", exc)

        # Meta beacon last, covering both passes — freshness must not be
        # stamped if the paper pass wrote rows but then crashed pre-meta.
        try:
            conn.execute(
                """INSERT INTO slice_gate_meta
                     (id, last_run_at, candidates_tested, n_demoted, n_killed)
                   VALUES (1, ?, ?, ?, ?)
                   ON CONFLICT (id) DO UPDATE SET
                     last_run_at = EXCLUDED.last_run_at,
                     candidates_tested = EXCLUDED.candidates_tested,
                     n_demoted = EXCLUDED.n_demoted,
                     n_killed = EXCLUDED.n_killed""",
                (now, len(candidates) + n_live_eval,
                 n_gate_demoted + n_live_demoted,
                 n_gate_killed + n_live_killed),
            )
        except Exception as exc:
            logger.warning("slice_gate meta write failed: %s", exc)

        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "candidates_evaluated": len(candidates),
            "promoted_or_refreshed": n_promoted,
            "demoted": n_demoted,
            "held": n_held,
            "gate_demoted": n_gate_demoted,
            "gate_killed": n_gate_killed,
            "live_evaluated": n_live_eval,
            "live_demoted": n_live_demoted,
            "live_killed": n_live_killed,
            "live_cleared": n_live_cleared,
            "top_candidates": sorted(candidates, key=lambda c: -c["pnl"])[:5],
        }
    finally:
        try: conn.close()
        except Exception: pass


# P5 reader cache: (loaded_at, {(sig, cat): status}, meta_fresh). The live
# fast lane is 2-4s — per-signal SQL would be a real regression.
_GATE_CACHE: dict[str, Any] = {"at": 0.0, "rows": {}, "fresh": False}
_GATE_CACHE_TTL = 60.0
_GATE_MAX_AGE_SEC = 48 * 3600


def get_slice_gate(signal_type: str, category: str | None,
                   db_path: str | None = None,
                   max_age_sec: int = _GATE_MAX_AGE_SEC,
                   ) -> tuple[str | None, bool]:
    """P5: (status, fresh) for a (signal × category) slice.

    fresh=False ⇒ the table is stale/never-ran — callers MUST fall back to
    their existing hardcoded behavior (fail-safe, not fail-closed).

    Category matching bridges the two granularities in play: the promoter
    keys on markets.subcategory ('science/weather') while executor `cat` is
    classifier output ('science'). Match exact, parent-prefix, or any child.
    """
    now = time.time()
    if now - _GATE_CACHE["at"] > _GATE_CACHE_TTL:
        rows: dict[tuple[str, str], str] = {}
        fresh = False
        try:
            conn = get_connection(db_path) if db_path else get_connection()
            try:
                meta = conn.execute(
                    "SELECT last_run_at FROM slice_gate_meta WHERE id = 1"
                ).fetchone()
                if meta and meta[0] and (now - int(meta[0])) <= max_age_sec:
                    fresh = True
                    for r in conn.execute(
                            "SELECT signal_type, category, status FROM slice_gate"
                    ).fetchall():
                        rows[(r[0], r[1])] = r[2]
            finally:
                try: conn.close()
                except Exception: pass
        except Exception:
            rows, fresh = {}, False
        _GATE_CACHE.update({"at": now, "rows": rows, "fresh": fresh})

    if not _GATE_CACHE["fresh"]:
        return (None, False)
    rows = _GATE_CACHE["rows"]
    cat = (category or "other").lower()
    hit = rows.get((signal_type, cat))
    if hit:
        return (hit, True)
    # executor cat may be the PARENT of a promoter subdomain ('science' vs
    # 'science/weather'): any gated child blocks the parent...
    for (sig, c), status in rows.items():
        if sig == signal_type and c.startswith(cat + "/"):
            return (status, True)
    # ...and a gated parent covers a child lookup.
    if "/" in cat:
        hit = rows.get((signal_type, cat.split("/")[0]))
        if hit:
            return (hit, True)
    return (None, True)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_promoter())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
