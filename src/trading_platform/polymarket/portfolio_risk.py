"""Portfolio-level risk monitor.

Three checks:
  1. Topic concentration — group open positions by topic stem; flag clusters
     with > 2 positions or > 20% of bankroll in one cluster.
  2. Direction concentration — net SELL vs BUY exposure.
  3. VaR(5%) — simple historical: 5th-percentile single-day P&L from
     last 30d, scaled by current open-position count.

All read-only; writes a summary to logs and an optional table if a
risk_alert needs to fire. Designed to be invoked from daily_system_review
and from the live executor's pre-trade gate.
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import defaultdict
from typing import Any

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.polymarket_live_executor import _topic_stem

logger = logging.getLogger(__name__)

VAR_LOOKBACK_DAYS = 30
VAR_QUANTILE = 0.05
CLUSTER_MAX_POSITIONS = 3
CLUSTER_MAX_BANKROLL_FRAC = 0.20

# R2 pre-trade cluster gate knobs. The 60s TTL is mandatory — the fast lane
# is 2-4s and a per-signal assess() (35ms warm, 3 queries) is a regression.
CLUSTER_GATE_TTL_SEC = float(os.environ.get("CLUSTER_GATE_TTL_SEC", "60"))
CLUSTER_GATE_STALE_MAX_SEC = float(os.environ.get("CLUSTER_GATE_STALE_MAX_SEC", "300"))
# Below this equity the snapshot is presumed broken (stale-balance-API
# defense: the CLOB balance froze at $5.43 for 20 runs on 2026-05) —
# degrade rather than block everything against a phantom denominator.
CLUSTER_GATE_EQUITY_FLOOR = float(os.environ.get("CLUSTER_GATE_EQUITY_FLOOR", "20"))
CLUSTER_GATE_ENFORCE = os.environ.get("CLUSTER_GATE_ENFORCE", "1").lower() in ("1", "true", "yes")


def _equity_snapshot(conn) -> dict[str, float]:
    """Return current equity components — drives concentration thresholds."""
    row = conn.execute(
        """
        SELECT usdc_balance, total_equity, open_cost_basis, open_count
          FROM live_equity_snapshots ORDER BY ts DESC LIMIT 1
        """
    ).fetchone()
    if not row:
        return {"usdc": 0.0, "equity": 0.0, "cost": 0.0, "n_open": 0}
    return {
        "usdc": float(row[0] or 0),
        "equity": float(row[1] or 0),
        "cost": float(row[2] or 0),
        "n_open": int(row[3] or 0),
    }


def _open_positions(conn) -> list[dict[str, Any]]:
    # R2: widened to include 'submitted' — the EXECUTOR's own open-position
    # definition (dedup gate). A just-submitted order is committed capital;
    # excluding it let a burst of same-cluster signals under-count exposure.
    # assess() and check_cluster_cap share this helper so the two
    # definitions can never drift again.
    rows = conn.execute(
        """
        SELECT id, signal_type, direction, size_usd, question, condition_id
          FROM live_trades
         WHERE dry_run = 0 AND status IN ('submitted', 'matched', 'live')
           AND realized_pnl IS NULL
        """
    ).fetchall()
    return [
        {
            "id": r[0], "signal_type": r[1], "direction": r[2],
            "size_usd": float(r[3] or 0), "question": r[4] or "",
            "condition_id": r[5],
        }
        for r in rows
    ]


def _cluster_snapshot(conn) -> tuple[dict[str, dict], dict]:
    """({stem: {n, exposure, buy_exposure, sell_exposure}}, equity_dict).

    Exposure counts BOTH directions — BUY 'Musk 200-219' and SELL 'Musk
    240+' are the same directional bet on one event, not a hedge (the 7/6
    lesson: one game held 6 correlated positions)."""
    eq = _equity_snapshot(conn)
    clusters: dict[str, dict] = {}
    for p in _open_positions(conn):
        stem = _topic_stem(p.get("question")) or "unclustered"
        c = clusters.setdefault(stem, {"n": 0, "exposure": 0.0,
                                       "buy_exposure": 0.0, "sell_exposure": 0.0})
        c["n"] += 1
        c["exposure"] += p["size_usd"]
        if p["direction"] == "BUY":
            c["buy_exposure"] += p["size_usd"]
        else:
            c["sell_exposure"] += p["size_usd"]
    return clusters, eq


# ── R2 pre-trade cluster gate (cached, marginal-delta, fail-safe) ─────────
_CACHE: dict[str, Any] = {"ts": 0.0, "clusters": None, "equity": None}
_CACHE_LOCK = threading.Lock()


def invalidate_cluster_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.update({"ts": 0.0, "clusters": None, "equity": None})


def _refresh_cache(db_path: str | None) -> None:
    """Refresh under lock iff TTL expired; on error keep last-good."""
    with _CACHE_LOCK:
        if time.time() - _CACHE["ts"] <= CLUSTER_GATE_TTL_SEC:
            return
        try:
            conn = get_connection(db_path) if db_path else get_connection()
            try:
                clusters, eq = _cluster_snapshot(conn)
            finally:
                try: conn.close()
                except Exception: pass
            _CACHE.update({"ts": time.time(), "clusters": clusters, "equity": eq})
            net = sum(c["sell_exposure"] for c in clusters.values()) \
                - sum(c["buy_exposure"] for c in clusters.values())
            logger.info(
                "[cluster-gate] refreshed: %d clusters, equity=$%.2f, "
                "net_short=$%.2f (shadow-only metric)",
                len(clusters), eq.get("equity", 0), net)
        except Exception as exc:
            logger.error("[cluster-gate] refresh failed (keeping last-good): %s", exc)


def check_cluster_cap(question: str | None, size_usd: float,
                      db_path: str | None = None) -> dict[str, Any]:
    """Would THIS trade push its topic cluster past 20% of bankroll?

    Marginal-delta semantics: blocks the trade that would CREATE the breach
    — strictly stronger than flagging an existing breach after the fact.
    Circuit: BREACH → fail-CLOSED (pure dict math on cached data);
    refresh/equity problems → DEGRADED, fail-OPEN with degraded=True so the
    caller alerts loudly but trading is never halted by a broken snapshot
    (dedup/topic/event/N7 gates remain active).
    """
    stem = _topic_stem(question or "")
    if not stem or len(stem) < 8:  # mirrors the topic-cap skip
        return {"allowed": True, "degraded": False, "reason": "no_stem",
                "stem": stem}
    _refresh_cache(db_path)
    with _CACHE_LOCK:
        clusters, eq, ts = _CACHE["clusters"], _CACHE["equity"], _CACHE["ts"]
    age = time.time() - ts if ts else None
    if clusters is None or age is None or age > CLUSTER_GATE_STALE_MAX_SEC:
        return {"allowed": True, "degraded": True, "stem": stem,
                "reason": f"cluster_gate degraded: snapshot "
                          f"{'never loaded' if clusters is None else f'{age:.0f}s old'}"}
    equity = float((eq or {}).get("equity") or 0)
    if equity <= CLUSTER_GATE_EQUITY_FLOOR:
        return {"allowed": True, "degraded": True, "stem": stem,
                "reason": f"cluster_gate degraded: equity ${equity:.2f} <= "
                          f"floor ${CLUSTER_GATE_EQUITY_FLOOR:.0f} "
                          f"(stale-balance defense)"}
    c = clusters.get(stem, {"n": 0, "exposure": 0.0})
    projected = c["exposure"] + float(size_usd)
    frac = projected / equity
    out = {"stem": stem, "cluster_n": c["n"],
           "cluster_exposure": round(c["exposure"], 2),
           "projected_frac": round(frac, 4), "equity": round(equity, 2),
           "degraded": False}
    if frac > CLUSTER_MAX_BANKROLL_FRAC:
        out["allowed"] = False
        out["reason"] = (f"cluster_cap: ${c['exposure']:.2f}+${size_usd:.2f} "
                         f"= {frac:.1%} of ${equity:.2f} equity > "
                         f"{CLUSTER_MAX_BANKROLL_FRAC:.0%} on '{stem[:40]}'")
    else:
        out["allowed"] = True
        out["reason"] = f"ok {frac:.1%} of equity"
    return out


def record_open(question: str | None, size_usd: float) -> None:
    """After a successful submit: bump the cached cluster in place so
    back-to-back signals in one burst see each other's exposure without
    waiting out the TTL. The next refresh trues it up from the DB."""
    stem = _topic_stem(question or "")
    if not stem:
        return
    with _CACHE_LOCK:
        if _CACHE["clusters"] is None:
            return
        c = _CACHE["clusters"].setdefault(
            stem, {"n": 0, "exposure": 0.0,
                   "buy_exposure": 0.0, "sell_exposure": 0.0})
        c["n"] += 1
        c["exposure"] += float(size_usd)


def _cluster_by_topic(positions: list[dict]) -> dict[str, list[dict]]:
    clusters: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        stem = _topic_stem(p.get("question")) or "unclustered"
        clusters[stem].append(p)
    return dict(clusters)


def _historical_var(conn, equity: float) -> float | None:
    """5%-quantile of daily P&L over the last N days. Returns absolute $."""
    cutoff = int(time.time()) - VAR_LOOKBACK_DAYS * 86400
    rows = conn.execute(
        """
        SELECT date_trunc('day', to_timestamp(exit_ts)) AS day,
               SUM(realized_pnl) AS pnl
          FROM live_trades
         WHERE dry_run = 0 AND realized_pnl IS NOT NULL
           AND exit_ts >= %s
         GROUP BY day
        """,
        (cutoff,),
    ).fetchall()
    pnls = sorted(float(r[1] or 0) for r in rows)
    if len(pnls) < 5:
        return None
    idx = max(0, int(len(pnls) * VAR_QUANTILE))
    return pnls[idx]


def assess() -> dict[str, Any]:
    """Run all checks. Returns a flat dict with flags and metrics."""
    conn = get_connection()
    try:
        clusters_map, eq = _cluster_snapshot(conn)

        # Topic clustering (same helper as the R2 pre-trade gate — the two
        # open-position definitions can no longer drift)
        cluster_flags = []
        for stem, c in clusters_map.items():
            n = c["n"]
            cluster_exposure = c["exposure"]
            frac = cluster_exposure / eq["equity"] if eq["equity"] > 0 else 0
            if n > CLUSTER_MAX_POSITIONS or frac > CLUSTER_MAX_BANKROLL_FRAC:
                cluster_flags.append({
                    "stem": stem[:60], "n": n,
                    "exposure": round(cluster_exposure, 2),
                    "frac_bankroll": round(frac, 3),
                })

        # Direction net exposure
        buy_exp = sum(c["buy_exposure"] for c in clusters_map.values())
        sell_exp = sum(c["sell_exposure"] for c in clusters_map.values())
        net = sell_exp - buy_exp

        # VaR (PG-only date functions; degrade to None rather than fail the
        # whole assessment on other backends / transient errors)
        try:
            var5 = _historical_var(conn, eq["equity"])
        except Exception as exc:
            logger.debug("VaR query failed (degrading to None): %s", exc)
            var5 = None
        var_frac = abs(var5) / eq["equity"] if (var5 is not None and eq["equity"] > 0) else None

        result = {
            "ts": int(time.time()),
            "equity": eq["equity"],
            "n_open": eq["n_open"],
            "buy_exposure": round(buy_exp, 2),
            "sell_exposure": round(sell_exp, 2),
            "net_short": round(net, 2),
            "var5_dollars": round(var5, 2) if var5 is not None else None,
            "var5_frac": round(var_frac, 4) if var_frac is not None else None,
            "cluster_flags": cluster_flags,
        }
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    r = assess()
    print("=== Portfolio risk snapshot ===")
    print(f"  equity:      ${r['equity']:.2f}")
    print(f"  n_open:      {r['n_open']}")
    print(f"  buy_exp:     ${r['buy_exposure']:.2f}")
    print(f"  sell_exp:    ${r['sell_exposure']:.2f}  (net short ${r['net_short']:.2f})")
    if r["var5_dollars"] is not None:
        print(f"  VaR(5%, {VAR_LOOKBACK_DAYS}d): ${r['var5_dollars']:.2f}  ({r['var5_frac']*100:.1f}% of equity)")
    else:
        print(f"  VaR(5%): insufficient data")
    if r["cluster_flags"]:
        print(f"  cluster flags: {len(r['cluster_flags'])}")
        for c in r["cluster_flags"]:
            print(f"    [{c['n']} pos, ${c['exposure']:.2f}, {c['frac_bankroll']*100:.1f}% bankroll] {c['stem']}")
    else:
        print("  cluster flags: none")


if __name__ == "__main__":
    main()
