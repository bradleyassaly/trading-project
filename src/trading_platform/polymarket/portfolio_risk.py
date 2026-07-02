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
    rows = conn.execute(
        """
        SELECT id, signal_type, direction, size_usd, question, condition_id
          FROM live_trades
         WHERE dry_run = 0 AND status IN ('matched', 'live')
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
        eq = _equity_snapshot(conn)
        positions = _open_positions(conn)

        # Topic clustering
        clusters = _cluster_by_topic(positions)
        cluster_flags = []
        for stem, ps in clusters.items():
            n = len(ps)
            cluster_exposure = sum(p["size_usd"] for p in ps)
            frac = cluster_exposure / eq["equity"] if eq["equity"] > 0 else 0
            if n > CLUSTER_MAX_POSITIONS or frac > CLUSTER_MAX_BANKROLL_FRAC:
                cluster_flags.append({
                    "stem": stem[:60], "n": n,
                    "exposure": round(cluster_exposure, 2),
                    "frac_bankroll": round(frac, 3),
                })

        # Direction net exposure
        buy_exp = sum(p["size_usd"] for p in positions if p["direction"] == "BUY")
        sell_exp = sum(p["size_usd"] for p in positions if p["direction"] == "SELL")
        net = sell_exp - buy_exp

        # VaR
        var5 = _historical_var(conn, eq["equity"])
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
