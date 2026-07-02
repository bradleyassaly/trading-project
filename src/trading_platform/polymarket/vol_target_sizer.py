"""Volatility-target stake sizer.

Replaces the flat base stake with one that shrinks/grows with realized
portfolio volatility. Goal: keep daily VaR(5%) at a target fraction of
equity regardless of regime.

Logic:
  realized_vol_7d = stdev(daily_pnl_last_7d)
  target_daily_loss = equity * VOL_TARGET_FRAC   (e.g. 0.03 = 3%)
  scale = clip(target_daily_loss / max(realized_vol_7d, MIN_VOL_FLOOR),
               MIN_SCALE, MAX_SCALE)
  adjusted_stake = base_stake * scale

Stake is bounded [$1, $25] by the kill_switch ladder; this sits BETWEEN
the base/ladder cap and the multiplier — same way signal_sizing_multipliers
is layered today.

Env toggles:
  VOL_TARGET_FRAC=0.03         # target daily loss as % equity
  VOL_TARGET_MIN_SCALE=0.5     # never shrink below 50%
  VOL_TARGET_MAX_SCALE=1.5     # never grow above 150%
  ENABLE_VOL_TARGET=0          # shadow mode: log only, don't apply

Read by polymarket_live_executor at sizing time; in shadow mode the
return is logged but ignored.
"""
from __future__ import annotations

import logging
import math
import os
import statistics
import time

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

VOL_TARGET_FRAC = float(os.environ.get("VOL_TARGET_FRAC", "0.03"))
VOL_TARGET_MIN_SCALE = float(os.environ.get("VOL_TARGET_MIN_SCALE", "0.5"))
VOL_TARGET_MAX_SCALE = float(os.environ.get("VOL_TARGET_MAX_SCALE", "1.5"))
MIN_VOL_FLOOR = 1.0  # $1/day floor — prevents divide-by-tiny
ENABLE_VOL_TARGET = os.environ.get("ENABLE_VOL_TARGET", "0").lower() in ("1", "true", "yes")


def _equity(conn) -> float:
    row = conn.execute(
        "SELECT total_equity FROM live_equity_snapshots ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    return float(row[0] or 0) if row else 0.0


def _realized_vol_7d(conn) -> float | None:
    cutoff = int(time.time()) - 7 * 86400
    rows = conn.execute(
        """
        SELECT date_trunc('day', to_timestamp(exit_ts))::date AS day,
               SUM(realized_pnl) AS pnl
          FROM live_trades
         WHERE dry_run = 0 AND realized_pnl IS NOT NULL
           AND exit_ts >= %s
         GROUP BY day
        """,
        (cutoff,),
    ).fetchall()
    pnls = [float(r[1] or 0) for r in rows]
    if len(pnls) < 3:
        return None
    return statistics.stdev(pnls)


def scale_for_today() -> dict[str, float | None]:
    """Compute the size multiplier for the current portfolio state.

    Returns a dict with the scale, the inputs, and an `applied` flag —
    True if ENABLE_VOL_TARGET, else it's shadow-only.
    """
    conn = get_connection()
    try:
        eq = _equity(conn)
        vol = _realized_vol_7d(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if eq <= 0 or vol is None:
        return {
            "scale": 1.0, "equity": eq, "realized_vol_7d": vol,
            "target_daily_loss": None, "applied": False,
            "reason": "insufficient data",
        }

    target = eq * VOL_TARGET_FRAC
    raw_scale = target / max(vol, MIN_VOL_FLOOR)
    scale = max(VOL_TARGET_MIN_SCALE, min(VOL_TARGET_MAX_SCALE, raw_scale))
    return {
        "scale": round(scale, 4),
        "equity": round(eq, 2),
        "realized_vol_7d": round(vol, 2),
        "target_daily_loss": round(target, 2),
        "raw_scale": round(raw_scale, 4),
        "applied": ENABLE_VOL_TARGET,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    r = scale_for_today()
    print("=== Vol-target sizer ===")
    print(f"  equity:           ${r.get('equity')}")
    print(f"  realized_vol_7d:  ${r.get('realized_vol_7d')}")
    print(f"  target_daily_loss: ${r.get('target_daily_loss')}")
    print(f"  raw_scale:        {r.get('raw_scale')}")
    print(f"  applied_scale:    {r.get('scale')}")
    print(f"  applied:          {r.get('applied')}")


if __name__ == "__main__":
    main()
