"""Wallet earliness score — simple recency-weighted alpha boost.

Definition (2026-04-24):
    earliness(wallet, category) = win_rate_7d - win_rate_lifetime

A positive score = wallet is improving in this category. A wallet with
70% WR in the last 7 days but a 50% lifetime WR has earliness +0.20
and gets a stake/confidence boost. Symmetric: a wallet whose recent WR
is below lifetime gets a negative score and is de-emphasized.

Why it matters: lifetime WR is survivorship-biased. Wallets that were
fast-and-correct in 2025 may not be still. Tracking the trend separates
"this used to be smart money" from "this currently is smart money."

Surface this via `get_earliness_boost(wallet, category)` which returns
a multiplier in [0.7, 1.3] — 1.0 means neutral, capped both directions
to keep one signal from dominating sizing.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

# Tunables
LOOKBACK_RECENT_DAYS = 7
MIN_RECENT_TRADES = 5      # below this we have no signal — return neutral 1.0
MAX_BOOST = 1.3
MIN_BOOST = 0.7


def compute_earliness(
    wallet: str, category: str, db_path: str | None = None,
) -> dict[str, Any] | None:
    """Return EWMA-weighted {recent_wr, lifetime_wr, delta, n_recent, n_lifetime}
    for the wallet/category, or None when insufficient data.

    2026-04-25: switched from a hard 7d-window snapshot to an EWMA over
    the full trade history. The 7d cliff was producing noisy/None for
    wallets that traded 8d ago vs 6d ago — same wallet, same skill,
    very different gate decision. EWMA with half-life of 7d makes the
    estimate continuous and robust to bursty traders.
    """
    EWMA_HALF_LIFE_DAYS = 7.0
    decay_per_day = 0.5 ** (1.0 / EWMA_HALF_LIFE_DAYS)
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        try:
            rows = conn.execute(
                """SELECT timestamp AS ts,
                          CASE WHEN COALESCE(pnl, 0) > 0 THEN 1 ELSE 0 END AS won
                     FROM wallet_trades
                    WHERE wallet = %s AND category = %s
                      AND pnl IS NOT NULL AND pnl_reliable = 1
                    ORDER BY timestamp DESC""",
                (wallet.lower(), category.lower()),
            ).fetchall()
        except Exception:
            rows = []
    finally:
        try: conn.close()
        except Exception: pass
    if not rows:
        return None
    n_total = len(rows)
    if n_total < MIN_RECENT_TRADES:
        return None
    now = int(time.time())
    sum_w = 0.0
    sum_w_won = 0.0
    n_lifetime = 0
    n_lifetime_won = 0
    for ts, won in rows:
        try:
            ts_int = int(ts)
        except (TypeError, ValueError):
            continue
        days_ago = max(0.0, (now - ts_int) / 86400.0)
        weight = decay_per_day ** days_ago
        sum_w += weight
        if won:
            sum_w_won += weight
        n_lifetime += 1
        if won:
            n_lifetime_won += 1
    if sum_w <= 0:
        return None
    recent_wr = sum_w_won / sum_w  # EWMA-weighted recent WR
    lifetime_wr = n_lifetime_won / n_lifetime if n_lifetime else 0.0
    return {
        "recent_wr": round(recent_wr, 3),
        "lifetime_wr": round(lifetime_wr, 3),
        "delta": round(recent_wr - lifetime_wr, 3),
        "n_recent": n_lifetime,  # all trades contribute, just weighted
        "n_lifetime": n_lifetime,
        "ewma_half_life_days": EWMA_HALF_LIFE_DAYS,
    }


def get_earliness_boost(
    wallet: str, category: str, db_path: str | None = None,
) -> float:
    """Multiplier in [MIN_BOOST, MAX_BOOST]. 1.0 when no signal.

    Mapping: a delta of +0.20 (recent WR 20pp above lifetime) → 1.3.
    A delta of -0.20 → 0.7. Linear in between.
    """
    info = compute_earliness(wallet, category, db_path=db_path)
    if not info:
        return 1.0
    delta = info["delta"]
    # Linear: each +0.10 delta = +0.15 boost. Clip to bounds.
    boost = 1.0 + delta * 1.5
    return max(MIN_BOOST, min(MAX_BOOST, boost))
