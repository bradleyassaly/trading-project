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
    """Return {"recent_wr", "lifetime_wr", "delta", "n_recent", "n_lifetime"}
    for the wallet/category, or None when insufficient data."""
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        cutoff = int(time.time()) - LOOKBACK_RECENT_DAYS * 86400
        try:
            row = conn.execute(
                """SELECT
                     SUM(CASE WHEN ts >= %s THEN 1 ELSE 0 END) AS n_recent,
                     SUM(CASE WHEN ts >= %s AND won = 1 THEN 1 ELSE 0 END) AS w_recent,
                     COUNT(*) AS n_total,
                     SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) AS w_total
                   FROM (
                     SELECT timestamp AS ts,
                            CASE WHEN COALESCE(pnl, 0) > 0 THEN 1 ELSE 0 END AS won
                       FROM wallet_trades
                      WHERE wallet = %s AND category = %s
                        AND pnl IS NOT NULL AND pnl_reliable = 1
                   ) t""",
                (cutoff, cutoff, wallet.lower(), category.lower()),
            ).fetchone()
        except Exception:
            row = None
    finally:
        try: conn.close()
        except Exception: pass
    if not row or row[2] is None or int(row[2]) == 0:
        return None
    n_recent = int(row[0] or 0)
    w_recent = int(row[1] or 0)
    n_total = int(row[2])
    w_total = int(row[3] or 0)
    if n_recent < MIN_RECENT_TRADES:
        return None
    recent_wr = w_recent / n_recent
    lifetime_wr = w_total / n_total
    return {
        "recent_wr": round(recent_wr, 3),
        "lifetime_wr": round(lifetime_wr, 3),
        "delta": round(recent_wr - lifetime_wr, 3),
        "n_recent": n_recent,
        "n_lifetime": n_total,
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
