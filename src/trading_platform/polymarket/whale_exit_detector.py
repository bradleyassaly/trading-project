"""Whale exit divergence detector.

We have whale_entry — when a watched wallet enters a position. We do
NOT systematically track when watched wallets EXIT. The hypothesis:
when 2+ tier-1 whales exit the same market within 1 hour, the price
moves 5-15% against the original direction within hours.

This is a contrarian signal — the smart money is leaving and we want
to either follow (sell what they sold) or fade (buy the other side
expecting a reversal of the move that brought them in).

Implementation: scan wallet_trades for sells/closes by tier-1 wallets
in the last hour. When 2+ tier-1 whales exit the same condition_id,
emit `whale_exit_cluster` signal on the OPPOSITE side of their original
entry direction (= they sold → bet against the side they were long on).

Behind PHASE_G_WHALE_EXIT_ENABLED. Daily backtest will reveal real EV.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "whale_exit_cluster"
ENV_FLAG = "PHASE_G_WHALE_EXIT_ENABLED"
EXIT_WINDOW_SECONDS = 3600   # 1 hour
MIN_DISTINCT_EXITS = 2
EXIT_AMOUNT_USD = 1000        # min size for an exit to count as significant


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def run_detector() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "fired": 0}
    t0 = time.time()
    conn = get_connection()
    fired = 0
    try:
        cutoff = int(time.time()) - EXIT_WINDOW_SECONDS
        # Find condition_ids with 2+ tier-1 sells in last hour (size_usd >= 1000)
        # NB: wallet_trades schema has columns (wallet, condition_id, side, size_usd, timestamp)
        rows = conn.execute(
            """SELECT wt.condition_id,
                      COUNT(DISTINCT wt.wallet) AS n_distinct,
                      AVG(wt.price) AS avg_exit_price
                 FROM wallet_trades wt
                 JOIN wallet_alpha_scores was ON LOWER(was.wallet) = LOWER(wt.wallet)
                WHERE wt.timestamp > ?
                  AND wt.side IN ('SELL', 'sell', 'CLOSE', 'close')
                  AND (wt.size * wt.price) >= ?
                  AND was.is_copyable = 1
                GROUP BY wt.condition_id
               HAVING COUNT(DISTINCT wt.wallet) >= ?""",
            (cutoff, EXIT_AMOUNT_USD, MIN_DISTINCT_EXITS),
        ).fetchall()
        for cid, n_distinct, avg_exit in rows:
            # Look up market info
            try:
                m = conn.execute(
                    """SELECT slug, question, yes_token_id, no_token_id, subcategory
                         FROM markets WHERE condition_id = ?""",
                    (cid,),
                ).fetchone()
            except Exception:
                m = None
            if not m or not m[2]:
                continue
            slug, question, yes_tid, no_tid, subcat = m
            # Side: we bet the OPPOSITE of where the whales were
            # holding. If they were long (entered YES) and now selling,
            # bet NO. We don't track entries cleanly here, so: assume
            # they sold YES and bet NO. Refine when entry_side is recorded.
            side = "NO"
            entry_price = 1.0 - float(avg_exit or 0.5)
            # Confidence proportional to distinct-whale count + size
            conf = round(min(0.80, 0.50 + 0.10 * (n_distinct - 1)), 4)
            payload = {
                "signal_type": SIGNAL_TYPE,
                "condition_id": cid,
                "side": side, "direction": "BUY",
                "entry_price": entry_price, "price": entry_price,
                "confidence": conf,
                "wallet": "whale_exit_detector", "wallet_tier": "synthetic",
                "slug": slug or "", "question": (question or "")[:200],
                "yes_token_id": yes_tid, "no_token_id": no_tid,
                "category": (subcat or "other").split("/")[0] if subcat else "other",
                "fired_at": int(time.time()),
                "n_distinct_exits": int(n_distinct),
            }
            try:
                from trading_platform.polymarket.polymarket_paper_executor import (
                    PolymarketPaperExecutor,
                )
                executor = PolymarketPaperExecutor()
                trade = executor.execute_signal(payload)
                if trade:
                    logger.info(
                        "[WHALE_EXIT] PLACED %s @ %.3f conf=%.2f n_exits=%d",
                        side, entry_price, conf, n_distinct,
                    )
                    fired += 1
            except Exception as exc:
                logger.debug("[WHALE_EXIT] emit failed: %s", exc)
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "fired": fired,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_detector())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
