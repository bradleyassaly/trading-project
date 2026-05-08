"""Cross-signal confluence detector.

Hypothesis: when 2+ distinct signals fire on the same market within
a short window, the combined signal is higher conviction than either
alone. e.g. wallet_reversal + market_maker_flip on the same condition_id
within 5 minutes = a "confluence" signal.

This module scans market_signals for confluences and emits a new
signal_type 'confluence_2plus' (with a `combined_signals` field
listing the constituent signals). The combined signal goes through
the standard paper executor pipeline and gets its own EV slice in
signal_category_ev.

Run every 60s. Reads last 10min of market_signals.

Once we have N=10 confluence resolutions, we'll know whether the
combination beats the individual signals' EV.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "confluence_2plus"
WINDOW_SECONDS = 300  # 5min — signals firing within this window count as confluence
LOOKBACK_SECONDS = 600  # scan last 10min of market_signals
MIN_DISTINCT_SIGNALS = 2

# Signals that are noise (excluded from confluence consideration)
NOISE_SIGNALS = {"price_velocity", "order_flow_imbalance"}


def _enabled() -> bool:
    import os
    return os.environ.get("PHASE_G_CONFLUENCE_ENABLED", "").lower() in ("1", "true", "yes")


def run_detector() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": "PHASE_G_CONFLUENCE_ENABLED not set", "fired": 0}
    t0 = time.time()
    conn = get_connection()
    fired = 0
    try:
        cutoff = int(time.time()) - LOOKBACK_SECONDS
        # Find condition_ids with 2+ distinct signals in window
        candidates = conn.execute(
            """SELECT condition_id,
                      COUNT(DISTINCT signal_type) AS n_distinct,
                      MIN(fired_at) AS first_fire,
                      MAX(fired_at) AS last_fire
                 FROM market_signals
                WHERE fired_at > ?
                  AND signal_type NOT IN ('price_velocity', 'order_flow_imbalance')
                GROUP BY condition_id
               HAVING COUNT(DISTINCT signal_type) >= 2""",
            (cutoff,),
        ).fetchall()
        for cid, n_distinct, first_ts, last_ts in candidates:
            # Span of fires must be within WINDOW_SECONDS
            if (last_ts - first_ts) > WINDOW_SECONDS:
                continue
            # Pull the constituent signals + market info.
            # market_signals uses `direction` (BUY/SELL) rather than a
            # separate `side` column; `price` is the entry price; there
            # is no `wallet_tier` column — NULL fills that slot so all
            # downstream index positions stay the same.
            constituents = conn.execute(
                """SELECT signal_type, direction AS side, direction, confidence,
                          price AS entry_price, wallet, NULL AS wallet_tier,
                          slug, question, category
                     FROM market_signals
                    WHERE condition_id = ? AND fired_at > ?
                      AND signal_type NOT IN ('price_velocity', 'order_flow_imbalance')
                    ORDER BY fired_at DESC""",
                (cid, cutoff),
            ).fetchall()
            if not constituents or len(constituents) < MIN_DISTINCT_SIGNALS:
                continue
            # Pick the strongest direction by aggregate confidence
            yes_conf = sum(float(c[3] or 0) for c in constituents
                           if (c[1] or "").upper() in ("YES", "BUY", "BUY_YES"))
            no_conf = sum(float(c[3] or 0) for c in constituents
                          if (c[1] or "").upper() in ("NO", "SELL", "BUY_NO"))
            if abs(yes_conf - no_conf) < 0.10:
                continue  # ambivalent confluence — skip
            side = "YES" if yes_conf > no_conf else "NO"
            # Average entry price across constituents (most-recent)
            avg_entry = sum(float(c[4] or 0.5) for c in constituents) / len(constituents)
            # Average confidence on the winning side, boosted 20% for confluence
            # (divide sum by total constituents, not just winning-side count, to
            # penalise mixed signals; still cap at 0.85 for safety)
            avg_conf = max(yes_conf, no_conf) / max(len(constituents), 1)
            combined_conf = min(0.85, avg_conf * 1.2)
            # Use most recent fire's wallet info for tier
            top = constituents[0]
            payload = {
                "signal_type": SIGNAL_TYPE,
                "condition_id": cid,
                "side": side, "direction": "BUY",
                "entry_price": avg_entry, "price": avg_entry,
                "confidence": round(combined_conf, 4),
                "wallet": "confluence_detector", "wallet_tier": top[6] or "synthetic",
                "slug": top[7] or "", "question": (top[8] or "")[:200],
                "category": top[9] or "other",
                "fired_at": int(time.time()),
                "combined_signals": [c[0] for c in constituents],
                "n_constituents": len(constituents),
            }
            try:
                from trading_platform.polymarket.polymarket_paper_executor import (
                    PolymarketPaperExecutor,
                )
                executor = PolymarketPaperExecutor()
                trade = executor.execute_signal(payload)
                if trade:
                    logger.info(
                        "[CONFLUENCE] PLACED %s @ %.3f conf=%.2f from %s",
                        side, avg_entry, combined_conf, payload["combined_signals"],
                    )
                    fired += 1
            except Exception as exc:
                logger.debug("[CONFLUENCE] emit failed: %s", exc)
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
