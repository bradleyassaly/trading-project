"""Cross-platform arbitrage scanner — Polymarket vs Kalshi.

Hypothesis: same outcome quoted on PM and Kalshi can diverge by
50-200 bps for minutes due to (a) different liquidity providers,
(b) latency between platforms, (c) regional flows. Buying the cheaper
side and (optionally) hedging the dearer side captures the spread
minus fees.

Why it's complementary:
  - Pure structural edge — independent of any wallet/news/calibration
  - Fires only when a real numerical gap exists
  - Cross-platform breadth means more sample/day than single-platform
    momentum

How it fires:
  - Run every 60s
  - For each PM market with a Kalshi twin (mapped by hand or LLM):
    * Fetch PM mid-price, Kalshi mid-price
    * If |PM - Kalshi| > MIN_GAP_BPS basis points (after subtracting
      round-trip fees), emit a BUY signal on the cheaper side
  - Signal goes through paper executor as `cross_platform_arb`

The Kalshi mapping is the heavy part. Initial scope: just expose the
scanning machinery + map a small set of identical question pairs by
hand. Future: LLM-driven matcher.

Behind PHASE_D_ARB_ENABLED. Tier-C until validated.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "cross_platform_arb"
ENV_FLAG = "PHASE_D_ARB_ENABLED"

# Minimum gap to fire (after fees). 50 bps = 0.5%. Round-trip fee on
# Polymarket is roughly 0-30bps; Kalshi is 0%. So 50bps net is real edge.
MIN_GAP = 0.005

# Maximum gap to fire — beyond this it's likely a market mapping bug
# rather than a genuine spread.
MAX_GAP = 0.20

# Minimum liquidity on the side we'd buy
MIN_DEPTH_USD = 100.0


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _load_mappings(conn) -> list[dict[str, Any]]:
    """Pull (pm_condition_id, kalshi_ticker, mapping_quality) rows.

    Schema: cross_platform_market_map(pm_condition_id, kalshi_ticker,
    quality, last_verified). Created lazily — empty by default. Populate
    via API or migration script.
    """
    try:
        rows = conn.execute(
            """SELECT pm_condition_id, kalshi_ticker, quality
                 FROM cross_platform_market_map
                WHERE quality >= 0.8"""
        ).fetchall()
    except Exception:
        # Schema doesn't exist yet — bootstrap empty
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS cross_platform_market_map (
                    pm_condition_id TEXT NOT NULL,
                    kalshi_ticker   TEXT NOT NULL,
                    quality         DOUBLE PRECISION DEFAULT 1.0,
                    last_verified   BIGINT,
                    PRIMARY KEY (pm_condition_id, kalshi_ticker)
                )"""
            )
            conn.commit()
        except Exception:
            pass
        return []
    return [
        {"pm_condition_id": r[0], "kalshi_ticker": r[1], "quality": float(r[2] or 1.0)}
        for r in rows
    ]


def _pm_mid(conn, condition_id: str) -> float | None:
    """Read latest PM YES mid-price."""
    try:
        row = conn.execute(
            """SELECT outcome_prices, yes_token_id, no_token_id
                 FROM markets WHERE condition_id = ?""",
            (condition_id,),
        ).fetchone()
        if not row:
            return None
        prices = row[0]
        if isinstance(prices, str):
            prices = json.loads(prices)
        if isinstance(prices, list) and prices:
            return float(prices[0])
    except Exception:
        return None
    return None


def _kalshi_mid(ticker: str) -> float | None:
    """Read latest Kalshi YES mid-price. Reads from kalshi_markets if
    the table exists; else returns None.
    """
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                """SELECT yes_bid, yes_ask FROM kalshi_markets
                    WHERE ticker = ? ORDER BY last_updated DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        bid = float(row[0] or 0.0)
        ask = float(row[1] or 0.0)
        if bid <= 0 or ask <= 0:
            return None
        return (bid + ask) / 2.0
    except Exception:
        return None


def _emit_signal(pm_cid: str, pm_mid: float, kalshi_mid: float, gap: float) -> None:
    """Buy the cheaper side. Confidence proportional to gap magnitude."""
    # Cheaper side: if pm < kalshi, buy PM YES; if pm > kalshi, buy PM NO
    # (i.e. effective "bet against PM YES being too high relative to Kalshi")
    if pm_mid < kalshi_mid:
        side = "YES"
        entry_price = pm_mid
    else:
        side = "NO"
        entry_price = 1.0 - pm_mid
    direction = "BUY"
    # Confidence: 0.50 + 0.4 × gap magnitude (caps at 0.90)
    conf = round(min(0.90, 0.50 + 4.0 * abs(gap)), 4)
    now_ts = int(time.time())
    payload = {
        "signal_type": SIGNAL_TYPE,
        "condition_id": pm_cid,
        "side": side, "direction": direction,
        "entry_price": entry_price, "price": entry_price,
        "confidence": conf,
        "wallet": "cross_platform_arb",
        "wallet_tier": "synthetic",
        "category": "wallet_derived",  # routed through paper categorizer
        "fired_at": now_ts,
        "pm_mid": round(pm_mid, 4),
        "kalshi_mid": round(kalshi_mid, 4),
        "gap_bps": round(gap * 10000, 1),
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        paper_trade = executor.execute_signal(payload)
        if paper_trade:
            logger.info(
                "[ARB] PLACED %s pm=%.3f kalshi=%.3f gap=%+.0fbps conf=%.2f",
                side, pm_mid, kalshi_mid, gap * 10000, conf,
            )
    except Exception as exc:
        logger.debug("[ARB] paper executor failed: %s", exc)


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "pairs": 0, "fired": 0}
    t0 = time.time()
    conn = get_connection()
    fired = 0
    pairs = 0
    try:
        mappings = _load_mappings(conn)
        pairs = len(mappings)
        for m in mappings:
            pm_mid = _pm_mid(conn, m["pm_condition_id"])
            if pm_mid is None:
                continue
            kalshi_mid = _kalshi_mid(m["kalshi_ticker"])
            if kalshi_mid is None:
                continue
            gap = pm_mid - kalshi_mid  # positive = PM more expensive
            if not (MIN_GAP <= abs(gap) <= MAX_GAP):
                continue
            _emit_signal(m["pm_condition_id"], pm_mid, kalshi_mid, gap)
            fired += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "pairs_scanned": pairs, "fired": fired,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
