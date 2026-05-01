"""BTC 5-minute momentum strategy — high-sample-rate alpha source.

Hypothesis: Polymarket "Bitcoin Up or Down" 5-minute markets exhibit
exploitable order-book imbalance + price velocity in the final
60-90 seconds before resolution. Market-makers withdraw quotes near
expiry, leaving an inefficient spread that respects directional flow.

Why it's complementary to whale-follow:
  - 5-min cycle = ~288 markets/day on BTC alone (vs ~9 whale_entry/h)
  - Pure technical (no wallet history needed)
  - Brier feedback in minutes, not days — accelerates calibration

Why it might work:
  - Last-minute order-book imbalance reflects late-arriving info
    (Binance perp moves, news, large flows) that hasn't been arbed
    out due to the 5-min market's narrow audience
  - The 60-90s window is short enough that random walk dominates
    fundamental drift — pure microstructure edge

How it fires:
  - Run every 60s while BTC 5-min markets are open
  - For each open BTC market with end_date_iso in [now+30s, now+5min]:
    * Pull 30s price velocity + bid/ask depth ratio
    * If both signals align directionally + magnitude > threshold,
      emit a signal with side = directional_bias
  - Gates through the standard paper executor

Behind PHASE_C_BTC_5MIN_ENABLED. Paper-only initially. Tier-C
sizing (0.25×) until n>=20 resolutions accumulate.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "btc_5min_imbalance"
ENV_FLAG = "PHASE_C_BTC_5MIN_ENABLED"

# Window: only fire when resolution is between MIN and MAX seconds away.
# < 30s = too late to fill; > 5min = too early, market still random.
MIN_SECONDS_TO_RESOLVE = 30
MAX_SECONDS_TO_RESOLVE = 90

# Imbalance threshold: |buy_depth - sell_depth| / total_depth > X
MIN_DEPTH_IMBALANCE = 0.30  # 30% skew

# Velocity threshold: |delta(price, 30s)| > X
MIN_PRICE_VELOCITY = 0.03  # 3% in 30s

# Minimum confidence to emit (further gated by paper executor floor)
MIN_CONFIDENCE = 0.30

# Pattern matching BTC 5-min markets (PM names: "Bitcoin Up or Down -
# April 30, 12:00PM-12:05PM ET" or similar)
BTC_5MIN_PATTERNS = [
    re.compile(r"bitcoin\s+up\s+or\s+down", re.IGNORECASE),
    re.compile(r"btc\s+up\s+or\s+down", re.IGNORECASE),
    re.compile(r"bitcoin.*\d+:\d{2}\s*(am|pm)?-\d+:\d{2}", re.IGNORECASE),
]


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _is_btc_5min(question: str) -> bool:
    if not question:
        return False
    return any(p.search(question) for p in BTC_5MIN_PATTERNS)


def _candidate_markets(conn) -> list[dict[str, Any]]:
    """Pull BTC 5-min markets resolving in the [30s, 5min] window."""
    now = time.time()
    rows = conn.execute(
        """SELECT condition_id, slug, question, end_date_iso,
                  yes_token_id, no_token_id, outcome_prices, volume_24h
             FROM markets
            WHERE end_date_iso IS NOT NULL
              AND yes_token_id IS NOT NULL
              AND no_token_id IS NOT NULL"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        cid, slug, q, end_iso, yes_tid, no_tid, prices_raw, vol = r
        if not _is_btc_5min(q or ""):
            continue
        try:
            from datetime import datetime, timezone
            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
            end_dt = datetime.fromisoformat(clean)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            secs_to_resolve = end_dt.timestamp() - now
        except Exception:
            continue
        if not (MIN_SECONDS_TO_RESOLVE <= secs_to_resolve <= MAX_SECONDS_TO_RESOLVE):
            continue
        yes_price = None
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if isinstance(prices, list) and prices:
                yes_price = float(prices[0])
        except Exception:
            yes_price = None
        if yes_price is None or yes_price < 0.10 or yes_price > 0.90:
            # Skip extreme prices — already resolved-in-spirit
            continue
        out.append({
            "condition_id": cid, "slug": slug or "", "question": q or "",
            "end_iso": end_iso, "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "secs_to_resolve": secs_to_resolve,
            "volume_24h": float(vol or 0),
        })
    return out


def _orderbook_imbalance(conn, condition_id: str) -> tuple[float, float, float]:
    """Return (yes_bid_depth, yes_ask_depth, imbalance_signed).

    imbalance = (bid - ask) / (bid + ask), in [-1, 1]. Positive = buy
    pressure on YES (bid wall). Negative = sell pressure (ask wall).
    Reads the most recent order_book row.
    """
    try:
        row = conn.execute(
            """SELECT bid_size_usd, ask_size_usd
                 FROM order_book_snapshots
                WHERE condition_id = ?
                ORDER BY ts DESC LIMIT 1""",
            (condition_id,),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return 0.0, 0.0, 0.0
    bid = float(row[0] or 0.0)
    ask = float(row[1] or 0.0)
    total = bid + ask
    if total <= 0:
        return bid, ask, 0.0
    return bid, ask, (bid - ask) / total


def _price_velocity(conn, condition_id: str) -> float:
    """Signed 30-second price delta on YES token. Reads market_ticks."""
    cutoff = int(time.time()) - 30
    try:
        rows = conn.execute(
            """SELECT ts, yes_price FROM market_ticks
                WHERE condition_id = ? AND ts > ?
                ORDER BY ts ASC""",
            (condition_id, cutoff),
        ).fetchall()
    except Exception:
        return 0.0
    if len(rows) < 2:
        return 0.0
    return float(rows[-1][1] or 0.0) - float(rows[0][1] or 0.0)


def _confidence(imbalance: float, velocity: float) -> float:
    """Higher confidence when both signals align directionally + magnitude."""
    # Magnitudes
    imb_mag = min(abs(imbalance), 1.0)
    vel_mag = min(abs(velocity) / 0.10, 1.0)  # 10% velocity = max contribution
    # Alignment — both same sign?
    if imbalance * velocity <= 0:
        return 0.0  # disagreement = no edge
    base = 0.50
    return round(min(0.85, base + 0.20 * imb_mag + 0.15 * vel_mag), 4)


def _emit_signal(market: dict, imbalance: float, velocity: float) -> None:
    direction_yes = (imbalance + velocity) > 0
    side = "YES" if direction_yes else "NO"
    direction = "BUY"
    entry_price = market["yes_price"] if direction_yes else (1.0 - market["yes_price"])
    conf = _confidence(imbalance, velocity)
    if conf < MIN_CONFIDENCE:
        return
    now_ts = int(time.time())
    payload = {
        "signal_type": SIGNAL_TYPE,
        "condition_id": market["condition_id"],
        "side": side,
        "direction": direction,
        "entry_price": entry_price,
        "price": entry_price,
        "confidence": conf,
        "wallet": "btc_5min_strategy",
        "wallet_tier": "synthetic",
        "slug": market["slug"],
        "question": market["question"][:200],
        "yes_token_id": market["yes_token_id"],
        "no_token_id": market["no_token_id"],
        "category": "crypto",
        "fired_at": now_ts,
        "ob_imbalance": round(imbalance, 4),
        "price_velocity_30s": round(velocity, 4),
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        paper_trade = executor.execute_signal(payload)
        if paper_trade:
            logger.info(
                "[BTC_5MIN] PLACED %s @ %.3f conf=%.2f imb=%+.2f vel=%+.3f t-%.0fs",
                side, entry_price, conf, imbalance, velocity, market["secs_to_resolve"],
            )
        else:
            logger.debug(
                "[BTC_5MIN] gated %s @ %.3f conf=%.2f",
                side, entry_price, conf,
            )
    except Exception as exc:
        logger.debug("[BTC_5MIN] paper executor failed: %s", exc)


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "candidates": 0, "fired": 0}
    t0 = time.time()
    conn = get_connection()
    try:
        candidates = _candidate_markets(conn)
        fired = 0
        for m in candidates:
            bid, ask, imbalance = _orderbook_imbalance(conn, m["condition_id"])
            velocity = _price_velocity(conn, m["condition_id"])
            if abs(imbalance) < MIN_DEPTH_IMBALANCE and abs(velocity) < MIN_PRICE_VELOCITY:
                continue
            _emit_signal(m, imbalance, velocity)
            fired += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "candidates": len(candidates) if 'candidates' in dir() else 0,
        "fired": fired if 'fired' in dir() else 0,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
