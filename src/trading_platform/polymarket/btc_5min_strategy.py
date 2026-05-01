"""Crypto 5-minute momentum strategy — high-sample-rate alpha source.

Hypothesis: Polymarket "Up or Down" 5-minute markets across 7 crypto
tickers (BTC, ETH, SOL, DOGE, BNB, XRP, HYPE) exhibit exploitable
order-book imbalance + price velocity in the final 60-90 seconds
before resolution. Market-makers withdraw quotes near expiry, leaving
an inefficient spread that respects directional flow.

Why it's complementary to whale-follow:
  - 5-min cycle × 7 tickers = ~2,000 markets/day potential
  - Pure technical (no wallet history needed)
  - Brier feedback in minutes, not days

How it fires (post-2026-04-30 rewrite):
  - Run every 60s
  - Fetch active 5-min crypto markets DIRECTLY from Gamma API (the
    internal markets table filters by min_volume which excludes these)
  - For each market with secs_to_resolve ∈ [30, 90]:
    * Compute price velocity from market_ticks (always available)
    * Optionally compute order-book imbalance from
      order_book_snapshots (graceful no-op if table empty)
  - Fire when EITHER signal exceeds threshold; bonus confidence when
    BOTH align directionally
  - Submits via paper executor like all other strategies

Behind PHASE_C_BTC_5MIN_ENABLED. Tier-C sizing (0.25×) until
n>=20 resolutions accumulate to validate.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


SIGNAL_TYPE = "btc_5min_imbalance"
ENV_FLAG = "PHASE_C_BTC_5MIN_ENABLED"

# Window: only fire when resolution is between MIN and MAX seconds away
MIN_SECONDS_TO_RESOLVE = 30
MAX_SECONDS_TO_RESOLVE = 90

# Imbalance threshold (when OB data available)
MIN_DEPTH_IMBALANCE = 0.30  # 30% skew

# Velocity threshold (always-available signal)
MIN_PRICE_VELOCITY = 0.03  # 3% in 30s

# Minimum confidence to emit
MIN_CONFIDENCE = 0.30

GAMMA_BASE = "https://gamma-api.polymarket.com"

# 7 crypto tickers running 5-min markets per Polymarket as of 2026-04-30
CRYPTO_TICKERS = ("Bitcoin", "Ethereum", "Solana", "Dogecoin", "BNB", "XRP", "Hyperliquid")
UPDOWN_PATTERN = re.compile(
    r"^(" + "|".join(CRYPTO_TICKERS) + r")\s+Up\s+or\s+Down",
    re.IGNORECASE,
)


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _fetch_active_5min_markets() -> list[dict[str, Any]]:
    """Pull the current active 5-min crypto markets from the Gamma API.

    Bypasses the internal universe filter (which excludes low-volume
    5-min markets via min_volume=$10K). Returns dicts compatible with
    the strategy's downstream candidate format.
    """
    out: list[dict[str, Any]] = []
    # Pull recent active markets, sorted by start desc — the 5-min
    # markets are continuously generated so they're always near the top
    params = {"limit": "100", "closed": "false", "active": "true",
              "order": "startDate", "ascending": "false"}
    url = f"{GAMMA_BASE}/markets?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "trading-platform"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("[BTC_5MIN] gamma fetch failed: %s", exc)
        return []

    now = time.time()
    for m in data if isinstance(data, list) else []:
        q = m.get("question", "")
        if not UPDOWN_PATTERN.match(q):
            continue
        end_iso = m.get("endDate", "")
        if not end_iso:
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
        try:
            prices = json.loads(m.get("outcomePrices", "[]"))
            yes_price = float(prices[0]) if prices else None
        except Exception:
            yes_price = None
        if yes_price is None or not (0.10 <= yes_price <= 0.90):
            continue
        try:
            token_ids = json.loads(m.get("clobTokenIds", "[]"))
            yes_tid = token_ids[0] if len(token_ids) > 0 else None
            no_tid = token_ids[1] if len(token_ids) > 1 else None
        except Exception:
            yes_tid, no_tid = None, None
        out.append({
            "condition_id": m.get("conditionId"),
            "slug": m.get("slug", ""),
            "question": q,
            "end_iso": end_iso,
            "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "secs_to_resolve": secs_to_resolve,
            "volume_24h": float(m.get("volume24hr", 0) or 0),
        })
    return out


def _orderbook_imbalance(conn, condition_id: str) -> tuple[float, float, float]:
    """Optional OB imbalance from order_book_snapshots (graceful no-op).

    Returns (bid_depth, ask_depth, signed_imbalance). Imbalance ∈ [-1, 1].
    Positive = buy pressure on YES, negative = sell pressure.
    Returns (0,0,0) when the table doesn't exist or has no rows for
    this condition.
    """
    try:
        row = conn.execute(
            """SELECT yes_bid_depth_usd, yes_ask_depth_usd
                 FROM order_book_snapshots
                WHERE condition_id = ?
                  AND ts > ?
                ORDER BY ts DESC LIMIT 1""",
            (condition_id, int(time.time()) - 30),
        ).fetchone()
    except Exception:
        return 0.0, 0.0, 0.0
    if not row:
        return 0.0, 0.0, 0.0
    bid = float(row[0] or 0.0)
    ask = float(row[1] or 0.0)
    total = bid + ask
    if total <= 0:
        return bid, ask, 0.0
    return bid, ask, (bid - ask) / total


def _price_velocity(conn, condition_id: str) -> float:
    """Signed 30s YES-price delta from market_ticks. Falls back to 0
    when no recent ticks (e.g. low-volume market just opened)."""
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


def _confidence(imbalance: float, velocity: float, has_ob: bool) -> float:
    """Higher confidence when both signals align directionally + magnitude.

    When OB data unavailable (has_ob=False), confidence falls back to
    velocity-only with a slight discount (no orthogonal confirmation).
    """
    vel_mag = min(abs(velocity) / 0.10, 1.0)
    imb_mag = min(abs(imbalance), 1.0)
    if has_ob:
        # Both signals required to align directionally
        if imbalance * velocity < 0:
            return 0.0  # disagreement = no edge
        return round(min(0.85, 0.50 + 0.20 * imb_mag + 0.15 * vel_mag), 4)
    # Velocity-only path: discount + slightly higher floor
    return round(min(0.75, 0.45 + 0.25 * vel_mag), 4)


def _emit_signal(market: dict, imbalance: float, velocity: float, has_ob: bool) -> None:
    # Direction: bias toward whichever signal dominates
    if has_ob:
        bias = imbalance + velocity
    else:
        bias = velocity
    direction_yes = bias > 0
    side = "YES" if direction_yes else "NO"
    direction = "BUY"
    entry_price = market["yes_price"] if direction_yes else (1.0 - market["yes_price"])
    conf = _confidence(imbalance, velocity, has_ob)
    if conf < MIN_CONFIDENCE:
        return
    now_ts = int(time.time())
    # Identify ticker for category routing + downstream attribution
    ticker = (market["question"].split()[0] or "Crypto").lower()
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
        "subcategory": f"crypto/{ticker}",
        "fired_at": now_ts,
        "ob_imbalance": round(imbalance, 4),
        "price_velocity_30s": round(velocity, 4),
        "has_ob_data": has_ob,
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        paper_trade = executor.execute_signal(payload)
        if paper_trade:
            logger.info(
                "[CRYPTO_5MIN] PLACED %s %s @ %.3f conf=%.2f imb=%+.2f vel=%+.3f t-%.0fs%s",
                ticker, side, entry_price, conf, imbalance, velocity,
                market["secs_to_resolve"], " [VEL_ONLY]" if not has_ob else "",
            )
        else:
            logger.debug(
                "[CRYPTO_5MIN] gated %s %s @ %.3f conf=%.2f",
                ticker, side, entry_price, conf,
            )
    except Exception as exc:
        logger.debug("[CRYPTO_5MIN] paper executor failed: %s", exc)


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "candidates": 0, "fired": 0}
    t0 = time.time()
    candidates = _fetch_active_5min_markets()
    fired = 0
    velocity_only_used = 0
    if not candidates:
        return {
            "elapsed_seconds": round(time.time() - t0, 2),
            "candidates": 0, "fired": 0,
            "note": "no active 5-min markets in [30s, 90s] window",
        }
    conn = get_connection()
    try:
        for m in candidates:
            bid, ask, imbalance = _orderbook_imbalance(conn, m["condition_id"])
            velocity = _price_velocity(conn, m["condition_id"])
            has_ob = bid > 0 or ask > 0
            if not has_ob:
                velocity_only_used += 1
            # Threshold check — at least one signal must exceed
            if abs(imbalance) < MIN_DEPTH_IMBALANCE and abs(velocity) < MIN_PRICE_VELOCITY:
                continue
            _emit_signal(m, imbalance, velocity, has_ob)
            fired += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "candidates": len(candidates),
        "fired": fired,
        "velocity_only_count": velocity_only_used,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
