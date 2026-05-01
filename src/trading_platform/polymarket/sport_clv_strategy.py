"""Sport pre-game closing-line-value strategy.

Hypothesis: in liquid sports markets (soccer, NBA, NFL, UFC) on
Polymarket, sharp money typically arrives in the last 2-6 hours
before tip-off. Following sharp moves (large directional flow that
moves the line >5% in <2h) into the close captures that information
edge.

Why it's complementary:
  - Sport-specific signal — independent of crypto, politics, etc
  - Massive sample density during sports seasons (NBA: ~10 games/day;
    NFL Sunday: ~14 games; soccer year-round: hundreds)
  - Gambling literature documents persistent CLV anomaly: lines that
    close >X% off where they opened tend to expire on the moved side
    more often than implied probability suggests

How it fires:
  - Run every 15min during pre-game windows
  - For each sport market with end_date_iso ∈ [now+30min, now+12h]:
    * Pull current YES price
    * Compare to YES price 6h ago (from market_ticks)
    * If |delta| > MIN_LINE_MOVE_PCT, fire FOLLOW signal in direction
      of the move
  - Direction-of-move signals = follow sharp money

Behind PHASE_E_SPORT_CLV_ENABLED. Tier-C until validated.
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


SIGNAL_TYPE = "sport_pregame_clv"
ENV_FLAG = "PHASE_E_SPORT_CLV_ENABLED"

# Pre-game window — too close = market has already absorbed the move
MIN_HOURS_TO_RESOLVE = 0.5
MAX_HOURS_TO_RESOLVE = 12.0

# Line-move lookback
LOOKBACK_HOURS = 6.0

# Minimum move to fire on (as absolute fraction)
MIN_LINE_MOVE = 0.05

# Maximum move (above this = likely news/injury, fade rather than follow)
MAX_LINE_MOVE = 0.20

# Entry-price band — extreme prices have asymmetric payoff/risk
ENTRY_PRICE_LOW = 0.20
ENTRY_PRICE_HIGH = 0.80

# Sport keywords (paper executor's categorizer also normalizes these)
SPORT_PATTERNS = [
    re.compile(r"\bvs\.?\b", re.IGNORECASE),
    re.compile(r"NBA|NFL|NHL|MLB|UFC|ATP|WTA|Premier League|La Liga|"
               r"Serie A|Bundesliga|Champions League|Wimbledon|US Open|"
               r"Stanley Cup|World Series|Super Bowl",
               re.IGNORECASE),
    re.compile(r"\b(spread|moneyline|over\s*/\s*under|O/U)\b", re.IGNORECASE),
]


def _enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").lower() in ("1", "true", "yes")


def _is_sport(question: str) -> bool:
    if not question:
        return False
    return any(p.search(question) for p in SPORT_PATTERNS)


def _candidate_markets(conn) -> list[dict[str, Any]]:
    now = time.time()
    rows = conn.execute(
        """SELECT condition_id, slug, question, end_date_iso,
                  yes_token_id, no_token_id, outcome_prices, volume_24h,
                  subcategory
             FROM markets
            WHERE end_date_iso IS NOT NULL
              AND yes_token_id IS NOT NULL
              AND no_token_id IS NOT NULL"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        cid, slug, q, end_iso, yes_tid, no_tid, prices_raw, vol, subcat = r
        sub_lower = (subcat or "").lower()
        if not sub_lower.startswith("sports") and not _is_sport(q or ""):
            continue
        try:
            from datetime import datetime, timezone
            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
            end_dt = datetime.fromisoformat(clean)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            hours = (end_dt.timestamp() - now) / 3600
        except Exception:
            continue
        if not (MIN_HOURS_TO_RESOLVE <= hours <= MAX_HOURS_TO_RESOLVE):
            continue
        yes_price = None
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if isinstance(prices, list) and prices:
                yes_price = float(prices[0])
        except Exception:
            yes_price = None
        if yes_price is None or not (ENTRY_PRICE_LOW <= yes_price <= ENTRY_PRICE_HIGH):
            continue
        out.append({
            "condition_id": cid, "slug": slug or "", "question": q or "",
            "end_iso": end_iso, "yes_token_id": yes_tid, "no_token_id": no_tid,
            "yes_price": yes_price, "hours_to_resolve": hours,
            "volume_24h": float(vol or 0),
        })
    return out


def _line_move(conn, condition_id: str) -> float | None:
    """Return signed YES-price delta over LOOKBACK_HOURS. None if no
    history.

    2026-04-30: market_ticks is sparse for sport markets (most have
    <10 ticks per 6h window). Fall back to fetching the market's
    oneDayPriceChange / oneHourPriceChange directly from the Gamma
    API when ticks are insufficient. Coarse but unblocks the strategy.
    """
    cutoff = int(time.time()) - int(LOOKBACK_HOURS * 3600)
    try:
        rows = conn.execute(
            """SELECT ts, yes_price FROM market_ticks
                WHERE condition_id = ? AND ts > ?
                ORDER BY ts ASC""",
            (condition_id, cutoff),
        ).fetchall()
    except Exception:
        rows = []
    if len(rows) >= 2:
        return float(rows[-1][1] or 0.0) - float(rows[0][1] or 0.0)
    # Fallback: hit Gamma API for fresh price-change fields. Slow but
    # only triggered when ticks are sparse — typically <10 candidates
    # per run, so an acceptable cost.
    try:
        import json as _json
        import urllib.request as _ur
        url = f"https://gamma-api.polymarket.com/markets?condition_ids={condition_id}"
        req = _ur.Request(url, headers={"User-Agent": "trading-platform"})
        with _ur.urlopen(req, timeout=5) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        if not isinstance(data, list) or not data:
            return None
        m = data[0]
        one_day = m.get("oneDayPriceChange")
        one_hour = m.get("oneHourPriceChange")
        if one_day is not None:
            return float(one_day) * (LOOKBACK_HOURS / 24.0)
        if one_hour is not None:
            return float(one_hour) * LOOKBACK_HOURS
    except Exception as exc:
        logger.debug("[SPORT_CLV] gamma fallback failed: %s", exc)
    return None


def _confidence(line_move: float, hours_to_resolve: float) -> float:
    """Higher conf for larger moves + closer to game time."""
    move_lift = 0.30 * min(abs(line_move) / MAX_LINE_MOVE, 1.0)
    time_lift = 0.15 * (1 - hours_to_resolve / MAX_HOURS_TO_RESOLVE)
    return round(min(0.85, max(0.30, 0.45 + move_lift + time_lift)), 4)


def _emit_signal(market: dict, line_move: float) -> None:
    side = "YES" if line_move > 0 else "NO"
    direction = "BUY"
    entry_price = market["yes_price"] if line_move > 0 else (1.0 - market["yes_price"])
    conf = _confidence(line_move, market["hours_to_resolve"])
    now_ts = int(time.time())
    payload = {
        "signal_type": SIGNAL_TYPE,
        "condition_id": market["condition_id"],
        "side": side, "direction": direction,
        "entry_price": entry_price, "price": entry_price,
        "confidence": conf,
        "wallet": "sport_clv_strategy",
        "wallet_tier": "synthetic",
        "slug": market["slug"], "question": market["question"][:200],
        "yes_token_id": market["yes_token_id"], "no_token_id": market["no_token_id"],
        "category": "sports",
        "fired_at": now_ts,
        "line_move_6h": round(line_move, 4),
        "hours_to_resolve": round(market["hours_to_resolve"], 2),
    }
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            PolymarketPaperExecutor,
        )
        executor = PolymarketPaperExecutor()
        paper_trade = executor.execute_signal(payload)
        if paper_trade:
            logger.info(
                "[SPORT_CLV] PLACED %s @ %.3f conf=%.2f move=%+.3f hrs=%.1f",
                side, entry_price, conf, line_move, market["hours_to_resolve"],
            )
    except Exception as exc:
        logger.debug("[SPORT_CLV] paper executor failed: %s", exc)


def run_pipeline() -> dict[str, Any]:
    if not _enabled():
        return {"skipped": f"{ENV_FLAG} not set", "candidates": 0, "fired": 0}
    t0 = time.time()
    conn = get_connection()
    fired = 0
    candidates: list = []
    try:
        candidates = _candidate_markets(conn)
        for m in candidates:
            move = _line_move(conn, m["condition_id"])
            if move is None:
                continue
            if not (MIN_LINE_MOVE <= abs(move) <= MAX_LINE_MOVE):
                continue
            _emit_signal(m, move)
            fired += 1
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "elapsed_seconds": round(time.time() - t0, 2),
        "candidates": len(candidates), "fired": fired,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
